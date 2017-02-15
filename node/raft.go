// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package node

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"encoding/json"
	"sync/atomic"

	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/absolute8511/ZanRedisDB/raft"
	"github.com/absolute8511/ZanRedisDB/raft/raftpb"
	"github.com/absolute8511/ZanRedisDB/snap"
	"github.com/absolute8511/ZanRedisDB/transport/rafthttp"
	"github.com/absolute8511/ZanRedisDB/wal"
	"github.com/absolute8511/ZanRedisDB/wal/walpb"
	"github.com/coreos/etcd/pkg/fileutil"
	"github.com/coreos/etcd/pkg/idutil"
	"github.com/coreos/etcd/pkg/types"
	"golang.org/x/net/context"
)

const (
	DefaultSnapCount = 50000

	// HealthInterval is the minimum time the cluster should be healthy
	// before accepting add member requests.
	HealthInterval = 5 * time.Second

	// max number of in-flight snapshot messages allows to have
	maxInFlightMsgSnap        = 16
	releaseDelayAfterSnapshot = 30 * time.Second
)

type Snapshot interface {
	GetData() ([]byte, error)
}

type DataStorage interface {
	CleanData() error
	RestoreFromSnapshot(bool, raftpb.Snapshot) error
	GetSnapshot(term uint64, index uint64) (Snapshot, error)
}

type applyInfo struct {
	ents     []raftpb.Entry
	snapshot raftpb.Snapshot
	raftDone chan struct{}
}

type MemberInfo struct {
	// the replica id
	ID uint64 `json:"id"`
	// the node id replica belong
	NodeID    uint64 `json:"node_id"`
	GroupName string `json:"group_name"`
	// group id the replica belong (different from namespace)
	GroupID     uint64   `json:"group_id"`
	Broadcast   string   `json:"broadcast"`
	RpcPort     int      `json:"rpc_port"`
	HttpAPIPort int      `json:"http_api_port"`
	RaftURLs    []string `json:"peer_urls"`
	DataDir     string   `json:"data_dir"`
}

// A key-value stream backed by raft
type raftNode struct {
	commitC chan<- applyInfo // entries committed to log (k,v)
	config  *RaftConfig

	memMutex  sync.Mutex
	members   map[uint64]*MemberInfo
	join      bool   // node is joining an existing cluster
	lastIndex uint64 // index of log at start
	lead      uint64

	// raft backing for the commit/error channel
	node        raft.Node
	raftStorage *raft.MemoryStorage
	wal         *wal.WAL

	snapshotter *snap.Snapshotter

	transport         *rafthttp.Transport
	stopc             chan struct{} // signals proposal channel closed
	reqIDGen          *idutil.Generator
	wg                sync.WaitGroup
	ds                DataStorage
	msgSnapC          chan raftpb.Message
	inflightSnapshots int64
	description       string
}

// newRaftNode initiates a raft instance and returns a committed log entry
// channel and error channel. Proposals for log updates are sent over the
// provided the proposal channel. All log entries are replayed over the
// commit channel, followed by a nil message (to indicate the channel is
// current), then new log entries.
func newRaftNode(rconfig *RaftConfig, transport *rafthttp.Transport,
	join bool, ds DataStorage) (<-chan applyInfo, *raftNode) {

	commitC := make(chan applyInfo, 1000)
	if rconfig.SnapCount <= 0 {
		rconfig.SnapCount = DefaultSnapCount
	}
	if rconfig.SnapCatchup <= 0 {
		rconfig.SnapCatchup = rconfig.SnapCount / 2
	}

	rc := &raftNode{
		commitC:     commitC,
		config:      rconfig,
		members:     make(map[uint64]*MemberInfo),
		join:        join,
		raftStorage: raft.NewMemoryStorage(),
		stopc:       make(chan struct{}),
		ds:          ds,
		reqIDGen:    idutil.NewGenerator(uint16(rconfig.ID), time.Now()),
		msgSnapC:    make(chan raftpb.Message, maxInFlightMsgSnap),
		transport:   transport,
		// rest of structure populated after WAL replay
	}
	snapDir := rc.config.SnapDir
	if !fileutil.Exist(snapDir) {
		if err := os.MkdirAll(snapDir, common.DIR_PERM); err != nil {
			nodeLog.Fatalf("cannot create dir for snapshot (%v)", err)
		}
	}
	rc.snapshotter = snap.New(snapDir)
	rc.description = rc.config.GroupName + "-" + strconv.Itoa(int(rc.config.ID))
	return commitC, rc
}

// openWAL returns a WAL ready for reading.
func (rc *raftNode) openWAL(snapshot *raftpb.Snapshot) *wal.WAL {
	if !wal.Exist(rc.config.WALDir) {
		if err := os.MkdirAll(rc.config.WALDir, common.DIR_PERM); err != nil {
			nodeLog.Fatalf("cannot create dir for wal (%v)", err)
		}

		var m MemberInfo
		m.ID = uint64(rc.config.ID)
		m.GroupName = rc.config.GroupName
		m.GroupID = rc.config.GroupID
		d, _ := json.Marshal(m)
		w, err := wal.Create(rc.config.WALDir, d)
		if err != nil {
			nodeLog.Fatalf("create wal error (%v)", err)
		}
		return w
	}

	var walsnap walpb.Snapshot
	if snapshot != nil {
		walsnap.Index, walsnap.Term = snapshot.Metadata.Index, snapshot.Metadata.Term
	}
	w, err := wal.Open(rc.config.WALDir, walsnap)
	if err != nil {
		nodeLog.Fatalf("error loading wal (%v)", err)
	}

	return w
}

// replayWAL replays WAL entries into the raft instance.
func (rc *raftNode) replayWAL(snapshot *raftpb.Snapshot) *wal.WAL {
	w := rc.openWAL(snapshot)
	meta, st, ents, err := w.ReadAll()
	if err != nil {
		w.Close()
		nodeLog.Fatalf("failed to read WAL (%v)", err)
	}
	rc.Infof("wal meta: %v, restart with: %v", string(meta), st.String())
	var m MemberInfo
	err = json.Unmarshal(meta, &m)
	if err != nil {
		w.Close()
		nodeLog.Fatalf("meta is wrong: %v", err)
	}
	if m.ID != uint64(rc.config.ID) ||
		m.GroupID != rc.config.GroupID {
		w.Close()
		nodeLog.Fatalf("meta starting mismatch config: %v, %v", m, rc.config)
	}

	if snapshot != nil {
		rc.raftStorage.ApplySnapshot(*snapshot)
	}
	// append to storage so raft starts at the right place in log
	rc.raftStorage.Append(ents)
	// send nil once lastIndex is published so client knows commit channel is current
	if len(ents) > 0 {
		rc.lastIndex = ents[len(ents)-1].Index
	}
	rc.Infof("replaying WAL (%v) at lastIndex : %v", len(ents), rc.lastIndex)
	rc.raftStorage.SetHardState(st)
	return w
}

func (rc *raftNode) startRaft(ds DataStorage) {
	walDir := rc.config.WALDir
	oldwal := wal.Exist(walDir)

	c := &raft.Config{
		ID:              uint64(rc.config.ID),
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         rc.raftStorage,
		MaxSizePerMsg:   1024 * 1024,
		MaxInflightMsgs: 256,
		CheckQuorum:     true,
		Logger:          nodeLog,
		Group: raftpb.Group{NodeId: rc.config.nodeConfig.NodeID,
			Name: rc.config.GroupName, GroupId: rc.config.GroupID,
			RaftReplicaId: uint64(rc.config.ID)},
	}

	if oldwal {
		rc.restartNode(c, ds)
	} else {
		rc.ds.CleanData()
		rc.wal = rc.openWAL(nil)
		rpeers := make([]raft.Peer, 0, len(rc.config.RaftPeers))
		for _, v := range rc.config.RaftPeers {
			var m MemberInfo
			m.GroupID = rc.config.GroupID
			m.ID = v.ReplicaID
			m.RaftURLs = append(m.RaftURLs, v.RaftAddr)
			m.NodeID = v.NodeID
			d, _ := json.Marshal(m)
			rpeers = append(rpeers,
				raft.Peer{ReplicaID: v.ReplicaID, NodeID: v.NodeID, Context: d})
		}

		startPeers := rpeers
		var m MemberInfo
		m.ID = uint64(rc.config.ID)
		m.NodeID = uint64(rc.config.nodeConfig.NodeID)
		m.GroupID = rc.config.GroupID
		m.GroupName = rc.config.GroupName
		m.DataDir = rc.config.DataDir
		m.RaftURLs = append(m.RaftURLs, rc.config.RaftAddr)
		m.Broadcast = rc.config.nodeConfig.BroadcastAddr
		m.HttpAPIPort = rc.config.nodeConfig.HttpAPIPort
		data, _ := json.Marshal(m)

		if rc.join {
			startPeers = nil
		} else {
			// always update myself to cluster while first start, to allow all the members in
			// the cluster be notified the newest info of this node.
			_, ok := rc.config.RaftPeers[uint64(rc.config.ID)]
			if ok {
				cc := &raftpb.ConfChange{
					Type:      raftpb.ConfChangeUpdateNode,
					ReplicaID: m.ID,
					NodeGroup: raftpb.Group{NodeId: m.NodeID,
						GroupId: uint64(rc.config.GroupID), RaftReplicaId: m.ID},
					Context: data,
				}
				rc.wg.Add(1)
				go func(confChange raftpb.ConfChange) {
					defer rc.wg.Done()
					rc.proposeMyself(confChange)
				}(*cc)
			}
		}
		rc.node = raft.StartNode(c, startPeers)
	}
	rc.initForTransport()
	rc.wg.Add(1)
	go func() {
		defer rc.wg.Done()
		rc.serveChannels()
	}()
}

func (rc *raftNode) initForTransport() {
	if len(rc.members) == 0 {
		for _, v := range rc.config.RaftPeers {
			if v.NodeID != rc.config.nodeConfig.NodeID {
				rc.transport.AddPeer(types.ID(v.NodeID), []string{v.RaftAddr})
			}
		}
	}
	for _, m := range rc.members {
		if m.NodeID != uint64(rc.config.nodeConfig.NodeID) {
			rc.transport.AddPeer(types.ID(m.NodeID), m.RaftURLs)
		}
	}
}

func (rc *raftNode) proposeMyself(cc raftpb.ConfChange) {
	for {
		time.Sleep(time.Second)
		cc.ID = rc.reqIDGen.Next()
		rc.Infof("propose myself conf : %v", cc.String())
		err := rc.node.ProposeConfChange(context.TODO(), cc)
		if err != nil {
			rc.Infof("failed to propose the conf : %v", err)
		}
		select {
		case <-rc.stopc:
			return
		default:
		}
		// check if ok
		time.Sleep(time.Second)
		rc.memMutex.Lock()
		mine, ok := rc.members[uint64(rc.config.ID)]
		rc.memMutex.Unlock()
		if ok {
			if mine.DataDir != "" &&
				mine.GroupName != "" &&
				mine.Broadcast != "" &&
				mine.HttpAPIPort > 0 {
				return
			}
		}
	}
}

func (rc *raftNode) restartNode(c *raft.Config, ds DataStorage) {
	snapshot, err := rc.snapshotter.Load()
	if err != nil && err != snap.ErrNoSnapshot {
		nodeLog.Panic(err)
	}
	if err == snap.ErrNoSnapshot || raft.IsEmptySnap(*snapshot) {
		rc.Infof("loading no snapshot \n")
		rc.ds.CleanData()
	} else {
		rc.Infof("loading snapshot at term %d and index %d, snap: %v",
			snapshot.Metadata.Term,
			snapshot.Metadata.Index, snapshot.Metadata.ConfState)
		if err := rc.ds.RestoreFromSnapshot(true, *snapshot); err != nil {
			nodeLog.Panic(err)
		}
	}

	rc.wal = rc.replayWAL(snapshot)
	rc.node = raft.RestartNode(c)
	advanceTicksForElection(rc.node, c.ElectionTick)
}

func advanceTicksForElection(n raft.Node, electionTicks int) {
	for i := 0; i < electionTicks-1; i++ {
		n.Tick()
	}
}

func (rc *raftNode) StopNode() {
	close(rc.stopc)
	rc.wg.Wait()
	rc.Infof("raft node stopped")
}

func (rc *raftNode) stop() {
	close(rc.commitC)
	rc.node.Stop()
	rc.Infof("raft node stopping")
}

func newSnapshotReaderCloser() io.ReadCloser {
	pr, pw := io.Pipe()
	go func() {
		// TODO: write state machine snapshot data to pw
		pw.CloseWithError(nil)
	}()
	return pr
}

func (rc *raftNode) handleSendSnapshot(np *nodeProgress) {
	select {
	case m := <-rc.msgSnapC:
		snapData, err := rc.snapshotter.Load()
		if err != nil {
			rc.Infof("load snapshot error : %v", err)
			rc.ReportSnapshot(m.To, m.ToGroup, raft.SnapshotFailure)
			return
		}
		if snapData.Metadata.Index > np.appliedi {
			rc.Infof("load snapshot error, snapshot index should not great than applied: %v", snapData.Metadata, np)
			rc.ReportSnapshot(m.To, m.ToGroup, raft.SnapshotFailure)
			return
		}
		atomic.AddInt64(&rc.inflightSnapshots, 1)
		m.Snapshot = *snapData
		snapRC := newSnapshotReaderCloser()
		//TODO: copy snapshot data and send snapshot to follower
		snapMsg := snap.NewMessage(m, snapRC, 0)
		rc.Infof("begin send snapshot: %v", snapMsg.String())
		rc.transport.SendSnapshot(*snapMsg)
		rc.wg.Add(1)
		go func() {
			defer rc.wg.Done()
			select {
			case isOK := <-snapMsg.CloseNotify():
				if isOK {
					select {
					case <-time.After(releaseDelayAfterSnapshot):
					case <-rc.stopc:
					}
				}
				atomic.AddInt64(&rc.inflightSnapshots, -1)
			case <-rc.stopc:
				return
			}
		}()
	default:
	}
}

func (rc *raftNode) beginSnapshot(snapi uint64, confState raftpb.ConfState) error {
	// here we can just begin snapshot, to freeze the state of storage
	// and we can copy data async below
	// TODO: do we need the snapshot while we already make our data stable on disk?
	// maybe we can just same some meta data.
	snapTerm, err := rc.raftStorage.Term(snapi)
	if err != nil {
		nodeLog.Panicf("failed to get term from apply index: %v", err)
	}
	rc.Infof("begin get snapshot at: %v-%v", snapTerm, snapi)
	sn, err := rc.ds.GetSnapshot(snapTerm, snapi)
	if err != nil {
		return err
	}
	rc.Infof("get snapshot object done: %v", snapi)

	rc.wg.Add(1)
	go func() {
		defer rc.wg.Done()
		data, err := sn.GetData()
		if err != nil {
			panic(err)
		}
		rc.Infof("snapshot data : %v\n", string(data))
		rc.Infof("create snapshot with conf : %v\n", confState)
		// TODO: now we can do the actually snapshot for copy
		snap, err := rc.raftStorage.CreateSnapshot(snapi, &confState, data)
		if err != nil {
			if err == raft.ErrSnapOutOfDate {
				return
			}
			panic(err)
		}
		if err := rc.saveSnap(snap); err != nil {
			panic(err)
		}
		rc.Infof("saved snapshot at index %d", snap.Metadata.Index)

		compactIndex := uint64(1)
		if snapi > uint64(rc.config.SnapCatchup) {
			compactIndex = snapi - uint64(rc.config.SnapCatchup)
		}
		if err := rc.raftStorage.Compact(compactIndex); err != nil {
			if err == raft.ErrCompacted {
				return
			}
			panic(err)
		}
		rc.Infof("compacted log at index %d", compactIndex)
	}()
	return nil
}

func (rc *raftNode) saveSnap(snap raftpb.Snapshot) error {
	walSnap := walpb.Snapshot{
		Index: snap.Metadata.Index,
		Term:  snap.Metadata.Term,
	}
	if err := rc.wal.SaveSnapshot(walSnap); err != nil {
		return err
	}
	if err := rc.snapshotter.SaveSnap(snap); err != nil {
		return err
	}
	return rc.wal.ReleaseLockTo(snap.Metadata.Index)
}

// publishEntries writes committed log entries to commit channel and returns
// whether all entries could be published.
func (rc *raftNode) publishEntries(ents []raftpb.Entry, snapshot raftpb.Snapshot, raftDone chan struct{}) {
	select {
	case rc.commitC <- applyInfo{ents: ents, snapshot: snapshot, raftDone: raftDone}:
	case <-rc.stopc:
		return
	}
}

func (rc *raftNode) applyConfChange(cc raftpb.ConfChange, confState *raftpb.ConfState) (bool, error) {
	// TODO: validate configure change here
	*confState = *rc.node.ApplyConfChange(cc)
	switch cc.Type {
	case raftpb.ConfChangeAddNode:
		rc.Infof("conf change : node add : %v\n", cc.String())
		if len(cc.Context) > 0 {
			var m MemberInfo
			err := json.Unmarshal(cc.Context, &m)
			if err != nil {
				nodeLog.Fatalf("error conf context: %v", err)
			} else {
				m.ID = cc.ReplicaID
				if m.NodeID == 0 {
					nodeLog.Fatalf("invalid member info: %v", m)
					return false, errors.New("add member should include node id ")
				}
				rc.memMutex.Lock()
				if _, ok := rc.members[m.ID]; ok {
					rc.Infof("node already exist in cluster: %v\n", m)
					rc.memMutex.Unlock()
				} else {
					rc.members[m.ID] = &m
					rc.memMutex.Unlock()
					if m.NodeID != rc.config.nodeConfig.NodeID {
						rc.transport.AddPeer(types.ID(m.NodeID), m.RaftURLs)
					}
				}
				rc.Infof("node added to the cluster: %v\n", m)
			}
		}
	case raftpb.ConfChangeRemoveNode:
		rc.memMutex.Lock()
		delete(rc.members, cc.ReplicaID)
		rc.memMutex.Unlock()
		if cc.ReplicaID == uint64(rc.config.ID) {
			rc.Infof("I've been removed from the cluster! Shutting down.")
			return true, nil
		}
		// TODO: check if all replicas is deleted on the node
		// if no any replica we can remove peer from transport
		// rc.transport.RemovePeer(types.ID(cc.NodeID))
	case raftpb.ConfChangeUpdateNode:
		var m MemberInfo
		json.Unmarshal(cc.Context, &m)
		rc.Infof("node updated to the cluster: %v-%v\n", cc.String(), m)
		rc.memMutex.Lock()
		rc.members[cc.ReplicaID] = &m
		rc.memMutex.Unlock()

		if cc.NodeGroup.NodeId != uint64(rc.config.nodeConfig.NodeID) {
			rc.transport.UpdatePeer(types.ID(cc.NodeGroup.NodeId), m.RaftURLs)
		}
	}
	return false, nil
}

func (rc *raftNode) serveChannels() {
	purgeDone := make(chan struct{})
	go rc.purgeFile(purgeDone)
	defer func() {
		// wait purge stopped to avoid purge the files after wal closed
		<-purgeDone
		rc.stop()
		rc.wal.Close()
	}()

	// event loop on raft state machine updates
	isLeader := false
	for {
		select {
		// store raft entries to wal, then publish over commit channel
		case rd := <-rc.node.Ready():
			if rd.SoftState != nil {
				if lead := atomic.LoadUint64(&rc.lead); rd.SoftState.Lead != raft.None && lead != rd.SoftState.Lead {
					rc.Infof("leader changed from %v to %v", lead, rd.SoftState)
				}
				atomic.StoreUint64(&rc.lead, rd.SoftState.Lead)
				isLeader = rd.RaftState == raft.StateLeader
			}
			raftDone := make(chan struct{}, 1)
			rc.publishEntries(rd.CommittedEntries, rd.Snapshot, raftDone)
			if isLeader {
				rc.sendMessages(rd.Messages)
			}
			if err := rc.wal.Save(rd.HardState, rd.Entries); err != nil {
				nodeLog.Fatalf("raft save wal error: %v", err)
			}
			if !raft.IsEmptySnap(rd.Snapshot) {
				if err := rc.saveSnap(rd.Snapshot); err != nil {
					nodeLog.Fatalf("raft save snap error: %v", err)
				}
				rc.raftStorage.ApplySnapshot(rd.Snapshot)
				rc.Infof("raft applied incoming snapshot at index: %v", rd.Snapshot.String())
			}
			rc.raftStorage.Append(rd.Entries)
			if !isLeader {
				rc.sendMessages(rd.Messages)
			}
			raftDone <- struct{}{}
			rc.node.Advance()
		case <-rc.stopc:
			return
		}
	}
}

func (rc *raftNode) sendMessages(msgs []raftpb.Message) {
	sentAppResp := false
	for i := len(msgs) - 1; i >= 0; i-- {
		if msgs[i].Type == raftpb.MsgAppResp {
			if sentAppResp {
				msgs[i].To = 0
			} else {
				sentAppResp = true
			}
		} else if msgs[i].Type == raftpb.MsgSnap {
			// The msgSnap only contains the most recent snapshot of store meta without actually data.
			// So we need to redirect the msgSnap to server merging in the
			// current state machine snapshot.
			rc.Infof("some node request snapshot: %v", msgs[i].String())
			select {
			case rc.msgSnapC <- msgs[i]:
			default:
				// drop msgSnap if the inflight chan if full.
			}
			msgs[i].To = 0
		}
	}
	rc.transport.Send(msgs)
}

func (rc *raftNode) Lead() uint64 { return atomic.LoadUint64(&rc.lead) }
func (rc *raftNode) isLead() bool { return atomic.LoadUint64(&rc.lead) == uint64(rc.config.ID) }

type memberSorter []*MemberInfo

func (self memberSorter) Less(i, j int) bool {
	return self[i].ID < self[j].ID
}
func (self memberSorter) Swap(i, j int) {
	self[i], self[j] = self[j], self[i]
}
func (self memberSorter) Len() int {
	return len(self)
}

func (rc *raftNode) GetMembers() []*MemberInfo {
	rc.memMutex.Lock()
	mems := make(memberSorter, 0, len(rc.members))
	for id, m := range rc.members {
		m.ID = id
		mems = append(mems, m)
	}
	rc.memMutex.Unlock()
	sort.Sort(memberSorter(mems))
	return mems
}

func (rc *raftNode) GetLeadMember() *MemberInfo {
	rc.memMutex.Lock()
	m, ok := rc.members[rc.Lead()]
	rc.memMutex.Unlock()
	if ok {
		return m
	}
	return nil
}

func (rc *raftNode) RestoreMembers(mems []*MemberInfo) {
	rc.memMutex.Lock()
	rc.members = make(map[uint64]*MemberInfo)
	for _, m := range mems {
		if _, ok := rc.members[m.ID]; ok {
		} else {
			rc.members[m.ID] = m
			rc.Infof("node added to the cluster: %v\n", m)
		}
	}
	if rc.transport != nil && rc.transport.IsStarted() {
		for _, m := range rc.members {
			if m.NodeID != uint64(rc.config.nodeConfig.NodeID) {
				rc.transport.RemovePeer(types.ID(m.NodeID))
				rc.transport.AddPeer(types.ID(m.NodeID), m.RaftURLs)
			}
		}
	}
	rc.memMutex.Unlock()
}

func (rc *raftNode) Process(ctx context.Context, m raftpb.Message) error {
	if rc.node == nil {
		return nil
	}
	return rc.node.Step(ctx, m)
}

func (rc *raftNode) IsIDRemoved(id uint64, group raftpb.Group) bool { return false }
func (rc *raftNode) ReportUnreachable(id uint64, group raftpb.Group) {
	//rc.Infof("report node %v in group %v unreachable", id, group)
	rc.node.ReportUnreachable(id, group)
}
func (rc *raftNode) ReportSnapshot(id uint64, gp raftpb.Group, status raft.SnapshotStatus) {
	rc.Infof("node %v in group %v snapshot status: %v", id, gp, status)
	rc.node.ReportSnapshot(id, gp, status)
}

func (rc *raftNode) purgeFile(done chan struct{}) {
	defer func() {
		rc.Infof("purge exit")
		close(done)
	}()
	var serrc, werrc <-chan error
	serrc = fileutil.PurgeFile(rc.config.SnapDir, "snap", 10, time.Minute*10, rc.stopc)
	werrc = fileutil.PurgeFile(rc.config.WALDir, "wal", 10, time.Minute*10, rc.stopc)
	select {
	case e := <-werrc:
		rc.Infof("failed to purge wal file %v", e)
	case e := <-serrc:
		rc.Infof("failed to purge snap file %v", e)
	case <-rc.stopc:
		return
	}
}

func (rc *raftNode) Infof(f string, args ...interface{}) {
	msg := fmt.Sprintf(f, args...)
	nodeLog.InfoDepth(1, fmt.Sprintf("%v: %s", rc.Descrp(), msg))
}

func (rc *raftNode) Descrp() string {
	return rc.description
}
