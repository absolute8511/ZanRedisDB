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

// A key-value stream backed by raft
type raftNode struct {
	commitC chan<- applyInfo // entries committed to log (k,v)
	config  *RaftConfig

	memMutex  sync.Mutex
	members   map[uint64]*common.MemberInfo
	join      bool   // node is joining an existing cluster
	lastIndex uint64 // index of log at start
	lead      uint64

	// raft backing for the commit/error channel
	node        raft.Node
	raftStorage *raft.MemoryStorage
	wal         *wal.WAL

	snapshotter *snap.Snapshotter

	transport           *rafthttp.Transport
	stopc               chan struct{} // signals proposal channel closed
	reqIDGen            *idutil.Generator
	wgAsync             sync.WaitGroup
	wgServe             sync.WaitGroup
	ds                  DataStorage
	msgSnapC            chan raftpb.Message
	inflightSnapshots   int64
	description         string
	readStateC          chan raft.ReadState
	memberCnt           int32
	newLeaderChan       chan string
	lastLeaderChangedTs int64
	stopping            int32
}

// newRaftNode initiates a raft instance and returns a committed log entry
// channel and error channel. Proposals for log updates are sent over the
// provided the proposal channel. All log entries are replayed over the
// commit channel, followed by a nil message (to indicate the channel is
// current), then new log entries.
func newRaftNode(rconfig *RaftConfig, transport *rafthttp.Transport,
	join bool, ds DataStorage, newLeaderChan chan string) (<-chan applyInfo, *raftNode) {

	commitC := make(chan applyInfo, 1000)
	if rconfig.SnapCount <= 0 {
		rconfig.SnapCount = DefaultSnapCount
	}
	if rconfig.SnapCatchup <= 0 {
		rconfig.SnapCatchup = rconfig.SnapCount / 2
	}

	rc := &raftNode{
		commitC:       commitC,
		config:        rconfig,
		members:       make(map[uint64]*common.MemberInfo),
		join:          join,
		raftStorage:   raft.NewMemoryStorage(),
		stopc:         make(chan struct{}),
		ds:            ds,
		reqIDGen:      idutil.NewGenerator(uint16(rconfig.ID), time.Now()),
		msgSnapC:      make(chan raftpb.Message, maxInFlightMsgSnap),
		transport:     transport,
		readStateC:    make(chan raft.ReadState, 1),
		newLeaderChan: newLeaderChan,
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

		var m common.MemberInfo
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
	var m common.MemberInfo
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

func (rc *raftNode) startRaft(ds DataStorage) error {
	walDir := rc.config.WALDir
	oldwal := wal.Exist(walDir)

	elecTick := rc.config.nodeConfig.ElectionTick
	if elecTick < 10 {
		elecTick = 10
	}
	c := &raft.Config{
		ID:              uint64(rc.config.ID),
		ElectionTick:    elecTick,
		HeartbeatTick:   elecTick / 10,
		Storage:         rc.raftStorage,
		MaxSizePerMsg:   1024 * 1024,
		MaxInflightMsgs: 256,
		CheckQuorum:     true,
		PreVote:         true,
		Logger:          nodeLog,
		Group: raftpb.Group{NodeId: rc.config.nodeConfig.NodeID,
			Name: rc.config.GroupName, GroupId: rc.config.GroupID,
			RaftReplicaId: uint64(rc.config.ID)},
	}

	if oldwal {
		err := rc.restartNode(c, ds)
		if err != nil {
			return err
		}
	} else {
		rc.ds.CleanData()
		rc.wal = rc.openWAL(nil)
		rpeers := make([]raft.Peer, 0, len(rc.config.RaftPeers))
		for _, v := range rc.config.RaftPeers {
			var m common.MemberInfo
			m.GroupID = rc.config.GroupID
			m.GroupName = rc.config.GroupName
			m.ID = v.ReplicaID
			m.RaftURLs = append(m.RaftURLs, v.RaftAddr)
			m.NodeID = v.NodeID
			d, _ := json.Marshal(m)
			rpeers = append(rpeers,
				raft.Peer{ReplicaID: v.ReplicaID, NodeID: v.NodeID, Context: d})
		}

		startPeers := rpeers
		if rc.join {
			startPeers = nil
		}
		rc.node = raft.StartNode(c, startPeers)
	}
	rc.initForTransport()
	rc.wgServe.Add(1)
	go func() {
		defer rc.wgServe.Done()
		rc.serveChannels()
	}()
	return nil
}

func (rc *raftNode) initForTransport() {
	if len(rc.members) == 0 {
		for _, v := range rc.config.RaftPeers {
			if v.NodeID != rc.config.nodeConfig.NodeID {
				rc.transport.UpdatePeer(types.ID(v.NodeID), []string{v.RaftAddr})
			}
		}
	}
	for _, m := range rc.members {
		if m.NodeID != uint64(rc.config.nodeConfig.NodeID) {
			rc.transport.UpdatePeer(types.ID(m.NodeID), m.RaftURLs)
		}
	}
}

func (rc *raftNode) restartNode(c *raft.Config, ds DataStorage) error {
	snapshot, err := rc.snapshotter.Load()
	if err != nil && err != snap.ErrNoSnapshot {
		nodeLog.Warning(err)
		return err
	}
	if err == snap.ErrNoSnapshot || raft.IsEmptySnap(*snapshot) {
		rc.Infof("loading no snapshot \n")
		rc.ds.CleanData()
	} else {
		rc.Infof("loading snapshot at term %d and index %d, snap: %v",
			snapshot.Metadata.Term,
			snapshot.Metadata.Index, snapshot.Metadata.ConfState)
		if err := rc.ds.RestoreFromSnapshot(true, *snapshot); err != nil {
			nodeLog.Error(err)
			return err
		}
	}

	rc.wal = rc.replayWAL(snapshot)
	rc.node = raft.RestartNode(c)
	advanceTicksForElection(rc.node, c.ElectionTick)
	return nil
}

func advanceTicksForElection(n raft.Node, electionTicks int) {
	for i := 0; i < electionTicks-1; i++ {
		n.Tick()
	}
}

func (rc *raftNode) StopNode() {
	if !atomic.CompareAndSwapInt32(&rc.stopping, 0, 1) {
		return
	}
	close(rc.stopc)
	rc.wgServe.Wait()
	rc.Infof("raft node stopped")
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
		rc.wgAsync.Add(1)
		go func() {
			defer rc.wgAsync.Done()
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

	rc.wgAsync.Add(1)
	go func() {
		defer rc.wgAsync.Done()
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

func (rc *raftNode) maybeTryElection() {
	// to avoid election at the same time, we only allow the smallest node to elect
	smallest := rc.config.ID
	for _, v := range rc.config.RaftPeers {
		if v.ReplicaID < smallest {
			smallest = v.ReplicaID
		}
	}
	rc.Infof("replica %v should advance to elect, mine is: %v", smallest, rc.config.ID)
	if smallest == rc.config.ID {
		advanceTicksForElection(rc.node, rc.config.nodeConfig.ElectionTick*2)
	}
}

// return (self removed, any conf changed, error)
func (rc *raftNode) applyConfChange(cc raftpb.ConfChange, confState *raftpb.ConfState) (bool, bool, error) {
	// TODO: validate configure change here
	*confState = *rc.node.ApplyConfChange(cc)
	confChanged := false
	switch cc.Type {
	case raftpb.ConfChangeAddNode:
		rc.Infof("conf change : node add : %v\n", cc.String())
		if len(cc.Context) > 0 {
			var m common.MemberInfo
			err := json.Unmarshal(cc.Context, &m)
			if err != nil {
				nodeLog.Fatalf("error conf context: %v", err)
			} else {
				m.ID = cc.ReplicaID
				if m.NodeID == 0 {
					nodeLog.Fatalf("invalid member info: %v", m)
					return false, confChanged, errors.New("add member should include node id ")
				}
				rc.memMutex.Lock()
				memNum := len(rc.members)
				if _, ok := rc.members[m.ID]; ok {
					rc.Infof("node already exist in cluster: %v\n", m)
					rc.memMutex.Unlock()
				} else {
					confChanged = true
					rc.members[m.ID] = &m
					memNum++
					atomic.StoreInt32(&rc.memberCnt, int32(memNum))
					rc.memMutex.Unlock()
					if m.NodeID != rc.config.nodeConfig.NodeID {
						rc.transport.UpdatePeer(types.ID(m.NodeID), m.RaftURLs)
					}
					rc.Infof("node added to the cluster: %v\n", m)
				}
				if rc.Lead() == raft.None && memNum >= 2 && memNum >= len(rc.config.RaftPeers) {
					rc.maybeTryElection()
				}
			}
		}
	case raftpb.ConfChangeRemoveNode:
		rc.memMutex.Lock()
		delete(rc.members, cc.ReplicaID)
		confChanged = true
		atomic.StoreInt32(&rc.memberCnt, int32(len(rc.members)))
		rc.memMutex.Unlock()
		if cc.ReplicaID == uint64(rc.config.ID) {
			rc.Infof("I've been removed from the cluster! Shutting down.")
			return true, confChanged, nil
		}
	case raftpb.ConfChangeUpdateNode:
		var m common.MemberInfo
		json.Unmarshal(cc.Context, &m)
		rc.Infof("node updated to the cluster: %v-%v\n", cc.String(), m)
		rc.memMutex.Lock()
		oldm := rc.members[cc.ReplicaID]
		rc.members[cc.ReplicaID] = &m
		if oldm != nil && !oldm.IsEqual(&m) {
			confChanged = true
		}
		atomic.StoreInt32(&rc.memberCnt, int32(len(rc.members)))
		rc.memMutex.Unlock()

		if cc.NodeGroup.NodeId != uint64(rc.config.nodeConfig.NodeID) {
			rc.transport.UpdatePeer(types.ID(cc.NodeGroup.NodeId), m.RaftURLs)
		}
	}
	return false, confChanged, nil
}

func (rc *raftNode) serveChannels() {
	purgeDone := make(chan struct{})
	go rc.purgeFile(purgeDone)
	defer func() {
		// wait purge stopped to avoid purge the files after wal closed
		<-purgeDone
		close(rc.commitC)
		rc.Infof("raft node stopping")
		// wait all async operation done
		rc.wgAsync.Wait()
		rc.node.Stop()
		rc.wal.Close()
	}()

	// event loop on raft state machine updates
	isLeader := false
	for {
		select {
		// store raft entries to wal, then publish over commit channel
		case rd := <-rc.node.Ready():
			if rd.SoftState != nil {
				isLeader = rd.RaftState == raft.StateLeader
				if lead := atomic.LoadUint64(&rc.lead); rd.SoftState.Lead != raft.None && lead != rd.SoftState.Lead {
					rc.Infof("leader changed from %v to %v", lead, rd.SoftState)
					atomic.StoreInt64(&rc.lastLeaderChangedTs, time.Now().UnixNano())
					if isLeader {
						rc.triggerNewLeader()
					}
				}
				atomic.StoreUint64(&rc.lead, rd.SoftState.Lead)
			}
			if len(rd.ReadStates) != 0 {
				select {
				case rc.readStateC <- rd.ReadStates[len(rd.ReadStates)-1]:
				case <-time.After(time.Second):
					nodeLog.Infof("timeout sending read state")
				case <-rc.stopc:
					return
				}
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
		} else if msgs[i].Type == raftpb.MsgVoteResp {
			rc.Infof("send vote resp : %v", msgs[i].String())
		}
	}
	rc.transport.Send(msgs)
}

func (rc *raftNode) Lead() uint64 { return atomic.LoadUint64(&rc.lead) }
func (rc *raftNode) IsLead() bool { return atomic.LoadUint64(&rc.lead) == uint64(rc.config.ID) }

type memberSorter []*common.MemberInfo

func (self memberSorter) Less(i, j int) bool {
	return self[i].ID < self[j].ID
}
func (self memberSorter) Swap(i, j int) {
	self[i], self[j] = self[j], self[i]
}
func (self memberSorter) Len() int {
	return len(self)
}

func (rc *raftNode) GetMembers() []*common.MemberInfo {
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

func (rc *raftNode) GetLeadMember() *common.MemberInfo {
	rc.memMutex.Lock()
	m, ok := rc.members[rc.Lead()]
	rc.memMutex.Unlock()
	if ok {
		return m
	}
	return nil
}

func (rc *raftNode) RestoreMembers(mems []*common.MemberInfo) {
	rc.memMutex.Lock()
	rc.members = make(map[uint64]*common.MemberInfo)
	for _, m := range mems {
		if _, ok := rc.members[m.ID]; ok {
		} else {
			rc.members[m.ID] = m
			rc.Infof("node added to the cluster: %v\n", m)
		}
	}
	atomic.StoreInt32(&rc.memberCnt, int32(len(rc.members)))
	if rc.transport != nil && rc.transport.IsStarted() {
		for _, m := range rc.members {
			if m.NodeID != uint64(rc.config.nodeConfig.NodeID) {
				//rc.transport.RemovePeer(types.ID(m.NodeID))
				rc.transport.UpdatePeer(types.ID(m.NodeID), m.RaftURLs)
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

func (rc *raftNode) getLastLeaderChangedTime() int64 {
	return atomic.LoadInt64(&rc.lastLeaderChangedTs)
}

func (rc *raftNode) triggerNewLeader() {
	select {
	case rc.newLeaderChan <- rc.config.GroupName:
	default:
	}
}

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
