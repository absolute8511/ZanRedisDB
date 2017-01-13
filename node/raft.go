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
	"io"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"encoding/json"
	"net/http"
	"net/url"
	"sync/atomic"

	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/coreos/etcd/etcdserver/stats"
	"github.com/coreos/etcd/pkg/fileutil"
	"github.com/coreos/etcd/pkg/idutil"
	"github.com/coreos/etcd/pkg/types"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/rafthttp"
	"github.com/coreos/etcd/snap"
	"github.com/coreos/etcd/wal"
	"github.com/coreos/etcd/wal/walpb"
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
	Clear() error
	RestoreFromSnapshot(bool, raftpb.Snapshot) error
	GetSnapshot(term uint64, index uint64) (Snapshot, error)
}

type applyInfo struct {
	ents     []raftpb.Entry
	snapshot raftpb.Snapshot
	raftDone chan struct{}
}

type MemberInfo struct {
	ID          uint64   `json:"id"`
	ClusterName string   `json:"cluster_name"`
	Namespace   string   `json:"namespace"`
	ClusterID   uint64   `json:"cluster_id"`
	Broadcast   string   `json:"broadcast"`
	RpcPort     int      `json:"rpc_port"`
	HttpAPIPort int      `json:"http_api_port"`
	RaftURLs    []string `json:"peer_urls"`
	DataDir     string   `json:"data_dir"`
}

// A key-value stream backed by raft
type raftNode struct {
	proposeC    <-chan []byte          // proposed messages (k,v)
	confChangeC chan raftpb.ConfChange // proposed cluster config changes
	commitC     chan<- applyInfo       // entries committed to log (k,v)
	errorC      chan error             // errors from raft session
	config      *RaftConfig

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
	httpstopc         chan struct{} // signals http server to shutdown
	httpdonec         chan struct{} // signals http server shutdown complete
	reqIDGen          *idutil.Generator
	wg                sync.WaitGroup
	ds                DataStorage
	msgSnapC          chan raftpb.Message
	inflightSnapshots int64
}

// newRaftNode initiates a raft instance and returns a committed log entry
// channel and error channel. Proposals for log updates are sent over the
// provided the proposal channel. All log entries are replayed over the
// commit channel, followed by a nil message (to indicate the channel is
// current), then new log entries. To shutdown, close proposeC and read errorC.
func newRaftNode(rconfig *RaftConfig, join bool, ds DataStorage, proposeC <-chan []byte,
	confChangeC chan raftpb.ConfChange) (<-chan applyInfo, <-chan error, *raftNode) {

	commitC := make(chan applyInfo, 1000)
	errorC := make(chan error)
	if rconfig.SnapCount <= 0 {
		rconfig.SnapCount = DefaultSnapCount
	}
	if rconfig.SnapCatchup <= 0 {
		rconfig.SnapCatchup = rconfig.SnapCount / 2
	}

	rc := &raftNode{
		proposeC:    proposeC,
		confChangeC: confChangeC,
		commitC:     commitC,
		errorC:      errorC,
		config:      rconfig,
		members:     make(map[uint64]*MemberInfo),
		join:        join,
		raftStorage: raft.NewMemoryStorage(),
		stopc:       make(chan struct{}),
		httpstopc:   make(chan struct{}),
		httpdonec:   make(chan struct{}),
		ds:          ds,
		reqIDGen:    idutil.NewGenerator(uint16(rconfig.ID), time.Now()),
		msgSnapC:    make(chan raftpb.Message, maxInFlightMsgSnap),
		// rest of structure populated after WAL replay
	}
	return commitC, errorC, rc
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
		nodeLog.Infof("conf change : node add : %v\n", cc.NodeID)
		if len(cc.Context) > 0 {
			var m MemberInfo
			err := json.Unmarshal(cc.Context, &m)
			if err != nil {
				nodeLog.Fatalf("error conf context: %v", err)
			} else {
				m.ID = cc.NodeID
				rc.memMutex.Lock()
				if _, ok := rc.members[cc.NodeID]; ok {
					nodeLog.Infof("node already exist in cluster: %v-%v\n", cc.NodeID, m)
					rc.memMutex.Unlock()
				} else {
					rc.members[cc.NodeID] = &m
					rc.memMutex.Unlock()
					if cc.NodeID != uint64(rc.config.ID) {
						rc.transport.AddPeer(types.ID(cc.NodeID), m.RaftURLs)
					}
				}
				nodeLog.Infof("node added to the cluster: %v-%v\n", cc.NodeID, m)
			}
		}
	case raftpb.ConfChangeRemoveNode:
		rc.memMutex.Lock()
		delete(rc.members, cc.NodeID)
		rc.memMutex.Unlock()
		if cc.NodeID == uint64(rc.config.ID) {
			nodeLog.Info("I've been removed from the cluster! Shutting down.")
			return true, nil
		}
		rc.transport.RemovePeer(types.ID(cc.NodeID))
	case raftpb.ConfChangeUpdateNode:
		var m MemberInfo
		json.Unmarshal(cc.Context, &m)
		nodeLog.Infof("node updated to the cluster: %v-%v\n", cc.NodeID, m)
		rc.memMutex.Lock()
		rc.members[cc.NodeID] = &m
		rc.memMutex.Unlock()

		if cc.NodeID != uint64(rc.config.ID) {
			rc.transport.UpdatePeer(types.ID(cc.NodeID), m.RaftURLs)
		}
	}
	return false, nil
}

// openWAL returns a WAL ready for reading.
func (rc *raftNode) openWAL(snapshot *raftpb.Snapshot) *wal.WAL {
	if !wal.Exist(rc.config.WALDir) {
		if err := os.MkdirAll(rc.config.WALDir, common.DIR_PERM); err != nil {
			nodeLog.Fatalf("cannot create dir for wal (%v)", err)
		}

		var m MemberInfo
		m.ID = uint64(rc.config.ID)
		m.ClusterName = "test-cluster-part1"
		m.ClusterID = rc.config.ClusterID
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
	nodeLog.Infof("wal meta: %v, restart with: %v", string(meta), st.String())
	var m MemberInfo
	err = json.Unmarshal(meta, &m)
	if err != nil {
		w.Close()
		nodeLog.Fatalf("meta is wrong: %v", err)
	}
	if m.ID != uint64(rc.config.ID) ||
		m.ClusterID != rc.config.ClusterID {
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
	nodeLog.Infof("replaying WAL (%v) at lastIndex : %v\n", len(ents), rc.lastIndex)
	rc.raftStorage.SetHardState(st)
	return w
}

func (rc *raftNode) startRaft(ds DataStorage) {
	snapDir := rc.config.SnapDir
	walDir := rc.config.WALDir
	if !fileutil.Exist(snapDir) {
		if err := os.MkdirAll(snapDir, common.DIR_PERM); err != nil {
			nodeLog.Fatalf("cannot create dir for snapshot (%v)", err)
		}
	}
	rc.snapshotter = snap.New(snapDir)
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
	}

	if oldwal {
		rc.restartNode(c, ds)
	} else {
		rc.wal = rc.openWAL(nil)
		rpeers := make([]raft.Peer, 0, len(rc.config.RaftPeers))
		for id, v := range rc.config.RaftPeers {
			var m MemberInfo
			m.ClusterID = rc.config.ClusterID
			m.ID = uint64(id)
			m.RaftURLs = append(m.RaftURLs, v)
			d, _ := json.Marshal(m)
			rpeers = append(rpeers, raft.Peer{ID: uint64(id), Context: d})
		}

		startPeers := rpeers
		var m MemberInfo
		m.ID = uint64(rc.config.ID)
		m.ClusterID = rc.config.ClusterID
		m.Namespace = rc.config.Namespace
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
			_, ok := rc.config.RaftPeers[rc.config.ID]
			if ok {
				cc := &raftpb.ConfChange{
					Type:    raftpb.ConfChangeUpdateNode,
					NodeID:  m.ID,
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

	ss := &stats.ServerStats{}
	ss.Initialize()

	rc.transport = &rafthttp.Transport{
		DialTimeout: time.Second * 5,
		ID:          types.ID(rc.config.ID),
		ClusterID:   types.ID(rc.config.ClusterID),
		Raft:        rc,
		Snapshotter: rc.snapshotter,
		ServerStats: ss,
		LeaderStats: stats.NewLeaderStats(strconv.Itoa(rc.config.ID)),
		ErrorC:      rc.errorC,
	}

	rc.transport.Start()
	if len(rc.members) == 0 {
		for id, v := range rc.config.RaftPeers {
			if id != rc.config.ID {
				rc.transport.AddPeer(types.ID(id), []string{v})
			}
		}
	}
	for _, m := range rc.members {
		if m.ID != uint64(rc.config.ID) {
			rc.transport.AddPeer(types.ID(m.ID), m.RaftURLs)
		}
	}

	rc.wg.Add(1)
	go func() {
		defer rc.wg.Done()
		rc.serveRaft()
	}()
	rc.wg.Add(1)
	go func() {
		defer rc.wg.Done()
		rc.serveChannels()
	}()
	rc.wg.Add(1)
	go func() {
		defer rc.wg.Done()
		rc.purgeFile()
	}()
}

func (rc *raftNode) proposeMyself(cc raftpb.ConfChange) {
	for {
		time.Sleep(time.Second)
		select {
		case rc.confChangeC <- cc:
		case <-rc.stopc:
			return
		}
		// check if ok
		time.Sleep(time.Second)
		rc.memMutex.Lock()
		mine, ok := rc.members[uint64(rc.config.ID)]
		rc.memMutex.Unlock()
		if ok {
			if mine.DataDir != "" &&
				mine.Namespace != "" &&
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
		nodeLog.Infof("loading no snapshot \n")
		rc.ds.Clear()
	} else {
		nodeLog.Infof("loading snapshot at term %d and index %d, snap: %v\n", snapshot.Metadata.Term,
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
	nodeLog.Info("raft node stopped")
}

// stop closes http, closes all channels, and stops raft.
func (rc *raftNode) stop() {
	rc.stopHTTP()
	close(rc.commitC)
	rc.node.Stop()
	nodeLog.Info("raft node stopping")
}

func (rc *raftNode) stopHTTP() {
	rc.transport.Stop()
	close(rc.httpstopc)
	<-rc.httpdonec
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
			nodeLog.Infof("load snapshot error : %v", err)
			rc.ReportSnapshot(m.To, raft.SnapshotFailure)
			return
		}
		if snapData.Metadata.Index > np.appliedi {
			nodeLog.Infof("load snapshot error, snapshot index should not great than applied: %v", snapData.Metadata, np)
			rc.ReportSnapshot(m.To, raft.SnapshotFailure)
			return
		}
		atomic.AddInt64(&rc.inflightSnapshots, 1)
		m.Snapshot = *snapData
		snapRC := newSnapshotReaderCloser()
		//TODO: copy snapshot data and send snapshot to follower
		snapMsg := snap.NewMessage(m, snapRC, 0)
		nodeLog.Infof("begin send snapshot: %v", snapMsg.String())
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
	nodeLog.Infof("begin get snapshot at: %v-%v", snapTerm, snapi)
	sn, err := rc.ds.GetSnapshot(snapTerm, snapi)
	if err != nil {
		return err
	}
	nodeLog.Infof("get snapshot object done: %v", snapi)

	rc.wg.Add(1)
	go func() {
		defer rc.wg.Done()
		data, err := sn.GetData()
		if err != nil {
			panic(err)
		}
		nodeLog.Infof("snapshot data : %v\n", string(data))
		nodeLog.Infof("create snapshot with conf : %v\n", confState)
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
		nodeLog.Infof("saved snapshot at index %d", snap.Metadata.Index)

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
		nodeLog.Infof("compacted log at index %d", compactIndex)
	}()
	return nil
}

func (rc *raftNode) serveChannels() {
	defer rc.wal.Close()

	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	// send proposals over raft
	go func() {
		for rc.proposeC != nil && rc.confChangeC != nil {
			select {
			case <-rc.stopc:
				return
			case prop, ok := <-rc.proposeC:
				if !ok {
					rc.proposeC = nil
				} else {
					// blocks until accepted by raft state machine
					rc.node.Propose(context.TODO(), prop)
				}

			case cc, ok := <-rc.confChangeC:
				if !ok {
					rc.confChangeC = nil
				} else {
					cc.ID = rc.reqIDGen.Next()
					nodeLog.Infof("propose the conf change: %v", cc.String())
					err := rc.node.ProposeConfChange(context.TODO(), cc)
					if err != nil {
						nodeLog.Infof("failed to propose the conf change: %v", err)
					}
				}
			}
		}
	}()

	// event loop on raft state machine updates
	isLeader := false
	for {
		select {
		case <-ticker.C:
			rc.node.Tick()

		// store raft entries to wal, then publish over commit channel
		case rd := <-rc.node.Ready():
			if rd.SoftState != nil {
				if lead := atomic.LoadUint64(&rc.lead); rd.SoftState.Lead != raft.None && lead != rd.SoftState.Lead {
					nodeLog.Infof("leader changed from %v to %v", lead, rd.SoftState)
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
				nodeLog.Infof("raft applied incoming snapshot at index: %v", rd.Snapshot.String())
			}
			rc.raftStorage.Append(rd.Entries)
			if !isLeader {
				rc.sendMessages(rd.Messages)
			}
			raftDone <- struct{}{}
			rc.node.Advance()
		case <-rc.stopc:
			rc.stop()
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
			nodeLog.Infof("some node request snapshot: %v", msgs[i].String())
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

func (rc *raftNode) serveRaft() {
	url, err := url.Parse(rc.config.RaftAddr)
	if err != nil {
		nodeLog.Fatalf("Failed parsing URL (%v)", err)
	}

	ln, err := common.NewStoppableListener(url.Host, rc.httpstopc)
	if err != nil {
		nodeLog.Fatalf("Failed to listen rafthttp (%v)", err)
	}

	err = (&http.Server{Handler: rc.transport.Handler()}).Serve(ln)
	select {
	case <-rc.httpstopc:
	default:
		nodeLog.Fatalf("Failed to serve rafthttp (%v)", err)
	}
	close(rc.httpdonec)
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
			nodeLog.Infof("node added to the cluster: %v\n", m)
		}
	}
	if rc.transport != nil {
		rc.transport.RemoveAllPeers()
		for _, m := range rc.members {
			if m.ID != uint64(rc.config.ID) {
				rc.transport.AddPeer(types.ID(m.ID), m.RaftURLs)
			}
		}
	}
	rc.memMutex.Unlock()
}

func (rc *raftNode) Process(ctx context.Context, m raftpb.Message) error {
	return rc.node.Step(ctx, m)
}
func (rc *raftNode) IsIDRemoved(id uint64) bool  { return false }
func (rc *raftNode) ReportUnreachable(id uint64) { rc.node.ReportUnreachable(id) }
func (rc *raftNode) ReportSnapshot(id uint64, status raft.SnapshotStatus) {
	nodeLog.Infof("send to %v snapshot status: %v", id, status)
	rc.node.ReportSnapshot(id, status)
}

func (rc *raftNode) purgeFile() {
	var serrc, werrc <-chan error
	serrc = fileutil.PurgeFile(rc.config.SnapDir, "snap", 10, time.Minute*10, rc.stopc)
	werrc = fileutil.PurgeFile(rc.config.WALDir, "wal", 10, time.Minute*10, rc.stopc)
	select {
	case e := <-werrc:
		nodeLog.Infof("failed to purge wal file %v", e)
	case e := <-serrc:
		nodeLog.Infof("failed to purge snap file %v", e)
	case <-rc.stopc:
		return
	}
}
