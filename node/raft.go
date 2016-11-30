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
	"fmt"
	"log"
	"net"
	"os"
	"path"
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
	DefaultSnapCount = 100000

	// HealthInterval is the minimum time the cluster should be healthy
	// before accepting add member requests.
	HealthInterval = 5 * time.Second

	// max number of in-flight snapshot messages allows to have
	maxInFlightMsgSnap        = 16
	releaseDelayAfterSnapshot = 30 * time.Second
)

var snapshotCatchUpEntriesN uint64 = 100000

type DataStorage interface {
	Clear() error
	RestoreFromSnapshot(bool, raftpb.Snapshot) error
	GetSnapshot() ([]byte, error)
}

type applyCommitEntry struct {
	commitEntry raftpb.Entry
	snapshot    raftpb.Snapshot
}

type MemberInfo struct {
	ID          uint64   `json:"id"`
	ClusterName string   `json:"cluster_name"`
	ClusterID   uint64   `json:"cluster_id"`
	Broadcast   string   `json:"broadcast"`
	RpcPort     int      `json:"rpc_port"`
	RaftURLs    []string `json:"peer_urls"`
	DataDir     string   `json:"data_dir"`
}

// A key-value stream backed by raft
type raftNode struct {
	proposeC    <-chan []byte            // proposed messages (k,v)
	confChangeC <-chan raftpb.ConfChange // proposed cluster config changes
	commitC     chan<- *applyCommitEntry // entries committed to log (k,v)
	errorC      chan<- error             // errors from raft session

	clusterID     uint64
	id            int // client ID for raft session
	DataDir       string
	localRaftAddr string
	peers         map[int]string // raft peer URLs
	memMutex      sync.Mutex
	members       map[uint64]*MemberInfo
	join          bool   // node is joining an existing cluster
	waldir        string // path to WAL directory
	snapdir       string // path to snapshot directory
	lastIndex     uint64 // index of log at start
	lead          uint64

	confState     raftpb.ConfState
	snapshotIndex uint64
	appliedIndex  uint64

	// raft backing for the commit/error channel
	node        raft.Node
	raftStorage *raft.MemoryStorage
	wal         *wal.WAL

	snapshotter *snap.Snapshotter

	snapCount uint64
	transport *rafthttp.Transport
	stopc     chan struct{} // signals proposal channel closed
	httpstopc chan struct{} // signals http server to shutdown
	httpdonec chan struct{} // signals http server shutdown complete
	reqIDGen  *idutil.Generator
	wg        sync.WaitGroup
	ds        DataStorage
}

// newRaftNode initiates a raft instance and returns a committed log entry
// channel and error channel. Proposals for log updates are sent over the
// provided the proposal channel. All log entries are replayed over the
// commit channel, followed by a nil message (to indicate the channel is
// current), then new log entries. To shutdown, close proposeC and read errorC.
func newRaftNode(clusterID uint64, id int, localRaftAddr string, raftDataDir string,
	peers map[int]string, join bool, ds DataStorage, proposeC <-chan []byte,
	confChangeC <-chan raftpb.ConfChange) (<-chan *applyCommitEntry, <-chan error, *raftNode) {

	commitC := make(chan *applyCommitEntry)
	errorC := make(chan error)

	rc := &raftNode{
		proposeC:      proposeC,
		confChangeC:   confChangeC,
		commitC:       commitC,
		errorC:        errorC,
		clusterID:     clusterID,
		id:            id,
		localRaftAddr: localRaftAddr,
		peers:         peers,
		members:       make(map[uint64]*MemberInfo),
		join:          join,
		raftStorage:   raft.NewMemoryStorage(),
		snapCount:     DefaultSnapCount,
		stopc:         make(chan struct{}),
		httpstopc:     make(chan struct{}),
		httpdonec:     make(chan struct{}),
		DataDir:       raftDataDir,
		ds:            ds,
		reqIDGen:      idutil.NewGenerator(uint16(id), time.Now()),

		// rest of structure populated after WAL replay
	}
	rc.waldir = path.Join(raftDataDir, fmt.Sprintf("wal-%d", id))
	rc.snapdir = path.Join(raftDataDir, fmt.Sprintf("snap-%d", id))
	return commitC, errorC, rc
}

func (rc *raftNode) saveSnap(snap raftpb.Snapshot) error {
	if err := rc.snapshotter.SaveSnap(snap); err != nil {
		return err
	}
	walSnap := walpb.Snapshot{
		Index: snap.Metadata.Index,
		Term:  snap.Metadata.Term,
	}
	if err := rc.wal.SaveSnapshot(walSnap); err != nil {
		return err
	}
	return rc.wal.ReleaseLockTo(snap.Metadata.Index)
}

func (rc *raftNode) entriesToApply(ents []raftpb.Entry) (nents []raftpb.Entry) {
	if len(ents) == 0 {
		return
	}
	firstIdx := ents[0].Index
	if firstIdx > rc.appliedIndex+1 {
		log.Fatalf("first index of committed entry[%d] should <= progress.appliedIndex[%d] 1", firstIdx, rc.appliedIndex)
	}
	if rc.appliedIndex-firstIdx+1 < uint64(len(ents)) {
		nents = ents[rc.appliedIndex-firstIdx+1:]
	}
	return
}

// publishEntries writes committed log entries to commit channel and returns
// whether all entries could be published.
func (rc *raftNode) publishEntries(ents []raftpb.Entry) bool {
	for i := range ents {
		switch ents[i].Type {
		case raftpb.EntryNormal:
			if len(ents[i].Data) == 0 {
				// ignore empty messages
				break
			}
			select {
			case rc.commitC <- &applyCommitEntry{commitEntry: ents[i]}:
			case <-rc.stopc:
				return false
			}

		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			cc.Unmarshal(ents[i].Data)
			rc.confState = *rc.node.ApplyConfChange(cc)
			switch cc.Type {
			case raftpb.ConfChangeAddNode:
				log.Printf("conf change : node add : %v\n", cc.NodeID)
				if len(cc.Context) > 0 {
					var m MemberInfo
					err := json.Unmarshal(cc.Context, &m)
					if err != nil {
						log.Printf("error conf context: %v", err)
					} else {
						m.ID = cc.NodeID
						rc.memMutex.Lock()
						if _, ok := rc.members[cc.NodeID]; ok {
							log.Printf("node already exist in cluster: %v-%v\n", cc.NodeID, m)
							rc.memMutex.Unlock()
						} else {
							rc.members[cc.NodeID] = &m
							rc.memMutex.Unlock()
							if cc.NodeID != uint64(rc.id) {
								rc.transport.AddPeer(types.ID(cc.NodeID), m.RaftURLs)
							}
						}
						log.Printf("node added to the cluster: %v-%v\n", cc.NodeID, m)
					}
				}
			case raftpb.ConfChangeRemoveNode:
				rc.memMutex.Lock()
				delete(rc.members, cc.NodeID)
				rc.memMutex.Unlock()
				if cc.NodeID == uint64(rc.id) {
					log.Println("I've been removed from the cluster! Shutting down.")
					return false
				}
				rc.transport.RemovePeer(types.ID(cc.NodeID))
			case raftpb.ConfChangeUpdateNode:
				var m MemberInfo
				json.Unmarshal(cc.Context, &m)
				log.Printf("node updated to the cluster: %v-%v\n", cc.NodeID, m)
				rc.memMutex.Lock()
				rc.members[cc.NodeID] = &m
				rc.memMutex.Unlock()

				if cc.NodeID != uint64(rc.id) {
					rc.transport.UpdatePeer(types.ID(cc.NodeID), m.RaftURLs)
				}
			}
		}

		// after commit, update appliedIndex
		rc.appliedIndex = ents[i].Index
		if ents[i].Index == rc.lastIndex {
			log.Printf("replay finished at index: %v\n", rc.lastIndex)
		}
	}
	return true
}

// openWAL returns a WAL ready for reading.
func (rc *raftNode) openWAL(snapshot *raftpb.Snapshot) *wal.WAL {
	if !wal.Exist(rc.waldir) {
		if err := os.MkdirAll(rc.waldir, common.DIR_PERM); err != nil {
			log.Fatalf("cannot create dir for wal (%v)", err)
		}

		var m MemberInfo
		m.ID = uint64(rc.id)
		m.ClusterName = "test-cluster-part1"
		m.ClusterID = rc.clusterID
		d, _ := json.Marshal(m)
		w, err := wal.Create(rc.waldir, d)
		if err != nil {
			log.Fatalf("create wal error (%v)", err)
		}
		return w
	}

	var walsnap walpb.Snapshot
	if snapshot != nil {
		walsnap.Index, walsnap.Term = snapshot.Metadata.Index, snapshot.Metadata.Term
	}
	w, err := wal.Open(rc.waldir, walsnap)
	if err != nil {
		log.Fatalf("error loading wal (%v)", err)
	}

	return w
}

// replayWAL replays WAL entries into the raft instance.
func (rc *raftNode) replayWAL(snapshot *raftpb.Snapshot) *wal.WAL {
	w := rc.openWAL(snapshot)
	meta, st, ents, err := w.ReadAll()
	if err != nil {
		w.Close()
		log.Fatalf("failed to read WAL (%v)", err)
	}
	log.Printf("wal meta: %v, restart with: %v", string(meta), st.String())

	if snapshot != nil {
		rc.raftStorage.ApplySnapshot(*snapshot)
	}
	// append to storage so raft starts at the right place in log
	rc.raftStorage.Append(ents)
	// send nil once lastIndex is published so client knows commit channel is current
	if len(ents) > 0 {
		rc.lastIndex = ents[len(ents)-1].Index
	}
	log.Printf("replaying WAL (%v) at lastIndex : %v\n", len(ents), rc.lastIndex)
	rc.raftStorage.SetHardState(st)
	return w
}

func (rc *raftNode) writeError(err error) {
	rc.stopHTTP()
	close(rc.commitC)
	rc.errorC <- err
	close(rc.errorC)
	rc.node.Stop()
}

func (rc *raftNode) startRaft(ds DataStorage) {
	if !fileutil.Exist(rc.snapdir) {
		if err := os.MkdirAll(rc.snapdir, common.DIR_PERM); err != nil {
			log.Fatalf("cannot create dir for snapshot (%v)", err)
		}
	}
	rc.snapshotter = snap.New(rc.snapdir)
	oldwal := wal.Exist(rc.waldir)

	c := &raft.Config{
		ID:              uint64(rc.id),
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         rc.raftStorage,
		MaxSizePerMsg:   1024 * 1024,
		MaxInflightMsgs: 256,
		CheckQuorum:     true,
	}

	if oldwal {
		rc.restartNode(c, ds)
	} else {
		rc.wal = rc.openWAL(nil)
		rpeers := make([]raft.Peer, 0, len(rc.peers))
		for id, v := range rc.peers {
			var m MemberInfo
			m.ClusterID = rc.clusterID
			m.ClusterName = ""
			m.ID = uint64(id)
			if id == rc.id {
				m.DataDir = rc.DataDir
				u, err := url.Parse(rc.localRaftAddr)
				if err != nil {
				}
				m.Broadcast, _, err = net.SplitHostPort(u.Host)
				m.RaftURLs = append(m.RaftURLs, rc.localRaftAddr)
			} else {
				m.RaftURLs = append(m.RaftURLs, v)
			}
			d, _ := json.Marshal(m)
			rpeers = append(rpeers, raft.Peer{ID: uint64(id), Context: d})
		}

		startPeers := rpeers
		if rc.join {
			startPeers = nil
		}
		rc.node = raft.StartNode(c, startPeers)
	}

	ss := &stats.ServerStats{}
	ss.Initialize()

	rc.transport = &rafthttp.Transport{
		ID:          types.ID(rc.id),
		ClusterID:   types.ID(rc.clusterID),
		Raft:        rc,
		ServerStats: ss,
		LeaderStats: stats.NewLeaderStats(strconv.Itoa(rc.id)),
		ErrorC:      make(chan error),
	}

	rc.transport.Start()
	for id, v := range rc.peers {
		if id != rc.id {
			rc.transport.AddPeer(types.ID(id), []string{v})
		}
	}

	for _, m := range rc.members {
		if m.ID != uint64(rc.id) {
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
}

func (rc *raftNode) restartNode(c *raft.Config, ds DataStorage) {
	snapshot, err := rc.snapshotter.Load()
	if err != nil && err != snap.ErrNoSnapshot {
		log.Panic(err)
	}
	if err == snap.ErrNoSnapshot || raft.IsEmptySnap(*snapshot) {
		log.Printf("loading no snapshot \n")
		rc.ds.Clear()
	} else {
		log.Printf("loading snapshot at term %d and index %d, snap: %v\n", snapshot.Metadata.Term,
			snapshot.Metadata.Index, snapshot.Metadata.ConfState)
		if err := rc.ds.RestoreFromSnapshot(true, *snapshot); err != nil {
			log.Panic(err)
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
	log.Println("raft node stopped")
}

// stop closes http, closes all channels, and stops raft.
func (rc *raftNode) stop() {
	rc.stopHTTP()
	close(rc.commitC)
	close(rc.errorC)
	rc.node.Stop()
	log.Println("raft node stopping")
}

func (rc *raftNode) stopHTTP() {
	rc.transport.Stop()
	close(rc.httpstopc)
	<-rc.httpdonec
}

func (rc *raftNode) publishSnapshot(snapshotToSave raftpb.Snapshot) {
	if raft.IsEmptySnap(snapshotToSave) {
		return
	}

	// signaled to load snapshot
	log.Printf("publishing snapshot at index %d, snapshot: %v\n", rc.snapshotIndex, snapshotToSave.String())
	defer log.Printf("finished publishing snapshot at index %d\n", rc.snapshotIndex)

	if snapshotToSave.Metadata.Index <= rc.appliedIndex {
		log.Fatalf("snapshot index [%d] should > progress.appliedIndex [%d] + 1", snapshotToSave.Metadata.Index, rc.appliedIndex)
	}

	if err := rc.ds.RestoreFromSnapshot(false, snapshotToSave); err != nil {
		log.Panic(err)
	}

	rc.transport.RemoveAllPeers()
	rc.memMutex.Lock()
	for _, m := range rc.members {
		if m.ID != uint64(rc.id) {
			rc.transport.AddPeer(types.ID(m.ID), m.RaftURLs)
		}
	}
	rc.memMutex.Unlock()
	rc.confState = snapshotToSave.Metadata.ConfState
	rc.snapshotIndex = snapshotToSave.Metadata.Index
	rc.appliedIndex = snapshotToSave.Metadata.Index
}

func (rc *raftNode) maybeTriggerSnapshot() {
	if rc.appliedIndex-rc.snapshotIndex <= rc.snapCount {
		return
	}

	log.Printf("start snapshot [applied index: %d | last snapshot index: %d]", rc.appliedIndex, rc.snapshotIndex)
	// here we can just begin snapshot, to freeze the state of storage
	// and we can copy data async below
	data, err := rc.ds.GetSnapshot()
	if err != nil {
		log.Panic(err)
	}

	rc.wg.Add(1)
	go func(snapi uint64, confState raftpb.ConfState) {
		defer rc.wg.Done()
		log.Printf("create snapshot with conf : %v\n", confState)
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
		log.Printf("saved snapshot at index %d", snap.Metadata.Index)

		compactIndex := uint64(1)
		if rc.appliedIndex > snapshotCatchUpEntriesN {
			compactIndex = snapi - snapshotCatchUpEntriesN
		}
		if err := rc.raftStorage.Compact(compactIndex); err != nil {
			if err == raft.ErrCompacted {
				return
			}
			panic(err)
		}
		log.Printf("compacted log at index %d", compactIndex)
	}(rc.appliedIndex, rc.confState)

	rc.snapshotIndex = rc.appliedIndex
}

func (rc *raftNode) serveChannels() {
	snap, err := rc.raftStorage.Snapshot()
	if err != nil {
		panic(err)
	}
	rc.confState = snap.Metadata.ConfState
	rc.snapshotIndex = snap.Metadata.Index
	rc.appliedIndex = snap.Metadata.Index
	log.Printf("starting conf state: %v\n", rc.confState)

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
					err := rc.node.ProposeConfChange(context.TODO(), cc)
					if err != nil {
						log.Printf("failed to propose the conf change: %v", err)
					}
				}
			}
		}
	}()

	// event loop on raft state machine updates
	for {
		select {
		case <-ticker.C:
			rc.node.Tick()

		// store raft entries to wal, then publish over commit channel
		case rd := <-rc.node.Ready():
			if rd.SoftState != nil {
				if lead := atomic.LoadUint64(&rc.lead); rd.SoftState.Lead != raft.None && lead != rd.SoftState.Lead {
					log.Printf("leader changed from %v to %v", lead, rd.SoftState)
				}
				atomic.StoreUint64(&rc.lead, rd.SoftState.Lead)
				//islead = rd.RaftState == raft.StateLeader
			}
			rc.wal.Save(rd.HardState, rd.Entries)
			if !raft.IsEmptySnap(rd.Snapshot) {
				rc.saveSnap(rd.Snapshot)
				rc.raftStorage.ApplySnapshot(rd.Snapshot)
				rc.publishSnapshot(rd.Snapshot)
			}
			rc.raftStorage.Append(rd.Entries)
			rc.transport.Send(rd.Messages)
			if ok := rc.publishEntries(rc.entriesToApply(rd.CommittedEntries)); !ok {
				rc.stop()
				return
			}
			if rc.appliedIndex <= rc.lastIndex {
				// replaying local log
			} else {
				rc.maybeTriggerSnapshot()
			}

			rc.node.Advance()

		case err := <-rc.transport.ErrorC:
			rc.writeError(err)
			return

		case <-rc.stopc:
			rc.stop()
			return
		}
	}
}

func (rc *raftNode) serveRaft() {
	url, err := url.Parse(rc.localRaftAddr)
	if err != nil {
		log.Fatalf("Failed parsing URL (%v)", err)
	}

	ln, err := common.NewStoppableListener(url.Host, rc.httpstopc)
	if err != nil {
		log.Fatalf("Failed to listen rafthttp (%v)", err)
	}

	err = (&http.Server{Handler: rc.transport.Handler()}).Serve(ln)
	select {
	case <-rc.httpstopc:
	default:
		log.Fatalf("Failed to serve rafthttp (%v)", err)
	}
	close(rc.httpdonec)
}

func (rc *raftNode) Lead() uint64 { return atomic.LoadUint64(&rc.lead) }
func (rc *raftNode) isLead() bool { return atomic.LoadUint64(&rc.lead) == uint64(rc.id) }

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
	for _, m := range mems {
		if m.ID != uint64(rc.id) {
			if _, ok := rc.members[m.ID]; ok {
			} else {
				rc.members[m.ID] = m
				log.Printf("node added to the cluster: %v\n", m)
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
	log.Printf("send to %v snapshot status: %v", id, status)
	rc.node.ReportSnapshot(id, status)
}
