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

	"github.com/youzan/ZanRedisDB/common"
	"github.com/youzan/ZanRedisDB/pkg/fileutil"
	"github.com/youzan/ZanRedisDB/pkg/idutil"
	"github.com/youzan/ZanRedisDB/pkg/types"
	"github.com/youzan/ZanRedisDB/raft"
	"github.com/youzan/ZanRedisDB/raft/raftpb"
	"github.com/youzan/ZanRedisDB/snap"
	"github.com/youzan/ZanRedisDB/transport/rafthttp"
	"github.com/youzan/ZanRedisDB/wal"
	"github.com/youzan/ZanRedisDB/wal/walpb"
	"golang.org/x/net/context"
)

const (
	DefaultSnapCount = 160000

	// HealthInterval is the minimum time the cluster should be healthy
	// before accepting add member requests.
	HealthInterval = 5 * time.Second

	// max number of in-flight snapshot messages allows to have
	maxInFlightMsgSnap        = 16
	releaseDelayAfterSnapshot = 30 * time.Second
	maxSizePerMsg             = 1024 * 1024
	maxInflightMsgs           = 256
)

type Snapshot interface {
	GetData() ([]byte, error)
}

type IRaftPersistStorage interface {
	// Save function saves ents and state to the underlying stable storage.
	// Save MUST block until st and ents are on stable storage.
	Save(st raftpb.HardState, ents []raftpb.Entry) error
	// SaveSnap function saves snapshot to the underlying stable storage.
	SaveSnap(snap raftpb.Snapshot) error
	Load() (*raftpb.Snapshot, string, error)
	// Close closes the Storage and performs finalization.
	Close() error
}

type DataStorage interface {
	CleanData() error
	RestoreFromSnapshot(bool, raftpb.Snapshot) error
	GetSnapshot(term uint64, index uint64) (Snapshot, error)
	Stop()
}

type applyInfo struct {
	ents                []raftpb.Entry
	snapshot            raftpb.Snapshot
	applySnapshotResult chan error
	raftDone            chan struct{}
	applyWaitDone       chan struct{}
}

// A key-value stream backed by raft
type raftNode struct {
	commitC chan<- applyInfo // entries committed to log (k,v)
	config  *RaftConfig

	memMutex    sync.Mutex
	members     map[uint64]*common.MemberInfo
	learnerMems map[uint64]*common.MemberInfo
	join        bool   // node is joining an existing cluster
	lastIndex   uint64 // index of log at start
	lead        uint64

	// raft backing for the commit/error channel
	node        raft.Node
	raftStorage raft.IExtRaftStorage
	wal         *wal.WAL

	persistStorage IRaftPersistStorage

	transport           rafthttp.Transporter
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
	replayRunning       int32
}

// newRaftNode initiates a raft instance and returns a committed log entry
// channel and error channel. Proposals for log updates are sent over the
// provided the proposal channel. All log entries are replayed over the
// commit channel, followed by a nil message (to indicate the channel is
// current), then new log entries.
func newRaftNode(rconfig *RaftConfig, transport *rafthttp.Transport,
	join bool, ds DataStorage, rs raft.IExtRaftStorage, newLeaderChan chan string) (<-chan applyInfo, *raftNode, error) {

	commitC := make(chan applyInfo, 5000)
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
		learnerMems:   make(map[uint64]*common.MemberInfo),
		join:          join,
		raftStorage:   rs,
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
			nodeLog.Errorf("cannot create dir for snapshot (%v)", err)
			return nil, nil, err
		}
	}
	rc.persistStorage = NewRaftPersistStorage(nil, snap.New(snapDir))
	rc.description = rc.config.GroupName + "-" + strconv.Itoa(int(rc.config.ID))
	return commitC, rc, nil
}

// openWAL returns a WAL ready for reading.
func (rc *raftNode) openWAL(snapshot *raftpb.Snapshot, readOld bool) (*wal.WAL, []byte, raftpb.HardState, []raftpb.Entry, error) {
	var hardState raftpb.HardState
	if !wal.Exist(rc.config.WALDir) {
		if err := os.MkdirAll(rc.config.WALDir, common.DIR_PERM); err != nil {
			nodeLog.Errorf("cannot create dir for wal (%v)", err)
			return nil, nil, hardState, nil, err
		}

		var m common.MemberInfo
		m.ID = uint64(rc.config.ID)
		m.GroupName = rc.config.GroupName
		m.GroupID = rc.config.GroupID
		d, _ := json.Marshal(m)
		w, err := wal.Create(rc.config.WALDir, d, rc.config.OptimizedFsync)
		if err != nil {
			nodeLog.Errorf("create wal error (%v)", err)
		}
		return w, d, hardState, nil, err
	}

	var walsnap walpb.Snapshot
	if snapshot != nil {
		walsnap.Index, walsnap.Term = snapshot.Metadata.Index, snapshot.Metadata.Term
	}
	repaired := false
	var err error
	var w *wal.WAL
	for {
		w, err = wal.Open(rc.config.WALDir, walsnap, rc.config.OptimizedFsync)
		if err != nil {
			nodeLog.Errorf("error loading wal (%v)", err)
			return w, nil, hardState, nil, err
		}
		if readOld {
			meta, st, ents, err := w.ReadAll()
			if err != nil {
				w.Close()
				nodeLog.Errorf("failed to read WAL (%v)", err)
				if repaired || err != io.ErrUnexpectedEOF {
					nodeLog.Errorf("read wal error and cannot be repaire")
				} else {
					if !wal.Repair(rc.config.WALDir) {
						nodeLog.Errorf("read wal error and cannot be repaire")
					} else {
						nodeLog.Infof("wal repaired")
						repaired = true
						continue
					}
				}
			}
			return w, meta, st, ents, err
		} else {
			break
		}
	}
	return w, nil, hardState, nil, err
}

func (rc *raftNode) replayWALForSyncLearner(snapshot *raftpb.Snapshot) error {
	// TODO: for sync learner, we do not need replay any logs before the remote synced term-index
	// and also, to avoid snapshot from leader while new sync learner started, we can get the remote
	// cluster the newest synced term-index, and add faked to wal with that term-index. In this way we can force the leader
	// to send just logs after term-index.
	return nil
}

// replayWAL replays WAL entries into the raft instance.
func (rc *raftNode) replayWAL(snapshot *raftpb.Snapshot, forceStandalone bool) error {
	w, meta, st, ents, err := rc.openWAL(snapshot, true)
	if err != nil {
		return err
	}

	rc.Infof("wal meta: %v, restart with: %v", string(meta), st.String())
	var m common.MemberInfo
	err = json.Unmarshal(meta, &m)
	if err != nil {
		w.Close()
		nodeLog.Errorf("meta is wrong: %v", err)
		return err
	}
	if m.ID != uint64(rc.config.ID) ||
		m.GroupID != rc.config.GroupID {
		w.Close()
		nodeLog.Errorf("meta starting mismatch config: %v, %v", m, rc.config)
		return err
	}
	if rs, ok := rc.persistStorage.(*raftPersistStorage); ok {
		rs.WAL = w
	}

	if forceStandalone {
		// discard the previously uncommitted entries
		for i, ent := range ents {
			if ent.Index > st.Commit {
				nodeLog.Infof("discarding %d uncommitted WAL entries ", len(ents)-i)
				ents = ents[:i]
				break
			}
		}

		ids, grps := getIDsAndGroups(snapshot, ents)

		m := common.MemberInfo{
			ID:        rc.config.ID,
			NodeID:    rc.config.nodeConfig.NodeID,
			GroupName: rc.config.GroupName,
			GroupID:   rc.config.GroupID,
		}
		m.RaftURLs = append(m.RaftURLs, rc.config.RaftAddr)
		ctx, _ := json.Marshal(m)
		// force add self node groups
		if _, ok := grps[rc.config.ID]; !ok {
			grps[rc.config.ID] = raftpb.Group{
				NodeId:        rc.config.nodeConfig.NodeID,
				Name:          rc.config.GroupName,
				GroupId:       rc.config.GroupID,
				RaftReplicaId: rc.config.ID,
			}
		}
		// force append the configuration change entries
		toAppEnts := createConfigChangeEnts(ctx, ids, grps, rc.config.ID, st.Term, st.Commit)
		ents = append(ents, toAppEnts...)

		// force commit newly appended entries
		err := w.Save(raftpb.HardState{}, toAppEnts)
		if err != nil {
			nodeLog.Errorf("force commit error: %v", err)
			return err
		}
		if len(ents) != 0 {
			st.Commit = ents[len(ents)-1].Index
		}
	}

	if snapshot != nil {
		rc.raftStorage.ApplySnapshot(*snapshot)
	}
	rc.raftStorage.SetHardState(st)
	// append to storage so raft starts at the right place in log
	rc.raftStorage.Append(ents)
	// send nil once lastIndex is published so client knows commit channel is current
	if len(ents) > 0 {
		rc.lastIndex = ents[len(ents)-1].Index
		atomic.StoreInt32(&rc.replayRunning, 1)
	} else {
		atomic.StoreInt32(&rc.replayRunning, 0)
	}
	rc.Infof("replaying WAL (%v) at lastIndex : %v", len(ents), rc.lastIndex)
	return nil
}

func (rc *raftNode) IsReplayFinished() bool {
	return atomic.LoadInt32(&rc.replayRunning) == 0
}

func (rc *raftNode) MarkReplayFinished() {
	atomic.StoreInt32(&rc.replayRunning, 0)
}

func (rc *raftNode) startRaft(ds DataStorage, standalone bool) error {
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
		MaxSizePerMsg:   maxSizePerMsg,
		MaxInflightMsgs: maxInflightMsgs,
		CheckQuorum:     true,
		PreVote:         true,
		Logger:          nodeLog,
		Group: raftpb.Group{NodeId: rc.config.nodeConfig.NodeID,
			Name: rc.config.GroupName, GroupId: rc.config.GroupID,
			RaftReplicaId: uint64(rc.config.ID)},
	}

	if oldwal {
		snapshot, _, err := rc.persistStorage.Load()
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

		if standalone {
			err = rc.restartAsStandaloneNode(c, snapshot)
		} else {
			err = rc.restartNode(c, snapshot)
		}
		if err != nil {
			return err
		}
	} else {
		rc.ds.CleanData()
		w, _, _, _, err := rc.openWAL(nil, false)
		if err != nil {
			return err
		}
		if rs, ok := rc.persistStorage.(*raftPersistStorage); ok {
			rs.WAL = w
		}
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

		isLearner := rc.config.nodeConfig.LearnerRole != ""
		startPeers := rpeers
		if rc.join {
			startPeers = nil
		}
		rc.node = raft.StartNode(c, startPeers, isLearner)
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
				if rc.join {
					rc.transport.AddRemote(types.ID(v.NodeID), []string{v.RaftAddr})
				} else {
					rc.transport.UpdatePeer(types.ID(v.NodeID), []string{v.RaftAddr})
				}
			}
		}
	}
	for _, m := range rc.members {
		if m.NodeID != uint64(rc.config.nodeConfig.NodeID) {
			rc.transport.UpdatePeer(types.ID(m.NodeID), m.RaftURLs)
		}
	}
	for _, m := range rc.learnerMems {
		if m.NodeID != uint64(rc.config.nodeConfig.NodeID) {
			rc.transport.UpdatePeer(types.ID(m.NodeID), m.RaftURLs)
		}
	}
}

func (rc *raftNode) restartNode(c *raft.Config, snapshot *raftpb.Snapshot) error {
	var err error
	err = rc.replayWAL(snapshot, false)
	if err != nil {
		return err
	}
	rc.node = raft.RestartNode(c)
	advanceTicksForElection(rc.node, c.ElectionTick)
	return nil
}

func (rc *raftNode) restartAsStandaloneNode(cfg *raft.Config, snapshot *raftpb.Snapshot) error {
	var err error
	err = rc.replayWAL(snapshot, true)
	if err != nil {
		return err
	}
	rc.node = raft.RestartNode(cfg)
	return nil
}

// returns an ordered set of IDs included in the given snapshot and
// the entries. The given snapshot/entries can contain two kinds of
// ID-related entry:
// - ConfChangeAddNode, in which case the contained ID will be added into the set.
// - ConfChangeRemoveNode, in which case the contained ID will be removed from the set.
func getIDsAndGroups(snap *raftpb.Snapshot, ents []raftpb.Entry) ([]uint64, map[uint64]raftpb.Group) {
	ids := make(map[uint64]bool)
	grps := make(map[uint64]raftpb.Group)
	if snap != nil {
		for _, id := range snap.Metadata.ConfState.Nodes {
			ids[id] = true
		}
		for _, grp := range snap.Metadata.ConfState.Groups {
			grps[grp.RaftReplicaId] = *grp
		}
		for _, id := range snap.Metadata.ConfState.Learners {
			ids[id] = true
		}
		for _, grp := range snap.Metadata.ConfState.LearnerGroups {
			grps[grp.RaftReplicaId] = *grp
		}
	}
	for _, e := range ents {
		if e.Type != raftpb.EntryConfChange {
			continue
		}
		var cc raftpb.ConfChange
		cc.Unmarshal(e.Data)
		switch cc.Type {
		case raftpb.ConfChangeAddNode:
			ids[cc.ReplicaID] = true
			grps[cc.NodeGroup.RaftReplicaId] = cc.NodeGroup
		case raftpb.ConfChangeRemoveNode:
			delete(ids, cc.ReplicaID)
			delete(grps, cc.ReplicaID)
		case raftpb.ConfChangeUpdateNode:
			// do nothing
		default:
			nodeLog.Errorf("ConfChange Type should be either ConfChangeAddNode or ConfChangeRemoveNode!")
		}
	}
	sids := make(types.Uint64Slice, 0, len(ids))
	for id := range ids {
		sids = append(sids, id)
	}
	sort.Sort(sids)
	return []uint64(sids), grps
}

// createConfigChangeEnts creates a series of Raft entries (i.e.
// EntryConfChange) to remove the set of given IDs from the cluster. The ID
// `self` is _not_ removed, even if present in the set.
// If `self` is not inside the given ids, it creates a Raft entry to add a
// default member with the given `self`.
func createConfigChangeEnts(ctx []byte, ids []uint64, grps map[uint64]raftpb.Group,
	self uint64, term, index uint64) []raftpb.Entry {
	ents := make([]raftpb.Entry, 0)
	next := index + 1
	found := false
	for _, id := range ids {
		if id == self {
			found = true
			continue
		}
		cc := &raftpb.ConfChange{
			Type:      raftpb.ConfChangeRemoveNode,
			ReplicaID: id,
			NodeGroup: grps[id],
		}
		d, _ := cc.Marshal()
		e := raftpb.Entry{
			Type:  raftpb.EntryConfChange,
			Data:  d,
			Term:  term,
			Index: next,
		}
		ents = append(ents, e)
		next++
	}
	if !found {
		cc := &raftpb.ConfChange{
			Type:      raftpb.ConfChangeAddNode,
			ReplicaID: self,
			NodeGroup: grps[self],
			Context:   ctx,
		}
		d, _ := cc.Marshal()
		e := raftpb.Entry{
			Type:  raftpb.EntryConfChange,
			Data:  d,
			Term:  term,
			Index: next,
		}
		ents = append(ents, e)
	}
	return ents
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
		snapData, _, err := rc.persistStorage.Load()
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

func (rc *raftNode) beginSnapshot(snapTerm uint64, snapi uint64, confState raftpb.ConfState) error {
	// here we can just begin snapshot, to freeze the state of storage
	// and we can copy data async below
	// TODO: do we need the snapshot while we already make our data stable on disk?
	// maybe we can just same some meta data.
	rc.Infof("begin get snapshot at: %v-%v", snapTerm, snapi)
	sn, err := rc.ds.GetSnapshot(snapTerm, snapi)
	if err != nil {
		return err
	}
	rc.Infof("get snapshot object done: %v, state: %v", snapi, confState.String())

	rc.wgAsync.Add(1)
	go func() {
		defer rc.wgAsync.Done()
		data, err := sn.GetData()
		if err != nil {
			rc.Errorf("get snapshot data at index %d failed: %v", snapi, err)
			return
		}
		rc.Infof("snapshot data : %v\n", string(data))
		rc.Infof("create snapshot with conf : %v\n", confState)
		// TODO: now we can do the actually snapshot for copy
		snap, err := rc.raftStorage.CreateSnapshot(snapi, &confState, data)
		if err != nil {
			if err == raft.ErrSnapOutOfDate {
				return
			}
			rc.Errorf("create snapshot at index %d failed: %v", snapi, err)
			return
		}
		if err := rc.persistStorage.SaveSnap(snap); err != nil {
			rc.Errorf("save snapshot at index %v failed: %v", snap.Metadata, err)
			return
		}

		compactIndex := uint64(1)
		if snapi > uint64(rc.config.SnapCatchup) {
			compactIndex = snapi - uint64(rc.config.SnapCatchup)
		}
		rc.Infof("saved snapshot at index %d, compact to: %v", snap.Metadata.Index, compactIndex)
		if err := rc.raftStorage.Compact(compactIndex); err != nil {
			if err == raft.ErrCompacted {
				return
			}
			rc.Errorf("compact log at index %v failed: %v", compactIndex, err)
			return
		}
		rc.Infof("compacted log at index %d", compactIndex)
	}()
	return nil
}

// publishEntries writes committed log entries to commit channel and returns
// whether all entries could be published.
func (rc *raftNode) publishEntries(ents []raftpb.Entry, snapshot raftpb.Snapshot, snapResult chan error,
	raftDone chan struct{}, applyWaitDone chan struct{}) {
	select {
	case rc.commitC <- applyInfo{ents: ents, snapshot: snapshot, applySnapshotResult: snapResult,
		raftDone: raftDone, applyWaitDone: applyWaitDone}:
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
				nodeLog.Errorf("error conf context: %v", err)
				go rc.ds.Stop()
				return false, false, err
			} else {
				m.ID = cc.ReplicaID
				if m.NodeID == 0 {
					nodeLog.Errorf("invalid member info: %v", m)
					go rc.ds.Stop()
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
		rc.Infof("raft replica %v removed from the cluster!", cc.String())
		delete(rc.members, cc.ReplicaID)
		delete(rc.learnerMems, cc.ReplicaID)
		confChanged = true
		atomic.StoreInt32(&rc.memberCnt, int32(len(rc.members)))
		rc.memMutex.Unlock()
		if cc.ReplicaID == uint64(rc.config.ID) {
			rc.Infof("I've been removed from the cluster! Shutting down. %v", cc.String())
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
	case raftpb.ConfChangeAddLearnerNode:
		rc.Infof("got add learner change: %v", cc.String())
		var m common.MemberInfo
		err := json.Unmarshal(cc.Context, &m)
		if err != nil {
			nodeLog.Errorf("error conf context: %v", err)
			return false, false, err
		}
		m.ID = cc.ReplicaID
		if m.NodeID == 0 {
			nodeLog.Errorf("invalid member info: %v", m)
			return false, confChanged, errors.New("add member should include node id ")
		}
		rc.memMutex.Lock()
		if _, ok := rc.learnerMems[cc.ReplicaID]; !ok {
			rc.learnerMems[cc.ReplicaID] = &m
		}
		rc.memMutex.Unlock()
		confChanged = true
		if m.NodeID != rc.config.nodeConfig.NodeID {
			rc.transport.UpdatePeer(types.ID(m.NodeID), m.RaftURLs)
		}
	}
	return false, confChanged, nil
}

func (rc *raftNode) serveChannels() {
	purgeDone := make(chan struct{})
	raftReadyLoopC := make(chan struct{})
	go rc.purgeFile(purgeDone, raftReadyLoopC)
	defer func() {
		// wait purge stopped to avoid purge the files after wal closed
		close(raftReadyLoopC)
		<-purgeDone
		close(rc.commitC)
		rc.Infof("raft node stopping")
		// wait all async operation done
		rc.wgAsync.Wait()
		rc.node.Stop()
		rc.persistStorage.Close()
		rc.raftStorage.Close()
		rc.raftStorage = nil
	}()

	// event loop on raft state machine updates
	isMeNewLeader := false
	for {
		select {
		case <-rc.stopc:
			return
		// store raft entries to wal, then publish over commit channel
		case rd, ok := <-rc.node.Ready():
			if !ok {
				rc.Errorf("raft loop stopped")
				return
			}
			if rd.SoftState != nil {
				isMeNewLeader = (rd.RaftState == raft.StateLeader)
				oldLead := atomic.LoadUint64(&rc.lead)
				isMeLosingLeader := (oldLead == uint64(rc.config.ID)) && !isMeNewLeader
				if rd.SoftState.Lead != raft.None && oldLead != rd.SoftState.Lead {
					rc.Infof("leader changed from %v to %v", oldLead, rd.SoftState)
					atomic.StoreInt64(&rc.lastLeaderChangedTs, time.Now().UnixNano())
				}
				if rd.SoftState.Lead == raft.None && oldLead != raft.None {
					// TODO: handle proposal drop if leader is lost
					//rc.triggerLeaderLost()
				}
				if isMeNewLeader || isMeLosingLeader {
					rc.triggerLeaderChanged()
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
			var applyWaitDone chan struct{}
			waitApply := false
			if !isMeNewLeader {
				// Candidate or follower needs to wait for all pending configuration
				// changes to be applied before sending messages.
				// Otherwise we might incorrectly count votes (e.g. votes from removed members).
				// Also slow machine's follower raft-layer could proceed to become the leader
				// on its own single-node cluster, before apply-layer applies the config change.
				// We simply wait for ALL pending entries to be applied for now.
				// We might improve this later on if it causes unnecessary long blocking issues.
				for _, ent := range rd.CommittedEntries {
					if ent.Type == raftpb.EntryConfChange {
						waitApply = true
						nodeLog.Infof("need wait apply for config changed: %v", ent.String())
						break
					}
				}
				if waitApply {
					applyWaitDone = make(chan struct{})
				}
			}

			var applySnapshotResult chan error
			if !raft.IsEmptySnap(rd.Snapshot) {
				applySnapshotResult = make(chan error, 1)
			}

			// TODO: do we need publish entry if commitedentries and snapshot is empty?
			rc.publishEntries(rd.CommittedEntries, rd.Snapshot, applySnapshotResult, raftDone, applyWaitDone)

			if !raft.IsEmptySnap(rd.Snapshot) {
				// since the snapshot only has metadata, we need rsync the real snapshot data first.
				// if the real snapshot failed to pull, we need stop raft and retry restart later.
				rc.Infof("raft begin to apply incoming snapshot : %v", rd.Snapshot.String())
				select {
				case applyErr := <-applySnapshotResult:
					if applyErr != nil {
						rc.Errorf("wait apply snapshot error: %v", applyErr)
						go rc.ds.Stop()
						<-rc.stopc
						return
					}
				case <-rc.stopc:
					return
				}
			}
			if isMeNewLeader {
				rc.transport.Send(rc.processMessages(rd.Messages))
			}
			if err := rc.persistStorage.Save(rd.HardState, rd.Entries); err != nil {
				nodeLog.Errorf("raft save wal error: %v", err)
				go rc.ds.Stop()
				<-rc.stopc
				return
			}
			if !raft.IsEmptySnap(rd.Snapshot) {
				if err := rc.persistStorage.SaveSnap(rd.Snapshot); err != nil {
					rc.Errorf("raft save snap error: %v", err)
					go rc.ds.Stop()
					<-rc.stopc
					return
				}
				// TODO: do we need to notify to tell that the snapshot has been perisisted onto the disk?

				rc.raftStorage.ApplySnapshot(rd.Snapshot)
				rc.Infof("raft applied incoming snapshot done: %v", rd.Snapshot.String())
				if rd.Snapshot.Metadata.Index >= rc.lastIndex {
					if !rc.IsReplayFinished() {
						rc.Infof("replay finished at snapshot index: %v\n", rd.Snapshot.String())
						rc.MarkReplayFinished()
					}
				}
			}
			rc.raftStorage.Append(rd.Entries)
			if !isMeNewLeader {
				msgs := rc.processMessages(rd.Messages)
				raftDone <- struct{}{}
				if waitApply {
					s := time.Now()
					select {
					case <-applyWaitDone:
					case <-rc.stopc:
						return
					}
					cost := time.Since(s)
					if cost > time.Second {
						nodeLog.Infof("wait apply %v msgs done cost: %v", len(msgs), cost.String())
					}
				}
				rc.transport.Send(msgs)
			} else {
				raftDone <- struct{}{}
			}
			rc.node.Advance()
		}
	}
}

func (rc *raftNode) processMessages(msgs []raftpb.Message) []raftpb.Message {
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
		} else if msgs[i].Type == raftpb.MsgVoteResp || msgs[i].Type == raftpb.MsgPreVoteResp {
			rc.Infof("send vote resp : %v", msgs[i].String())
		} else if msgs[i].Type == raftpb.MsgVote || msgs[i].Type == raftpb.MsgPreVote {
			rc.Infof("process vote/prevote :%v ", msgs[i].String())
		}
	}
	return msgs
}

func (rc *raftNode) Lead() uint64  { return atomic.LoadUint64(&rc.lead) }
func (rc *raftNode) HasLead() bool { return atomic.LoadUint64(&rc.lead) != raft.None }
func (rc *raftNode) IsLead() bool  { return atomic.LoadUint64(&rc.lead) == uint64(rc.config.ID) }

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

func (rc *raftNode) GetMembersAndLeader() ([]*common.MemberInfo, *common.MemberInfo) {
	rc.memMutex.Lock()
	l := rc.Lead()
	var lm *common.MemberInfo
	mems := make(memberSorter, 0, len(rc.members))
	for _, m := range rc.members {
		tmp := *m
		mems = append(mems, &tmp)
		if tmp.ID == l {
			lm = &tmp
		}
	}
	rc.memMutex.Unlock()
	sort.Sort(memberSorter(mems))
	return mems, lm
}

func (rc *raftNode) IsLearnerMember(m common.MemberInfo) bool {
	rc.memMutex.Lock()
	mm, ok := rc.learnerMems[m.ID]
	rc.memMutex.Unlock()
	if !ok {
		return false
	}
	if mm.ID == m.ID && mm.NodeID == m.NodeID && mm.GroupID == m.GroupID {
		return true
	}
	return false
}

func (rc *raftNode) IsMember(m common.MemberInfo) bool {
	rc.memMutex.Lock()
	mm, ok := rc.members[m.ID]
	rc.memMutex.Unlock()
	if !ok {
		return false
	}
	if mm.ID == m.ID && mm.NodeID == m.NodeID &&
		mm.GroupID == m.GroupID {
		return true
	}
	return false
}

func (rc *raftNode) GetMembers() []*common.MemberInfo {
	mems, _ := rc.GetMembersAndLeader()
	return mems
}

func (rc *raftNode) GetLearners() []*common.MemberInfo {
	rc.memMutex.Lock()
	mems := make(memberSorter, 0, len(rc.members))
	for _, m := range rc.learnerMems {
		tmp := *m
		mems = append(mems, &tmp)
	}
	rc.memMutex.Unlock()
	sort.Sort(memberSorter(mems))
	return mems
}

func (rc *raftNode) GetLeadMember() *common.MemberInfo {
	var tmp common.MemberInfo
	rc.memMutex.Lock()
	m, ok := rc.members[rc.Lead()]
	if ok {
		tmp = *m
	}
	rc.memMutex.Unlock()
	if ok {
		return &tmp
	}
	return nil
}

func (rc *raftNode) RestoreMembers(si KVSnapInfo) {
	mems := si.Members
	learners := si.Learners
	rc.memMutex.Lock()
	rc.members = make(map[uint64]*common.MemberInfo)
	for _, m := range mems {
		if _, ok := rc.members[m.ID]; ok {
		} else {
			rc.members[m.ID] = m
			rc.Infof("node added to the cluster: %v\n", m)
		}
	}
	rc.learnerMems = make(map[uint64]*common.MemberInfo)
	for _, m := range learners {
		if _, ok := rc.learnerMems[m.ID]; ok {
		} else {
			rc.learnerMems[m.ID] = m
			rc.Infof("learner node added to the cluster: %v\n", m)
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
		for _, m := range rc.learnerMems {
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
		rc.Infof("dropping message since node is nil: %v", m.String())
		return nil
	}
	err := rc.node.Step(ctx, m)
	if err != nil {
		rc.Infof("dropping message since step failed: %v", m.String())
	}
	return err
}

func (rc *raftNode) getLastLeaderChangedTime() int64 {
	return atomic.LoadInt64(&rc.lastLeaderChangedTs)
}

func (rc *raftNode) triggerLeaderChanged() {
	select {
	case rc.newLeaderChan <- rc.config.GroupName:
	case <-rc.stopc:
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

func (rc *raftNode) SaveDBFrom(r io.Reader, msg raftpb.Message) (int64, error) {
	if rs, ok := rc.persistStorage.(*raftPersistStorage); ok {
		return rs.Snapshotter.SaveDBFrom(r, msg)
	}
	return 0, nil
}

func (rc *raftNode) purgeFile(done chan struct{}, stopC chan struct{}) {
	defer func() {
		rc.Infof("purge exit")
		close(done)
	}()
	if rc.config.nodeConfig == nil {
		// maybe in test
		return
	}
	keepBackup := rc.config.nodeConfig.KeepBackup
	keep := rc.config.nodeConfig.KeepWAL
	if keep <= 1 {
		keep = 20
	}
	if keepBackup <= 1 {
		keepBackup = 10
	}
	var serrc, werrc <-chan error
	serrc = fileutil.PurgeFile(rc.config.SnapDir, "snap", uint(keepBackup), time.Minute*10, rc.stopc)
	werrc = fileutil.PurgeFile(rc.config.WALDir, "wal", uint(keep), time.Minute*10, rc.stopc)
	select {
	case e := <-werrc:
		rc.Infof("failed to purge wal file %v", e)
	case e := <-serrc:
		rc.Infof("failed to purge snap file %v", e)
	case <-stopC:
		return
	}
}

func (rc *raftNode) Debugf(f string, args ...interface{}) {
	msg := fmt.Sprintf(f, args...)
	nodeLog.DebugDepth(1, fmt.Sprintf("%v: %s", rc.Descrp(), msg))
}

func (rc *raftNode) Infof(f string, args ...interface{}) {
	msg := fmt.Sprintf(f, args...)
	nodeLog.InfoDepth(1, fmt.Sprintf("%v: %s", rc.Descrp(), msg))
}

func (rc *raftNode) Errorf(f string, args ...interface{}) {
	msg := fmt.Sprintf(f, args...)
	nodeLog.ErrorDepth(1, fmt.Sprintf("%v: %s", rc.Descrp(), msg))
}

func (rc *raftNode) Descrp() string {
	return rc.description
}
