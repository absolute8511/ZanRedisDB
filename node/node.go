package node

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/context"

	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/absolute8511/ZanRedisDB/pkg/wait"
	"github.com/absolute8511/ZanRedisDB/raft"
	"github.com/absolute8511/ZanRedisDB/raft/raftpb"
	"github.com/absolute8511/ZanRedisDB/rockredis"
	"github.com/absolute8511/ZanRedisDB/transport/rafthttp"
)

var (
	errInvalidResponse      = errors.New("Invalid response type")
	errSyntaxError          = errors.New("syntax error")
	errUnknownData          = errors.New("unknown request data type")
	errTooMuchBatchSize     = errors.New("the batch size exceed the limit")
	errRaftNotReadyForWrite = errors.New("the raft is not ready for write")
)

const (
	RedisReq        int8 = 0
	HTTPReq         int8 = 1
	SchemaChangeReq int8 = 2
	proposeTimeout       = time.Second * 4
	maxBatchCmdNum       = 500
)

const (
	HttpProposeOp_Backup int = 1
)

type nodeProgress struct {
	confState raftpb.ConfState
	snapi     uint64
	appliedt  uint64
	appliedi  uint64
}

type internalReq struct {
	reqData InternalRaftRequest
	done    chan struct{}
}

type httpProposeData struct {
	ProposeOp  int
	NeedBackup bool
}

// a key-value node backed by raft
type KVNode struct {
	reqProposeC       chan *internalReq
	rn                *raftNode
	store             *KVStore
	sm                StateMachine
	stopping          int32
	stopChan          chan struct{}
	stopDone          chan struct{}
	w                 wait.Wait
	router            *common.CmdRouter
	deleteCb          func()
	clusterWriteStats common.WriteStats
	ns                string
	machineConfig     *MachineConfig
	wg                sync.WaitGroup
	commitC           <-chan applyInfo
	committedIndex    uint64
	clusterInfo       common.IClusterInfo
	expireHandler     *ExpireHandler
	expirationPolicy  common.ExpirationPolicy
}

type KVSnapInfo struct {
	*rockredis.BackupInfo
	Ver        int                  `json:"version"`
	BackupMeta []byte               `json:"backup_meta"`
	LeaderInfo *common.MemberInfo   `json:"leader_info"`
	Members    []*common.MemberInfo `json:"members"`
	Learners   []*common.MemberInfo `json:"learners"`
}

func (si *KVSnapInfo) GetData() ([]byte, error) {
	if si.BackupInfo != nil {
		meta, err := si.BackupInfo.GetResult()
		if err != nil {
			return nil, err
		}
		si.BackupMeta = meta
	}
	d, _ := json.Marshal(si)
	return d, nil
}

func NewKVNode(kvopts *KVOptions, machineConfig *MachineConfig, config *RaftConfig,
	transport *rafthttp.Transport, join bool, deleteCb func(),
	clusterInfo common.IClusterInfo, newLeaderChan chan string) (*KVNode, error) {
	config.WALDir = path.Join(config.DataDir, fmt.Sprintf("wal-%d", config.ID))
	config.SnapDir = path.Join(config.DataDir, fmt.Sprintf("snap-%d", config.ID))
	config.nodeConfig = machineConfig

	stopChan := make(chan struct{})
	fullName := config.GroupName + "-" + strconv.Itoa(int(config.ID))
	sm, err := NewStateMachine(fullName, kvopts, *machineConfig, config.ID, config.GroupName, clusterInfo, stopChan)
	if err != nil {
		return nil, err
	}
	s := &KVNode{
		reqProposeC:      make(chan *internalReq, 200),
		stopChan:         stopChan,
		stopDone:         make(chan struct{}),
		store:            nil,
		sm:               sm,
		w:                wait.New(),
		router:           common.NewCmdRouter(),
		deleteCb:         deleteCb,
		ns:               config.GroupName,
		machineConfig:    machineConfig,
		expirationPolicy: kvopts.ExpirationPolicy,
	}
	if kvsm, ok := sm.(*kvStoreSM); ok {
		kvsm.w = s.w
		s.store = kvsm.store
	}
	s.clusterInfo = clusterInfo
	s.expireHandler = NewExpireHandler(s)

	s.registerHandler()

	commitC, raftNode, err := newRaftNode(config, transport,
		join, s, newLeaderChan)
	if err != nil {
		return nil, err
	}
	s.rn = raftNode
	s.commitC = commitC
	return s, nil
}

func (nd *KVNode) Start(standalone bool) error {
	err := nd.rn.startRaft(nd, standalone)
	if err != nil {
		return err
	}
	// read commits from raft into KVStore map until error
	nd.wg.Add(1)
	go func() {
		defer nd.wg.Done()
		nd.applyCommits(nd.commitC)
	}()
	nd.wg.Add(1)
	go func() {
		defer nd.wg.Done()
		nd.handleProposeReq()
	}()

	nd.expireHandler.Start()
	return nil
}

func (nd *KVNode) StopRaft() {
	nd.rn.StopNode()
}

func (nd *KVNode) Stop() {
	if !atomic.CompareAndSwapInt32(&nd.stopping, 0, 1) {
		return
	}
	defer close(nd.stopDone)
	close(nd.stopChan)
	nd.expireHandler.Stop()
	nd.wg.Wait()
	nd.rn.StopNode()
	nd.sm.Close()
	// deleted cb should be called after stopped, otherwise it
	// may init the same node after deleted while the node is stopping.
	go nd.deleteCb()
	nd.rn.Infof("node %v stopped", nd.ns)
}

func (nd *KVNode) OptimizeDB() {
	nd.rn.Infof("node %v begin optimize db", nd.ns)
	nd.sm.Optimize()
	nd.rn.Infof("node %v end optimize db", nd.ns)
	// since we can not know whether leader or follower is done on optimize
	// we backup anyway after optimize
	p := &httpProposeData{
		ProposeOp:  HttpProposeOp_Backup,
		NeedBackup: true,
	}
	d, _ := json.Marshal(p)
	nd.HTTPPropose(d)
}

func (nd *KVNode) IsLead() bool {
	return nd.rn.IsLead()
}

func (nd *KVNode) GetRaftStatus() raft.Status {
	return nd.rn.node.Status()
}

// this is used for leader to determine whether a follower is up to date.
func (nd *KVNode) IsReplicaRaftReady(raftID uint64) bool {
	s := nd.rn.node.Status()
	pg, ok := s.Progress[raftID]
	if !ok {
		return false
	}
	if pg.State.String() == "ProgressStateReplicate" {
		if pg.Match+maxInflightMsgs >= s.Commit {
			return true
		}
	} else if pg.State.String() == "ProgressStateProbe" {
		if pg.Match+1 >= s.Commit {
			return true
		}
	}
	return false
}

func (nd *KVNode) GetLeadMember() *common.MemberInfo {
	return nd.rn.GetLeadMember()
}

func (nd *KVNode) GetMembers() []*common.MemberInfo {
	return nd.rn.GetMembers()
}

func (nd *KVNode) GetLearners() []*common.MemberInfo {
	return nd.rn.GetLearners()
}

func (nd *KVNode) GetLocalMemberInfo() *common.MemberInfo {
	if nd.rn == nil {
		return nil
	}
	var m common.MemberInfo
	m.ID = uint64(nd.rn.config.ID)
	m.NodeID = nd.rn.config.nodeConfig.NodeID
	m.GroupID = nd.rn.config.GroupID
	m.GroupName = nd.rn.config.GroupName
	m.RaftURLs = append(m.RaftURLs, nd.rn.config.RaftAddr)
	return &m
}

func (nd *KVNode) GetDBInternalStats() string {
	if s, ok := nd.sm.(*kvStoreSM); ok {
		return s.store.GetStatistics()
	}
	return ""
}

func (nd *KVNode) GetStats() common.NamespaceStats {
	ns := nd.sm.GetStats()
	ns.ClusterWriteStats = nd.clusterWriteStats.Copy()
	return ns
}

func (nd *KVNode) destroy() error {
	// should make sure stopped and wait other stopping finish
	nd.Stop()
	<-nd.stopDone
	nd.sm.Destroy()
	ts := strconv.Itoa(int(time.Now().UnixNano()))
	return os.Rename(nd.rn.config.DataDir,
		nd.rn.config.DataDir+"-deleted-"+ts)
}

func (nd *KVNode) CleanData() error {
	return nd.sm.CleanData()
}

func (nd *KVNode) GetHandler(cmd string) (common.CommandFunc, bool, bool) {
	return nd.router.GetCmdHandler(cmd)
}

func (nd *KVNode) GetMergeHandler(cmd string) (common.MergeCommandFunc, bool, bool) {
	return nd.router.GetMergeCmdHandler(cmd)
}

func (nd *KVNode) handleProposeReq() {
	var reqList BatchInternalRaftRequest
	reqList.Reqs = make([]*InternalRaftRequest, 0, 100)
	var lastReq *internalReq
	// TODO: combine pipeline and batch to improve performance
	// notice the maxPendingProposals config while using pipeline, avoid
	// sending too much pipeline which overflow the proposal buffer.
	//lastReqList := make([]*internalReq, 0, 1024)

	defer func() {
		if e := recover(); e != nil {
			buf := make([]byte, 4096)
			n := runtime.Stack(buf, false)
			buf = buf[0:n]
			nd.rn.Errorf("handle propose loop panic: %s:%v", buf, e)
		}
		for _, r := range reqList.Reqs {
			nd.w.Trigger(r.Header.ID, common.ErrStopped)
		}
		nd.rn.Infof("handle propose loop exit")
		for {
			select {
			case r := <-nd.reqProposeC:
				nd.w.Trigger(r.reqData.Header.ID, common.ErrStopped)
			default:
				return
			}
		}
	}()
	for {
		pc := nd.reqProposeC
		if len(reqList.Reqs) >= 1000 {
			pc = nil
		}
		select {
		case r := <-pc:
			reqList.Reqs = append(reqList.Reqs, &r.reqData)
			lastReq = r
		default:
			if len(reqList.Reqs) == 0 {
				select {
				case r := <-nd.reqProposeC:
					reqList.Reqs = append(reqList.Reqs, &r.reqData)
					lastReq = r
				case <-nd.stopChan:
					return
				}
			}
			reqList.ReqNum = int32(len(reqList.Reqs))
			buffer, err := reqList.Marshal()
			// buffer will be reused by raft?
			// TODO:buffer, err := reqList.MarshalTo()
			if err != nil {
				nd.rn.Infof("failed to marshal request: %v", err)
				for _, r := range reqList.Reqs {
					nd.w.Trigger(r.Header.ID, err)
				}
				reqList.Reqs = reqList.Reqs[:0]
				continue
			}
			lastReq.done = make(chan struct{})
			//nd.rn.Infof("handle req %v, marshal buffer: %v, raw: %v, %v", len(reqList.Reqs),
			//	realN, buffer, reqList.Reqs)
			start := lastReq.reqData.Header.Timestamp
			cost := time.Now().UnixNano() - start
			if cost >= int64(proposeTimeout.Nanoseconds())/2 {
				nd.rn.Infof("ignore slow for begin propose too late: %v, cost %v", len(reqList.Reqs), cost)
				for _, r := range reqList.Reqs {
					nd.w.Trigger(r.Header.ID, common.ErrQueueTimeout)
				}
			} else {
				leftProposeTimeout := proposeTimeout + time.Second - time.Duration(cost)

				ctx, cancel := context.WithTimeout(context.Background(), leftProposeTimeout)
				err = nd.rn.node.ProposeWithDrop(ctx, buffer, cancel)
				if err != nil {
					nd.rn.Infof("propose failed: %v, err: %v", len(reqList.Reqs), err.Error())
					for _, r := range reqList.Reqs {
						nd.w.Trigger(r.Header.ID, err)
					}
				} else {
					//lastReqList = append(lastReqList, lastReq)
					select {
					case <-lastReq.done:
					case <-ctx.Done():
						err := ctx.Err()
						waitLeader := false
						if err == context.DeadlineExceeded {
							waitLeader = true
							nd.rn.Infof("propose timeout: %v, %v", err.Error(), len(reqList.Reqs))
						}
						if err == context.Canceled {
							// proposal canceled can be caused by leader transfer or no leader
							err = ErrProposalCanceled
							waitLeader = true
							nd.rn.Infof("propose canceled : %v", len(reqList.Reqs))
						}
						for _, r := range reqList.Reqs {
							nd.w.Trigger(r.Header.ID, err)
						}
						if waitLeader {
							time.Sleep(proposeTimeout/100 + time.Millisecond*100)
						}
					case <-nd.stopChan:
						cancel()
						return
					}
				}
				cancel()
				cost = time.Now().UnixNano() - start
			}
			if cost >= int64(proposeTimeout.Nanoseconds())/2 {
				nd.rn.Infof("slow for batch propose: %v, cost %v", len(reqList.Reqs), cost)
			}
			for i := range reqList.Reqs {
				reqList.Reqs[i] = nil
			}
			reqList.Reqs = reqList.Reqs[:0]
			lastReq = nil
		}
	}
}

func (nd *KVNode) IsWriteReady() bool {
	return atomic.LoadInt32(&nd.rn.memberCnt) > int32(nd.rn.config.Replicator/2)
}

func (nd *KVNode) queueRequest(req *internalReq) (interface{}, error) {
	if !nd.IsWriteReady() {
		return nil, errRaftNotReadyForWrite
	}
	start := time.Now()
	req.reqData.Header.Timestamp = start.UnixNano()
	ch := nd.w.Register(req.reqData.Header.ID)
	select {
	case nd.reqProposeC <- req:
	default:
		select {
		case nd.reqProposeC <- req:
		case <-nd.stopChan:
			nd.w.Trigger(req.reqData.Header.ID, common.ErrStopped)
		case <-time.After(proposeTimeout / 2):
			nd.w.Trigger(req.reqData.Header.ID, common.ErrQueueTimeout)
		}
	}
	//nd.rn.Infof("queue request: %v", req.reqData.String())
	var err error
	var rsp interface{}
	var ok bool
	select {
	case rsp = <-ch:
		if req.done != nil {
			close(req.done)
		}
		if err, ok = rsp.(error); ok {
			rsp = nil
		} else {
			err = nil
		}
	case <-nd.stopChan:
		rsp = nil
		err = common.ErrStopped
	}
	nd.clusterWriteStats.UpdateWriteStats(int64(len(req.reqData.Data)), time.Since(start).Nanoseconds()/1000)
	if err == nil && !nd.IsWriteReady() {
		nd.rn.Infof("write request %v on raft success but raft member is less than replicator",
			req.reqData.String())
		return nil, errRaftNotReadyForWrite
	}
	return rsp, err
}

func (nd *KVNode) Propose(buf []byte) (interface{}, error) {
	h := &RequestHeader{
		ID:       nd.rn.reqIDGen.Next(),
		DataType: 0,
	}
	raftReq := InternalRaftRequest{
		Header: h,
		Data:   buf,
	}
	req := &internalReq{
		reqData: raftReq,
	}
	return nd.queueRequest(req)
}

func (nd *KVNode) HTTPPropose(buf []byte) (interface{}, error) {
	h := &RequestHeader{
		ID:       nd.rn.reqIDGen.Next(),
		DataType: int32(HTTPReq),
	}
	raftReq := InternalRaftRequest{
		Header: h,
		Data:   buf,
	}
	req := &internalReq{
		reqData: raftReq,
	}
	return nd.queueRequest(req)
}

func (nd *KVNode) ProposeChangeTableSchema(table string, sc *SchemaChange) error {
	h := &RequestHeader{
		ID:       nd.rn.reqIDGen.Next(),
		DataType: int32(SchemaChangeReq),
	}
	buf, _ := sc.Marshal()
	raftReq := InternalRaftRequest{
		Header: h,
		Data:   buf,
	}
	req := &internalReq{
		reqData: raftReq,
	}

	_, err := nd.queueRequest(req)
	return err
}

func (nd *KVNode) FillMyMemberInfo(m *common.MemberInfo) {
	m.RaftURLs = append(m.RaftURLs, nd.machineConfig.LocalRaftAddr)
}

func (nd *KVNode) ProposeAddLearner(m common.MemberInfo) error {
	if m.NodeID == nd.machineConfig.NodeID {
		nd.FillMyMemberInfo(&m)
	}
	data, _ := json.Marshal(m)
	cc := raftpb.ConfChange{
		Type:      raftpb.ConfChangeAddLearnerNode,
		ReplicaID: m.ID,
		NodeGroup: raftpb.Group{
			NodeId:        m.NodeID,
			Name:          m.GroupName,
			GroupId:       uint64(m.GroupID),
			RaftReplicaId: m.ID},
		Context: data,
	}
	return nd.proposeConfChange(cc)
}

func (nd *KVNode) ProposeAddMember(m common.MemberInfo) error {
	if m.NodeID == nd.machineConfig.NodeID {
		nd.FillMyMemberInfo(&m)
	}
	data, _ := json.Marshal(m)
	cc := raftpb.ConfChange{
		Type:      raftpb.ConfChangeAddNode,
		ReplicaID: m.ID,
		NodeGroup: raftpb.Group{
			NodeId:        m.NodeID,
			Name:          m.GroupName,
			GroupId:       uint64(m.GroupID),
			RaftReplicaId: m.ID},
		Context: data,
	}
	return nd.proposeConfChange(cc)
}

func (nd *KVNode) ProposeRemoveMember(m common.MemberInfo) error {
	cc := raftpb.ConfChange{
		Type:      raftpb.ConfChangeRemoveNode,
		ReplicaID: m.ID,
		NodeGroup: raftpb.Group{
			NodeId:        m.NodeID,
			Name:          m.GroupName,
			GroupId:       uint64(m.GroupID),
			RaftReplicaId: m.ID},
	}
	return nd.proposeConfChange(cc)
}

func (nd *KVNode) ProposeUpdateMember(m common.MemberInfo) error {
	if m.NodeID == nd.machineConfig.NodeID {
		nd.FillMyMemberInfo(&m)
	}
	data, _ := json.Marshal(m)
	cc := raftpb.ConfChange{
		Type:      raftpb.ConfChangeUpdateNode,
		ReplicaID: m.ID,
		NodeGroup: raftpb.Group{
			NodeId:        m.NodeID,
			Name:          m.GroupName,
			GroupId:       uint64(m.GroupID),
			RaftReplicaId: m.ID},
		Context: data,
	}
	return nd.proposeConfChange(cc)
}

func (nd *KVNode) proposeConfChange(cc raftpb.ConfChange) error {
	cc.ID = nd.rn.reqIDGen.Next()
	nd.rn.Infof("propose the conf change: %v", cc.String())
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(nd.machineConfig.TickMs*5)*time.Millisecond)
	err := nd.rn.node.ProposeConfChange(ctx, cc)
	if err != nil {
		nd.rn.Infof("failed to propose the conf change: %v", err)
	}
	cancel()
	return err
}

func (nd *KVNode) Tick() {
	nd.rn.node.Tick()
}

func (nd *KVNode) GetCommittedIndex() uint64 {
	return atomic.LoadUint64(&nd.committedIndex)
}

func (nd *KVNode) SetCommittedIndex(ci uint64) {
	atomic.StoreUint64(&nd.committedIndex, ci)
}

func (nd *KVNode) IsRaftSynced(checkCommitIndex bool) bool {
	if nd.rn.Lead() == raft.None {
		select {
		case <-time.After(time.Duration(nd.machineConfig.ElectionTick/10) * time.Millisecond * time.Duration(nd.machineConfig.TickMs)):
		case <-nd.stopChan:
			return false
		}
		if nd.rn.Lead() == raft.None {
			nodeLog.Infof("not synced, since no leader ")
			nd.rn.maybeTryElection()
			return false
		}
	}
	if nd.IsLead() {
		// leader always raft synced.
		return true
	}
	if !checkCommitIndex {
		return true
	}
	to := time.Second * 2
	req := make([]byte, 8)
	binary.BigEndian.PutUint64(req, nd.rn.reqIDGen.Next())
	ctx, cancel := context.WithTimeout(context.Background(), to)
	if err := nd.rn.node.ReadIndex(ctx, req); err != nil {
		cancel()
		if err == raft.ErrStopped {
		}
		nodeLog.Warningf("failed to get the read index from raft: %v", err)
		return false
	}
	cancel()

	var rs raft.ReadState
	var (
		timeout bool
		done    bool
	)
	for !timeout && !done {
		select {
		case rs := <-nd.rn.readStateC:
			done = bytes.Equal(rs.RequestCtx, req)
			if !done {
			}
		case <-time.After(to):
			nodeLog.Infof("timeout waiting for read index response")
			timeout = true
		case <-nd.stopChan:
			return false
		}
	}
	if !done {
		return false
	}
	ci := nd.GetCommittedIndex()
	if rs.Index <= 0 || ci >= rs.Index-1 {
		return true
	}
	nodeLog.Infof("not synced, committed %v, read index %v", ci, rs.Index)
	return false
}

func (nd *KVNode) applySnapshot(np *nodeProgress, applyEvent *applyInfo) error {
	if raft.IsEmptySnap(applyEvent.snapshot) {
		return nil
	}
	// signaled to load snapshot
	nd.rn.Infof("applying snapshot at index %d, snapshot: %v\n", np.snapi, applyEvent.snapshot.String())
	defer nd.rn.Infof("finished applying snapshot at index %d\n", np)

	if applyEvent.snapshot.Metadata.Index <= np.appliedi {
		nodeLog.Panicf("snapshot index [%d] should > progress.appliedIndex [%d] + 1",
			applyEvent.snapshot.Metadata.Index, np.appliedi)
	}

	// the snapshot restore may fail because of the remote snapshot is deleted
	// and can not rsync from any other nodes.
	// while starting we can not ignore or delete the snapshot since the wal may be cleaned on other snapshot.
	if err := nd.RestoreFromSnapshot(false, applyEvent.snapshot); err != nil {
		nodeLog.Error(err)
		go func() {
			select {
			case <-nd.stopChan:
			default:
				nd.Stop()
			}
		}()
		<-nd.stopChan
		return err
	}

	np.confState = applyEvent.snapshot.Metadata.ConfState
	np.snapi = applyEvent.snapshot.Metadata.Index
	np.appliedt = applyEvent.snapshot.Metadata.Term
	np.appliedi = applyEvent.snapshot.Metadata.Index
	return nil
}

func (nd *KVNode) applyEntry(evnt raftpb.Entry) bool {
	forceBackup := false
	if evnt.Data != nil {
		// try redis command
		var reqList BatchInternalRaftRequest
		parseErr := reqList.Unmarshal(evnt.Data)
		if parseErr != nil {
			nd.rn.Infof("parse request failed: %v, data len %v, entry: %v, raw:%v",
				parseErr, len(evnt.Data), evnt,
				string(evnt.Data))
		}
		if len(reqList.Reqs) != int(reqList.ReqNum) {
			nd.rn.Infof("request check failed %v, real len:%v",
				reqList, len(reqList.Reqs))
		}

		forceBackup = nd.sm.ApplyRaftRequest(reqList, evnt.Term, evnt.Index)
	}
	return forceBackup
}

func (nd *KVNode) applyAll(np *nodeProgress, applyEvent *applyInfo) (bool, bool) {
	var lastCommittedIndex uint64
	if len(applyEvent.ents) > 0 {
		lastCommittedIndex = applyEvent.ents[len(applyEvent.ents)-1].Index
	}
	if applyEvent.snapshot.Metadata.Index > lastCommittedIndex {
		lastCommittedIndex = applyEvent.snapshot.Metadata.Index
	}
	if lastCommittedIndex > nd.GetCommittedIndex() {
		nd.SetCommittedIndex(lastCommittedIndex)
	}
	snapErr := nd.applySnapshot(np, applyEvent)
	if applyEvent.applySnapshotResult != nil {
		select {
		case applyEvent.applySnapshotResult <- snapErr:
		case <-nd.stopChan:
			return false, false
		}
	}
	if snapErr != nil {
		nd.rn.Errorf("apply snapshot failed: %v", snapErr.Error())
		return false, false
	}
	if len(applyEvent.ents) == 0 {
		return false, false
	}
	firsti := applyEvent.ents[0].Index
	if firsti > np.appliedi+1 {
		nodeLog.Panicf("first index of committed entry[%d] should <= appliedi[%d] + 1", firsti, np.appliedi)
	}
	var ents []raftpb.Entry
	if np.appliedi+1-firsti < uint64(len(applyEvent.ents)) {
		ents = applyEvent.ents[np.appliedi+1-firsti:]
	}
	if len(ents) == 0 {
		return false, false
	}
	var shouldStop bool
	var confChanged bool
	forceBackup := false
	for i := range ents {
		evnt := ents[i]
		switch evnt.Type {
		case raftpb.EntryNormal:
			forceBackup = nd.applyEntry(evnt)
		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			cc.Unmarshal(evnt.Data)
			removeSelf, changed, _ := nd.rn.applyConfChange(cc, &np.confState)
			confChanged = changed
			shouldStop = shouldStop || removeSelf
		}
		np.appliedi = evnt.Index
		np.appliedt = evnt.Term
		if evnt.Index == nd.rn.lastIndex {
			nd.rn.Infof("replay finished at index: %v\n", evnt.Index)
			nd.rn.MarkReplayFinished()
		}
	}
	if shouldStop {
		nd.rn.Infof("I am removed from raft group: %v", nd.ns)
		go func() {
			time.Sleep(time.Second)
			select {
			case <-nd.stopChan:
			default:
				nd.destroy()
			}
		}()
		<-nd.stopChan
		return false, false
	}
	return confChanged, forceBackup
}

func (nd *KVNode) applyCommits(commitC <-chan applyInfo) {
	defer func() {
		nd.rn.Infof("apply commit exit")
	}()
	snap, err := nd.rn.raftStorage.Snapshot()
	if err != nil {
		panic(err)
	}
	np := nodeProgress{
		confState: snap.Metadata.ConfState,
		snapi:     snap.Metadata.Index,
		appliedt:  snap.Metadata.Term,
		appliedi:  snap.Metadata.Index,
	}
	nd.rn.Infof("starting state: %v\n", np)
	for {
		select {
		case <-nd.stopChan:
			return
		case ent, ok := <-commitC:
			if !ok {
				go func() {
					select {
					case <-nd.stopChan:
					default:
						nd.Stop()
					}
				}()
				return
			}
			if ent.raftDone == nil {
				nodeLog.Panicf("wrong events : %v", ent)
			}
			confChanged, forceBackup := nd.applyAll(&np, &ent)
			select {
			case <-ent.raftDone:
			case <-nd.stopChan:
				return
			}
			nd.maybeTriggerSnapshot(&np, confChanged, forceBackup)
			nd.rn.handleSendSnapshot(&np)
			if ent.applyWaitDone != nil {
				close(ent.applyWaitDone)
			}
		}
	}
}

func (nd *KVNode) maybeTriggerSnapshot(np *nodeProgress, confChanged bool, forceBackup bool) {
	if np.appliedi-np.snapi <= 0 {
		return
	}
	if np.appliedi <= nd.rn.lastIndex {
		// replaying local log
		if forceBackup {
			nd.rn.Infof("ignore backup while replaying [applied index: %d | last replay index: %d]", np.appliedi, nd.rn.lastIndex)
		}
		return
	}
	if nd.rn.Lead() == raft.None {
		return
	}

	if !forceBackup {
		if !confChanged && np.appliedi-np.snapi <= uint64(nd.rn.config.SnapCount) {
			return
		}
	}

	nd.rn.Infof("start snapshot [applied index: %d | last snapshot index: %d]", np.appliedi, np.snapi)
	err := nd.rn.beginSnapshot(np.appliedt, np.appliedi, np.confState)
	if err != nil {
		nd.rn.Infof("begin snapshot failed: %v", err)
		return
	}

	np.snapi = np.appliedi
}

func (nd *KVNode) GetSnapshot(term uint64, index uint64) (Snapshot, error) {
	si, err := nd.sm.GetSnapshot(term, index)
	if err != nil {
		return nil, err
	}
	si.Members, si.LeaderInfo = nd.rn.GetMembersAndLeader()
	si.Learners = nd.rn.GetLearners()
	return si, nil
}

func (nd *KVNode) RestoreFromSnapshot(startup bool, raftSnapshot raftpb.Snapshot) error {
	snapshot := raftSnapshot.Data
	var si KVSnapInfo
	err := json.Unmarshal(snapshot, &si)
	if err != nil {
		return err
	}
	nd.rn.RestoreMembers(si)
	nd.rn.Infof("should recovery from snapshot here: %v", raftSnapshot.String())
	err = nd.sm.RestoreFromSnapshot(startup, raftSnapshot)
	return err
}

func (nd *KVNode) CheckLocalBackup(snapData []byte) (bool, error) {
	var rs raftpb.Snapshot
	err := rs.Unmarshal(snapData)
	if err != nil {
		return false, err
	}
	if s, ok := nd.sm.(*kvStoreSM); ok {
		return checkLocalBackup(s.store, rs)
	}
	return false, nil
}

func (nd *KVNode) GetLastLeaderChangedTime() int64 {
	return nd.rn.getLastLeaderChangedTime()
}

func (nd *KVNode) ReportMeLeaderToCluster() {
	if nd.clusterInfo == nil {
		return
	}
	if nd.rn.IsLead() {
		nid, epoch, err := nd.clusterInfo.GetNamespaceLeader(nd.ns)
		if err != nil {
			nd.rn.Infof("get raft leader from cluster failed: %v", err)
			return
		}

		if nd.rn.config.nodeConfig.NodeID == nid {
			return
		}
		_, err = nd.clusterInfo.UpdateMeForNamespaceLeader(nd.ns, epoch)
		if err != nil {
			nd.rn.Infof("update raft leader to me failed: %v", err)
		} else {
			nd.rn.Infof("update %v raft leader to me : %v", nd.ns, nd.rn.config.ID)
		}
	}
}

// should not block long in this
func (nd *KVNode) OnRaftLeaderChanged() {
	nd.expireHandler.LeaderChanged()

	if nd.rn.IsLead() {
		go nd.ReportMeLeaderToCluster()
	}
}

func (nd *KVNode) Process(ctx context.Context, m raftpb.Message) error {
	return nd.rn.Process(ctx, m)
}

func (nd *KVNode) ReportUnreachable(id uint64, group raftpb.Group) {
	nd.rn.ReportUnreachable(id, group)
}

func (nd *KVNode) ReportSnapshot(id uint64, gp raftpb.Group, status raft.SnapshotStatus) {
	nd.rn.ReportSnapshot(id, gp, status)
}

func (nd *KVNode) SaveDBFrom(r io.Reader, msg raftpb.Message) (int64, error) {
	return nd.rn.SaveDBFrom(r, msg)
}

func (nd *KVNode) IsPeerRemoved(peerID uint64) bool { return false }
