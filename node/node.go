package node

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	math "math"
	"os"
	"path"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/context"

	"github.com/youzan/ZanRedisDB/common"
	"github.com/youzan/ZanRedisDB/pkg/wait"
	"github.com/youzan/ZanRedisDB/raft"
	"github.com/youzan/ZanRedisDB/raft/raftpb"
	"github.com/youzan/ZanRedisDB/rockredis"
	"github.com/youzan/ZanRedisDB/transport/rafthttp"
)

var enableSnapTransferTest = false
var enableSnapSaveTest = false
var enableSnapApplyTest = false
var enableSnapApplyRestoreStorageTest = false

func EnableSnapForTest(transfer bool, save bool, apply bool, restore bool) {
	enableSnapTransferTest = transfer
	enableSnapSaveTest = save
	enableSnapApplyTest = apply
	enableSnapApplyRestoreStorageTest = restore
}

var (
	errInvalidResponse      = errors.New("Invalid response type")
	errSyntaxError          = errors.New("syntax error")
	errUnknownData          = errors.New("unknown request data type")
	errTooMuchBatchSize     = errors.New("the batch size exceed the limit")
	errRaftNotReadyForWrite = errors.New("ERR_CLUSTER_CHANGED: the raft is not ready for write")
	errWrongNumberArgs      = errors.New("ERR wrong number of arguments for redis command")
	ErrReadIndexTimeout     = errors.New("wait read index timeout")
)

const (
	RedisReq        int8 = 0
	CustomReq       int8 = 1
	SchemaChangeReq int8 = 2
	proposeTimeout       = time.Second * 4
	proposeQueueLen      = 800
	raftSlow             = time.Millisecond * 200
	maxPoolIDLen         = 256
	waitPoolSize         = 6
	minPoolIDLen         = 4
)

const (
	ProposeOp_Backup                 int = 1
	ProposeOp_TransferRemoteSnap     int = 2
	ProposeOp_ApplyRemoteSnap        int = 3
	ProposeOp_RemoteConfChange       int = 4
	ProposeOp_ApplySkippedRemoteSnap int = 5
	ProposeOp_DeleteTable            int = 6
)

type DeleteTableRange struct {
	Table     string `json:"table,omitempty"`
	StartFrom []byte `json:"start_from,omitempty"`
	EndTo     []byte `json:"end_to,omitempty"`
	// to avoid drop all table data, this is needed to delete all data in table
	DeleteAll bool `json:"delete_all,omitempty"`
	Dryrun    bool `json:"dryrun,omitempty"`
}

func (dtr DeleteTableRange) CheckValid() error {
	if dtr.Table == "" {
		return errors.New("delete range must have table name")
	}
	if len(dtr.StartFrom) == 0 && len(dtr.EndTo) == 0 {
		if !dtr.DeleteAll {
			return errors.New("delete all must be true if deleting whole table")
		}
	}
	return nil
}

type nodeProgress struct {
	confState raftpb.ConfState
	snapi     uint64
	appliedt  uint64
	appliedi  uint64
}

type RequestResultCode int

const (
	ReqComplete RequestResultCode = iota
	ReqCancelled
	ReqTimeouted
)

type waitReqHeaders struct {
	wr   wait.WaitResult
	done chan struct{}
	reqs BatchInternalRaftRequest
	pool *sync.Pool
}

func (wrh *waitReqHeaders) release() {
	if wrh != nil {
		wrh.wr = nil
		if wrh.pool != nil {
			wrh.reqs.Reqs = wrh.reqs.Reqs[:0]
			wrh.pool.Put(wrh)
		}
	}
}

type waitReqPoolArray []*sync.Pool

func newWaitReqPoolArray() waitReqPoolArray {
	wa := make(waitReqPoolArray, waitPoolSize)
	for i := 0; i < len(wa); i++ {
		waitReqPool := &sync.Pool{}
		l := minPoolIDLen * int(math.Pow(float64(2), float64(i)))
		_ = l
		waitReqPool.New = func() interface{} {
			obj := &waitReqHeaders{}
			obj.reqs.Reqs = make([]InternalRaftRequest, 0, 1)
			obj.done = make(chan struct{}, 1)
			obj.pool = waitReqPool
			return obj
		}
		wa[i] = waitReqPool
	}
	return wa
}

func (wa waitReqPoolArray) getWaitReq(idLen int) *waitReqHeaders {
	if idLen > maxPoolIDLen {
		obj := &waitReqHeaders{}
		obj.reqs.Reqs = make([]InternalRaftRequest, 0, idLen)
		obj.done = make(chan struct{}, 1)
		return obj
	}
	index := 0
	for i := 0; i < waitPoolSize; i++ {
		index = i
		if idLen <= minPoolIDLen*int(math.Pow(float64(2), float64(i))) {
			break
		}
	}
	return wa[index].Get().(*waitReqHeaders)
}

type customProposeData struct {
	ProposeOp   int
	NeedBackup  bool
	SyncAddr    string
	SyncPath    string
	RemoteTerm  uint64
	RemoteIndex uint64
	Data        []byte
}

// a key-value node backed by raft
type KVNode struct {
	reqProposeC        *entryQueue
	readyC             chan struct{}
	rn                 *raftNode
	store              *KVStore
	sm                 StateMachine
	stopping           int32
	stopChan           chan struct{}
	stopDone           chan struct{}
	w                  wait.Wait
	router             *common.CmdRouter
	deleteCb           func()
	clusterWriteStats  common.WriteStats
	ns                 string
	machineConfig      *MachineConfig
	wg                 sync.WaitGroup
	commitC            <-chan applyInfo
	appliedIndex       uint64
	clusterInfo        common.IClusterInfo
	expireHandler      *ExpireHandler
	expirationPolicy   common.ExpirationPolicy
	remoteSyncedStates *remoteSyncedStateMgr
	applyWait          wait.WaitTime
	// used for read index
	readMu       sync.RWMutex
	readWaitC    chan struct{}
	readNotifier *notifier
	wrPools      waitReqPoolArray
}

type KVSnapInfo struct {
	*rockredis.BackupInfo
	Ver                int                    `json:"version"`
	BackupMeta         []byte                 `json:"backup_meta"`
	LeaderInfo         *common.MemberInfo     `json:"leader_info"`
	Members            []*common.MemberInfo   `json:"members"`
	Learners           []*common.MemberInfo   `json:"learners"`
	RemoteSyncedStates map[string]SyncedState `json:"remote_synced_states"`
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

func NewKVNode(kvopts *KVOptions, config *RaftConfig,
	transport *rafthttp.Transport, join bool, deleteCb func(),
	clusterInfo common.IClusterInfo, newLeaderChan chan string) (*KVNode, error) {
	config.WALDir = path.Join(config.DataDir, fmt.Sprintf("wal-%d", config.ID))
	config.SnapDir = path.Join(config.DataDir, fmt.Sprintf("snap-%d", config.ID))

	stopChan := make(chan struct{})
	w := wait.New()
	sm, err := NewStateMachine(kvopts, *config.nodeConfig, config.ID, config.GroupName, clusterInfo, w)
	if err != nil {
		return nil, err
	}
	s := &KVNode{
		reqProposeC:        newEntryQueue(proposeQueueLen, 1),
		readyC:             make(chan struct{}, 1),
		stopChan:           stopChan,
		stopDone:           make(chan struct{}),
		store:              nil,
		sm:                 sm,
		w:                  w,
		router:             common.NewCmdRouter(),
		deleteCb:           deleteCb,
		ns:                 config.GroupName,
		machineConfig:      config.nodeConfig,
		expirationPolicy:   kvopts.ExpirationPolicy,
		remoteSyncedStates: newRemoteSyncedStateMgr(),
		applyWait:          wait.NewTimeList(),
		readWaitC:          make(chan struct{}, 1),
		readNotifier:       newNotifier(),
		wrPools:            newWaitReqPoolArray(),
	}
	if kvsm, ok := sm.(*kvStoreSM); ok {
		s.store = kvsm.store
	}

	s.clusterInfo = clusterInfo
	s.expireHandler = NewExpireHandler(s)

	s.registerHandler()

	var rs raft.IExtRaftStorage
	if config.nodeConfig.UseRocksWAL && config.rockEng != nil {
		rs = raft.NewRocksStorage(
			config.ID,
			uint32(config.GroupID),
			config.nodeConfig.SharedRocksWAL,
			config.rockEng)
	} else {
		rs = raft.NewRealMemoryStorage()
	}
	commitC, raftNode, err := newRaftNode(config, transport,
		join, s, rs, newLeaderChan)
	if err != nil {
		return nil, err
	}
	s.rn = raftNode
	s.commitC = commitC
	return s, nil
}

func (nd *KVNode) Start(standalone bool) error {
	// handle start/stop carefully
	// if the object has self start()/stop() interface, and do not use the stopChan in KVNode,
	// then we should call start() directly without put into waitgroup, and call stop() when the KVNode.Stop() is invoked.
	// Otherwise, any other goroutine should be added into waitgroup and watch stopChan to
	// get notify for stop and we will wait goroutines in waitgroup when the KVNode.Stop() is invoked.

	err := nd.sm.Start()
	if err != nil {
		return err
	}
	err = nd.rn.startRaft(nd, standalone)
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
		nd.readIndexLoop()
	}()

	nd.expireHandler.Start()
	return nil
}

func (nd *KVNode) StopRaft() {
	nd.rn.StopNode()
}

func (nd *KVNode) IsStopping() bool {
	return atomic.LoadInt32(&nd.stopping) == 1
}

func (nd *KVNode) Stop() {
	if !atomic.CompareAndSwapInt32(&nd.stopping, 0, 1) {
		return
	}
	defer close(nd.stopDone)
	close(nd.stopChan)
	nd.reqProposeC.close()
	nd.expireHandler.Stop()
	nd.wg.Wait()
	nd.rn.StopNode()
	nd.sm.Close()
	// deleted cb should be called after stopped, otherwise it
	// may init the same node after deleted while the node is stopping.
	go nd.deleteCb()
	nd.rn.Infof("node %v stopped", nd.ns)
}

func (nd *KVNode) BackupDB() {
	p := &customProposeData{
		ProposeOp:  ProposeOp_Backup,
		NeedBackup: true,
	}
	d, _ := json.Marshal(p)
	nd.CustomPropose(d)
}

func (nd *KVNode) OptimizeDB(table string) {
	if nd.IsStopping() {
		return
	}
	nd.rn.Infof("node %v begin optimize db, table %v", nd.ns, table)
	defer nd.rn.Infof("node %v end optimize db", nd.ns)
	nd.sm.Optimize(table)
	// empty table means optimize for all data, so we backup to keep optimized data
	// after restart
	if table == "" {
		// since we can not know whether leader or follower is done on optimize
		// we backup anyway after optimize
		nd.BackupDB()
	}
}

func (nd *KVNode) DeleteRange(drange DeleteTableRange) error {
	if err := drange.CheckValid(); err != nil {
		return err
	}
	d, _ := json.Marshal(drange)
	p := &customProposeData{
		ProposeOp:  ProposeOp_DeleteTable,
		NeedBackup: false,
		Data:       d,
	}
	dd, _ := json.Marshal(p)
	_, err := nd.CustomPropose(dd)
	if err != nil {
		nd.rn.Infof("node %v delete table range %v failed: %v", nd.ns, drange, err)
	}
	nd.rn.Infof("node %v delete table range %v ", nd.ns, drange)
	return err
}

func (nd *KVNode) switchForLearnerLeader(isLearnerLeader bool) {
	logsm, ok := nd.sm.(*logSyncerSM)
	if ok {
		logsm.switchIgnoreSend(isLearnerLeader)
	}
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

func (nd *KVNode) SetMaxBackgroundOptions(maxCompact int, maxBackJobs int) error {
	if nd.store != nil {
		return nd.store.SetMaxBackgroundOptions(maxCompact, maxBackJobs)
	}
	return nil
}

func (nd *KVNode) GetWALDBInternalStats() map[string]interface{} {
	if nd.rn == nil {
		return nil
	}

	eng := nd.rn.config.rockEng
	if eng == nil {
		return nil
	}
	return eng.GetInternalStatus()
}

func (nd *KVNode) GetStats(table string) common.NamespaceStats {
	ns := nd.sm.GetStats(table)
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

func (nd *KVNode) ProposeInternal(ctx context.Context, irr InternalRaftRequest, cancel context.CancelFunc, start time.Time) (*waitReqHeaders, error) {
	wrh := nd.wrPools.getWaitReq(1)
	wrh.reqs.Reqs = append(wrh.reqs.Reqs, irr)
	wrh.reqs.ReqNum = 1
	wrh.reqs.Timestamp = irr.Header.Timestamp
	if len(wrh.done) != 0 {
		wrh.done = make(chan struct{}, 1)
	}
	poolCost := time.Since(start)
	buffer, err := wrh.reqs.Marshal()
	marshalCost := time.Since(start)
	// buffer will be reused by raft?
	// TODO:buffer, err := reqList.MarshalTo()
	if err != nil {
		wrh.release()
		return nil, err
	}
	wrh.wr = nd.w.RegisterWithC(irr.Header.ID, wrh.done)
	err = nd.rn.node.ProposeWithDrop(ctx, buffer, cancel)
	if err != nil {
		nd.rn.Infof("propose failed : %v", err.Error())
		nd.w.Trigger(irr.Header.ID, err)
		wrh.release()
		return nil, err
	}
	proposalCost := time.Since(start)
	if proposalCost >= raftSlow/2 {
		nd.rn.Infof("raft slow for propose buf: %v, cost %v-%v-%v",
			len(buffer), poolCost, marshalCost, proposalCost)
	}
	return wrh, nil
}

func (nd *KVNode) SetDynamicInfo(dync NamespaceDynamicConf) {
	if nd.rn != nil && nd.rn.config != nil {
		atomic.StoreInt32(&nd.rn.config.Replicator, int32(dync.Replicator))
	}
}

func (nd *KVNode) IsWriteReady() bool {
	// to allow write while replica changed from 1 to 2, we
	// should check if the replicator is 2
	rep := atomic.LoadInt32(&nd.rn.config.Replicator)
	if rep == 2 {
		return atomic.LoadInt32(&nd.rn.memberCnt) > 0
	}
	return atomic.LoadInt32(&nd.rn.memberCnt) > int32(rep/2)
}

func (nd *KVNode) ProposeRawAndWait(buffer []byte, term uint64, index uint64, raftTs int64) error {
	var reqList BatchInternalRaftRequest
	err := reqList.Unmarshal(buffer)
	if err != nil {
		nd.rn.Infof("propose raw failed: %v at (%v-%v)", err.Error(), term, index)
		return err
	}
	if nodeLog.Level() >= common.LOG_DETAIL {
		nd.rn.Infof("propose raw (%v): %v at (%v-%v)", len(buffer), buffer, term, index)
	}
	reqList.Type = FromClusterSyncer
	reqList.ReqId = nd.rn.reqIDGen.Next()
	reqList.OrigTerm = term
	reqList.OrigIndex = index
	if reqList.Timestamp != raftTs {
		return fmt.Errorf("invalid sync raft request for mismatch timestamp: %v vs %v", reqList.Timestamp, raftTs)
	}

	for _, req := range reqList.Reqs {
		// re-generate the req id to override the id from log
		req.Header.ID = nd.rn.reqIDGen.Next()
	}
	dataLen := reqList.Size()
	if dataLen <= len(buffer) {
		n, err := reqList.MarshalTo(buffer[:dataLen])
		if err != nil {
			return err
		}
		if n != dataLen {
			return errors.New("marshal length mismatch")
		}
	} else {
		buffer, err = reqList.Marshal()
		if err != nil {
			return err
		}
	}
	start := time.Now()

	// must register before propose
	wr := nd.w.Register(reqList.ReqId)
	ctx, cancel := context.WithTimeout(context.Background(), proposeTimeout)
	if nodeLog.Level() >= common.LOG_DETAIL {
		nd.rn.Infof("propose raw after rewrite(%v): %v at (%v-%v)", dataLen, buffer[:dataLen], term, index)
	}
	defer cancel()
	err = nd.rn.node.ProposeWithDrop(ctx, buffer[:dataLen], cancel)
	if err != nil {
		nd.w.Trigger(reqList.ReqId, err)
		return err
	}
	var ok bool
	var rsp interface{}
	select {
	case <-wr.WaitC():
		rsp = wr.GetResult()
		if err, ok = rsp.(error); ok {
			rsp = nil
		} else {
			err = nil
		}
	// we can avoid wait on stop since ctx will be returned (avoid concurrency performance degrade)
	//case <-nd.stopChan:
	//	err = common.ErrStopped
	case <-ctx.Done():
		err = ctx.Err()
		if err == context.DeadlineExceeded {
			nd.rn.Infof("propose timeout: %v", err.Error())
		}
		if err == context.Canceled {
			// proposal canceled can be caused by leader transfer or no leader
			err = ErrProposalCanceled
			nd.rn.Infof("propose canceled ")
		}
		nd.w.Trigger(reqList.ReqId, err)
	}
	cost := time.Since(start).Nanoseconds()
	for _, req := range reqList.Reqs {
		if req.Header.DataType == int32(RedisReq) {
			nd.clusterWriteStats.UpdateWriteStats(int64(len(req.Data)), cost/1000)
		}
	}
	if cost >= int64(proposeTimeout.Nanoseconds())/2 {
		nd.rn.Infof("slow for batch propose: %v, cost %v", len(reqList.Reqs), cost)
	}
	return err
}

func (nd *KVNode) queueRequest(req InternalRaftRequest) (interface{}, error) {
	if !nd.IsWriteReady() {
		return nil, errRaftNotReadyForWrite
	}
	if !nd.rn.HasLead() {
		return nil, ErrNodeNoLeader
	}
	start := time.Now()
	req.Header.Timestamp = start.UnixNano()
	ctx, cancel := context.WithTimeout(context.Background(), proposeTimeout)
	wrh, err := nd.ProposeInternal(ctx, req, cancel, start)
	if err != nil {
		return nil, err
	}
	//nd.rn.Infof("queue request: %v", req.reqData.String())
	var rsp interface{}
	var ok bool
	// will always return a response, timed out or get a error
	select {
	case <-ctx.Done():
		err = ctx.Err()
		if err == context.Canceled {
			// proposal canceled can be caused by leader transfer or no leader
			err = ErrProposalCanceled
		}
		nd.w.Trigger(req.Header.ID, err)
		<-wrh.wr.WaitC()
	case <-wrh.wr.WaitC():
	}
	rsp = wrh.wr.GetResult()
	cancel()
	wrh.release()

	if err, ok = rsp.(error); ok {
		rsp = nil
		//nd.rn.Infof("request return error: %v, %v", req.String(), err.Error())
	} else {
		err = nil
	}

	if req.Header.DataType == int32(RedisReq) {
		cost := time.Since(start)
		nd.clusterWriteStats.UpdateWriteStats(int64(len(req.Data)), cost.Nanoseconds()/1000)
		if err == nil && !nd.IsWriteReady() {
			nd.rn.Infof("write request %v on raft success but raft member is less than replicator",
				req.String())
			return nil, errRaftNotReadyForWrite
		}
		if cost >= time.Second {
			nd.rn.Infof("write request %v slow cost: %v",
				req.String(), cost)
		}
	}
	return rsp, err
}

func (nd *KVNode) Propose(buf []byte) (interface{}, error) {
	h := RequestHeader{
		ID:       nd.rn.reqIDGen.Next(),
		DataType: int32(RedisReq),
	}
	raftReq := InternalRaftRequest{
		Header: h,
		Data:   buf,
	}
	return nd.queueRequest(raftReq)
}

func (nd *KVNode) CustomPropose(buf []byte) (interface{}, error) {
	h := RequestHeader{
		ID:       nd.rn.reqIDGen.Next(),
		DataType: int32(CustomReq),
	}
	raftReq := InternalRaftRequest{
		Header: h,
		Data:   buf,
	}
	return nd.queueRequest(raftReq)
}

func (nd *KVNode) ProposeChangeTableSchema(table string, sc *SchemaChange) error {
	h := RequestHeader{
		ID:       nd.rn.reqIDGen.Next(),
		DataType: int32(SchemaChangeReq),
	}
	buf, _ := sc.Marshal()
	raftReq := InternalRaftRequest{
		Header: h,
		Data:   buf,
	}

	_, err := nd.queueRequest(raftReq)
	return err
}

func (nd *KVNode) FillMyMemberInfo(m *common.MemberInfo) {
	m.RaftURLs = append(m.RaftURLs, nd.machineConfig.LocalRaftAddr)
}

func (nd *KVNode) ProposeAddLearner(m common.MemberInfo) error {
	if m.NodeID == nd.machineConfig.NodeID {
		nd.FillMyMemberInfo(&m)
	}
	if nd.rn.IsLearnerMember(m) {
		return nil
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
	if nd.rn.IsMember(m) {
		return nil
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
	succ := nd.rn.node.Tick()
	if !succ {
		nd.rn.Infof("miss tick, current commit channel: %v", len(nd.commitC))
		nd.rn.node.NotifyEventCh()
	}
}

func (nd *KVNode) GetAppliedIndex() uint64 {
	return atomic.LoadUint64(&nd.appliedIndex)
}

func (nd *KVNode) SetAppliedIndex(ci uint64) {
	atomic.StoreUint64(&nd.appliedIndex, ci)
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
	to := time.Second * 5
	ctx, cancel := context.WithTimeout(context.Background(), to)
	err := nd.linearizableReadNotify(ctx)
	cancel()

	if err != nil {
		nodeLog.Infof("wait raft not synced,  %v", err.Error())
		return false
	}
	return true
}

func (nd *KVNode) linearizableReadNotify(ctx context.Context) error {
	nd.readMu.RLock()
	n := nd.readNotifier
	nd.readMu.RUnlock()

	// signal linearizable loop for current notify if it hasn't been already
	select {
	case nd.readWaitC <- struct{}{}:
	default:
	}

	// wait for read state notification
	select {
	case <-n.c:
		return n.err
	case <-ctx.Done():
		return ctx.Err()
	case <-nd.stopChan:
		return common.ErrStopped
	}
}

func (nd *KVNode) readIndexLoop() {
	var rs raft.ReadState
	to := time.Second * 5
	for {
		req := make([]byte, 8)
		id1 := nd.rn.reqIDGen.Next()
		binary.BigEndian.PutUint64(req, id1)
		select {
		case <-nd.readWaitC:
		case <-nd.stopChan:
			return
		}
		nextN := newNotifier()
		nd.readMu.Lock()
		nr := nd.readNotifier
		nd.readNotifier = nextN
		nd.readMu.Unlock()

		ctx, cancel := context.WithTimeout(context.Background(), to)
		if err := nd.rn.node.ReadIndex(ctx, req); err != nil {
			cancel()
			nr.notify(err)
			if err == raft.ErrStopped {
				return
			}
			nodeLog.Warningf("failed to get the read index from raft: %v", err)
			continue
		}
		cancel()

		var (
			timeout bool
			done    bool
		)
		for !timeout && !done {
			select {
			case rs := <-nd.rn.readStateC:
				done = bytes.Equal(rs.RequestCtx, req)
				if !done {
					// a previous request might time out. now we should ignore the response of it and
					// continue waiting for the response of the current requests.
					id2 := uint64(0)
					if len(rs.RequestCtx) == 8 {
						id2 = binary.BigEndian.Uint64(rs.RequestCtx)
					}
					nodeLog.Infof("ignored out of date read index: %v, %v", id2, id1)
				}
			case <-time.After(to):
				nodeLog.Infof("timeout waiting for read index response: %v", id1)
				timeout = true
				nr.notify(ErrReadIndexTimeout)
			case <-nd.stopChan:
				return
			}
		}
		if !done {
			continue
		}
		ai := nd.GetAppliedIndex()
		if ai < rs.Index && rs.Index > 0 {
			select {
			case <-nd.applyWait.Wait(rs.Index):
			case <-nd.stopChan:
				return
			}
		}
		nr.notify(nil)
	}
}

func (nd *KVNode) applySnapshot(np *nodeProgress, applyEvent *applyInfo) {
	if raft.IsEmptySnap(applyEvent.snapshot) {
		return
	}
	// signaled to load snapshot
	nd.rn.Infof("applying snapshot at index %d, snapshot: %v\n", np.snapi, applyEvent.snapshot.String())
	defer nd.rn.Infof("finished applying snapshot at index %v\n", np)

	if applyEvent.snapshot.Metadata.Index <= np.appliedi {
		nodeLog.Panicf("snapshot index [%d] should > progress.appliedIndex [%d] + 1",
			applyEvent.snapshot.Metadata.Index, np.appliedi)
	}
	err := nd.PrepareSnapshot(applyEvent.snapshot)
	if enableSnapTransferTest {
		err = errors.New("auto test failed in snapshot transfer")
	}
	if applyEvent.applySnapshotResult != nil {
		select {
		case applyEvent.applySnapshotResult <- err:
		case <-nd.stopChan:
		}
	}
	if err != nil {
		nd.rn.Errorf("prepare snapshot failed: %v", err.Error())
		go func() {
			select {
			case <-nd.stopChan:
			default:
				nd.Stop()
			}
		}()
		<-nd.stopChan
		return
	}

	// need wait raft to persist the snap onto disk here
	select {
	case <-applyEvent.raftDone:
	case <-nd.stopChan:
		return
	}

	// the snapshot restore may fail because of the remote snapshot is deleted
	// and can not rsync from any other nodes.
	// while starting we can not ignore or delete the snapshot since the wal may be cleaned on other snapshot.
	if enableSnapApplyTest {
		err = errors.New("failed to restore from snapshot in failed test")
	} else {
		err = nd.RestoreFromSnapshot(applyEvent.snapshot)
	}
	if err != nil {
		nd.rn.Errorf("restore snapshot failed: %v", err.Error())
		go func() {
			select {
			case <-nd.stopChan:
			default:
				nd.Stop()
			}
		}()
		<-nd.stopChan
		return
	}

	np.confState = applyEvent.snapshot.Metadata.ConfState
	np.snapi = applyEvent.snapshot.Metadata.Index
	np.appliedt = applyEvent.snapshot.Metadata.Term
	np.appliedi = applyEvent.snapshot.Metadata.Index
}

// return (self removed, any conf changed, error)
func (nd *KVNode) applyConfChangeEntry(evnt raftpb.Entry, confState *raftpb.ConfState) (bool, bool, error) {
	var cc raftpb.ConfChange
	cc.Unmarshal(evnt.Data)
	removeSelf, changed, err := nd.rn.applyConfChange(cc, confState)
	nd.sm.ApplyRaftConfRequest(cc, evnt.Term, evnt.Index, nd.stopChan)
	return removeSelf, changed, err
}

func (nd *KVNode) applyEntry(evnt raftpb.Entry, isReplaying bool, batch IBatchOperator) bool {
	forceBackup := false
	if evnt.Data != nil {
		// try redis command
		var reqList BatchInternalRaftRequest
		parseErr := reqList.Unmarshal(evnt.Data)
		if parseErr != nil {
			nd.rn.Infof("parse request failed: %v, data len %v, entry: %v, raw:%v",
				parseErr, len(evnt.Data), evnt,
				evnt.String())
		}
		if len(reqList.Reqs) != int(reqList.ReqNum) {
			nd.rn.Infof("request check failed %v, real len:%v",
				reqList, len(reqList.Reqs))
		}

		isRemoteSnapTransfer := false
		isRemoteSnapApply := false
		if reqList.Type == FromClusterSyncer {
			isApplied := nd.isAlreadyApplied(reqList)
			// check if retrying duplicate req, we can just ignore old retry
			if isApplied {
				nd.rn.Infof("request %v-%v ignored since older than synced", reqList.OrigTerm, reqList.OrigIndex)
				for _, req := range reqList.Reqs {
					if req.Header.ID > 0 && nd.w.IsRegistered(req.Header.ID) {
						nd.w.Trigger(req.Header.ID, nil)
					}
				}
				// used for grpc raft proposal, will notify that all the raft logs in this batch is done.
				if reqList.ReqId > 0 {
					nd.w.Trigger(reqList.ReqId, nil)
				}
				return false
			}
			isRemoteSnapTransfer, isRemoteSnapApply = nd.preprocessRemoteSnapApply(reqList)
		}
		var retErr error
		forceBackup, retErr = nd.sm.ApplyRaftRequest(isReplaying, batch, reqList, evnt.Term, evnt.Index, nd.stopChan)
		if reqList.Type == FromClusterSyncer {
			nd.postprocessRemoteSnapApply(reqList, isRemoteSnapTransfer, isRemoteSnapApply, retErr)
		}
	}
	return forceBackup
}

func (nd *KVNode) applyEntries(np *nodeProgress, applyEvent *applyInfo) (bool, bool) {
	if len(applyEvent.ents) == 0 {
		return false, false
	}
	firsti := applyEvent.ents[0].Index
	if firsti > np.appliedi+1 {
		nodeLog.Errorf("first index of committed entry[%d] should <= appliedi[%d] + 1", firsti, np.appliedi)
		go func() {
			select {
			case <-nd.stopChan:
			default:
				nd.Stop()
			}
		}()
		return false, false
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
	batch := nd.sm.GetBatchOperator()
	for i := range ents {
		evnt := ents[i]
		isReplaying := evnt.Index <= nd.rn.lastIndex
		switch evnt.Type {
		case raftpb.EntryNormal:
			needBackup := nd.applyEntry(evnt, isReplaying, batch)
			if needBackup {
				forceBackup = true
			}
		case raftpb.EntryConfChange:
			if batch != nil {
				batch.CommitBatch()
			}
			removeSelf, changed, _ := nd.applyConfChangeEntry(evnt, &np.confState)
			if changed {
				confChanged = changed
			}
			shouldStop = shouldStop || removeSelf
		}
		np.appliedi = evnt.Index
		np.appliedt = evnt.Term
		if evnt.Index == nd.rn.lastIndex {
			nd.rn.Infof("replay finished at index: %v\n", evnt.Index)
			nd.rn.MarkReplayFinished()
		}
	}
	if batch != nil {
		batch.CommitBatch()
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

func (nd *KVNode) applyAll(np *nodeProgress, applyEvent *applyInfo) (bool, bool) {
	nd.applySnapshot(np, applyEvent)
	start := time.Now()
	confChanged, forceBackup := nd.applyEntries(np, applyEvent)
	cost := time.Since(start)
	if cost > raftSlow {
		nd.rn.Infof("raft apply slow cost: %v, number %v", cost, len(applyEvent.ents))
	}

	lastIndex := np.appliedi
	if applyEvent.snapshot.Metadata.Index > lastIndex {
		lastIndex = applyEvent.snapshot.Metadata.Index
	}
	if lastIndex > nd.GetAppliedIndex() {
		nd.SetAppliedIndex(lastIndex)
	}
	nd.applyWait.Trigger(lastIndex)
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
	// init applied index
	lastIndex := np.appliedi
	if lastIndex > nd.GetAppliedIndex() {
		nd.SetAppliedIndex(lastIndex)
	}
	nd.applyWait.Trigger(lastIndex)

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

			// wait for the raft routine to finish the disk writes before triggering a
			// snapshot. or applied index might be greater than the last index in raft
			// storage, since the raft routine might be slower than apply routine.
			select {
			case <-ent.raftDone:
			case <-nd.stopChan:
				return
			}
			if ent.applyWaitDone != nil {
				close(ent.applyWaitDone)
			}
			if len(commitC) == 0 {
				nd.rn.node.NotifyEventCh()
			}
			nd.maybeTriggerSnapshot(&np, confChanged, forceBackup)
			nd.rn.handleSendSnapshot(&np)
		}
	}
}

func (nd *KVNode) maybeTriggerSnapshot(np *nodeProgress, confChanged bool, forceBackup bool) {
	if np.appliedi-np.snapi <= 0 {
		return
	}
	// we need force backup if too much logs since last snapshot
	behandToomuch := np.appliedi-np.snapi > uint64(nd.rn.config.SnapCount*5)
	if np.appliedi <= nd.rn.lastIndex && !behandToomuch {
		// replaying local log
		if forceBackup {
			nd.rn.Infof("ignore backup while replaying [applied index: %d | last replay index: %d]", np.appliedi, nd.rn.lastIndex)
		}
		return
	}
	if nd.rn.Lead() == raft.None && !behandToomuch {
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
	si.RemoteSyncedStates = nd.remoteSyncedStates.Clone()
	si.Learners = nd.rn.GetLearners()
	return si, nil
}

func (nd *KVNode) PrepareSnapshot(raftSnapshot raftpb.Snapshot) error {
	snapshot := raftSnapshot.Data
	var si KVSnapInfo
	err := json.Unmarshal(snapshot, &si)
	if err != nil {
		return err
	}
	nd.rn.RestoreMembers(si)
	nd.rn.Infof("prepare snapshot here: %v", raftSnapshot.String())
	err = nd.sm.PrepareSnapshot(raftSnapshot, nd.stopChan)
	return err
}

func (nd *KVNode) RestoreFromSnapshot(raftSnapshot raftpb.Snapshot) error {
	nd.rn.Infof("should recovery from snapshot here: %v", raftSnapshot.String())
	err := nd.sm.RestoreFromSnapshot(raftSnapshot, nd.stopChan)
	if err != nil {
		return err
	}
	snapshot := raftSnapshot.Data
	var si KVSnapInfo
	err = json.Unmarshal(snapshot, &si)
	if err != nil {
		return err
	}
	nd.remoteSyncedStates.RestoreStates(si.RemoteSyncedStates)
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
		changed, err := nd.clusterInfo.UpdateMeForNamespaceLeader(nd.ns)
		if err != nil {
			nd.rn.Infof("update raft leader to me failed: %v", err)
		} else if changed {
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
	// avoid prepare snapshot while the node is starting
	if m.Type == raftpb.MsgSnap && !raft.IsEmptySnap(m.Snapshot) {
		// we prepare the snapshot data here before we send install snapshot message to raft
		// to avoid block raft loop while transfer the snapshot data
		nd.rn.SetPrepareSnapshot(true)
		defer nd.rn.SetPrepareSnapshot(false)
		nd.rn.Infof("prepare transfer snapshot : %v\n", m.Snapshot.String())
		defer nd.rn.Infof("transfer snapshot done : %v\n", m.Snapshot.String())
		if enableSnapTransferTest {
			go nd.Stop()
			return errors.New("auto test failed in snapshot transfer")
		}
		err := nd.sm.PrepareSnapshot(m.Snapshot, nd.stopChan)
		if err != nil {
			// we ignore here to allow retry in the raft loop
			nd.rn.Infof("transfer snapshot failed: %v, %v", m.Snapshot.String(), err.Error())
		}
	}
	return nd.rn.Process(ctx, m)
}

func (nd *KVNode) UpdateSnapshotState(term uint64, index uint64) {
	if nd.sm != nil {
		nd.sm.UpdateSnapshotState(term, index)
	}
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
