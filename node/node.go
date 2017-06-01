package node

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/context"

	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/absolute8511/ZanRedisDB/raft"
	"github.com/absolute8511/ZanRedisDB/raft/raftpb"
	"github.com/absolute8511/ZanRedisDB/rockredis"
	"github.com/absolute8511/ZanRedisDB/transport/rafthttp"
	"github.com/coreos/etcd/pkg/wait"
	"github.com/tidwall/redcon"
)

var (
	errInvalidResponse      = errors.New("Invalid response type")
	errSyntaxError          = errors.New("syntax error")
	errUnknownData          = errors.New("unknown request data type")
	errTooMuchBatchSize     = errors.New("the batch size exceed the limit")
	errRaftNotReadyForWrite = errors.New("the raft is not ready for write")
)

const (
	RedisReq       int8 = 0
	HTTPReq        int8 = 1
	proposeTimeout      = time.Second * 10
)

type nodeProgress struct {
	confState raftpb.ConfState
	snapi     uint64
	appliedi  uint64
}

type internalReq struct {
	reqData InternalRaftRequest
	done    chan struct{}
}

// a key-value node backed by raft
type KVNode struct {
	reqProposeC       chan *internalReq
	rn                *raftNode
	store             *KVStore
	stopping          int32
	stopChan          chan struct{}
	w                 wait.Wait
	router            *common.CmdRouter
	deleteCb          func()
	dbWriteStats      common.WriteStats
	clusterWriteStats common.WriteStats
	ns                string
	machineConfig     *MachineConfig
	wg                sync.WaitGroup
	commitC           <-chan applyInfo
	committedIndex    uint64
	clusterInfo       common.IClusterInfo
}

type KVSnapInfo struct {
	*rockredis.BackupInfo
	BackupMeta []byte               `json:"backup_meta"`
	LeaderInfo *common.MemberInfo   `json:"leader_info"`
	Members    []*common.MemberInfo `json:"members"`
}

func (self *KVSnapInfo) GetData() ([]byte, error) {
	meta, err := self.BackupInfo.GetResult()
	if err != nil {
		return nil, err
	}
	self.BackupMeta = meta
	d, _ := json.Marshal(self)
	return d, nil
}

func NewKVNode(kvopts *KVOptions, machineConfig *MachineConfig, config *RaftConfig,
	transport *rafthttp.Transport, join bool, deleteCb func(),
	clusterInfo common.IClusterInfo, newLeaderChan chan string) (*KVNode, error) {
	config.WALDir = path.Join(config.DataDir, fmt.Sprintf("wal-%d", config.ID))
	config.SnapDir = path.Join(config.DataDir, fmt.Sprintf("snap-%d", config.ID))
	config.nodeConfig = machineConfig

	store, err := NewKVStore(kvopts)
	if err != nil {
		return nil, err
	}
	s := &KVNode{
		reqProposeC:   make(chan *internalReq, 200),
		stopChan:      make(chan struct{}),
		store:         store,
		w:             wait.New(),
		router:        common.NewCmdRouter(),
		deleteCb:      deleteCb,
		ns:            config.GroupName,
		machineConfig: machineConfig,
	}
	s.clusterInfo = clusterInfo

	s.registerHandler()
	s.registerExpiredCallBack()

	commitC, raftNode, err := newRaftNode(config, transport,
		join, s, newLeaderChan)
	if err != nil {
		return nil, err
	}
	s.rn = raftNode
	s.commitC = commitC
	return s, nil
}

func (self *KVNode) Start(standalone bool) error {
	err := self.rn.startRaft(self, standalone)
	if err != nil {
		return err
	}
	// read commits from raft into KVStore map until error
	self.wg.Add(1)
	go func() {
		defer self.wg.Done()
		self.applyCommits(self.commitC)
	}()
	self.wg.Add(1)
	go func() {
		defer self.wg.Done()
		self.handleProposeReq()
	}()
	return nil
}

func (self *KVNode) StopRaft() {
	self.rn.StopNode()
}

func (self *KVNode) Stop() {
	if !atomic.CompareAndSwapInt32(&self.stopping, 0, 1) {
		return
	}
	close(self.stopChan)
	go self.deleteCb()
	self.wg.Wait()
	self.rn.StopNode()
	self.store.Close()
	self.rn.Infof("node %v stopped", self.ns)
}

func (self *KVNode) OptimizeDB() {
	self.store.CompactRange()
}

func (self *KVNode) IsLead() bool {
	return self.rn.IsLead()
}

func (self *KVNode) GetRaftStatus() raft.Status {
	return self.rn.node.Status()
}

func (self *KVNode) GetLeadMember() *common.MemberInfo {
	return self.rn.GetLeadMember()
}

func (self *KVNode) GetMembers() []*common.MemberInfo {
	return self.rn.GetMembers()
}

func (self *KVNode) GetLocalMemberInfo() *common.MemberInfo {
	if self.rn == nil {
		return nil
	}
	var m common.MemberInfo
	m.ID = uint64(self.rn.config.ID)
	m.NodeID = self.rn.config.nodeConfig.NodeID
	m.GroupID = self.rn.config.GroupID
	m.GroupName = self.rn.config.GroupName
	m.RaftURLs = append(m.RaftURLs, self.rn.config.RaftAddr)
	return &m
}

func (self *KVNode) GetDBInternalStats() string {
	return self.store.GetStatistics()
}

func (self *KVNode) GetStats() common.NamespaceStats {
	tbs := self.store.GetTables()
	var ns common.NamespaceStats
	ns.DBWriteStats = self.dbWriteStats.Copy()
	ns.ClusterWriteStats = self.clusterWriteStats.Copy()
	ns.InternalStats = self.store.GetInternalStatus()

	for t := range tbs {
		cnt, err := self.store.GetTableKeyCount(t)
		if err != nil {
			continue
		}
		var ts common.TableStats
		ts.Name = string(t)
		ts.KeyNum = cnt
		ns.TStats = append(ns.TStats, ts)
	}
	//self.rn.Infof(self.store.GetStatistics())
	return ns
}

func (self *KVNode) destroy() error {
	self.Stop()
	self.store.Destroy()
	ts := strconv.Itoa(int(time.Now().UnixNano()))
	return os.Rename(self.rn.config.DataDir,
		self.rn.config.DataDir+"-deleted-"+ts)
}

func (self *KVNode) CleanData() error {
	return self.store.CleanData()
}

func (self *KVNode) GetHandler(cmd string) (common.CommandFunc, bool, bool) {
	return self.router.GetCmdHandler(cmd)
}

func (self *KVNode) GetMergeHandler(cmd string) (common.MergeCommandFunc, bool) {
	return self.router.GetMergeCmdHandler(cmd)
}

func (self *KVNode) registerExpiredCallBack() {
	ttlChecker := self.store.GetTTLChecker()

	ttlChecker.RegisterKVExpired(self.createOnExpiredFunc("del"))
	ttlChecker.RegisterListExpired(self.createOnExpiredFunc("lclear"))
	ttlChecker.RegisterHashExpired(self.createOnExpiredFunc("hclear"))
	ttlChecker.RegisterSetExpired(self.createOnExpiredFunc("sclear"))
	ttlChecker.RegisterZSetExpired(self.createOnExpiredFunc("zclear"))
}

func (self *KVNode) registerHandler() {
	// for kv
	self.router.Register(false, "get", wrapReadCommandK(self.getCommand))
	self.router.Register(false, "mget", wrapReadCommandKK(self.mgetCommand))
	self.router.Register(false, "exists", wrapReadCommandK(self.existsCommand))
	self.router.Register(true, "set", wrapWriteCommandKV(self, self.setCommand))
	self.router.Register(true, "setnx", wrapWriteCommandKV(self, self.setnxCommand))
	self.router.Register(true, "mset", wrapWriteCommandKVKV(self, self.msetCommand))
	self.router.Register(true, "incr", wrapWriteCommandK(self, self.incrCommand))
	self.router.Register(true, "del", wrapWriteCommandKK(self, self.delCommand))
	self.router.Register(false, "plget", self.plgetCommand)
	self.router.Register(true, "plset", self.plsetCommand)
	// for hash
	self.router.Register(false, "hget", wrapReadCommandKSubkey(self.hgetCommand))
	self.router.Register(false, "hgetall", wrapReadCommandK(self.hgetallCommand))
	self.router.Register(false, "hkeys", wrapReadCommandK(self.hkeysCommand))
	self.router.Register(false, "hexists", wrapReadCommandKSubkey(self.hexistsCommand))
	self.router.Register(false, "hmget", wrapReadCommandKSubkeySubkey(self.hmgetCommand))
	self.router.Register(false, "hlen", wrapReadCommandK(self.hlenCommand))
	self.router.Register(true, "hset", wrapWriteCommandKSubkeyV(self, self.hsetCommand))
	self.router.Register(true, "hmset", wrapWriteCommandKSubkeyVSubkeyV(self, self.hmsetCommand))
	self.router.Register(true, "hdel", wrapWriteCommandKSubkeySubkey(self, self.hdelCommand))
	self.router.Register(true, "hincrby", wrapWriteCommandKSubkeyV(self, self.hincrbyCommand))
	self.router.Register(true, "hclear", wrapWriteCommandK(self, self.hclearCommand))
	// for list
	self.router.Register(false, "lindex", wrapReadCommandKSubkey(self.lindexCommand))
	self.router.Register(false, "llen", wrapReadCommandK(self.llenCommand))
	self.router.Register(false, "lrange", wrapReadCommandKAnySubkey(self.lrangeCommand))
	self.router.Register(true, "lpop", wrapWriteCommandK(self, self.lpopCommand))
	self.router.Register(true, "lpush", wrapWriteCommandKVV(self, self.lpushCommand))
	self.router.Register(true, "lset", self.lsetCommand)
	self.router.Register(true, "ltrim", self.ltrimCommand)
	self.router.Register(true, "rpop", wrapWriteCommandK(self, self.rpopCommand))
	self.router.Register(true, "rpush", wrapWriteCommandKVV(self, self.rpushCommand))
	self.router.Register(true, "lclear", wrapWriteCommandK(self, self.lclearCommand))
	// for zset
	self.router.Register(false, "zscore", wrapReadCommandKSubkey(self.zscoreCommand))
	self.router.Register(false, "zcount", wrapReadCommandKAnySubkey(self.zcountCommand))
	self.router.Register(false, "zcard", wrapReadCommandK(self.zcardCommand))
	self.router.Register(false, "zlexcount", wrapReadCommandKAnySubkey(self.zlexcountCommand))
	self.router.Register(false, "zrange", wrapReadCommandKAnySubkey(self.zrangeCommand))
	self.router.Register(false, "zrevrange", wrapReadCommandKAnySubkey(self.zrevrangeCommand))
	self.router.Register(false, "zrangebylex", wrapReadCommandKAnySubkey(self.zrangebylexCommand))
	self.router.Register(false, "zrangebyscore", wrapReadCommandKAnySubkey(self.zrangebyscoreCommand))
	self.router.Register(false, "zrevrangebyscore", wrapReadCommandKAnySubkey(self.zrevrangebyscoreCommand))
	self.router.Register(false, "zrank", wrapReadCommandKSubkey(self.zrankCommand))
	self.router.Register(false, "zrevrank", wrapReadCommandKSubkey(self.zrevrankCommand))
	self.router.Register(true, "zadd", self.zaddCommand)
	self.router.Register(true, "zincrby", self.zincrbyCommand)
	self.router.Register(true, "zrem", wrapWriteCommandKSubkeySubkey(self, self.zremCommand))
	self.router.Register(true, "zremrangebyrank", self.zremrangebyrankCommand)
	self.router.Register(true, "zremrangebyscore", self.zremrangebyscoreCommand)
	self.router.Register(true, "zremrangebylex", self.zremrangebylexCommand)
	self.router.Register(true, "zclear", wrapWriteCommandK(self, self.zclearCommand))
	// for set
	self.router.Register(false, "scard", wrapReadCommandK(self.scardCommand))
	self.router.Register(false, "sismember", wrapReadCommandKSubkey(self.sismemberCommand))
	self.router.Register(false, "smembers", wrapReadCommandK(self.smembersCommand))
	self.router.Register(true, "sadd", wrapWriteCommandKSubkeySubkey(self, self.saddCommand))
	self.router.Register(true, "srem", wrapWriteCommandKSubkeySubkey(self, self.sremCommand))
	self.router.Register(true, "sclear", wrapWriteCommandK(self, self.sclearCommand))
	self.router.Register(true, "smclear", wrapWriteCommandKK(self, self.smclearCommand))
	// for ttl
	self.router.Register(false, "ttl", wrapReadCommandK(self.ttlCommand))
	self.router.Register(false, "httl", wrapReadCommandK(self.httlCommand))
	self.router.Register(false, "lttl", wrapReadCommandK(self.lttlCommand))
	self.router.Register(false, "sttl", wrapReadCommandK(self.sttlCommand))
	self.router.Register(false, "zttl", wrapReadCommandK(self.zttlCommand))

	self.router.Register(true, "setex", wrapWriteCommandKVV(self, self.setexCommand))
	self.router.Register(true, "expire", wrapWriteCommandKV(self, self.expireCommand))
	self.router.Register(true, "hexpire", wrapWriteCommandKV(self, self.hashExpireCommand))
	self.router.Register(true, "lexpire", wrapWriteCommandKV(self, self.listExpireCommand))
	self.router.Register(true, "sexpire", wrapWriteCommandKV(self, self.setExpireCommand))
	self.router.Register(true, "zexpire", wrapWriteCommandKV(self, self.zsetExpireCommand))

	self.router.Register(true, "persist", wrapWriteCommandK(self, self.persistCommand))
	self.router.Register(true, "hpersist", wrapWriteCommandK(self, self.persistCommand))
	self.router.Register(true, "lpersist", wrapWriteCommandK(self, self.persistCommand))
	self.router.Register(true, "spersist", wrapWriteCommandK(self, self.persistCommand))
	self.router.Register(true, "zpersist", wrapWriteCommandK(self, self.persistCommand))

	// for scan
	self.router.Register(false, "hscan", wrapReadCommandKAnySubkey(self.hscanCommand))
	self.router.Register(false, "sscan", wrapReadCommandKAnySubkey(self.sscanCommand))
	self.router.Register(false, "zscan", wrapReadCommandKAnySubkey(self.zscanCommand))

	//for cross mutil partion
	self.router.RegisterMerge("scan", wrapMergeCommand(self.scanCommand))
	self.router.RegisterMerge("advscan", self.advanceScanCommand)

	// only write command need to be registered as internal
	// kv
	self.router.RegisterInternal("del", self.localDelCommand)
	self.router.RegisterInternal("set", self.localSetCommand)
	self.router.RegisterInternal("setnx", self.localSetnxCommand)
	self.router.RegisterInternal("mset", self.localMSetCommand)
	self.router.RegisterInternal("incr", self.localIncrCommand)
	self.router.RegisterInternal("plset", self.localPlsetCommand)
	// hash
	self.router.RegisterInternal("hset", self.localHSetCommand)
	self.router.RegisterInternal("hmset", self.localHMsetCommand)
	self.router.RegisterInternal("hdel", self.localHDelCommand)
	self.router.RegisterInternal("hincrby", self.localHIncrbyCommand)
	self.router.RegisterInternal("hclear", self.localHclearCommand)
	// list
	self.router.RegisterInternal("lpop", self.localLpopCommand)
	self.router.RegisterInternal("lpush", self.localLpushCommand)
	self.router.RegisterInternal("lset", self.localLsetCommand)
	self.router.RegisterInternal("ltrim", self.localLtrimCommand)
	self.router.RegisterInternal("rpop", self.localRpopCommand)
	self.router.RegisterInternal("rpush", self.localRpushCommand)
	self.router.RegisterInternal("lclear", self.localLclearCommand)
	// zset
	self.router.RegisterInternal("zadd", self.localZaddCommand)
	self.router.RegisterInternal("zincrby", self.localZincrbyCommand)
	self.router.RegisterInternal("zrem", self.localZremCommand)
	self.router.RegisterInternal("zremrangebyrank", self.localZremrangebyrankCommand)
	self.router.RegisterInternal("zremrangebyscore", self.localZremrangebyscoreCommand)
	self.router.RegisterInternal("zremrangebylex", self.localZremrangebylexCommand)
	self.router.RegisterInternal("zclear", self.localZclearCommand)
	// set
	self.router.RegisterInternal("sadd", self.localSadd)
	self.router.RegisterInternal("srem", self.localSrem)
	self.router.RegisterInternal("sclear", self.localSclear)
	self.router.RegisterInternal("smclear", self.localSmclear)
	// expire
	self.router.RegisterInternal("setex", self.localSetexCommand)
	self.router.RegisterInternal("expire", self.localExpireCommand)
	self.router.RegisterInternal("lexpire", self.localListExpireCommand)
	self.router.RegisterInternal("hexpire", self.localHashExpireCommand)
	self.router.RegisterInternal("sexpire", self.localSetExpireCommand)
	self.router.RegisterInternal("zexpire", self.localZSetExpireCommand)
	self.router.RegisterInternal("persist", self.localPersistCommand)
	self.router.RegisterInternal("hpersist", self.localHashPersistCommand)
	self.router.RegisterInternal("lpersist", self.localListPersistCommand)
	self.router.RegisterInternal("spersist", self.localSetPersistCommand)
	self.router.RegisterInternal("zpersist", self.localZSetPersistCommand)
}

func (self *KVNode) handleProposeReq() {
	var reqList BatchInternalRaftRequest
	reqList.Reqs = make([]*InternalRaftRequest, 0, 100)
	var lastReq *internalReq
	defer func() {
		if e := recover(); e != nil {
			buf := make([]byte, 4096)
			n := runtime.Stack(buf, false)
			buf = buf[0:n]
			self.rn.Errorf("handle propose loop panic: %s:%v", buf, e)
		}
		for _, r := range reqList.Reqs {
			self.w.Trigger(r.Header.ID, common.ErrStopped)
		}
		self.rn.Infof("handle propose loop exit")
		for {
			select {
			case r := <-self.reqProposeC:
				self.w.Trigger(r.reqData.Header.ID, common.ErrStopped)
			default:
				return
			}
		}
	}()
	for {
		select {
		case r := <-self.reqProposeC:
			reqList.Reqs = append(reqList.Reqs, &r.reqData)
			lastReq = r
		default:
			if len(reqList.Reqs) == 0 {
				select {
				case r := <-self.reqProposeC:
					reqList.Reqs = append(reqList.Reqs, &r.reqData)
					lastReq = r
				case <-self.stopChan:
					return
				}
			}
			reqList.ReqNum = int32(len(reqList.Reqs))
			buffer, err := reqList.Marshal()
			if err != nil {
				self.rn.Infof("failed to marshal request: %v", err)
				for _, r := range reqList.Reqs {
					self.w.Trigger(r.Header.ID, err)
				}
				reqList.Reqs = reqList.Reqs[:0]
				continue
			}
			lastReq.done = make(chan struct{})
			//self.rn.Infof("handle req %v, marshal buffer: %v, raw: %v, %v", len(reqList.Reqs),
			//	realN, buffer, reqList.Reqs)
			start := lastReq.reqData.Header.Timestamp
			ctx, cancel := context.WithTimeout(context.Background(), proposeTimeout)
			self.rn.node.Propose(ctx, buffer)
			select {
			case <-lastReq.done:
			case <-ctx.Done():
				err := ctx.Err()
				for _, r := range reqList.Reqs {
					self.w.Trigger(r.Header.ID, err)
				}
			case <-self.stopChan:
				cancel()
				return
			}
			cancel()
			cost := (time.Now().UnixNano() - start) / 1000 / 1000 / 1000
			if cost >= int64(proposeTimeout.Seconds())/2 {
				self.rn.Infof("slow for batch: %v, %v", len(reqList.Reqs), cost)
			}
			reqList.Reqs = reqList.Reqs[:0]
			lastReq = nil
		}
	}
}

func (self *KVNode) IsWriteReady() bool {
	return atomic.LoadInt32(&self.rn.memberCnt) > int32(self.rn.config.Replicator/2)
}

func (self *KVNode) queueRequest(req *internalReq) (interface{}, error) {
	if !self.IsWriteReady() {
		return nil, errRaftNotReadyForWrite
	}
	start := time.Now()
	req.reqData.Header.Timestamp = start.UnixNano()
	ch := self.w.Register(req.reqData.Header.ID)
	select {
	case self.reqProposeC <- req:
	default:
		select {
		case self.reqProposeC <- req:
		case <-self.stopChan:
			self.w.Trigger(req.reqData.Header.ID, common.ErrStopped)
		case <-time.After(proposeTimeout):
			self.w.Trigger(req.reqData.Header.ID, common.ErrTimeout)
		}
	}
	//self.rn.Infof("queue request: %v", req.reqData.String())
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
	case <-self.stopChan:
		rsp = nil
		err = common.ErrStopped
	}
	self.clusterWriteStats.UpdateWriteStats(int64(len(req.reqData.Data)), time.Since(start).Nanoseconds()/1000)
	if err == nil && !self.IsWriteReady() {
		self.rn.Infof("write request %v on raft success but raft member is less than replicator",
			req.reqData.String())
		return nil, errRaftNotReadyForWrite
	}
	return rsp, err
}

func (self *KVNode) Propose(buf []byte) (interface{}, error) {
	h := &RequestHeader{
		ID:       self.rn.reqIDGen.Next(),
		DataType: 0,
	}
	raftReq := InternalRaftRequest{
		Header: h,
		Data:   buf,
	}
	req := &internalReq{
		reqData: raftReq,
	}
	return self.queueRequest(req)
}

func (self *KVNode) HTTPPropose(buf []byte) (interface{}, error) {
	h := &RequestHeader{
		ID:       self.rn.reqIDGen.Next(),
		DataType: int32(HTTPReq),
	}
	raftReq := InternalRaftRequest{
		Header: h,
		Data:   buf,
	}
	req := &internalReq{
		reqData: raftReq,
	}
	return self.queueRequest(req)
}

func (self *KVNode) FillMyMemberInfo(m *common.MemberInfo) {
	m.RaftURLs = append(m.RaftURLs, self.machineConfig.LocalRaftAddr)
}

func (self *KVNode) ProposeAddMember(m common.MemberInfo) error {
	if m.NodeID == self.machineConfig.NodeID {
		self.FillMyMemberInfo(&m)
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
	return self.proposeConfChange(cc)
}

func (self *KVNode) ProposeRemoveMember(m common.MemberInfo) error {
	cc := raftpb.ConfChange{
		Type:      raftpb.ConfChangeRemoveNode,
		ReplicaID: m.ID,
		NodeGroup: raftpb.Group{
			NodeId:        m.NodeID,
			Name:          m.GroupName,
			GroupId:       uint64(m.GroupID),
			RaftReplicaId: m.ID},
	}
	return self.proposeConfChange(cc)
}

func (self *KVNode) ProposeUpdateMember(m common.MemberInfo) error {
	if m.NodeID == self.machineConfig.NodeID {
		self.FillMyMemberInfo(&m)
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
	return self.proposeConfChange(cc)
}

func (self *KVNode) proposeConfChange(cc raftpb.ConfChange) error {
	cc.ID = self.rn.reqIDGen.Next()
	self.rn.Infof("propose the conf change: %v", cc.String())
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
	err := self.rn.node.ProposeConfChange(ctx, cc)
	if err != nil {
		self.rn.Infof("failed to propose the conf change: %v", err)
	}
	cancel()
	return err
}

func (self *KVNode) Tick() {
	self.rn.node.Tick()
}

func (self *KVNode) GetCommittedIndex() uint64 {
	return atomic.LoadUint64(&self.committedIndex)
}

func (self *KVNode) SetCommittedIndex(ci uint64) {
	atomic.StoreUint64(&self.committedIndex, ci)
}

func (self *KVNode) IsRaftSynced(checkCommitIndex bool) bool {
	if self.rn.Lead() == raft.None {
		select {
		case <-time.After(time.Duration(self.machineConfig.ElectionTick/10) * time.Millisecond * time.Duration(self.machineConfig.TickMs)):
		case <-self.stopChan:
			return false
		}
		if self.rn.Lead() == raft.None {
			nodeLog.Infof("not synced, since no leader ")
			self.rn.maybeTryElection()
			return false
		}
	}
	if !checkCommitIndex {
		return true
	}
	to := time.Second * 2
	req := make([]byte, 8)
	binary.BigEndian.PutUint64(req, self.rn.reqIDGen.Next())
	ctx, cancel := context.WithTimeout(context.Background(), to)
	if err := self.rn.node.ReadIndex(ctx, req); err != nil {
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
		case rs := <-self.rn.readStateC:
			done = bytes.Equal(rs.RequestCtx, req)
			if !done {
			}
		case <-time.After(to):
			nodeLog.Infof("timeout waiting for read index response")
			timeout = true
		case <-self.stopChan:
			return false
		}
	}
	if !done {
		return false
	}
	ci := self.GetCommittedIndex()
	if rs.Index <= 0 || ci >= rs.Index-1 {
		return true
	}
	nodeLog.Infof("not synced, committed %v, read index %v", ci, rs.Index)
	return false
}

func (self *KVNode) applySnapshot(np *nodeProgress, applyEvent *applyInfo) {
	if raft.IsEmptySnap(applyEvent.snapshot) {
		return
	}
	// signaled to load snapshot
	self.rn.Infof("applying snapshot at index %d, snapshot: %v\n", np.snapi, applyEvent.snapshot.String())
	defer self.rn.Infof("finished applying snapshot at index %d\n", np)

	if applyEvent.snapshot.Metadata.Index <= np.appliedi {
		nodeLog.Panicf("snapshot index [%d] should > progress.appliedIndex [%d] + 1",
			applyEvent.snapshot.Metadata.Index, np.appliedi)
	}

	if err := self.RestoreFromSnapshot(false, applyEvent.snapshot); err != nil {
		nodeLog.Error(err)
		go func() {
			select {
			case <-self.stopChan:
			default:
				self.Stop()
			}
		}()
	}

	np.confState = applyEvent.snapshot.Metadata.ConfState
	np.snapi = applyEvent.snapshot.Metadata.Index
	np.appliedi = applyEvent.snapshot.Metadata.Index
}

func (self *KVNode) applyAll(np *nodeProgress, applyEvent *applyInfo) bool {
	var lastCommittedIndex uint64
	if len(applyEvent.ents) > 0 {
		lastCommittedIndex = applyEvent.ents[len(applyEvent.ents)-1].Index
	}
	if applyEvent.snapshot.Metadata.Index > lastCommittedIndex {
		lastCommittedIndex = applyEvent.snapshot.Metadata.Index
	}
	if lastCommittedIndex > self.GetCommittedIndex() {
		self.SetCommittedIndex(lastCommittedIndex)
	}
	self.applySnapshot(np, applyEvent)
	if len(applyEvent.ents) == 0 {
		return false
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
		return false
	}
	var shouldStop bool
	var confChanged bool
	for i := range ents {
		evnt := ents[i]
		switch evnt.Type {
		case raftpb.EntryNormal:
			if evnt.Data != nil {
				start := time.Now()
				// try redis command
				var reqList BatchInternalRaftRequest
				parseErr := reqList.Unmarshal(evnt.Data)
				if parseErr != nil {
					self.rn.Infof("parse request failed: %v, data len %v, entry: %v, raw:%v",
						parseErr, len(evnt.Data), evnt,
						string(evnt.Data))
				}
				if len(reqList.Reqs) != int(reqList.ReqNum) {
					self.rn.Infof("request check failed %v, real len:%v",
						reqList, len(reqList.Reqs))
				}
				var batchSet [][]byte
				if len(reqList.Reqs) > 1 {
					batchSet = append(batchSet, []byte("mset"))
				}
				var batchReqIDList []uint64
				for reqIndex, req := range reqList.Reqs {
					reqID := req.Header.ID
					if req.Header.DataType == 0 {
						cmd, err := redcon.Parse(req.Data)
						if err != nil {
							self.w.Trigger(reqID, err)
						} else {
							cmdName := strings.ToLower(string(cmd.Args[0]))
							if batchSet != nil {
								if cmdName == "set" {
									batchSet = append(batchSet, cmd.Args[1:]...)
									batchReqIDList = append(batchReqIDList, reqID)
								}
								if cmdName == "set" && reqIndex < len(reqList.Reqs)-1 {
									continue
								}
								if len(batchSet) > 1 {
									h, _ := self.router.GetInternalCmdHandler(string(batchSet[0]))
									cmdStart := time.Now()
									_, err := h(redcon.Command{Raw: nil, Args: batchSet}, req.Header.Timestamp)
									cmdCost := time.Since(cmdStart)
									self.dbWriteStats.UpdateWriteStats(int64(len(batchSet[2]))*int64(len(batchReqIDList)), cmdCost.Nanoseconds()/1000)
									// write the future response or error
									for _, rid := range batchReqIDList {
										if err != nil {
											self.w.Trigger(rid, err)
										} else {
											self.w.Trigger(rid, nil)
										}
									}
									batchSet = nil
									batchReqIDList = nil
								}
								if cmdName == "set" {
									continue
								}
							}
							h, ok := self.router.GetInternalCmdHandler(cmdName)
							if !ok {
								self.rn.Infof("unsupported redis command: %v", cmd)
								self.w.Trigger(reqID, common.ErrInvalidCommand)
							} else {
								cmdStart := time.Now()
								v, err := h(cmd, req.Header.Timestamp)
								cmdCost := time.Since(cmdStart)
								if cmdCost >= time.Second {
									self.rn.Infof("slow write command: %v, cost: %v", string(cmd.Raw), cmdCost)
								}
								self.dbWriteStats.UpdateWriteStats(int64(len(cmd.Raw)), cmdCost.Nanoseconds()/1000)
								// write the future response or error
								if err != nil {
									self.w.Trigger(reqID, err)
								} else {
									self.w.Trigger(reqID, v)
								}
							}
						}
					} else if req.Header.DataType == int32(HTTPReq) {
						//TODO: try other protocol command
						self.w.Trigger(reqID, errUnknownData)
					} else {
						self.w.Trigger(reqID, errUnknownData)
					}
				}
				cost := time.Since(start)
				if cost >= proposeTimeout/2 {
					self.rn.Infof("slow for batch write db: %v, %v", len(reqList.Reqs), cost)
				}

			}
		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			cc.Unmarshal(evnt.Data)
			removeSelf, changed, _ := self.rn.applyConfChange(cc, &np.confState)
			confChanged = changed
			shouldStop = shouldStop || removeSelf
		}
		np.appliedi = evnt.Index
		if evnt.Index == self.rn.lastIndex {
			self.rn.Infof("replay finished at index: %v\n", evnt.Index)
		}
	}
	if shouldStop {
		self.rn.Infof("I am removed from raft group: %v", self.ns)
		go func() {
			time.Sleep(time.Second)
			select {
			case <-self.stopChan:
			default:
				self.Stop()
			}
		}()
		return false
	}
	return confChanged
}

func (self *KVNode) applyCommits(commitC <-chan applyInfo) {
	defer func() {
		self.rn.Infof("apply commit exit")
	}()
	snap, err := self.rn.raftStorage.Snapshot()
	if err != nil {
		panic(err)
	}
	np := nodeProgress{
		confState: snap.Metadata.ConfState,
		snapi:     snap.Metadata.Index,
		appliedi:  snap.Metadata.Index,
	}
	self.rn.Infof("starting state: %v\n", np)
	for {
		select {
		case ent, ok := <-commitC:
			if !ok {
				go func() {
					select {
					case <-self.stopChan:
					default:
						self.Stop()
					}
				}()
				return
			}
			if ent.raftDone == nil {
				nodeLog.Panicf("wrong events : %v", ent)
			}
			confChanged := self.applyAll(&np, &ent)
			<-ent.raftDone
			self.maybeTriggerSnapshot(&np, confChanged)
			self.rn.handleSendSnapshot(&np)
			if ent.applyWaitDone != nil {
				close(ent.applyWaitDone)
			}
		case <-self.stopChan:
			return
		}
	}
}

func (self *KVNode) maybeTriggerSnapshot(np *nodeProgress, confChanged bool) {
	if np.appliedi-np.snapi <= 0 {
		return
	}
	if !confChanged && np.appliedi-np.snapi <= uint64(self.rn.config.SnapCount) {
		return
	}
	if np.appliedi <= self.rn.lastIndex {
		// replaying local log
		return
	}
	if self.rn.Lead() == raft.None {
		return
	}

	self.rn.Infof("start snapshot [applied index: %d | last snapshot index: %d]", np.appliedi, np.snapi)
	err := self.rn.beginSnapshot(np.appliedi, np.confState)
	if err != nil {
		self.rn.Infof("begin snapshot failed: %v", err)
		return
	}

	np.snapi = np.appliedi
}

func (self *KVNode) GetSnapshot(term uint64, index uint64) (Snapshot, error) {
	// use the rocksdb backup/checkpoint interface to backup data
	var si KVSnapInfo
	si.BackupInfo = self.store.Backup(term, index)
	if si.BackupInfo == nil {
		return nil, errors.New("failed to begin backup: maybe too much backup running")
	}
	si.WaitReady()
	si.LeaderInfo = self.rn.GetLeadMember()
	si.Members = self.rn.GetMembers()
	return &si, nil
}

func (self *KVNode) RestoreFromSnapshot(startup bool, raftSnapshot raftpb.Snapshot) error {
	snapshot := raftSnapshot.Data
	var si KVSnapInfo
	err := json.Unmarshal(snapshot, &si)
	if err != nil {
		return err
	}
	self.rn.RestoreMembers(si.Members)
	self.rn.Infof("should recovery from snapshot here: %v", raftSnapshot.String())
	// while startup we can use the local snapshot to restart,
	// but while running, we should install the leader's snapshot,
	// so we need remove local and sync from leader
	retry := 0
	for retry < 3 {
		hasBackup, _ := self.checkLocalBackup(raftSnapshot)
		if !hasBackup {
			self.rn.Infof("local no backup for snapshot, copy from remote\n")
			syncAddr, syncDir := self.GetValidBackupInfo(raftSnapshot)
			if syncAddr == "" && syncDir == "" {
				return errors.New("no backup available from others")
			}
			// copy backup data from the remote leader node, and recovery backup from it
			// if local has some old backup data, we should use rsync to sync the data file
			// use the rocksdb backup/checkpoint interface to backup data
			err = common.RunFileSync(syncAddr,
				path.Join(rockredis.GetBackupDir(syncDir),
					rockredis.GetCheckpointDir(raftSnapshot.Metadata.Term, raftSnapshot.Metadata.Index)),
				self.store.GetBackupDir())
			if err != nil {
				self.rn.Infof("failed to copy snapshot: %v", err)
				retry++
				time.Sleep(time.Second)
				continue
			}
		}
		err = self.store.Restore(raftSnapshot.Metadata.Term, raftSnapshot.Metadata.Index)
		if err == nil {
			return nil
		}
		select {
		case <-self.stopChan:
			return err
		default:
		}
		retry++
		time.Sleep(time.Second)
		self.rn.Infof("failed to restore snapshot: %v", err)
	}
	return err
}

func (self *KVNode) CheckLocalBackup(snapData []byte) (bool, error) {
	var rs raftpb.Snapshot
	err := rs.Unmarshal(snapData)
	if err != nil {
		return false, err
	}
	return self.checkLocalBackup(rs)
}

func (self *KVNode) checkLocalBackup(rs raftpb.Snapshot) (bool, error) {
	var si KVSnapInfo
	err := json.Unmarshal(rs.Data, &si)
	if err != nil {
		self.rn.Infof("unmarshal snap meta failed: %v", string(rs.Data))
		return false, err
	}
	return self.store.IsLocalBackupOK(rs.Metadata.Term, rs.Metadata.Index)
}

type deadlinedConn struct {
	Timeout time.Duration
	net.Conn
}

func (c *deadlinedConn) Read(b []byte) (n int, err error) {
	c.Conn.SetReadDeadline(time.Now().Add(c.Timeout))
	return c.Conn.Read(b)
}

func (c *deadlinedConn) Write(b []byte) (n int, err error) {
	c.Conn.SetWriteDeadline(time.Now().Add(c.Timeout))
	return c.Conn.Write(b)
}

func newDeadlineTransport(timeout time.Duration) *http.Transport {
	transport := &http.Transport{
		Dial: func(netw, addr string) (net.Conn, error) {
			c, err := net.DialTimeout(netw, addr, timeout)
			if err != nil {
				return nil, err
			}
			return &deadlinedConn{timeout, c}, nil
		},
	}
	return transport
}

func (self *KVNode) GetValidBackupInfo(raftSnapshot raftpb.Snapshot) (string, string) {
	// we need find the right backup data match with the raftsnapshot
	// for each cluster member, it need check the term+index and the backup meta to
	// make sure the data is valid
	syncAddr := ""
	syncDir := ""
	h := self.machineConfig.BroadcastAddr

	retry := 0
	var snapSyncInfoList []common.SnapshotSyncInfo
	var err error
	for retry < 3 {
		retry++
		snapSyncInfoList, err = self.clusterInfo.GetSnapshotSyncInfo(self.ns)
		if err != nil {
			self.rn.Infof("get snapshot info failed: %v", err)
			select {
			case <-self.stopChan:
				break
			case <-time.After(time.Second):
			}
		} else {
			break
		}
	}

	self.rn.Infof("current cluster raft nodes info: %v", snapSyncInfoList)
	for _, ssi := range snapSyncInfoList {
		if ssi.ReplicaID == uint64(self.rn.config.ID) {
			continue
		}

		c := http.Client{Transport: newDeadlineTransport(time.Second)}
		body, _ := raftSnapshot.Marshal()
		req, _ := http.NewRequest("GET", "http://"+ssi.RemoteAddr+":"+
			ssi.HttpAPIPort+common.APICheckBackup+"/"+self.ns, bytes.NewBuffer(body))
		rsp, err := c.Do(req)
		if err != nil {
			self.rn.Infof("request error: %v", err)
			continue
		}
		rsp.Body.Close()
		if ssi.RemoteAddr == h {
			if ssi.DataRoot == self.machineConfig.DataRootDir {
				// the leader is old mine, try find another leader
				self.rn.Infof("data dir can not be same if on local: %v, %v", ssi, self.machineConfig)
				continue
			}
			// local node with different directory
			syncAddr = ""
			syncDir = path.Join(ssi.DataRoot, self.ns)
		} else {
			// for remote snapshot, we do rsync from remote module
			syncAddr = ssi.RemoteAddr
			syncDir = path.Join(ssi.RsyncModule, self.ns)
		}
		break
	}
	self.rn.Infof("should recovery from : %v, %v", syncAddr, syncDir)
	return syncAddr, syncDir
}

func (self *KVNode) GetLastLeaderChangedTime() int64 {
	return self.rn.getLastLeaderChangedTime()
}

func (self *KVNode) ReportMeRaftLeader() {
	if self.clusterInfo == nil {
		return
	}
	nid, epoch, err := self.clusterInfo.GetNamespaceLeader(self.ns)
	if err != nil {
		self.rn.Infof("get raft leader from cluster failed: %v", err)
		return
	}

	ttlChecker := self.store.GetTTLChecker()

	if self.rn.IsLead() {
		if self.rn.config.nodeConfig.NodeID == nid {
			return
		}
		_, err = self.clusterInfo.UpdateMeForNamespaceLeader(self.ns, epoch)
		if err != nil {
			self.rn.Infof("update raft leader to me failed: %v", err)
		} else {
			self.rn.Infof("update %v raft leader to me : %v", self.ns, self.rn.config.ID)
		}
		//leader should start the TTLChecker to handle the expired data
		ttlChecker.Start()
	} else {
		ttlChecker.Stop()
	}
}

func (self *KVNode) Process(ctx context.Context, m raftpb.Message) error {
	return self.rn.Process(ctx, m)
}

func (self *KVNode) ReportUnreachable(id uint64, group raftpb.Group) {
	self.rn.ReportUnreachable(id, group)
}

func (self *KVNode) ReportSnapshot(id uint64, gp raftpb.Group, status raft.SnapshotStatus) {
	self.rn.ReportSnapshot(id, gp, status)
}

func (self *KVNode) SaveDBFrom(r io.Reader, msg raftpb.Message) (int64, error) {
	return self.rn.SaveDBFrom(r, msg)
}
