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
	"github.com/absolute8511/redcon"
	"github.com/coreos/etcd/pkg/wait"
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
	proposeTimeout       = time.Second * 10
	maxBatchCmdNum       = 500
)

const (
	HttpProposeOp_Backup int = 1
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

type httpProposeData struct {
	ProposeOp  int
	NeedBackup bool
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
	expireHandler     *ExpireHandler
	expirationPolicy  common.ExpirationPolicy
}

type KVSnapInfo struct {
	*rockredis.BackupInfo
	BackupMeta []byte               `json:"backup_meta"`
	LeaderInfo *common.MemberInfo   `json:"leader_info"`
	Members    []*common.MemberInfo `json:"members"`
}

func (si *KVSnapInfo) GetData() ([]byte, error) {
	meta, err := si.BackupInfo.GetResult()
	if err != nil {
		return nil, err
	}
	si.BackupMeta = meta
	d, _ := json.Marshal(si)
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
		reqProposeC:      make(chan *internalReq, 200),
		stopChan:         make(chan struct{}),
		store:            store,
		w:                wait.New(),
		router:           common.NewCmdRouter(),
		deleteCb:         deleteCb,
		ns:               config.GroupName,
		machineConfig:    machineConfig,
		expirationPolicy: kvopts.ExpirationPolicy,
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

	nd.wg.Add(1)
	go func() {
		defer nd.wg.Done()
		nd.expireHandler.Start()
	}()

	return nil
}

func (nd *KVNode) StopRaft() {
	nd.rn.StopNode()
}

func (nd *KVNode) Stop() {
	if !atomic.CompareAndSwapInt32(&nd.stopping, 0, 1) {
		return
	}
	close(nd.stopChan)
	go nd.deleteCb()
	nd.expireHandler.Stop()
	nd.wg.Wait()
	nd.rn.StopNode()
	nd.store.Close()
	nd.rn.Infof("node %v stopped", nd.ns)
}

func (nd *KVNode) OptimizeDB() {
	nd.store.CompactRange()
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

func (nd *KVNode) GetLeadMember() *common.MemberInfo {
	return nd.rn.GetLeadMember()
}

func (nd *KVNode) GetMembers() []*common.MemberInfo {
	return nd.rn.GetMembers()
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
	return nd.store.GetStatistics()
}

func (nd *KVNode) GetStats() common.NamespaceStats {
	tbs := nd.store.GetTables()
	var ns common.NamespaceStats
	ns.DBWriteStats = nd.dbWriteStats.Copy()
	ns.ClusterWriteStats = nd.clusterWriteStats.Copy()
	ns.InternalStats = nd.store.GetInternalStatus()

	for t := range tbs {
		cnt, err := nd.store.GetTableKeyCount(t)
		if err != nil {
			continue
		}
		var ts common.TableStats
		ts.Name = string(t)
		ts.KeyNum = cnt
		ns.TStats = append(ns.TStats, ts)
	}
	//nd.rn.Infof(nd.store.GetStatistics())
	return ns
}

func (nd *KVNode) destroy() error {
	nd.Stop()
	nd.store.Destroy()
	ts := strconv.Itoa(int(time.Now().UnixNano()))
	return os.Rename(nd.rn.config.DataDir,
		nd.rn.config.DataDir+"-deleted-"+ts)
}

func (nd *KVNode) CleanData() error {
	if err := nd.store.CleanData(); err != nil {
		return err
	}
	return nil
}

func (nd *KVNode) GetHandler(cmd string) (common.CommandFunc, bool, bool) {
	return nd.router.GetCmdHandler(cmd)
}

func (nd *KVNode) GetMergeHandler(cmd string) (common.MergeCommandFunc, bool) {
	return nd.router.GetMergeCmdHandler(cmd)
}

func (nd *KVNode) registerHandler() {
	// for kv
	nd.router.Register(false, "get", wrapReadCommandK(nd.getCommand))
	nd.router.Register(false, "mget", wrapReadCommandKK(nd.mgetCommand))
	nd.router.Register(false, "exists", wrapReadCommandK(nd.existsCommand))
	nd.router.Register(true, "set", wrapWriteCommandKV(nd, nd.setCommand))
	nd.router.Register(true, "setnx", wrapWriteCommandKV(nd, nd.setnxCommand))
	nd.router.Register(true, "mset", wrapWriteCommandKVKV(nd, nd.msetCommand))
	nd.router.Register(true, "incr", wrapWriteCommandK(nd, nd.incrCommand))
	nd.router.Register(true, "del", wrapWriteCommandKK(nd, nd.delCommand))
	nd.router.Register(false, "plget", nd.plgetCommand)
	nd.router.Register(true, "plset", nd.plsetCommand)
	// for hash
	nd.router.Register(false, "hget", wrapReadCommandKSubkey(nd.hgetCommand))
	nd.router.Register(false, "hgetall", wrapReadCommandK(nd.hgetallCommand))
	nd.router.Register(false, "hkeys", wrapReadCommandK(nd.hkeysCommand))
	nd.router.Register(false, "hexists", wrapReadCommandKSubkey(nd.hexistsCommand))
	nd.router.Register(false, "hmget", wrapReadCommandKSubkeySubkey(nd.hmgetCommand))
	nd.router.Register(false, "hlen", wrapReadCommandK(nd.hlenCommand))
	nd.router.Register(true, "hset", wrapWriteCommandKSubkeyV(nd, nd.hsetCommand))
	nd.router.Register(true, "hmset", wrapWriteCommandKSubkeyVSubkeyV(nd, nd.hmsetCommand))
	nd.router.Register(true, "hdel", wrapWriteCommandKSubkeySubkey(nd, nd.hdelCommand))
	nd.router.Register(true, "hincrby", wrapWriteCommandKSubkeyV(nd, nd.hincrbyCommand))
	nd.router.Register(true, "hclear", wrapWriteCommandK(nd, nd.hclearCommand))
	// for list
	nd.router.Register(false, "lindex", wrapReadCommandKSubkey(nd.lindexCommand))
	nd.router.Register(false, "llen", wrapReadCommandK(nd.llenCommand))
	nd.router.Register(false, "lrange", wrapReadCommandKAnySubkey(nd.lrangeCommand))
	nd.router.Register(true, "lfixkey", wrapWriteCommandK(nd, nd.lfixkeyCommand))
	nd.router.Register(true, "lpop", wrapWriteCommandK(nd, nd.lpopCommand))
	nd.router.Register(true, "lpush", wrapWriteCommandKVV(nd, nd.lpushCommand))
	nd.router.Register(true, "lset", nd.lsetCommand)
	nd.router.Register(true, "ltrim", nd.ltrimCommand)
	nd.router.Register(true, "rpop", wrapWriteCommandK(nd, nd.rpopCommand))
	nd.router.Register(true, "rpush", wrapWriteCommandKVV(nd, nd.rpushCommand))
	nd.router.Register(true, "lclear", wrapWriteCommandK(nd, nd.lclearCommand))
	// for zset
	nd.router.Register(false, "zscore", wrapReadCommandKSubkey(nd.zscoreCommand))
	nd.router.Register(false, "zcount", wrapReadCommandKAnySubkey(nd.zcountCommand))
	nd.router.Register(false, "zcard", wrapReadCommandK(nd.zcardCommand))
	nd.router.Register(false, "zlexcount", wrapReadCommandKAnySubkey(nd.zlexcountCommand))
	nd.router.Register(false, "zrange", wrapReadCommandKAnySubkey(nd.zrangeCommand))
	nd.router.Register(false, "zrevrange", wrapReadCommandKAnySubkey(nd.zrevrangeCommand))
	nd.router.Register(false, "zrangebylex", wrapReadCommandKAnySubkey(nd.zrangebylexCommand))
	nd.router.Register(false, "zrangebyscore", wrapReadCommandKAnySubkey(nd.zrangebyscoreCommand))
	nd.router.Register(false, "zrevrangebyscore", wrapReadCommandKAnySubkey(nd.zrevrangebyscoreCommand))
	nd.router.Register(false, "zrank", wrapReadCommandKSubkey(nd.zrankCommand))
	nd.router.Register(false, "zrevrank", wrapReadCommandKSubkey(nd.zrevrankCommand))
	nd.router.Register(true, "zadd", nd.zaddCommand)
	nd.router.Register(true, "zincrby", nd.zincrbyCommand)
	nd.router.Register(true, "zrem", wrapWriteCommandKSubkeySubkey(nd, nd.zremCommand))
	nd.router.Register(true, "zremrangebyrank", nd.zremrangebyrankCommand)
	nd.router.Register(true, "zremrangebyscore", nd.zremrangebyscoreCommand)
	nd.router.Register(true, "zremrangebylex", nd.zremrangebylexCommand)
	nd.router.Register(true, "zclear", wrapWriteCommandK(nd, nd.zclearCommand))
	// for set
	nd.router.Register(false, "scard", wrapReadCommandK(nd.scardCommand))
	nd.router.Register(false, "sismember", wrapReadCommandKSubkey(nd.sismemberCommand))
	nd.router.Register(false, "smembers", wrapReadCommandK(nd.smembersCommand))
	nd.router.Register(true, "sadd", wrapWriteCommandKSubkeySubkey(nd, nd.saddCommand))
	nd.router.Register(true, "srem", wrapWriteCommandKSubkeySubkey(nd, nd.sremCommand))
	nd.router.Register(true, "sclear", wrapWriteCommandK(nd, nd.sclearCommand))
	nd.router.Register(true, "smclear", wrapWriteCommandKK(nd, nd.smclearCommand))
	// for ttl
	nd.router.Register(false, "ttl", wrapReadCommandK(nd.ttlCommand))
	nd.router.Register(false, "httl", wrapReadCommandK(nd.httlCommand))
	nd.router.Register(false, "lttl", wrapReadCommandK(nd.lttlCommand))
	nd.router.Register(false, "sttl", wrapReadCommandK(nd.sttlCommand))
	nd.router.Register(false, "zttl", wrapReadCommandK(nd.zttlCommand))

	nd.router.Register(true, "setex", wrapWriteCommandKVV(nd, nd.setexCommand))
	nd.router.Register(true, "expire", wrapWriteCommandKV(nd, nd.expireCommand))
	nd.router.Register(true, "hexpire", wrapWriteCommandKV(nd, nd.hashExpireCommand))
	nd.router.Register(true, "lexpire", wrapWriteCommandKV(nd, nd.listExpireCommand))
	nd.router.Register(true, "sexpire", wrapWriteCommandKV(nd, nd.setExpireCommand))
	nd.router.Register(true, "zexpire", wrapWriteCommandKV(nd, nd.zsetExpireCommand))

	nd.router.Register(true, "persist", wrapWriteCommandK(nd, nd.persistCommand))
	nd.router.Register(true, "hpersist", wrapWriteCommandK(nd, nd.persistCommand))
	nd.router.Register(true, "lpersist", wrapWriteCommandK(nd, nd.persistCommand))
	nd.router.Register(true, "spersist", wrapWriteCommandK(nd, nd.persistCommand))
	nd.router.Register(true, "zpersist", wrapWriteCommandK(nd, nd.persistCommand))

	// for scan
	nd.router.Register(false, "hscan", wrapReadCommandKAnySubkey(nd.hscanCommand))
	nd.router.Register(false, "sscan", wrapReadCommandKAnySubkey(nd.sscanCommand))
	nd.router.Register(false, "zscan", wrapReadCommandKAnySubkey(nd.zscanCommand))

	//for cross mutil partion
	nd.router.RegisterMerge("scan", wrapMergeCommand(nd.scanCommand))
	nd.router.RegisterMerge("advscan", nd.advanceScanCommand)
	nd.router.RegisterMerge("fullscan", nd.fullScanCommand)
	nd.router.RegisterMerge("hidx.from", nd.hindexSearchCommand)

	// only write command need to be registered as internal
	// kv
	nd.router.RegisterInternal("del", nd.localDelCommand)
	nd.router.RegisterInternal("set", nd.localSetCommand)
	nd.router.RegisterInternal("setnx", nd.localSetnxCommand)
	nd.router.RegisterInternal("mset", nd.localMSetCommand)
	nd.router.RegisterInternal("incr", nd.localIncrCommand)
	nd.router.RegisterInternal("plset", nd.localPlsetCommand)
	// hash
	nd.router.RegisterInternal("hset", nd.localHSetCommand)
	nd.router.RegisterInternal("hmset", nd.localHMsetCommand)
	nd.router.RegisterInternal("hdel", nd.localHDelCommand)
	nd.router.RegisterInternal("hincrby", nd.localHIncrbyCommand)
	nd.router.RegisterInternal("hclear", nd.localHclearCommand)
	nd.router.RegisterInternal("hmclear", nd.localHMClearCommand)
	// list
	nd.router.RegisterInternal("lfixkey", nd.localLfixkeyCommand)
	nd.router.RegisterInternal("lpop", nd.localLpopCommand)
	nd.router.RegisterInternal("lpush", nd.localLpushCommand)
	nd.router.RegisterInternal("lset", nd.localLsetCommand)
	nd.router.RegisterInternal("ltrim", nd.localLtrimCommand)
	nd.router.RegisterInternal("rpop", nd.localRpopCommand)
	nd.router.RegisterInternal("rpush", nd.localRpushCommand)
	nd.router.RegisterInternal("lclear", nd.localLclearCommand)
	nd.router.RegisterInternal("lmclear", nd.localLMClearCommand)
	// zset
	nd.router.RegisterInternal("zadd", nd.localZaddCommand)
	nd.router.RegisterInternal("zincrby", nd.localZincrbyCommand)
	nd.router.RegisterInternal("zrem", nd.localZremCommand)
	nd.router.RegisterInternal("zremrangebyrank", nd.localZremrangebyrankCommand)
	nd.router.RegisterInternal("zremrangebyscore", nd.localZremrangebyscoreCommand)
	nd.router.RegisterInternal("zremrangebylex", nd.localZremrangebylexCommand)
	nd.router.RegisterInternal("zclear", nd.localZclearCommand)
	nd.router.RegisterInternal("zmclear", nd.localZMClearCommand)
	// set
	nd.router.RegisterInternal("sadd", nd.localSadd)
	nd.router.RegisterInternal("srem", nd.localSrem)
	nd.router.RegisterInternal("sclear", nd.localSclear)
	nd.router.RegisterInternal("smclear", nd.localSmclear)
	// expire
	nd.router.RegisterInternal("setex", nd.localSetexCommand)
	nd.router.RegisterInternal("expire", nd.localExpireCommand)
	nd.router.RegisterInternal("lexpire", nd.localListExpireCommand)
	nd.router.RegisterInternal("hexpire", nd.localHashExpireCommand)
	nd.router.RegisterInternal("sexpire", nd.localSetExpireCommand)
	nd.router.RegisterInternal("zexpire", nd.localZSetExpireCommand)
	nd.router.RegisterInternal("persist", nd.localPersistCommand)
	nd.router.RegisterInternal("hpersist", nd.localHashPersistCommand)
	nd.router.RegisterInternal("lpersist", nd.localListPersistCommand)
	nd.router.RegisterInternal("spersist", nd.localSetPersistCommand)
	nd.router.RegisterInternal("zpersist", nd.localZSetPersistCommand)
}

func (nd *KVNode) handleProposeReq() {
	var reqList BatchInternalRaftRequest
	reqList.Reqs = make([]*InternalRaftRequest, 0, 100)
	var lastReq *internalReq
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
		select {
		case r := <-nd.reqProposeC:
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
			ctx, cancel := context.WithTimeout(context.Background(), proposeTimeout*2)
			nd.rn.node.Propose(ctx, buffer)
			select {
			case <-lastReq.done:
			case <-ctx.Done():
				err := ctx.Err()
				for _, r := range reqList.Reqs {
					nd.w.Trigger(r.Header.ID, err)
				}
			case <-nd.stopChan:
				cancel()
				return
			}
			cancel()
			cost := (time.Now().UnixNano() - start) / 1000 / 1000 / 1000
			if cost >= int64(proposeTimeout.Seconds())/2 {
				nd.rn.Infof("slow for batch: %v, %v", len(reqList.Reqs), cost)
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
			nd.w.Trigger(req.reqData.Header.ID, common.ErrTimeout)
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
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
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

func (nd *KVNode) applySnapshot(np *nodeProgress, applyEvent *applyInfo) {
	if raft.IsEmptySnap(applyEvent.snapshot) {
		return
	}
	// signaled to load snapshot
	nd.rn.Infof("applying snapshot at index %d, snapshot: %v\n", np.snapi, applyEvent.snapshot.String())
	defer nd.rn.Infof("finished applying snapshot at index %d\n", np)

	if applyEvent.snapshot.Metadata.Index <= np.appliedi {
		nodeLog.Panicf("snapshot index [%d] should > progress.appliedIndex [%d] + 1",
			applyEvent.snapshot.Metadata.Index, np.appliedi)
	}

	if err := nd.RestoreFromSnapshot(false, applyEvent.snapshot); err != nil {
		nodeLog.Error(err)
		go func() {
			select {
			case <-nd.stopChan:
			default:
				nd.Stop()
			}
		}()
	}

	np.confState = applyEvent.snapshot.Metadata.ConfState
	np.snapi = applyEvent.snapshot.Metadata.Index
	np.appliedi = applyEvent.snapshot.Metadata.Index
}

// return if configure changed and whether need force backup
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
	nd.applySnapshot(np, applyEvent)
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
			if evnt.Data != nil {
				start := time.Now()
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
				batching := false
				var batchReqIDList []uint64
				var batchReqRspList []interface{}
				var batchStart time.Time
				dupCheckMap := make(map[string]bool, len(reqList.Reqs))
				for reqIndex, req := range reqList.Reqs {
					reqID := req.Header.ID
					if req.Header.DataType == 0 {
						cmd, err := redcon.Parse(req.Data)
						if err != nil {
							nd.w.Trigger(reqID, err)
						} else {
							cmdStart := time.Now()
							cmdName := strings.ToLower(string(cmd.Args[0]))
							_, pk, _ := common.ExtractNamesapce(cmd.Args[1])
							_, ok := dupCheckMap[string(pk)]
							handled := false
							// TODO: table counter can be batched???
							if nd.store.IsBatchableWrite(cmdName) &&
								len(batchReqIDList) < maxBatchCmdNum &&
								!ok {
								if !batching {
									err := nd.store.BeginBatchWrite()
									if err != nil {
										nd.rn.Infof("begin batch command %v failed: %v, %v", cmdName, string(cmd.Raw), err)
										nd.w.Trigger(reqID, err)
										continue
									}
									batchStart = time.Now()
									batching = true
								}
								handled = true
								h, ok := nd.router.GetInternalCmdHandler(cmdName)
								if !ok {
									nd.rn.Infof("unsupported redis command: %v", cmdName)
									nd.w.Trigger(reqID, common.ErrInvalidCommand)
								} else {
									if pk != nil {
										dupCheckMap[string(pk)] = true
									}
									v, err := h(cmd, req.Header.Timestamp)
									if err != nil {
										nd.rn.Infof("redis command %v error: %v, cmd: %v", cmdName, err, cmd)
										nd.w.Trigger(reqID, err)
										continue
									}
									batchReqIDList = append(batchReqIDList, reqID)
									batchReqRspList = append(batchReqRspList, v)
									nd.dbWriteStats.UpdateSizeStats(int64(len(cmd.Raw)))
								}
								if nodeLog.Level() > common.LOG_DETAIL {
									nd.rn.Infof("batching redis command: %v", cmdName)
								}
								if reqIndex < len(reqList.Reqs)-1 {
									continue
								}
							}
							if batching {
								err := nd.store.CommitBatchWrite()
								dupCheckMap = make(map[string]bool, len(reqList.Reqs))
								batching = false
								batchCost := time.Since(batchStart)
								if nodeLog.Level() >= common.LOG_DETAIL {
									nd.rn.Infof("batching command number: %v", len(batchReqIDList))
								}
								// write the future response or error
								for idx, rid := range batchReqIDList {
									if err != nil {
										nd.w.Trigger(rid, err)
									} else {
										nd.w.Trigger(rid, batchReqRspList[idx])
									}
								}
								if batchCost >= time.Second {
									nd.rn.Infof("slow batch write command: %v, batch: %v, cost: %v",
										cmdName, len(batchReqIDList), batchCost)
								}
								if len(batchReqIDList) > 0 {
									nd.dbWriteStats.UpdateLatencyStats(batchCost.Nanoseconds() / int64(len(batchReqIDList)) / 1000)
								}
								batchReqIDList = batchReqIDList[:0]
								batchReqRspList = batchReqRspList[:0]
							}
							if handled {
								continue
							}

							h, ok := nd.router.GetInternalCmdHandler(cmdName)
							if !ok {
								nd.rn.Infof("unsupported redis command: %v", cmd)
								nd.w.Trigger(reqID, common.ErrInvalidCommand)
							} else {
								v, err := h(cmd, req.Header.Timestamp)
								cmdCost := time.Since(cmdStart)
								if cmdCost >= time.Second {
									nd.rn.Infof("slow write command: %v, cost: %v", string(cmd.Raw), cmdCost)
								}
								nd.dbWriteStats.UpdateWriteStats(int64(len(cmd.Raw)), cmdCost.Nanoseconds()/1000)
								// write the future response or error
								if err != nil {
									nd.rn.Infof("redis command %v error: %v, cmd: %v", cmdName, err, string(cmd.Raw))
									nd.w.Trigger(reqID, err)
								} else {
									nd.w.Trigger(reqID, v)
								}
							}
						}
					} else {
						if batching {
							err := nd.store.CommitBatchWrite()
							dupCheckMap = make(map[string]bool, len(reqList.Reqs))
							batching = false
							batchCost := time.Since(batchStart)
							// write the future response or error
							for _, rid := range batchReqIDList {
								if err != nil {
									nd.w.Trigger(rid, err)
								} else {
									nd.w.Trigger(rid, nil)
								}
							}
							if batchCost >= time.Second {
								nd.rn.Infof("slow batch write command batch: %v, cost: %v",
									len(batchReqIDList), batchCost)
							}
							if len(batchReqIDList) > 0 {
								nd.dbWriteStats.UpdateLatencyStats(batchCost.Nanoseconds() / int64(len(batchReqIDList)) / 1000)
							}
							batchReqIDList = batchReqIDList[:0]
						}
						if req.Header.DataType == int32(HTTPReq) {
							var p httpProposeData
							err := json.Unmarshal(req.Data, &p)
							if err != nil {
								nd.rn.Infof("failed to unmarshal http propose: %v", req.String())
								nd.w.Trigger(reqID, err)
							}
							if p.ProposeOp == HttpProposeOp_Backup {
								nd.rn.Infof("got force backup request")
								forceBackup = true
								nd.w.Trigger(reqID, nil)
							} else {
								nd.w.Trigger(reqID, errUnknownData)
							}
						} else if req.Header.DataType == int32(SchemaChangeReq) {
							nd.rn.Infof("handle schema change: %v", string(req.Data))
							var sc SchemaChange
							err := sc.Unmarshal(req.Data)
							if err != nil {
								nd.rn.Infof("schema data error: %v, %v", string(req.Data), err)
								nd.w.Trigger(reqID, err)
							} else {
								err = nd.handleSchemaUpdate(sc)
								nd.w.Trigger(reqID, err)
							}
						} else {
							nd.w.Trigger(reqID, errUnknownData)
						}
					}
				}
				cost := time.Since(start)
				if cost >= proposeTimeout/2 {
					nd.rn.Infof("slow for batch write db: %v, %v", len(reqList.Reqs), cost)
				}
			}
		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			cc.Unmarshal(evnt.Data)
			removeSelf, changed, _ := nd.rn.applyConfChange(cc, &np.confState)
			confChanged = changed
			shouldStop = shouldStop || removeSelf
		}
		np.appliedi = evnt.Index
		if evnt.Index == nd.rn.lastIndex {
			nd.rn.Infof("replay finished at index: %v\n", evnt.Index)
		}
	}
	if shouldStop {
		nd.rn.Infof("I am removed from raft group: %v", nd.ns)
		go func() {
			time.Sleep(time.Second)
			select {
			case <-nd.stopChan:
			default:
				nd.Stop()
			}
		}()
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
		appliedi:  snap.Metadata.Index,
	}
	nd.rn.Infof("starting state: %v\n", np)
	for {
		select {
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
			<-ent.raftDone
			nd.maybeTriggerSnapshot(&np, confChanged, forceBackup)
			nd.rn.handleSendSnapshot(&np)
			if ent.applyWaitDone != nil {
				close(ent.applyWaitDone)
			}
		case <-nd.stopChan:
			return
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
	err := nd.rn.beginSnapshot(np.appliedi, np.confState)
	if err != nil {
		nd.rn.Infof("begin snapshot failed: %v", err)
		return
	}

	np.snapi = np.appliedi
}

func (nd *KVNode) GetSnapshot(term uint64, index uint64) (Snapshot, error) {
	// use the rocksdb backup/checkpoint interface to backup data
	var si KVSnapInfo
	si.BackupInfo = nd.store.Backup(term, index)
	if si.BackupInfo == nil {
		return nil, errors.New("failed to begin backup: maybe too much backup running")
	}
	si.WaitReady()
	si.Members, si.LeaderInfo = nd.rn.GetMembersAndLeader()
	return &si, nil
}

func (nd *KVNode) RestoreFromSnapshot(startup bool, raftSnapshot raftpb.Snapshot) error {
	snapshot := raftSnapshot.Data
	var si KVSnapInfo
	err := json.Unmarshal(snapshot, &si)
	if err != nil {
		return err
	}
	nd.rn.RestoreMembers(si.Members)
	nd.rn.Infof("should recovery from snapshot here: %v", raftSnapshot.String())
	// while startup we can use the local snapshot to restart,
	// but while running, we should install the leader's snapshot,
	// so we need remove local and sync from leader
	retry := 0
	for retry < 3 {
		hasBackup, _ := nd.checkLocalBackup(raftSnapshot)
		if !hasBackup {
			nd.rn.Infof("local no backup for snapshot, copy from remote\n")
			syncAddr, syncDir := nd.GetValidBackupInfo(raftSnapshot, retry)
			if syncAddr == "" && syncDir == "" {
				return errors.New("no backup available from others")
			}
			// copy backup data from the remote leader node, and recovery backup from it
			// if local has some old backup data, we should use rsync to sync the data file
			// use the rocksdb backup/checkpoint interface to backup data
			err = common.RunFileSync(syncAddr,
				path.Join(rockredis.GetBackupDir(syncDir),
					rockredis.GetCheckpointDir(raftSnapshot.Metadata.Term, raftSnapshot.Metadata.Index)),
				nd.store.GetBackupDir())
			if err != nil {
				nd.rn.Infof("failed to copy snapshot: %v", err)
				retry++
				time.Sleep(time.Second)
				continue
			}
		}
		err = nd.store.Restore(raftSnapshot.Metadata.Term, raftSnapshot.Metadata.Index)
		if err == nil {
			return nil
		}
		select {
		case <-nd.stopChan:
			return err
		default:
		}
		retry++
		time.Sleep(time.Second)
		nd.rn.Infof("failed to restore snapshot: %v", err)
	}
	return err
}

func (nd *KVNode) CheckLocalBackup(snapData []byte) (bool, error) {
	var rs raftpb.Snapshot
	err := rs.Unmarshal(snapData)
	if err != nil {
		return false, err
	}
	return nd.checkLocalBackup(rs)
}

func (nd *KVNode) checkLocalBackup(rs raftpb.Snapshot) (bool, error) {
	var si KVSnapInfo
	err := json.Unmarshal(rs.Data, &si)
	if err != nil {
		nd.rn.Infof("unmarshal snap meta failed: %v", string(rs.Data))
		return false, err
	}
	return nd.store.IsLocalBackupOK(rs.Metadata.Term, rs.Metadata.Index)
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

func (nd *KVNode) GetValidBackupInfo(raftSnapshot raftpb.Snapshot, retry int) (string, string) {
	// we need find the right backup data match with the raftsnapshot
	// for each cluster member, it need check the term+index and the backup meta to
	// make sure the data is valid
	syncAddr := ""
	syncDir := ""
	h := nd.machineConfig.BroadcastAddr

	innerRetry := 0
	var snapSyncInfoList []common.SnapshotSyncInfo
	var err error
	for innerRetry < 3 {
		innerRetry++
		snapSyncInfoList, err = nd.clusterInfo.GetSnapshotSyncInfo(nd.ns)
		if err != nil {
			nd.rn.Infof("get snapshot info failed: %v", err)
			select {
			case <-nd.stopChan:
				break
			case <-time.After(time.Second):
			}
		} else {
			break
		}
	}

	nd.rn.Infof("current cluster raft nodes info: %v", snapSyncInfoList)
	syncAddrList := make([]string, 0)
	syncDirList := make([]string, 0)
	for _, ssi := range snapSyncInfoList {
		if ssi.ReplicaID == uint64(nd.rn.config.ID) {
			continue
		}

		c := http.Client{Transport: newDeadlineTransport(time.Second)}
		body, _ := raftSnapshot.Marshal()
		req, _ := http.NewRequest("GET", "http://"+ssi.RemoteAddr+":"+
			ssi.HttpAPIPort+common.APICheckBackup+"/"+nd.ns, bytes.NewBuffer(body))
		rsp, err := c.Do(req)
		if err != nil || rsp.StatusCode != http.StatusOK {
			nd.rn.Infof("request error: %v, %v", err, rsp)
			continue
		}
		rsp.Body.Close()
		if ssi.RemoteAddr == h {
			if ssi.DataRoot == nd.machineConfig.DataRootDir {
				// the leader is old mine, try find another leader
				nd.rn.Infof("data dir can not be same if on local: %v, %v", ssi, nd.machineConfig)
				continue
			}
			// local node with different directory
			syncAddrList = append(syncAddrList, "")
			syncDirList = append(syncDirList, path.Join(ssi.DataRoot, nd.ns))
		} else {
			// for remote snapshot, we do rsync from remote module
			syncAddrList = append(syncAddrList, ssi.RemoteAddr)
			syncDirList = append(syncDirList, path.Join(ssi.RsyncModule, nd.ns))
		}
	}
	if len(syncAddrList) > 0 {
		syncAddr = syncAddrList[retry%len(syncAddrList)]
		syncDir = syncDirList[retry%len(syncDirList)]
	}
	nd.rn.Infof("should recovery from : %v, %v", syncAddr, syncDir)
	return syncAddr, syncDir
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
