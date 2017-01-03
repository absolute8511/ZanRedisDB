package node

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"errors"
	"log"
	"net"
	"net/url"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/absolute8511/ZanRedisDB/rockredis"
	"github.com/absolute8511/ZanRedisDB/store"
	"github.com/coreos/etcd/pkg/wait"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/tidwall/redcon"
)

var (
	errInvalidResponse  = errors.New("Invalid response type")
	errSyntaxError      = errors.New("syntax error")
	errUnknownData      = errors.New("unknown request data type")
	errTooMuchBatchSize = errors.New("the batch size exceed the limit")
)

const (
	RedisReq int8 = 0
	HTTPReq  int8 = 1
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
	proposeC          chan<- []byte // channel for proposing updates
	raftNode          *raftNode
	store             *store.KVStore
	stopping          int32
	stopChan          chan struct{}
	w                 wait.Wait
	router            *common.CmdRouter
	deleteCb          func()
	dbWriteStats      common.WriteStats
	clusterWriteStats common.WriteStats
}

type KVSnapInfo struct {
	*rockredis.BackupInfo
	BackupMeta []byte        `json:"backup_meta"`
	LeaderInfo *MemberInfo   `json:"leader_info"`
	Members    []*MemberInfo `json:"members"`
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

func NewKVNode(kvopts *store.KVOptions, clusterID uint64, id int, localRaftAddr string,
	peers map[int]string, join bool, deleteCb func()) (*KVNode, chan raftpb.ConfChange) {
	proposeC := make(chan []byte)
	confChangeC := make(chan raftpb.ConfChange)
	s := &KVNode{
		reqProposeC: make(chan *internalReq, 200),
		proposeC:    proposeC,
		store:       store.NewKVStore(kvopts),
		stopChan:    make(chan struct{}),
		w:           wait.New(),
		router:      common.NewCmdRouter(),
		deleteCb:    deleteCb,
	}
	s.registerHandler()
	commitC, errorC, raftNode := newRaftNode(clusterID, id, localRaftAddr, kvopts.DataDir,
		peers, join, s, proposeC, confChangeC)
	s.raftNode = raftNode

	raftNode.startRaft(s)
	// read commits from raft into KVStore map until error
	go s.applyCommits(commitC, errorC)
	go s.handleProposeReq()
	return s, confChangeC
}

func (self *KVNode) Stop() {
	if !atomic.CompareAndSwapInt32(&self.stopping, 0, 1) {
		return
	}
	self.raftNode.StopNode()
	self.store.Close()
	close(self.stopChan)
	go self.deleteCb()
}

func (self *KVNode) OptimizeDB() {
	self.store.CompactRange()
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
	log.Println(self.store.GetStatistics())
	return ns
}

func (self *KVNode) Clear() error {
	return self.store.Clear()
}

func (self *KVNode) GetHandler(cmd string) (common.CommandFunc, bool) {
	return self.router.GetCmdHandler(cmd)
}

func (self *KVNode) registerHandler() {
	// for kv
	self.router.Register("get", wrapReadCommandK(self.getCommand))
	self.router.Register("mget", wrapReadCommandKK(self.mgetCommand))
	self.router.Register("exists", wrapReadCommandK(self.existsCommand))
	self.router.Register("set", wrapWriteCommandKV(self, self.setCommand))
	self.router.Register("setnx", wrapWriteCommandKV(self, self.setnxCommand))
	self.router.Register("mset", wrapWriteCommandKVKV(self, self.msetCommand))
	self.router.Register("incr", wrapWriteCommandK(self, self.incrCommand))
	self.router.Register("del", wrapWriteCommandKK(self, self.delCommand))
	self.router.Register("plget", self.plgetCommand)
	self.router.Register("plset", self.plsetCommand)
	// for hash
	self.router.Register("hget", wrapReadCommandKSubkey(self.hgetCommand))
	self.router.Register("hgetall", wrapReadCommandK(self.hgetallCommand))
	self.router.Register("hkeys", wrapReadCommandK(self.hkeysCommand))
	self.router.Register("hexists", wrapReadCommandKSubkey(self.hexistsCommand))
	self.router.Register("hmget", wrapReadCommandKSubkeySubkey(self.hmgetCommand))
	self.router.Register("hlen", wrapReadCommandK(self.hlenCommand))
	self.router.Register("hset", wrapWriteCommandKSubkeyV(self, self.hsetCommand))
	self.router.Register("hmset", wrapWriteCommandKSubkeyVSubkeyV(self, self.hmsetCommand))
	self.router.Register("hdel", wrapWriteCommandKSubkeySubkey(self, self.hdelCommand))
	self.router.Register("hincrby", wrapWriteCommandKSubkeyV(self, self.hincrbyCommand))
	self.router.Register("hclear", wrapWriteCommandK(self, self.hclearCommand))
	// for list
	self.router.Register("lindex", wrapReadCommandKSubkey(self.lindexCommand))
	self.router.Register("llen", wrapReadCommandK(self.llenCommand))
	self.router.Register("lrange", wrapReadCommandKAnySubkey(self.lrangeCommand))
	self.router.Register("lpop", wrapWriteCommandK(self, self.lpopCommand))
	self.router.Register("lpush", wrapWriteCommandKVV(self, self.lpushCommand))
	self.router.Register("lset", self.lsetCommand)
	self.router.Register("ltrim", self.ltrimCommand)
	self.router.Register("rpop", wrapWriteCommandK(self, self.rpopCommand))
	self.router.Register("rpush", wrapWriteCommandKVV(self, self.rpushCommand))
	self.router.Register("lclear", wrapWriteCommandK(self, self.lclearCommand))
	// for zset
	self.router.Register("zscore", wrapReadCommandKSubkey(self.zscoreCommand))
	self.router.Register("zcount", wrapReadCommandKAnySubkey(self.zcountCommand))
	self.router.Register("zcard", wrapReadCommandK(self.zcardCommand))
	self.router.Register("zlexcount", wrapReadCommandKAnySubkey(self.zlexcountCommand))
	self.router.Register("zrange", wrapReadCommandKAnySubkey(self.zrangeCommand))
	self.router.Register("zrevrange", wrapReadCommandKAnySubkey(self.zrevrangeCommand))
	self.router.Register("zrangebylex", wrapReadCommandKAnySubkey(self.zrangebylexCommand))
	self.router.Register("zrangebyscore", wrapReadCommandKAnySubkey(self.zrangebyscoreCommand))
	self.router.Register("zrevrangebyscore", wrapReadCommandKAnySubkey(self.zrevrangebyscoreCommand))
	self.router.Register("zrank", wrapReadCommandKSubkey(self.zrankCommand))
	self.router.Register("zrevrank", wrapReadCommandKSubkey(self.zrevrankCommand))
	self.router.Register("zadd", self.zaddCommand)
	self.router.Register("zincrby", self.zincrbyCommand)
	self.router.Register("zrem", wrapWriteCommandKSubkeySubkey(self, self.zremCommand))
	self.router.Register("zremrangebyrank", self.zremrangebyrankCommand)
	self.router.Register("zremrangebyscore", self.zremrangebyscoreCommand)
	self.router.Register("zremrangebylex", self.zremrangebylexCommand)
	self.router.Register("zclear", wrapWriteCommandK(self, self.zclearCommand))

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
}

func (self *KVNode) handleProposeReq() {
	var reqList BatchInternalRaftRequest
	reqList.Reqs = make([]*InternalRaftRequest, 0, 100)
	buffer := make([]byte, 1024*1024)
	var lastReq *internalReq
	defer func() {
		if e := recover(); e != nil {
			buf := make([]byte, 4096)
			n := runtime.Stack(buf, false)
			buf = buf[0:n]
			log.Printf("handle propose loop panic: %s:%v", buf, e)
		}
		log.Printf("handle propose loop exit")
		for _, r := range reqList.Reqs {
			self.w.Trigger(r.Header.ID, common.ErrStopped)
		}
		for {
			select {
			case r := <-self.reqProposeC:
				self.w.Trigger(r.reqData.Header.ID, common.ErrStopped)
			default:
				break
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
			n := reqList.Size()
			if n > len(buffer) {
				buffer = make([]byte, n)
			}
			realN, err := reqList.MarshalTo(buffer)
			//log.Printf("handle req %v, marshal buffer: %v", len(reqList.Reqs), realN)
			if err != nil {
				log.Printf("failed to marshal request: %v", err)
				for _, r := range reqList.Reqs {
					self.w.Trigger(r.Header.ID, err)
				}
				reqList.Reqs = reqList.Reqs[:0]
				continue
			}
			lastReq.done = make(chan struct{})
			buffer = buffer[:realN]
			start := time.Now()
			self.proposeC <- buffer
			select {
			case <-lastReq.done:
			case <-self.stopChan:
				return
			}
			cost := time.Since(start)
			if len(reqList.Reqs) >= 100 && cost >= time.Second || (cost >= time.Second*2) {
				log.Printf("slow for batch: %v, %v", len(reqList.Reqs), cost)
			}
			reqList.Reqs = reqList.Reqs[:0]
			lastReq = nil
		}
	}
}

func (self *KVNode) queueRequest(req *internalReq) (interface{}, error) {
	start := time.Now()
	ch := self.w.Register(req.reqData.Header.ID)
	select {
	case self.reqProposeC <- req:
	default:
		select {
		case self.reqProposeC <- req:
		case <-self.stopChan:
			self.w.Trigger(req.reqData.Header.ID, common.ErrStopped)
		case <-time.After(time.Second * 3):
			self.w.Trigger(req.reqData.Header.ID, common.ErrTimeout)
		}
	}
	//log.Printf("queue request: %v", req.reqData.String())
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
	return rsp, err
}

func (self *KVNode) Propose(buf []byte) (interface{}, error) {
	h := &RequestHeader{
		ID:       self.raftNode.reqIDGen.Next(),
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
		ID:       self.raftNode.reqIDGen.Next(),
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

func (self *KVNode) applySnapshot(np *nodeProgress, applyEvent *applyInfo) {
	if raft.IsEmptySnap(applyEvent.snapshot) {
		return
	}
	// signaled to load snapshot
	log.Printf("applying snapshot at index %d, snapshot: %v\n", np.snapi, applyEvent.snapshot.String())
	defer log.Printf("finished applying snapshot at index %d\n", np.snapi)

	if applyEvent.snapshot.Metadata.Index <= np.appliedi {
		log.Fatalf("snapshot index [%d] should > progress.appliedIndex [%d] + 1",
			applyEvent.snapshot.Metadata.Index, np.appliedi)
	}

	if err := self.RestoreFromSnapshot(false, applyEvent.snapshot); err != nil {
		log.Panic(err)
	}

	self.raftNode.applySnapshot(applyEvent.snapshot)

	np.confState = applyEvent.snapshot.Metadata.ConfState
	np.snapi = applyEvent.snapshot.Metadata.Index
	np.appliedi = applyEvent.snapshot.Metadata.Index
}

func (self *KVNode) applyAll(np *nodeProgress, applyEvent *applyInfo) {
	self.applySnapshot(np, applyEvent)
	if len(applyEvent.ents) == 0 {
		return
	}
	firsti := applyEvent.ents[0].Index
	if firsti > np.appliedi+1 {
		log.Panicf("first index of committed entry[%d] should <= appliedi[%d] + 1", firsti, np.appliedi)
	}
	var ents []raftpb.Entry
	if np.appliedi+1-firsti < uint64(len(applyEvent.ents)) {
		ents = applyEvent.ents[np.appliedi+1-firsti:]
	}
	if len(ents) == 0 {
		return
	}
	var shouldStop bool
	var reqList BatchInternalRaftRequest
	for i := range ents {
		evnt := ents[i]
		switch evnt.Type {
		case raftpb.EntryNormal:
			if evnt.Data != nil {
				start := time.Now()
				// try redis command
				reqList.Reset()
				parseErr := reqList.Unmarshal(evnt.Data)
				if parseErr != nil {
					log.Printf("parse request failed: %v", parseErr)
				}
				for _, req := range reqList.Reqs {
					reqID := req.Header.ID
					if req.Header.DataType == 0 {
						cmd, err := redcon.Parse(req.Data)
						if err != nil {
							self.w.Trigger(reqID, err)
						} else {
							cmdName := strings.ToLower(string(cmd.Args[0]))
							h, ok := self.router.GetInternalCmdHandler(cmdName)
							if !ok {
								log.Printf("unsupported redis command: %v", cmd)
								self.w.Trigger(reqID, common.ErrInvalidCommand)
							} else {
								cmdStart := time.Now()
								v, err := h(cmd)
								cmdCost := time.Since(cmdStart)
								if cmdCost >= time.Millisecond*500 {
									log.Printf("slow write command: %v, cost: %v", string(cmd.Raw), cmdCost)
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
						// try other protocol command
						var dataKv kv
						dec := gob.NewDecoder(bytes.NewBuffer(req.Data))
						var err error
						if err = dec.Decode(&dataKv); err != nil {
							log.Fatalf("could not decode message (%v)\n", err)
						} else {
							err = self.store.LocalPut([]byte(dataKv.Key), []byte(dataKv.Val))
						}
						self.w.Trigger(reqID, err)
					} else {
						self.w.Trigger(reqID, errUnknownData)
					}
				}
				cost := time.Since(start)
				if len(reqList.Reqs) >= 100 && cost > time.Second || (cost > time.Second*2) {
					log.Printf("slow for batch write db: %v, %v", len(reqList.Reqs), cost)
				}

			}
		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			cc.Unmarshal(evnt.Data)
			removeSelf, _ := self.raftNode.applyConfChange(cc, &np.confState)
			shouldStop = shouldStop || removeSelf
		}
		np.appliedi = evnt.Index
		if evnt.Index == self.raftNode.lastIndex {
			log.Printf("replay finished at index: %v\n", evnt.Index)
		}
	}
	if shouldStop {
		go func() {
			time.Sleep(time.Second)
			select {
			case self.raftNode.errorC <- errors.New("my node removed"):
			default:
			}
		}()
	}
}

func (self *KVNode) applyCommits(commitC <-chan applyInfo, errorC <-chan error) {
	defer func() {
		self.Stop()
	}()
	snap, err := self.raftNode.raftStorage.Snapshot()
	if err != nil {
		panic(err)
	}
	np := nodeProgress{
		confState: snap.Metadata.ConfState,
		snapi:     snap.Metadata.Index,
		appliedi:  snap.Metadata.Index,
	}
	log.Printf("starting state: %v\n", np)
	for {
		select {
		case ent := <-commitC:
			self.applyAll(&np, &ent)
			self.maybeTriggerSnapshot(&np)
			self.raftNode.handleSendSnapshot()
		case err, ok := <-errorC:
			if !ok {
				return
			}
			log.Printf("error: %v", err)
			return
		case <-self.stopChan:
			return
		}
	}
}

func (self *KVNode) maybeTriggerSnapshot(np *nodeProgress) {
	if np.appliedi-np.snapi <= DefaultSnapCount {
		return
	}
	if np.appliedi <= self.raftNode.lastIndex {
		// replaying local log
		return
	}

	log.Printf("start snapshot [applied index: %d | last snapshot index: %d]", np.appliedi, np.snapi)
	err := self.raftNode.beginSnapshot(np.appliedi, np.confState)
	if err != nil {
		log.Printf("begin snapshot failed: %v", err)
		return
	}

	np.snapi = np.appliedi
}

func (self *KVNode) GetSnapshot() (Snapshot, error) {
	// use the rocksdb backup/checkpoint interface to backup data
	var si KVSnapInfo
	si.BackupInfo = self.store.Backup()
	if si.BackupInfo == nil {
		return nil, errors.New("failed to begin backup: maybe too much backup running")
	}
	si.WaitReady()
	si.LeaderInfo = self.raftNode.GetLeadMember()
	si.Members = self.raftNode.GetMembers()
	return &si, nil
}

func (self *KVNode) RestoreFromSnapshot(startup bool, raftSnapshot raftpb.Snapshot) error {
	snapshot := raftSnapshot.Data
	var si KVSnapInfo
	err := json.Unmarshal(snapshot, &si)
	if err != nil {
		return err
	}
	self.raftNode.RestoreMembers(si.Members)
	log.Printf("should recovery from snapshot here: %v, %v, %v", si.BackupMeta, si.LeaderInfo, si.Members)
	// while startup we can use the local snapshot to restart,
	// but while running, we should install the leader's snapshot,
	// so we need remove local and sync from leader
	hasBackup, _ := self.store.IsLocalBackupOK(si.BackupMeta)
	if !startup {
		if hasBackup {
			self.store.ClearBackup(si.BackupMeta)
			hasBackup = false
		}
	}
	if !hasBackup {
		// TODO: get leader from cluster
		log.Printf("local no backup for snapshot, copy from remote\n")
		remoteLeader := si.LeaderInfo
		if remoteLeader == nil {
			log.Printf("no leader found")
			// TODO: try find leader from cluster
			return errors.New("leader empty")
		}
		log.Printf("should recovery from leader: %v", remoteLeader)
		u, _ := url.Parse(self.raftNode.localRaftAddr)
		h, _, _ := net.SplitHostPort(u.Host)
		if remoteLeader.Broadcast == h {
			remoteLeader.Broadcast = ""
		}
		// copy backup data from the remote leader node, and recovery backup from it
		// if local has some old backup data, we should use rsync to sync the data file
		// use the rocksdb backup/checkpoint interface to backup data
		common.RunFileSync(remoteLeader.Broadcast, rockredis.GetBackupDir(remoteLeader.DataDir),
			self.store.GetBackupBase())
	}
	return self.store.Restore(si.BackupMeta)
}
