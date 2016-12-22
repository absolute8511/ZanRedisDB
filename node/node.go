package node

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"errors"
	"log"
	"net"
	"net/url"
	"path"
	"strings"
	"sync/atomic"
	"time"

	"github.com/absolute8511/ZanRedisDB/common"
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
	ID       uint64
	DataType int8
	Data     []byte
	doneC    chan struct{}
}

// a key-value node backed by raft
type KVNode struct {
	reqProposeC chan *internalReq
	proposeC    chan<- []byte // channel for proposing updates
	raftNode    *raftNode
	store       *store.KVStore
	stopping    int32
	stopChan    chan struct{}
	w           wait.Wait
	router      *common.CmdRouter
	deleteCb    func()
}

type KVSnapInfo struct {
	BackupMeta []byte        `json:"backup_meta"`
	LeaderInfo *MemberInfo   `json:"leader_info"`
	Members    []*MemberInfo `json:"members"`
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
	reqList := make([]*internalReq, 0, 100)
	var lastReq *internalReq
	defer func() {
		for _, r := range reqList {
			self.w.Trigger(r.ID, common.ErrStopped)
		}
		for {
			select {
			case r := <-self.reqProposeC:
				self.w.Trigger(r.ID, common.ErrStopped)
			default:
				break
			}
		}
	}()
	for {
		select {
		case r := <-self.reqProposeC:
			reqList = append(reqList, r)
			lastReq = r
		default:
			if len(reqList) == 0 {
				select {
				case r := <-self.reqProposeC:
					reqList = append(reqList, r)
					lastReq = r
				case <-self.stopChan:
					return
				}
			}
			d, _ := json.Marshal(reqList)
			self.proposeC <- d
			select {
			case <-lastReq.doneC:
			case <-self.stopChan:
				return
			}
			reqList = reqList[:0]
			lastReq = nil
		}
	}
}

func (self *KVNode) queueRequest(req *internalReq) (interface{}, error) {
	ch := self.w.Register(req.ID)
	select {
	case self.reqProposeC <- req:
	default:
		select {
		case self.reqProposeC <- req:
		case <-self.stopChan:
			self.w.Trigger(req.ID, common.ErrStopped)
		case <-time.After(time.Second * 3):
			self.w.Trigger(req.ID, common.ErrTimeout)
		}
	}
	select {
	case rsp := <-ch:
		close(req.doneC)
		if err, ok := rsp.(error); ok {
			return nil, err
		}
		return rsp, nil
	case <-self.stopChan:
		return nil, common.ErrStopped
	}
}

func (self *KVNode) Propose(buf []byte) (interface{}, error) {
	req := &internalReq{
		ID:       self.raftNode.reqIDGen.Next(),
		DataType: 0,
		Data:     buf,
		doneC:    make(chan struct{}),
	}
	return self.queueRequest(req)
}

func (self *KVNode) HTTPPropose(buf []byte) (interface{}, error) {
	req := &internalReq{
		ID:       self.raftNode.reqIDGen.Next(),
		DataType: HTTPReq,
		Data:     buf,
		doneC:    make(chan struct{}),
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
	for i := range ents {
		evnt := ents[i]
		switch evnt.Type {
		case raftpb.EntryNormal:
			if evnt.Data != nil {
				// try redis command
				var reqList []*internalReq
				parseErr := json.Unmarshal(evnt.Data, &reqList)
				if parseErr != nil {
					log.Printf("parse request failed: %v", parseErr)
				}
				for _, req := range reqList {
					if req.DataType == 0 {
						cmd, err := redcon.Parse(req.Data)
						if err != nil {
							self.w.Trigger(req.ID, err)
						} else {
							cmdName := strings.ToLower(string(cmd.Args[0]))
							h, ok := self.router.GetInternalCmdHandler(cmdName)
							if !ok {
								log.Printf("unsupported redis command: %v", cmd)
								self.w.Trigger(req.ID, common.ErrInvalidCommand)
							} else {
								v, err := h(cmd)
								// write the future response or error
								if err != nil {
									self.w.Trigger(req.ID, err)
								} else {
									self.w.Trigger(req.ID, v)
								}
							}
						}
					} else if req.DataType == HTTPReq {
						// try other protocol command
						var dataKv kv
						dec := gob.NewDecoder(bytes.NewBuffer(req.Data))
						var err error
						if err = dec.Decode(&dataKv); err != nil {
							log.Fatalf("could not decode message (%v)\n", err)
						} else {
							err = self.store.LocalPut([]byte(dataKv.Key), []byte(dataKv.Val))
						}
						self.w.Trigger(req.ID, err)
					} else {
						self.w.Trigger(req.ID, errUnknownData)
					}
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
	if np.appliedi-np.snapi <= self.raftNode.snapCount {
		return
	}
	if np.appliedi <= self.raftNode.lastIndex {
		// replaying local log
		return
	}

	log.Printf("start snapshot [applied index: %d | last snapshot index: %d]", np.appliedi, np.snapi)
	self.raftNode.beginSnapshot(np.appliedi, np.confState)

	np.snapi = np.appliedi
}

func (self *KVNode) GetSnapshot() ([]byte, error) {
	// use the rocksdb backup/checkpoint interface to backup data
	var si KVSnapInfo
	var err error
	si.BackupMeta, err = self.store.BackupStore()
	if err != nil {
		return nil, err
	}
	si.LeaderInfo = self.raftNode.GetLeadMember()
	si.Members = self.raftNode.GetMembers()
	backData, _ := json.Marshal(si)
	log.Printf("snapshot data : %v\n", string(backData))
	return backData, nil
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
	hasBackup, _ := self.store.IsBackupOK(si.BackupMeta)
	if !startup {
		if hasBackup {
			self.store.ClearStoreBackup(si.BackupMeta)
		}
	}
	if !hasBackup {
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
		common.RunFileSync(remoteLeader.Broadcast, path.Join(remoteLeader.DataDir, "rocksdb_backup"),
			self.store.GetBackupBase())
	}
	return self.store.RestoreStore(si.BackupMeta)
}
