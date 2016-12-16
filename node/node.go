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
	errInvalidResponse = errors.New("Invalid response type")
	errInvalidCommand  = errors.New("invalid command")
	errSyntaxError     = errors.New("syntax error")
	errStopped         = errors.New("the node stopped")
	errTimeout         = errors.New("queue request timeout")
	errUnknownData     = errors.New("unknown request data type")
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
}

type KVSnapInfo struct {
	BackupMeta []byte        `json:"backup_meta"`
	LeaderInfo *MemberInfo   `json:"leader_info"`
	Members    []*MemberInfo `json:"members"`
}

func NewKVNode(kvopts *store.KVOptions, clusterID uint64, id int, localRaftAddr string,
	peers map[int]string, join bool,
	confChangeC <-chan raftpb.ConfChange) (*KVNode, <-chan struct{}) {
	proposeC := make(chan []byte)
	s := &KVNode{
		reqProposeC: make(chan *internalReq, 200),
		proposeC:    proposeC,
		store:       store.NewKVStore(kvopts),
		stopChan:    make(chan struct{}),
		w:           wait.New(),
	}
	s.registerHandler()
	commitC, errorC, raftNode := newRaftNode(clusterID, id, localRaftAddr, kvopts.DataDir,
		peers, join, s, proposeC, confChangeC)
	s.raftNode = raftNode

	raftNode.startRaft(s)
	// read commits from raft into KVStore map until error
	go s.applyCommits(commitC, errorC)
	go s.handleProposeReq()
	return s, s.stopChan
}

func (self *KVNode) Stop() {
	if !atomic.CompareAndSwapInt32(&self.stopping, 0, 1) {
		return
	}
	self.raftNode.StopNode()
	self.store.Close()
	close(self.stopChan)
}

func (self *KVNode) Clear() error {
	return self.store.Clear()
}

func (self *KVNode) registerHandler() {
	// for kv
	common.RegisterRedisHandler("get", self.getCommand)
	common.RegisterRedisHandler("mget", self.mgetCommand)
	common.RegisterRedisHandler("exists", self.existsCommand)
	common.RegisterRedisHandler("set", self.setCommand)
	common.RegisterRedisHandler("setnx", self.setnxCommand)
	common.RegisterRedisHandler("mset", self.msetCommand)
	common.RegisterRedisHandler("incr", self.incrCommand)
	common.RegisterRedisHandler("del", self.delCommand)
	common.RegisterRedisHandler("plget", self.plgetCommand)
	common.RegisterRedisHandler("plset", self.plsetCommand)
	// for hash
	common.RegisterRedisHandler("hget", self.hgetCommand)
	common.RegisterRedisHandler("hgetall", self.hgetallCommand)
	common.RegisterRedisHandler("hkeys", self.hkeysCommand)
	common.RegisterRedisHandler("hexists", self.hexistsCommand)
	common.RegisterRedisHandler("hmget", self.hmgetCommand)
	common.RegisterRedisHandler("hlen", self.hlenCommand)
	common.RegisterRedisHandler("hset", self.hsetCommand)
	common.RegisterRedisHandler("hmset", self.hmsetCommand)
	common.RegisterRedisHandler("hdel", self.hdelCommand)
	common.RegisterRedisHandler("hincrby", self.hincrbyCommand)
	common.RegisterRedisHandler("hclear", self.hclearCommand)
	// for list
	common.RegisterRedisHandler("lindex", self.lindexCommand)
	common.RegisterRedisHandler("llen", self.llenCommand)
	common.RegisterRedisHandler("lrange", self.lrangeCommand)
	common.RegisterRedisHandler("lpop", self.lpopCommand)
	common.RegisterRedisHandler("lpush", self.lpushCommand)
	common.RegisterRedisHandler("lset", self.lsetCommand)
	common.RegisterRedisHandler("ltrim", self.ltrimCommand)
	common.RegisterRedisHandler("rpop", self.rpopCommand)
	common.RegisterRedisHandler("rpush", self.rpushCommand)
	common.RegisterRedisHandler("lclear", self.lclearCommand)
	// for zset
	common.RegisterRedisHandler("zscore", self.zscoreCommand)
	common.RegisterRedisHandler("zcount", self.zcountCommand)
	common.RegisterRedisHandler("zcard", self.zcardCommand)
	common.RegisterRedisHandler("zlexcount", self.zlexcountCommand)
	common.RegisterRedisHandler("zrange", self.zrangeCommand)
	common.RegisterRedisHandler("zrevrange", self.zrevrangeCommand)
	common.RegisterRedisHandler("zrangebylex", self.zrangebylexCommand)
	common.RegisterRedisHandler("zrangebyscore", self.zrangebyscoreCommand)
	common.RegisterRedisHandler("zrevrangebyscore", self.zrevrangebyscoreCommand)
	common.RegisterRedisHandler("zrank", self.zrankCommand)
	common.RegisterRedisHandler("zrevrank", self.zrevrankCommand)
	common.RegisterRedisHandler("zadd", self.zaddCommand)
	common.RegisterRedisHandler("zincrby", self.zincrbyCommand)
	common.RegisterRedisHandler("zrem", self.zremCommand)
	common.RegisterRedisHandler("zremrangebyrank", self.zremrangebyrankCommand)
	common.RegisterRedisHandler("zremrangebyscore", self.zremrangebyscoreCommand)
	common.RegisterRedisHandler("zremrangebylex", self.zremrangebylexCommand)
	common.RegisterRedisHandler("zclear", self.zclearCommand)

	// only write command need to be registered as internal
	// kv
	common.RegisterRedisInternalHandler("del", self.localDelCommand)
	common.RegisterRedisInternalHandler("set", self.localSetCommand)
	common.RegisterRedisInternalHandler("setnx", self.localSetnxCommand)
	common.RegisterRedisInternalHandler("mset", self.localMSetCommand)
	common.RegisterRedisInternalHandler("incr", self.localIncrCommand)
	common.RegisterRedisInternalHandler("plset", self.localPlsetCommand)
	// hash
	common.RegisterRedisInternalHandler("hset", self.localHSetCommand)
	common.RegisterRedisInternalHandler("hmset", self.localHMsetCommand)
	common.RegisterRedisInternalHandler("hdel", self.localHDelCommand)
	common.RegisterRedisInternalHandler("hincrby", self.localHIncrbyCommand)
	common.RegisterRedisInternalHandler("hclear", self.localHclearCommand)
	// list
	common.RegisterRedisInternalHandler("lpop", self.localLpopCommand)
	common.RegisterRedisInternalHandler("lpush", self.localLpushCommand)
	common.RegisterRedisInternalHandler("lset", self.localLsetCommand)
	common.RegisterRedisInternalHandler("ltrim", self.localLtrimCommand)
	common.RegisterRedisInternalHandler("rpop", self.localRpopCommand)
	common.RegisterRedisInternalHandler("rpush", self.localRpushCommand)
	common.RegisterRedisInternalHandler("lclear", self.localLclearCommand)
	// zset
	common.RegisterRedisInternalHandler("zadd", self.localZaddCommand)
	common.RegisterRedisInternalHandler("zincrby", self.localZincrbyCommand)
	common.RegisterRedisInternalHandler("zrem", self.localZremCommand)
	common.RegisterRedisInternalHandler("zremrangebyrank", self.localZremrangebyrankCommand)
	common.RegisterRedisInternalHandler("zremrangebyscore", self.localZremrangebyscoreCommand)
	common.RegisterRedisInternalHandler("zremrangebylex", self.localZremrangebylexCommand)
	common.RegisterRedisInternalHandler("zclear", self.localZclearCommand)
}

func (self *KVNode) handleProposeReq() {
	reqList := make([]*internalReq, 0, 100)
	var lastReq *internalReq
	defer func() {
		for _, r := range reqList {
			self.w.Trigger(r.ID, errStopped)
		}
		for {
			select {
			case r := <-self.reqProposeC:
				self.w.Trigger(r.ID, errStopped)
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
			self.w.Trigger(req.ID, errStopped)
		case <-time.After(time.Second * 3):
			self.w.Trigger(req.ID, errTimeout)
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
		return nil, errStopped
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
							h, ok := common.GetInternalHandler(cmdName)
							if !ok {
								log.Printf("unsupported redis command: %v", cmd)
								self.w.Trigger(req.ID, errInvalidCommand)
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
