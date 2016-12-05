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
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/tidwall/redcon"
)

type nodeProgress struct {
	confState raftpb.ConfState
	snapi     uint64
	appliedi  uint64
}

// a key-value node backed by raft
type KVNode struct {
	proposeC chan<- []byte // channel for proposing updates
	raftNode *raftNode
	store    *store.KVStore
	stopping int32
	stopChan chan struct{}
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
		proposeC: proposeC,
		store:    store.NewKVStore(kvopts),
		stopChan: make(chan struct{}),
	}
	s.registerHandler()
	commitC, errorC, raftNode := newRaftNode(clusterID, id, localRaftAddr, kvopts.DataDir,
		peers, join, s, proposeC, confChangeC)
	s.raftNode = raftNode

	raftNode.startRaft(s)
	// read commits from raft into KVStore map until error
	go s.applyCommits(commitC, errorC)
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
	common.RegisterRedisHandler("get", self.getCommand)
	common.RegisterRedisHandler("set", self.setCommand)
	common.RegisterRedisHandler("del", self.delCommand)
	common.RegisterRedisHandler("plget", self.plgetCommand)
	common.RegisterRedisHandler("plset", self.plsetCommand)

	// only write command need to be registered as internal
	common.RegisterRedisInternalHandler("del", self.localDelCommand)
	common.RegisterRedisInternalHandler("set", self.localSetCommand)
	common.RegisterRedisInternalHandler("plset", self.localPlsetCommand)
}

func (self *KVNode) Propose(buf []byte) (interface{}, error) {
	ch := make(chan error)
	self.proposeC <- buf
	// save ch to waiting list
	close(ch)
	err := <-ch
	return nil, err
}

func (self *KVNode) propose(buf bytes.Buffer) {
	self.proposeC <- buf.Bytes()
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
				cmd, err := redcon.Parse(evnt.Data)
				if err != nil {
					// try other protocol command
					var dataKv kv
					dec := gob.NewDecoder(bytes.NewBuffer(evnt.Data))
					if err := dec.Decode(&dataKv); err != nil {
						log.Fatalf("could not decode message (%v)\n", err)
					}
					self.store.LocalPut([]byte(dataKv.Key), []byte(dataKv.Val))
				} else {
					cmdName := strings.ToLower(string(cmd.Args[0]))
					h, ok := common.GetInternalHandler(cmdName)
					if !ok {
						log.Printf("unsupported redis command: %v", cmd)
					} else {
						v, err := h(cmd)
						_ = v
						_ = err
						// write the future response or error
					}
				}
				// TODO: notify the write client this write has been applied to cluster
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
