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
	"strings"

	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/absolute8511/ZanRedisDB/store"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/tidwall/redcon"
)

// a key-value node backed by raft
type KVNode struct {
	proposeC chan<- []byte // channel for proposing updates
	raftNode *raftNode
	store    *store.KVStore
}

type KVSnapInfo struct {
	BackupMeta []byte        `json:"backup_meta"`
	LeaderInfo *MemberInfo   `json:"leader_info"`
	Members    []*MemberInfo `json:"members"`
}

func NewKVNode(kvopts *store.KVOptions, clusterID uint64, id int, localRaftAddr string,
	peers map[int]string, join bool,
	confChangeC <-chan raftpb.ConfChange) (*KVNode, <-chan error) {
	proposeC := make(chan []byte)
	s := &KVNode{
		proposeC: proposeC,
		store:    store.NewKVStore(kvopts),
	}
	s.registerHandler()
	getSnapshot := func() ([]byte, error) { return s.GetSnapshot() }
	commitC, errorC, raftNode := newRaftNode(clusterID, id, localRaftAddr, kvopts.DataDir,
		peers, join, getSnapshot, proposeC, confChangeC)
	s.raftNode = raftNode

	// read commits from raft into KVStore map until error
	go s.readCommits(commitC, errorC)
	go raftNode.startRaft(s)
	return s, errorC
}

func (self *KVNode) Stop() {
	self.store.Close()
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

func (self *KVNode) readCommits(commitC <-chan *applyCommitEntry, errorC <-chan error) {
	for data := range commitC {
		// signaled to load snapshot
		if !raft.IsEmptySnap(data.snapshot) {
			log.Printf("loading snapshot at term %d and index %d\n", data.snapshot.Metadata.Term, data.snapshot.Metadata.Index)
			if err := self.RestoreFromSnapshot(data.snapshot); err != nil {
				log.Panic(err)
			}
		}

		if data.commitEntry.Data != nil {
			// try redis command
			cmd, err := redcon.Parse(data.commitEntry.Data)
			if err != nil {
				// try other protocol command
				var dataKv kv
				dec := gob.NewDecoder(bytes.NewBuffer(data.commitEntry.Data))
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
	}
	if err, ok := <-errorC; ok {
		log.Fatal(err)
	}
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
	log.Printf("snapshot data : %v\n", backData)
	return backData, nil
}

func (self *KVNode) RestoreFromSnapshot(raftSnapshot raftpb.Snapshot) error {
	snapshot := raftSnapshot.Data
	var si KVSnapInfo
	err := json.Unmarshal(snapshot, &si)
	if err != nil {
		return err
	}
	self.raftNode.RestoreMembers(si.Members)
	log.Printf("should recovery from snapshot here: %v", si)
	hasBackup, err := self.store.IsBackupOK(si.BackupMeta)
	if err != nil {
		return err
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
		common.RunFileSync(remoteLeader.Broadcast, self.store.GetBackupDir(),
			self.store.GetBackupBase())
	}
	return self.store.RestoreStore(si.BackupMeta)
}
