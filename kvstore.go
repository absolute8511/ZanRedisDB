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

package main

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"errors"
	"log"
	"net"
	"net/url"
	"os"
	"path"
	"sync"

	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/absolute8511/ZanRedisDB/rockredis"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
)

const (
	DIR_PERM  = 0755
	FILE_PERM = 0744
)

// a key-value store backed by raft
type kvstore struct {
	proposeC chan<- []byte // channel for proposing updates
	mu       sync.RWMutex
	DBEngine *rockredis.RockDB
	opts     *KVOptions
	raftNode *raftNode
}

type kvSnapInfo struct {
	BackupMeta []byte        `json:"backup_meta"`
	LeaderInfo *MemberInfo   `json:"leader_info"`
	Members    []*MemberInfo `json:"members"`
}

type KVOptions struct {
	DataDir string
	EngType string
}

type kv struct {
	Key   string
	Val   string
	Err   error
	ReqID int64
	// the write operation describe
	Op string
}

func newKVStore(kvopts *KVOptions, r *raftNode, proposeC chan<- []byte,
	commitC <-chan *applyCommitEntry, errorC <-chan error) *kvstore {
	s := &kvstore{
		proposeC: proposeC,
		opts:     kvopts,
		raftNode: r,
	}

	err := s.openDB()
	if err != nil {
		panic(err)
	}
	// read commits from raft into kvStore map until error
	go s.readCommits(commitC, errorC)
	return s
}

func (s *kvstore) openDB() error {
	var err error
	if s.opts.EngType == "rocksdb" {
		dir := path.Join(s.opts.DataDir, "rocksdb")
		os.MkdirAll(dir, DIR_PERM)
		cfg := rockredis.NewRockConfig()
		cfg.DataDir = dir
		s.DBEngine, err = rockredis.OpenRockDB(cfg)
	} else {
		return errors.New("Not recognized engine type")
	}
	return err
}

func (s *kvstore) Clear() error {
	s.DBEngine.Close()
	os.RemoveAll(path.Join(s.opts.DataDir, "rocksdb"))
	return s.openDB()
}

func (s *kvstore) LocalLookup(key string) (string, error) {
	value, err := s.DBEngine.Get([]byte(key))
	return string(value), err
}

func (s *kvstore) LocalDelete(key string) error {
	return s.DBEngine.Delete([]byte(key))
}

func (s *kvstore) LocalPut(key []byte, value []byte) error {
	err := s.DBEngine.Put(key, value)
	if err != nil {
		log.Printf("failed to write key %v to db: %v\n", string(key), err)
	}
	return err
}

func (s *kvstore) ReadRange(startKey string, endKey string, maxNum int) chan common.KVRecord {
	return s.DBEngine.ReadRange([]byte(startKey), []byte(endKey), maxNum)
}

func (s *kvstore) LocalWriteBatch(cmd ...common.WriteCmd) error {
	return nil
}

func (s *kvstore) Lookup(key string) (string, error) {
	// TODO: check master, determine the partition and the dest node
	return s.LocalLookup(key)
}

func (s *kvstore) Put(k string, v string) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(kv{Key: k, Val: v}); err != nil {
		log.Println(err)
		return err
	}
	s.propose(buf)
	// TODO: wait write done here
	return nil
}

func (s *kvstore) propose(buf bytes.Buffer) {
	s.proposeC <- buf.Bytes()
}

func (s *kvstore) readCommits(commitC <-chan *applyCommitEntry, errorC <-chan error) {
	for data := range commitC {
		// signaled to load snapshot
		if !raft.IsEmptySnap(data.snapshot) {
			log.Printf("loading snapshot at term %d and index %d\n", data.snapshot.Metadata.Term, data.snapshot.Metadata.Index)
			if err := s.RestoreFromSnapshot(data.snapshot); err != nil {
				log.Panic(err)
			}
		}

		if data.commitEntry.Data != nil {
			var dataKv kv
			dec := gob.NewDecoder(bytes.NewBuffer(data.commitEntry.Data))
			if err := dec.Decode(&dataKv); err != nil {
				log.Fatalf("could not decode message (%v)\n", err)
			}
			s.LocalPut([]byte(dataKv.Key), []byte(dataKv.Val))
			// TODO: notify the write client this write has been applied to cluster
		}
	}
	if err, ok := <-errorC; ok {
		log.Fatal(err)
	}
}

func (s *kvstore) getSnapshot() ([]byte, error) {
	// use the rocksdb backup/checkpoint interface to backup data
	var si kvSnapInfo
	var err error
	si.BackupMeta, err = s.DBEngine.Backup(s.opts.DataDir + "/rocksdb_backup")
	if err != nil {
		return nil, err
	}
	si.LeaderInfo = s.raftNode.GetLeadMember()
	si.Members = s.raftNode.GetMembers()
	backData, _ := json.Marshal(si)
	log.Printf("snapshot data : %v\n", backData)
	return backData, nil
}

func (s *kvstore) RestoreFromSnapshot(raftSnapshot raftpb.Snapshot) error {
	snapshot := raftSnapshot.Data
	var si kvSnapInfo
	err := json.Unmarshal(snapshot, &si)
	if err != nil {
		return err
	}
	s.raftNode.RestoreMembers(si.Members)
	log.Printf("should recovery from snapshot here: %v", si)
	localBackupPath := path.Join(s.opts.DataDir, "rocksdb_backup")
	hasBackup, err := s.DBEngine.IsLocalBackupOK(localBackupPath, si.BackupMeta)
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
		u, _ := url.Parse(s.raftNode.localRaftAddr)
		h, _, _ := net.SplitHostPort(u.Host)
		if remoteLeader.Broadcast == h {
			remoteLeader.Broadcast = ""
		}
		// copy backup data from the remote leader node, and recovery backup from it
		// if local has some old backup data, we should use rsync to sync the data file
		// use the rocksdb backup/checkpoint interface to backup data
		RunFileSync(remoteLeader.Broadcast, path.Join(remoteLeader.DataDir, "rocksdb_backup"), s.opts.DataDir)
	}
	return s.DBEngine.Restore(localBackupPath, si.BackupMeta)
}

func (s *kvstore) Close() {
	if s.DBEngine != nil {
		s.DBEngine.Close()
		s.DBEngine = nil
	}
}
