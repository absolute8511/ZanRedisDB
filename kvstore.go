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
	"time"

	"github.com/absolute8511/gorocksdb"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
)

const (
	DIR_PERM = 0750
)

// a key-value store backed by raft
type kvstore struct {
	proposeC chan<- []byte // channel for proposing updates
	mu       sync.RWMutex
	DBEngine *gorocksdb.DB
	opts     *KVOptions
	raftNode *raftNode
}

type kvSnapInfo struct {
	BackupID   int64         `json:"backup_id"`
	LeaderInfo *MemberInfo   `json:"leader_info"`
	Members    []*MemberInfo `json:"members"`
}

type KVOptions struct {
	DataDir          string
	DefaultReadOpts  *gorocksdb.ReadOptions
	DefaultWriteOpts *gorocksdb.WriteOptions
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
	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
	//bbto.SetBlockCache(gorocksdb.NewLRUCache(3 << 30))
	filter := gorocksdb.NewBloomFilter(10)
	bbto.SetFilterPolicy(filter)
	opts := gorocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(bbto)
	opts.SetCreateIfMissing(true)
	opts.SetMaxOpenFiles(1000000)

	var err error
	dir := path.Join(s.opts.DataDir, "rocksdb")
	os.MkdirAll(dir, DIR_PERM)
	s.DBEngine, err = gorocksdb.OpenDb(opts, dir)
	return err
}

func (s *kvstore) Clear() error {
	s.DBEngine.Close()
	os.RemoveAll(path.Join(s.opts.DataDir, "rocksdb"))
	return s.openDB()
}

func (s *kvstore) LocalLookup(key string) (string, error) {
	value, err := s.DBEngine.GetBytes(s.opts.DefaultReadOpts, []byte(key))
	return string(value), err
}

func (s *kvstore) LocalDelete(key string) error {
	err := s.DBEngine.Delete(s.opts.DefaultWriteOpts, []byte(key))
	return err
}

func (s *kvstore) LocalPut(key []byte, value []byte) error {
	err := s.DBEngine.Put(s.opts.DefaultWriteOpts, key, value)
	if err != nil {
		log.Printf("failed to write key %v to db: %v\n", string(key), err)
	}
	return err
}

func (s *kvstore) ReadRange(startKey string, endKey string, maxNum int) chan kv {
	retChan := make(chan kv, 10)
	go func() {
		ro := gorocksdb.NewDefaultReadOptions()
		ro.SetFillCache(false)
		it := s.DBEngine.NewIterator(ro)
		defer it.Close()
		it.Seek([]byte(startKey))
		for it = it; it.Valid(); it.Next() {
			key := it.Key()
			value := it.Value()
			retChan <- kv{Key: string(key.Data()), Val: string(value.Data())}
			key.Free()
			value.Free()
		}
		if err := it.Err(); err != nil {
			retChan <- kv{Err: err}
		}
		close(retChan)
	}()
	return retChan
}

func (s *kvstore) LocalWriteBatch() error {
	wb := gorocksdb.NewWriteBatch()
	// defer wb.Close or use wb.Clear and reuse.
	wb.Delete([]byte("foo"))
	wb.Put([]byte("foo"), []byte("bar"))
	wb.Put([]byte("bar"), []byte("foo"))
	err := s.DBEngine.Write(s.opts.DefaultWriteOpts, wb)
	if err != nil {
		log.Printf("batch write failed\n")
	}
	return err
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
	opts := gorocksdb.NewDefaultOptions()
	log.Printf("begin backup \n")
	start := time.Now()
	be, err := gorocksdb.OpenBackupEngine(opts, s.opts.DataDir+"/rocksdb_backup")
	if err != nil {
		log.Printf("backup engine failed: %v", err)
		return nil, err
	}
	err = be.CreateNewBackup(s.DBEngine)
	if err != nil {
		log.Printf("backup engine failed: %v", err)
		return nil, err
	}
	beInfo := be.GetInfo()
	cost := time.Now().Sub(start)
	log.Printf("backup done (cost %v), total backup : %v\n", cost.String(), beInfo.GetCount())
	lastID := beInfo.GetBackupId(beInfo.GetCount() - 1)
	for i := 0; i < beInfo.GetCount(); i++ {
		id := beInfo.GetBackupId(i)
		log.Printf("backup data :%v, timestamp: %v, files: %v, size: %v", id, beInfo.GetTimestamp(i), beInfo.GetNumFiles(i),
			beInfo.GetSize(i))
	}
	be.PurgeOldBackups(s.DBEngine, 3)
	beInfo.Destroy()
	be.Close()
	var si kvSnapInfo
	si.BackupID = lastID
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
	opts := gorocksdb.NewDefaultOptions()
	localBackupPath := path.Join(s.opts.DataDir, "rocksdb_backup")
	be, err := gorocksdb.OpenBackupEngine(opts, localBackupPath)
	if err != nil {
		log.Printf("backup engine open failed: %v", err)
		return err
	}
	beInfo := be.GetInfo()
	lastID := int64(0)
	if beInfo.GetCount() > 0 {
		lastID = beInfo.GetBackupId(beInfo.GetCount() - 1)
	}
	log.Printf("local total backup : %v, last: %v\n", beInfo.GetCount(), lastID)
	hasBackup := false
	for i := 0; i < beInfo.GetCount(); i++ {
		id := beInfo.GetBackupId(i)
		log.Printf("backup data :%v, timestamp: %v, files: %v, size: %v", id, beInfo.GetTimestamp(i), beInfo.GetNumFiles(i),
			beInfo.GetSize(i))
		if id == si.BackupID {
			hasBackup = true
			break
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
		u, _ := url.Parse(s.raftNode.localRaftAddr)
		h, _, _ := net.SplitHostPort(u.Host)
		if remoteLeader.Broadcast == h {
			remoteLeader.Broadcast = ""
		}
		// copy backup data from the remote leader node, and recovery backup from it
		// if local has some old backup data, we should use rsync to sync the data file
		// use the rocksdb backup/checkpoint interface to backup data
		RunFileSync(remoteLeader.Broadcast, path.Join(remoteLeader.DataDir, "rocksdb_backup"), s.opts.DataDir)
		be.Close()
		be, err = gorocksdb.OpenBackupEngine(opts, localBackupPath)
		if err != nil {
			log.Printf("backup engine open failed: %v", err)
			return err
		}
		beInfo = be.GetInfo()
		lastID = int64(0)
		if beInfo.GetCount() > 0 {
			lastID = beInfo.GetBackupId(beInfo.GetCount() - 1)
		}
		log.Printf("after sync total backup : %v, last: %v\n", beInfo.GetCount(), lastID)
		for i := 0; i < beInfo.GetCount(); i++ {
			id := beInfo.GetBackupId(i)
			log.Printf("backup data :%v, timestamp: %v, files: %v, size: %v", id, beInfo.GetTimestamp(i), beInfo.GetNumFiles(i),
				beInfo.GetSize(i))
		}
	}
	start := time.Now()
	log.Printf("begin restore\n")
	s.DBEngine.Close()
	restoreOpts := gorocksdb.NewRestoreOptions()
	dir := path.Join(s.opts.DataDir, "rocksdb")
	err = be.RestoreDBFromLatestBackup(dir, dir, restoreOpts)
	if err != nil {
		log.Printf("restore failed: %v\n", err)
		return err
	}
	log.Printf("restore done, cost: %v\n", time.Now().Sub(start))
	be.Close()
	s.openDB()
	return nil
}

func (s *kvstore) Close() {
	if s.opts.DefaultReadOpts != nil {
		s.opts.DefaultReadOpts.Destroy()
	}
	if s.opts.DefaultWriteOpts != nil {
		s.opts.DefaultWriteOpts.Destroy()
	}
	if s.DBEngine != nil {
		s.DBEngine.Close()
		s.DBEngine = nil
	}
}
