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
	"log"
	"os"
	"path"
	"strconv"
	"sync"

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
}

type KVOptions struct {
	DataDir          string
	DefaultReadOpts  *gorocksdb.ReadOptions
	DefaultWriteOpts *gorocksdb.WriteOptions
}

type kv struct {
	Key string
	Val string
	Err error
}

func newKVStore(kvopts *KVOptions, proposeC chan<- []byte,
	commitC <-chan *applyCommitEntry, errorC <-chan error) *kvstore {
	s := &kvstore{
		proposeC: proposeC,
		opts:     kvopts,
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

func (s *kvstore) Lookup(key string) (string, error) {
	value, err := s.DBEngine.GetBytes(s.opts.DefaultReadOpts, []byte(key))
	return string(value), err
}

func (s *kvstore) Delete(key string) error {
	err := s.DBEngine.Delete(s.opts.DefaultWriteOpts, []byte(key))
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

func (s *kvstore) WriteBatch() error {
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

func (s *kvstore) Propose(k string, v string) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(kv{Key: k, Val: v}); err != nil {
		log.Fatal(err)
	}
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
			err := s.DBEngine.Put(s.opts.DefaultWriteOpts, []byte(dataKv.Key), []byte(dataKv.Val))
			if err != nil {
				log.Fatalf("failed to write to db: %v\n", err)
			}
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
	log.Printf("backup done, total backup : %v\n", beInfo.GetCount())
	lastID := beInfo.GetBackupId(beInfo.GetCount() - 1)
	for i := 0; i < beInfo.GetCount(); i++ {
		id := beInfo.GetBackupId(i)
		log.Printf("backup data :%v, timestamp: %v, files: %v, size: %v", id, beInfo.GetTimestamp(i), beInfo.GetNumFiles(i),
			beInfo.GetSize(i))
	}
	be.PurgeOldBackups(s.DBEngine, 3)
	beInfo.Destroy()
	be.Close()
	// copy the db files to the snapshot dir
	return []byte(strconv.Itoa(int(lastID))), nil
}

func (s *kvstore) RestoreFromSnapshot(raftSnapshot raftpb.Snapshot) error {
	snapshot := raftSnapshot.Data
	log.Printf("should recovery from snapshot here: %v", string(snapshot))
	// copy backup data from the remote leader node, and recovery backup from it
	// if local has some old backup data, we should use rsync to sync the data file
	// use the rocksdb backup/checkpoint interface to backup data
	opts := gorocksdb.NewDefaultOptions()
	log.Printf("begin restore\n")
	be, err := gorocksdb.OpenBackupEngine(opts, s.opts.DataDir+"/rocksdb_backup")
	if err != nil {
		log.Printf("backup engine failed: %v\n", err)
		return err
	}
	restoreOpts := &gorocksdb.RestoreOptions{}
	err = be.RestoreDBFromLatestBackup(s.opts.DataDir, s.opts.DataDir, restoreOpts)
	if err != nil {
		log.Printf("restore failed: %v\n", err)
		return err
	}
	log.Printf("restore done\n")
	be.Close()
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
