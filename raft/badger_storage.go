/*
 * Copyright 2018 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package raft

import (
	"bytes"
	"encoding/binary"
	"math"
	"sync"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/dgraph-io/badger/options"

	pb "github.com/youzan/ZanRedisDB/raft/raftpb"
)

const maxDeleteBatch = 1000

// BadgerStorage implements the Storage interface backed by badger.
type BadgerStorage struct {
	// Protects access to all fields. Most methods of MemoryStorage are
	// run on the raft goroutine, but Append() is run on an application
	// goroutine.
	sync.Mutex
	hardState pb.HardState
	snapshot  pb.Snapshot

	entryDB             *badger.DB
	firstIndex          uint64
	id                  uint64
	gid                 uint32
	vlogTicker          *time.Ticker
	mandatoryVlogTicker *time.Ticker
}

func setBadgerOptions(opt badger.Options, dir string) badger.Options {
	opt.SyncWrites = false
	opt.Truncate = true
	opt.Dir = dir
	opt.ValueDir = dir
	opt.Logger = raftLogger

	//opt.ValueLogLoadingMode = options.MemoryMap
	opt.ValueLogLoadingMode = options.FileIO
	return opt
}

func NewBadgerStorage(id uint64, gid uint32, dir string) (*BadgerStorage, error) {
	raftLogger.Infof("using badger raft storage dir:%v", dir)
	opt := badger.LSMOnlyOptions
	opt = setBadgerOptions(opt, dir)
	opt.ValueLogMaxEntries = 10000 // Allow for easy space reclamation.

	// We should always force load LSM tables to memory, disregarding user settings, because
	// Raft.Advance hits the WAL many times. If the tables are not in memory, retrieval slows
	// down way too much, causing cluster membership issues. Because of prefix compression and
	// value separation provided by Badger, this is still better than using the memory based WAL
	// storage provided by the Raft library.
	opt.TableLoadingMode = options.LoadToRAM

	db, err := badger.Open(opt)
	if err != nil {
		return nil, err
	}
	ms := &BadgerStorage{
		entryDB: db,
		id:      id,
		gid:     gid,
	}
	ms.vlogTicker = time.NewTicker(1 * time.Minute)
	ms.mandatoryVlogTicker = time.NewTicker(10 * time.Minute)
	go ms.runVlogGC(db)
	snap, err := ms.Snapshot()
	if !IsEmptySnap(snap) {
		return ms, nil
	}

	_, err = ms.FirstIndex()
	if err == errNotFound {
		// When starting from scratch populate the list with a dummy entry at term zero.
		ents := make([]pb.Entry, 1)
		ms.reset(ents)
		//raftLogger.Infof("init badger db: %v", db)
	}
	return ms, nil
}

func (ms *BadgerStorage) runVlogGC(store *badger.DB) {
	// Get initial size on start.
	_, lastVlogSize := store.Size()
	const GB = int64(1 << 30)

	runGC := func() {
		var err error
		for err == nil {
			// If a GC is successful, immediately run it again.
			err = store.RunValueLogGC(0.6)
		}
		_, lastVlogSize = store.Size()
	}

	for {
		select {
		case <-ms.vlogTicker.C:
			_, currentVlogSize := store.Size()
			if currentVlogSize < lastVlogSize+GB {
				continue
			}
			runGC()
		case <-ms.mandatoryVlogTicker.C:
			runGC()
		}
	}
}

func (ms *BadgerStorage) Close() {
	ms.vlogTicker.Stop()
	ms.mandatoryVlogTicker.Stop()
	ms.entryDB.Close()
}

func (ms *BadgerStorage) entryKey(idx uint64) []byte {
	b := make([]byte, 20)
	binary.BigEndian.PutUint64(b[0:8], ms.id)
	binary.BigEndian.PutUint32(b[8:12], ms.gid)
	binary.BigEndian.PutUint64(b[12:20], idx)
	return b
}

func parseIndex(key []byte) uint64 {
	return binary.BigEndian.Uint64(key[12:20])
}

// reset resets the entries. Used for testing.
func (ms *BadgerStorage) reset(es []pb.Entry) error {
	// Clean out the state.
	batch := ms.entryDB.NewWriteBatch()
	defer batch.Cancel()

	if err := ms.deleteFrom(batch, 0); err != nil {
		return err
	}

	for _, e := range es {
		data, err := e.Marshal()
		if err != nil {
			return err
		}
		k := ms.entryKey(e.Index)
		if err := batch.Set(k, data, 0); err != nil {
			return err
		}
	}
	// clear cached index
	ms.setCachedFirstIndex(0)
	return batch.Flush()
}

func (ms *BadgerStorage) entryPrefix() []byte {
	b := make([]byte, 12)
	binary.BigEndian.PutUint64(b[0:8], ms.id)
	binary.BigEndian.PutUint32(b[8:12], ms.gid)
	return b
}

// InitialState implements the Storage interface.
func (ms *BadgerStorage) InitialState() (pb.HardState, pb.ConfState, error) {
	return ms.hardState, ms.snapshot.Metadata.ConfState, nil
}

// SetHardState saves the current HardState.
func (ms *BadgerStorage) SetHardState(st pb.HardState) error {
	ms.Lock()
	defer ms.Unlock()
	ms.hardState = st
	return nil
}

// Entries implements the Storage interface.
func (ms *BadgerStorage) Entries(lo, hi, maxSize uint64) ([]pb.Entry, error) {
	first, err := ms.FirstIndex()
	if err != nil {
		return nil, err
	}
	if lo < first {
		return nil, ErrCompacted
	}

	last, err := ms.LastIndex()
	if err != nil {
		return nil, err
	}
	if hi > last+1 {
		return nil, ErrUnavailable
	}

	return ms.allEntries(lo, hi, maxSize)
}

func (ms *BadgerStorage) seekEntry(e *pb.Entry, seekTo uint64, reverse bool) (uint64, error) {
	var index uint64
	err := ms.entryDB.View(func(txn *badger.Txn) error {
		opt := badger.DefaultIteratorOptions
		opt.PrefetchValues = false
		opt.Prefix = ms.entryPrefix()
		opt.Reverse = reverse
		itr := txn.NewIterator(opt)
		defer itr.Close()

		itr.Seek(ms.entryKey(seekTo))
		if !itr.Valid() {
			return errNotFound
		}
		item := itr.Item()
		index = parseIndex(item.Key())
		if e == nil {
			return nil
		}
		return item.Value(func(val []byte) error {
			return e.Unmarshal(val)
		})
	})
	return index, err
}

// Term implements the Storage interface.
func (ms *BadgerStorage) Term(idx uint64) (uint64, error) {
	first, err := ms.FirstIndex()
	if err != nil {
		return 0, err
	}
	if idx < first-1 {
		return 0, ErrCompacted
	}

	var e pb.Entry
	if _, err := ms.seekEntry(&e, idx, false); err == errNotFound {
		return 0, ErrUnavailable
	} else if err != nil {
		return 0, err
	}
	if idx < e.Index {
		return 0, ErrCompacted
	}
	return e.Term, nil
}

// LastIndex implements the Storage interface.
func (ms *BadgerStorage) LastIndex() (uint64, error) {
	return ms.seekEntry(nil, math.MaxUint64, true)
}

// FirstIndex implements the Storage interface.
func (ms *BadgerStorage) FirstIndex() (uint64, error) {
	index := ms.firstIndexCached()
	if index > 0 {
		return index, nil
	}
	index, err := ms.seekEntry(nil, 0, false)
	if err == nil {
		ms.setCachedFirstIndex(index + 1)
	}
	return index + 1, err
}

func (ms *BadgerStorage) setCachedFirstIndex(index uint64) {
	ms.Lock()
	ms.firstIndex = index
	ms.Unlock()
}

func (ms *BadgerStorage) firstIndexCached() uint64 {
	ms.Lock()
	defer ms.Unlock()
	snap := ms.snapshot
	if !IsEmptySnap(snap) {
		return snap.Metadata.Index + 1
	}
	if ms.firstIndex > 0 {
		return ms.firstIndex
	}
	return 0
}

// Delete all entries from [0, until), i.e. excluding until.
// Keep the entry at the snapshot index, for simplification of logic.
// It is the application's responsibility to not attempt to deleteUntil an index
// greater than raftLog.applied.
func (ms *BadgerStorage) deleteUntil(batch *badger.WriteBatch, until uint64, maxNum int) (bool, error) {
	keys := make([]string, 0, maxNum/2)
	hasMore := false
	err := ms.entryDB.View(func(txn *badger.Txn) error {
		opt := badger.DefaultIteratorOptions
		opt.PrefetchValues = false
		opt.Prefix = ms.entryPrefix()
		itr := txn.NewIterator(opt)
		defer itr.Close()

		start := ms.entryKey(0)
		first := true
		var index uint64
		for itr.Seek(start); itr.Valid(); itr.Next() {
			item := itr.Item()
			index = parseIndex(item.Key())
			if first {
				first = false
				if until <= index {
					return ErrCompacted
				}
			}
			if index >= until {
				break
			}
			keys = append(keys, string(item.Key()))
			if len(keys) > maxNum {
				hasMore = true
				break
			}
		}
		return nil
	})
	if err != nil {
		return hasMore, err
	}
	return hasMore, deleteKeys(batch, keys)
}

func deleteKeys(batch *badger.WriteBatch, keys []string) error {
	if len(keys) == 0 {
		return nil
	}

	for _, k := range keys {
		if err := batch.Delete([]byte(k)); err != nil {
			return err
		}
	}
	return nil
}

// NumEntries return the number of all entries in db
func (ms *BadgerStorage) NumEntries() (int, error) {
	var count int
	err := ms.entryDB.View(func(txn *badger.Txn) error {
		opt := badger.DefaultIteratorOptions
		opt.PrefetchValues = false
		opt.Prefix = ms.entryPrefix()
		itr := txn.NewIterator(opt)
		defer itr.Close()

		start := ms.entryKey(0)
		for itr.Seek(start); itr.Valid(); itr.Next() {
			count++
		}
		return nil
	})
	return count, err
}

func (ms *BadgerStorage) allEntries(lo, hi, maxSize uint64) (es []pb.Entry, rerr error) {
	err := ms.entryDB.View(func(txn *badger.Txn) error {
		if hi-lo == 1 { // We only need one entry.
			item, err := txn.Get(ms.entryKey(lo))
			if err != nil {
				return err
			}
			return item.Value(func(val []byte) error {
				var e pb.Entry
				if err = e.Unmarshal(val); err != nil {
					return err
				}
				es = append(es, e)
				return nil
			})
		}

		iopt := badger.DefaultIteratorOptions
		iopt.Prefix = ms.entryPrefix()
		itr := txn.NewIterator(iopt)
		defer itr.Close()

		start := ms.entryKey(lo)
		end := ms.entryKey(hi) // Not included in results.

		var size uint64
		first := true
		for itr.Seek(start); itr.Valid(); itr.Next() {
			item := itr.Item()
			var e pb.Entry
			if err := item.Value(func(val []byte) error {
				return e.Unmarshal(val)
			}); err != nil {
				return err
			}
			// If this Assert does not fail, then we can safely remove that strange append fix
			// below.
			//x.AssertTrue(e.Index > lastIndex && e.Index >= lo)
			//lastIndex = e.Index
			if bytes.Compare(item.Key(), end) >= 0 {
				break
			}
			size += uint64(e.Size())
			if size > maxSize && !first {
				break
			}
			es = append(es, e)
			first = false
		}
		return nil
	})
	return es, err
}

// Snapshot implements the Storage interface.
func (ms *BadgerStorage) Snapshot() (pb.Snapshot, error) {
	ms.Lock()
	defer ms.Unlock()
	return ms.snapshot, nil
}

// ApplySnapshot overwrites the contents of this Storage object with
// those of the given snapshot.
// delete all the entries up until the snapshot
// index. But, keep the raft entry at the snapshot index, to make it easier to build the logic; like
// the dummy entry in BadgerStorage.
func (ms *BadgerStorage) ApplySnapshot(snap pb.Snapshot) error {
	ms.Lock()

	//handle check for old snapshot being applied
	msIndex := ms.snapshot.Metadata.Index
	snapIndex := snap.Metadata.Index
	if msIndex >= snapIndex {
		ms.Unlock()
		return ErrSnapOutOfDate
	}
	ms.snapshot = snap
	// clear cached first index
	ms.firstIndex = 0
	ms.Unlock()

	batch := ms.entryDB.NewWriteBatch()
	defer batch.Cancel()
	e := pb.Entry{Term: snap.Metadata.Term, Index: snap.Metadata.Index}
	data, err := e.Marshal()
	if err != nil {
		return err
	}
	if err := batch.Set(ms.entryKey(e.Index), data, 0); err != nil {
		return err
	}
	return batch.Flush()
}

// CreateSnapshot makes a snapshot which can be retrieved with Snapshot() and
// can be used to reconstruct the state at that point.
// If any configuration changes have been made since the last compaction,
// the result of the last ApplyConfChange must be passed in.
func (ms *BadgerStorage) CreateSnapshot(i uint64, cs *pb.ConfState, data []byte) (pb.Snapshot, error) {
	first, err := ms.FirstIndex()
	if err != nil {
		return pb.Snapshot{}, err
	}
	if i < first {
		return pb.Snapshot{}, ErrSnapOutOfDate
	}

	var e pb.Entry
	if _, err := ms.seekEntry(&e, i, false); err != nil {
		return pb.Snapshot{}, err
	}
	if e.Index != i {
		return pb.Snapshot{}, errNotFound
	}

	ms.Lock()
	defer ms.Unlock()
	ms.snapshot.Metadata.Index = i
	ms.snapshot.Metadata.Term = e.Term
	if cs != nil {
		ms.snapshot.Metadata.ConfState = *cs
	}
	ms.snapshot.Data = data
	// clear cached first index
	ms.firstIndex = 0
	snap := ms.snapshot

	return snap, nil
}

// Compact discards all log entries prior to compactIndex.
// It is the application's responsibility to not attempt to compact an index
// greater than raftLog.applied.
func (ms *BadgerStorage) Compact(compactIndex uint64) error {
	first, err := ms.FirstIndex()
	if err != nil {
		return err
	}
	if compactIndex <= first-1 {
		return ErrCompacted
	}
	li, err := ms.LastIndex()
	if err != nil {
		return err
	}
	if compactIndex > li {
		raftLogger.Errorf("compact %d is out of bound lastindex(%d)", compactIndex, li)
	}
	ms.setCachedFirstIndex(0)
	for {
		batch := ms.entryDB.NewWriteBatch()
		defer batch.Cancel()
		hasMore, err := ms.deleteUntil(batch, compactIndex, maxDeleteBatch)
		if err != nil {
			return err
		}
		err = batch.Flush()
		if err != nil {
			return err
		}
		if !hasMore {
			return nil
		}
	}
}

// Append the new entries to storage.
// TODO (xiangli): ensure the entries are continuous and
// entries[0].Index > ms.entries[0].Index
func (ms *BadgerStorage) Append(entries []pb.Entry) error {
	batch := ms.entryDB.NewWriteBatch()
	defer batch.Cancel()
	err := ms.addEntries(batch, entries)
	if err != nil {
		return err
	}
	return batch.Flush()
}

func (ms *BadgerStorage) addEntries(batch *badger.WriteBatch, entries []pb.Entry) error {
	if len(entries) == 0 {
		return nil
	}

	first, err := ms.FirstIndex()
	if err != nil {
		return err
	}
	entryFirst := entries[0].Index
	entryLast := entryFirst + uint64(len(entries)) - 1

	// shortcut if there is no new entry.
	if entryLast < first {
		return nil
	}
	// truncate compacted entries
	if first > entryFirst {
		entries = entries[first-entryFirst:]
	}

	last, err := ms.LastIndex()
	if err != nil {
		return err
	}

	for _, e := range entries {
		k := ms.entryKey(e.Index)
		data, err := e.Marshal()
		if err != nil {
			return err
		}
		if err := batch.Set(k, data, 0); err != nil {
			return err
		}
	}
	laste := entries[len(entries)-1].Index
	if laste < last {
		return ms.deleteFrom(batch, laste+1)
	}
	return nil
}

// maybe control batch size outside
func (ms *BadgerStorage) deleteFrom(batch *badger.WriteBatch, from uint64) error {
	keys := make([]string, 0, 100)
	err := ms.entryDB.View(func(txn *badger.Txn) error {
		start := ms.entryKey(from)
		opt := badger.DefaultIteratorOptions
		opt.PrefetchValues = false
		opt.Prefix = ms.entryPrefix()
		itr := txn.NewIterator(opt)
		defer itr.Close()

		for itr.Seek(start); itr.Valid(); itr.Next() {
			key := itr.Item().Key()
			keys = append(keys, string(key))
		}
		return nil
	})
	if err != nil {
		return err
	}
	return deleteKeys(batch, keys)
}
