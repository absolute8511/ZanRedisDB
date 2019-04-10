package raft

import (
	"encoding/binary"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/youzan/ZanRedisDB/common"
	"github.com/youzan/ZanRedisDB/engine"
	pb "github.com/youzan/ZanRedisDB/raft/raftpb"
	"github.com/youzan/gorocksdb"
)

const (
	startSep      byte = ':'
	stopSep       byte = startSep + 1
	maxWriteBatch      = 1000
	slowStorage        = time.Millisecond * 100
)

// RocksStorage implements the Storage interface backed by rocksdb.
type RocksStorage struct {
	// Protects access to all fields. Most methods of MemoryStorage are
	// run on the raft goroutine, but Append() is run on an application
	// goroutine.
	sync.Mutex
	hardState pb.HardState
	snapshot  pb.Snapshot

	entryDB          *engine.RockEng
	wb               *gorocksdb.WriteBatch
	defaultWriteOpts *gorocksdb.WriteOptions
	defaultReadOpts  *gorocksdb.ReadOptions
	firstIndex       uint64
	lastIndex        uint64
	id               uint64
	gid              uint32
	engShared        bool
}

func NewRocksStorage(id uint64, gid uint32, shared bool, db *engine.RockEng) *RocksStorage {
	ms := &RocksStorage{
		entryDB:          db,
		wb:               gorocksdb.NewWriteBatch(),
		defaultWriteOpts: gorocksdb.NewDefaultWriteOptions(),
		defaultReadOpts:  gorocksdb.NewDefaultReadOptions(),
		id:               id,
		gid:              gid,
		engShared:        shared,
	}
	ms.defaultReadOpts.SetVerifyChecksums(false)
	ms.defaultReadOpts.SetFillCache(false)
	// read raft always hit non-deleted range
	ms.defaultReadOpts.SetIgnoreRangeDeletions(true)

	ms.defaultWriteOpts.DisableWAL(true)
	snap, err := ms.Snapshot()
	if !IsEmptySnap(snap) {
		return ms
	}

	_, err = ms.FirstIndex()
	if err == errNotFound {
		// When starting from scratch populate the list with a dummy entry at term zero.
		ents := make([]pb.Entry, 1)
		ms.reset(ents)
	}
	return ms
}

func (ms *RocksStorage) Eng() *engine.RockEng {
	return ms.entryDB
}

func (ms *RocksStorage) Close() {
	if !ms.engShared {
		ms.entryDB.CloseAll()
	}
}

func (ms *RocksStorage) entryKey(idx uint64) []byte {
	b := make([]byte, 20+1)
	binary.BigEndian.PutUint64(b[0:8], ms.id)
	binary.BigEndian.PutUint32(b[8:12], ms.gid)
	b[12] = startSep
	binary.BigEndian.PutUint64(b[13:21], idx)
	return b
}

func (ms *RocksStorage) parseIndex(key []byte) uint64 {
	return binary.BigEndian.Uint64(key[13:21])
}

// reset resets the entries. Used for testing.
func (ms *RocksStorage) reset(es []pb.Entry) error {
	// Clean out the state.
	batch := ms.wb
	batch.Clear()

	err := ms.deleteFrom(batch, 0)
	if err != nil {
		return err
	}
	err = ms.commitBatch(batch)
	if err != nil {
		return err
	}
	batch.Clear()

	err = ms.writeEnts(batch, es)
	if err != nil {
		return err
	}
	// clear cached index
	ms.setCachedFirstIndex(0)
	ms.setCachedLastIndex(0)
	return ms.commitBatch(batch)
}

func (ms *RocksStorage) entryPrefixStart() []byte {
	b := make([]byte, 13)
	binary.BigEndian.PutUint64(b[0:8], ms.id)
	binary.BigEndian.PutUint32(b[8:12], ms.gid)
	b[12] = startSep
	return b
}

func (ms *RocksStorage) entryPrefixEnd() []byte {
	b := make([]byte, 13)
	binary.BigEndian.PutUint64(b[0:8], ms.id)
	binary.BigEndian.PutUint32(b[8:12], ms.gid)
	b[12] = stopSep
	return b
}

// InitialState implements the Storage interface.
func (ms *RocksStorage) InitialState() (pb.HardState, pb.ConfState, error) {
	return ms.hardState, ms.snapshot.Metadata.ConfState, nil
}

// SetHardState saves the current HardState.
func (ms *RocksStorage) SetHardState(st pb.HardState) error {
	ms.Lock()
	defer ms.Unlock()
	ms.hardState = st
	return nil
}

// Entries implements the Storage interface.
func (ms *RocksStorage) Entries(lo, hi, maxSize uint64) ([]pb.Entry, error) {
	s := time.Now()
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

	es, err := ms.allEntries(lo, hi, maxSize)
	cost := time.Since(s)
	if cost > slowStorage {
		raftLogger.Infof("entries from raft storage slow: %v(%v-%v), %v", len(es), lo, hi, cost)
	}
	return es, err
}

func (ms *RocksStorage) seekEntry(e *pb.Entry, seekTo uint64, reverse bool) (uint64, error) {
	start := ms.entryKey(seekTo)
	stop := ms.entryPrefixEnd()
	if reverse {
		stop = start
		start = ms.entryPrefixStart()
	}
	//raftLogger.Infof("seek %v from %v to %v", seekTo, start, stop)
	opts := engine.IteratorOpts{
		Range:     engine.Range{Min: start, Max: stop, Type: common.RangeClose},
		Reverse:   reverse,
		IgnoreDel: true,
	}
	it, err := engine.NewDBRangeIteratorWithOpts(ms.entryDB.Eng(), opts)
	if err != nil {
		return 0, err
	}
	defer it.Close()
	if !it.Valid() {
		return 0, errNotFound
	}
	index := ms.parseIndex(it.Key())
	//raftLogger.Infof("seeked: %v", index)
	if e == nil {
		return index, nil
	}
	v := it.Value()
	err = e.Unmarshal(v)
	return index, err
}

// Term implements the Storage interface.
func (ms *RocksStorage) Term(idx uint64) (uint64, error) {
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
func (ms *RocksStorage) LastIndex() (uint64, error) {
	index := ms.lastIndexCached()
	if index > 0 {
		return index, nil
	}
	index, err := ms.seekEntry(nil, math.MaxUint64, true)
	if err != nil {
		raftLogger.Infof("failed to found last index: %v", err.Error())
	} else {
		ms.setCachedLastIndex(index)
	}
	return index, err
}

// FirstIndex implements the Storage interface.
func (ms *RocksStorage) FirstIndex() (uint64, error) {
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

func (ms *RocksStorage) setCachedFirstIndex(index uint64) {
	ms.Lock()
	ms.firstIndex = index
	ms.Unlock()
}

func (ms *RocksStorage) setCachedLastIndex(index uint64) {
	atomic.StoreUint64(&ms.lastIndex, index)
}

func (ms *RocksStorage) lastIndexCached() uint64 {
	return atomic.LoadUint64(&ms.lastIndex)
}

func (ms *RocksStorage) firstIndexCached() uint64 {
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
func (ms *RocksStorage) deleteUntil(batch *gorocksdb.WriteBatch, until uint64) error {
	start := ms.entryKey(0)
	stop := ms.entryKey(until)
	raftLogger.Infof("compact raft storage to %d, %v~%v ", until, start, stop)
	rg := gorocksdb.Range{
		Start: start,
		Limit: stop,
	}
	ms.entryDB.Eng().DeleteFilesInRange(rg)
	//batch.DeleteRange(start, stop)
	it, err := engine.NewDBRangeIterator(ms.entryDB.Eng(), start, stop, common.RangeROpen, false)
	if err != nil {
		return err
	}
	defer it.Close()
	cnt := 0
	for ; it.Valid(); it.Next() {
		batch.Delete(it.Key())
		cnt++
	}
	raftLogger.Infof("compact raft storage to %d , cnt: %v", until, cnt)
	ms.entryDB.AddDeletedCnt(int64(cnt))
	return nil
}

// NumEntries return the number of all entries in db
func (ms *RocksStorage) NumEntries() (int, error) {
	var count int
	start := ms.entryKey(0)
	stop := ms.entryPrefixEnd() // Not included in results.
	it, err := engine.NewDBRangeIterator(ms.entryDB.Eng(), start, stop, common.RangeROpen, false)
	if err != nil {
		return 0, err
	}
	defer it.Close()
	for ; it.Valid(); it.Next() {
		count++
	}
	return count, nil
}

func (ms *RocksStorage) allEntries(lo, hi, maxSize uint64) (es []pb.Entry, rerr error) {
	if hi-lo == 1 { // We only need one entry.
		v, err := ms.entryDB.Eng().GetBytesNoLock(ms.defaultReadOpts, ms.entryKey(lo))
		if err != nil {
			return nil, err
		}
		var e pb.Entry
		if err = e.Unmarshal(v); err != nil {
			raftLogger.Infof("failed to unmarshal: %v", v)
			return nil, err
		}
		es = append(es, e)
		return es, nil
	}
	start := ms.entryKey(lo)
	stop := ms.entryKey(hi) // Not included in results.
	opts := engine.IteratorOpts{
		Range:     engine.Range{Min: start, Max: stop, Type: common.RangeROpen},
		Reverse:   false,
		IgnoreDel: true,
	}
	it, err := engine.NewDBRangeIteratorWithOpts(ms.entryDB.Eng(), opts)
	if err != nil {
		return nil, err
	}
	defer it.Close()
	size := uint64(0)
	for ; it.Valid(); it.Next() {
		v := it.Value()
		var e pb.Entry
		if err = e.Unmarshal(v); err != nil {
			raftLogger.Infof("failed to unmarshal: %v", v)
			return nil, err
		}
		size += uint64(e.Size())
		if size > maxSize && len(es) > 0 {
			break
		}
		es = append(es, e)
	}
	return es, err
}

// Snapshot implements the Storage interface.
func (ms *RocksStorage) Snapshot() (pb.Snapshot, error) {
	ms.Lock()
	defer ms.Unlock()
	return ms.snapshot, nil
}

// ApplySnapshot overwrites the contents of this Storage object with
// those of the given snapshot.
// delete all the entries up until the snapshot
// index. But, keep the raft entry at the snapshot index, to make it easier to build the logic; like
// the dummy entry in RocksStorage.
func (ms *RocksStorage) ApplySnapshot(snap pb.Snapshot) error {
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
	ms.setCachedLastIndex(0)
	ms.Unlock()

	batch := ms.wb
	batch.Clear()
	e := pb.Entry{Term: snap.Metadata.Term, Index: snap.Metadata.Index}
	data, err := e.Marshal()
	if err != nil {
		return err
	}
	batch.Put(ms.entryKey(e.Index), data)
	err = ms.deleteUntil(batch, e.Index)
	if err != nil {
		return err
	}
	return ms.commitBatch(batch)
}

// CreateSnapshot makes a snapshot which can be retrieved with Snapshot() and
// can be used to reconstruct the state at that point.
// If any configuration changes have been made since the last compaction,
// the result of the last ApplyConfChange must be passed in.
func (ms *RocksStorage) CreateSnapshot(i uint64, cs *pb.ConfState, data []byte) (pb.Snapshot, error) {
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
	snap := ms.snapshot
	// no need clear first and last index in db since no changed on db

	return snap, nil
}

// Compact discards all log entries prior to compactIndex.
// It is the application's responsibility to not attempt to compact an index
// greater than raftLog.applied.
func (ms *RocksStorage) Compact(compactIndex uint64) error {
	// we should use seek here, since FirstIndex() will return snapshot index
	first, err := ms.seekEntry(nil, 0, false)
	if err != nil {
		return err
	}
	if compactIndex <= first {
		return ErrCompacted
	}
	li, err := ms.LastIndex()
	if err != nil {
		return err
	}
	if compactIndex > li {
		raftLogger.Errorf("compact %d is out of bound lastindex(%d)", compactIndex, li)
		return errors.New("compact is out of bound lastindex")
	}
	ms.setCachedFirstIndex(0)
	batch := ms.wb
	batch.Clear()
	err = ms.deleteUntil(batch, compactIndex)
	if err != nil {
		return err
	}
	return ms.commitBatch(batch)
}

func (ms *RocksStorage) commitBatch(batch *gorocksdb.WriteBatch) error {
	return ms.entryDB.Eng().Write(ms.defaultWriteOpts, batch)
}

// Append the new entries to storage.
func (ms *RocksStorage) Append(entries []pb.Entry) error {
	if len(entries) == 0 {
		return nil
	}
	s := time.Now()
	batch := ms.wb
	batch.Clear()
	err := ms.addEntries(batch, entries)
	if err != nil {
		return err
	}
	err = ms.commitBatch(batch)
	cost := time.Since(s)
	if cost > slowStorage {
		raftLogger.Infof("append to raft storage slow: %v, %v", len(entries), cost)
	}
	return err
}

func (ms *RocksStorage) addEntries(batch *gorocksdb.WriteBatch, entries []pb.Entry) error {
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

	ms.writeEnts(batch, entries)
	laste := entries[len(entries)-1].Index
	ms.setCachedLastIndex(laste)
	if laste < last {
		err = ms.deleteFrom(batch, laste+1)
		if err != nil {
			return err
		}
	}
	return nil
}

func (ms *RocksStorage) writeEnts(batch *gorocksdb.WriteBatch, es []pb.Entry) error {
	total := len(es)
	for idx, e := range es {
		data, err := e.Marshal()
		if err != nil {
			return err
		}
		k := ms.entryKey(e.Index)
		batch.Put(k, data)
		if (idx+1)%maxWriteBatch == 0 && idx < total-maxWriteBatch {
			err = ms.commitBatch(batch)
			if err != nil {
				return err
			}
			batch.Clear()
		}
	}
	return nil
}

func (ms *RocksStorage) deleteFrom(batch *gorocksdb.WriteBatch, from uint64) error {
	start := ms.entryKey(from)
	stop := ms.entryPrefixEnd()
	//batch.DeleteRange(start, stop)
	opts := engine.IteratorOpts{
		Range:     engine.Range{Min: start, Max: stop, Type: common.RangeROpen},
		Reverse:   false,
		IgnoreDel: true,
	}
	it, err := engine.NewDBRangeIteratorWithOpts(ms.entryDB.Eng(), opts)
	if err != nil {
		return err
	}
	defer it.Close()
	cnt := 0
	for ; it.Valid(); it.Next() {
		batch.Delete(it.Key())
		cnt++
	}
	ms.entryDB.AddDeletedCnt(int64(cnt))
	return nil
}
