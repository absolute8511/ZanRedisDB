package engine

import (
	"bytes"

	"github.com/youzan/ZanRedisDB/common"
	"github.com/youzan/gorocksdb"
)

// make sure keep the same with rockredis
const (
	// for data
	KVType   byte = 21
	HashType byte = 22
	tsLen         = 8
)

type Iterator interface {
	Next()
	Prev()
	Valid() bool
	Seek([]byte)
	SeekForPrev([]byte)
	SeekToFirst()
	SeekToLast()
	Close()
	RefKey() []byte
	Key() []byte
	RefValue() []byte
	Value() []byte
	NoTimestamp(vt byte)
}
type emptyIterator struct {
}

func (eit *emptyIterator) Valid() bool {
	return false
}

func (eit *emptyIterator) Next() {
}
func (eit *emptyIterator) Prev() {
}
func (eit *emptyIterator) Seek([]byte) {
}
func (eit *emptyIterator) SeekForPrev([]byte) {
}
func (eit *emptyIterator) SeekToFirst() {
}
func (eit *emptyIterator) SeekToLast() {
}
func (eit *emptyIterator) Close() {
}
func (eit *emptyIterator) RefKey() []byte {
	return nil
}
func (eit *emptyIterator) Key() []byte {
	return nil
}
func (eit *emptyIterator) RefValue() []byte {
	return nil
}
func (eit *emptyIterator) Value() []byte {
	return nil
}
func (eit *emptyIterator) NoTimestamp(vt byte) {
}

type Range struct {
	Min  []byte
	Max  []byte
	Type uint8
}

type Limit struct {
	Offset int
	Count  int
}

type DBIterator struct {
	*gorocksdb.Iterator
	snap         *gorocksdb.Snapshot
	ro           *gorocksdb.ReadOptions
	db           *gorocksdb.DB
	upperBound   *gorocksdb.IterBound
	lowerBound   *gorocksdb.IterBound
	removeTsType byte
}

// low_bound is inclusive
// upper bound is exclusive
func NewDBIterator(db *gorocksdb.DB, withSnap bool, prefixSame bool, lowbound []byte, upbound []byte, ignoreDel bool) (*DBIterator, error) {
	db.RLock()
	if !db.IsOpened() {
		db.RUnlock()
		return nil, common.ErrStopped
	}
	dbit := &DBIterator{
		db: db,
	}
	readOpts := gorocksdb.NewDefaultReadOptions()
	readOpts.SetFillCache(false)
	readOpts.SetVerifyChecksums(false)
	if prefixSame {
		readOpts.SetPrefixSameAsStart(true)
	}
	if lowbound != nil {
		dbit.lowerBound = gorocksdb.NewIterBound(lowbound)
		readOpts.SetIterLowerBound(dbit.lowerBound)
	}
	if upbound != nil {
		dbit.upperBound = gorocksdb.NewIterBound(upbound)
		readOpts.SetIterUpperBound(dbit.upperBound)
	}
	if ignoreDel {
		// may iterator some deleted keys still not compacted.
		readOpts.SetIgnoreRangeDeletions(true)
	}
	dbit.ro = readOpts
	var err error
	if withSnap {
		dbit.snap, err = db.NewSnapshot()
		if err != nil {
			dbit.Close()
			return nil, err
		}
		readOpts.SetSnapshot(dbit.snap)
	}
	dbit.Iterator, err = db.NewIterator(readOpts)
	if err != nil {
		dbit.Close()
		return nil, err
	}
	return dbit, nil
}

func (it *DBIterator) RefKey() []byte {
	return it.Iterator.Key().Data()
}

func (it *DBIterator) Key() []byte {
	return it.Iterator.Key().Bytes()
}

func (it *DBIterator) RefValue() []byte {
	v := it.Iterator.Value().Data()
	if (it.removeTsType == KVType || it.removeTsType == HashType) && len(v) >= tsLen {
		v = v[:len(v)-tsLen]
	}
	return v
}

func (it *DBIterator) Value() []byte {
	v := it.Iterator.Value().Bytes()
	if (it.removeTsType == KVType || it.removeTsType == HashType) && len(v) >= tsLen {
		v = v[:len(v)-tsLen]
	}
	return v
}

func (it *DBIterator) NoTimestamp(vt byte) {
	it.removeTsType = vt
}

func (it *DBIterator) Close() {
	if it.Iterator != nil {
		it.Iterator.Close()
	}
	if it.ro != nil {
		it.ro.Destroy()
	}
	if it.snap != nil {
		it.snap.Release()
	}
	if it.upperBound != nil {
		it.upperBound.Destroy()
	}
	if it.lowerBound != nil {
		it.lowerBound.Destroy()
	}
	it.db.RUnlock()
}

type IteratorOpts struct {
	Range
	Limit
	Reverse   bool
	IgnoreDel bool
	WithSnap  bool
}

func NewDBRangeLimitIteratorWithOpts(db *gorocksdb.DB, opts IteratorOpts) (*RangeLimitedIterator, error) {
	upperBound := opts.Max
	lowerBound := opts.Min
	if opts.Type&common.RangeROpen <= 0 && upperBound != nil {
		// range right not open, we need inclusive the max,
		// however upperBound is exclusive
		upperBound = append(upperBound, 0)
	}

	// TODO: avoid large offset, which will scan too much data.
	// Maybe return error to avoid impact the performance.

	//dbLog.Infof("iterator %v : %v", lowerBound, upperBound)
	dbit, err := NewDBIterator(db, opts.WithSnap, true, lowerBound, upperBound, opts.IgnoreDel)
	if err != nil {
		return nil, err
	}
	if !opts.Reverse {
		return NewRangeLimitIterator(dbit, &opts.Range,
			&opts.Limit), nil
	} else {
		return NewRevRangeLimitIterator(dbit, &opts.Range,
			&opts.Limit), nil
	}
}

// note: all the iterator use the prefix iterator flag. Which means it may skip the keys for different table
// prefix.
func NewDBRangeLimitIterator(db *gorocksdb.DB, min []byte, max []byte, rtype uint8,
	offset int, count int, reverse bool) (*RangeLimitedIterator, error) {

	opts := IteratorOpts{
		Reverse: reverse,
	}
	opts.Max = max
	opts.Min = min
	opts.Type = rtype
	opts.Offset = offset
	opts.Count = count

	return NewDBRangeLimitIteratorWithOpts(db, opts)
}

func NewSnapshotDBRangeLimitIterator(db *gorocksdb.DB, min []byte, max []byte, rtype uint8,
	offset int, count int, reverse bool) (*RangeLimitedIterator, error) {

	opts := IteratorOpts{
		Reverse: reverse,
	}
	opts.Max = max
	opts.Min = min
	opts.Type = rtype
	opts.Offset = offset
	opts.Count = count
	opts.WithSnap = true

	return NewDBRangeLimitIteratorWithOpts(db, opts)
}

func NewDBRangeIterator(db *gorocksdb.DB, min []byte, max []byte, rtype uint8,
	reverse bool) (*RangeLimitedIterator, error) {
	opts := IteratorOpts{
		Reverse: reverse,
	}
	opts.Max = max
	opts.Min = min
	opts.Type = rtype
	return NewDBRangeIteratorWithOpts(db, opts)
}

func NewDBRangeIteratorWithOpts(db *gorocksdb.DB, opts IteratorOpts) (*RangeLimitedIterator, error) {
	upperBound := opts.Max
	lowerBound := opts.Min
	if opts.Type&common.RangeROpen <= 0 && upperBound != nil {
		// range right not open, we need inclusive the max,
		// however upperBound is exclusive
		upperBound = append(upperBound, 0)
	}
	dbit, err := NewDBIterator(db, opts.WithSnap, true, lowerBound, upperBound, opts.IgnoreDel)
	if err != nil {
		return nil, err
	}
	if !opts.Reverse {
		return NewRangeIterator(dbit, &opts.Range), nil
	} else {
		return NewRevRangeIterator(dbit, &opts.Range), nil
	}
}

func NewSnapshotDBRangeIterator(db *gorocksdb.DB, min []byte, max []byte, rtype uint8,
	reverse bool) (*RangeLimitedIterator, error) {
	opts := IteratorOpts{
		Reverse: reverse,
	}
	opts.Max = max
	opts.Min = min
	opts.Type = rtype
	opts.WithSnap = true

	return NewDBRangeIteratorWithOpts(db, opts)
}

type RangeLimitedIterator struct {
	Iterator
	l Limit
	r Range
	// maybe step should not auto increase, we need count for actually element
	step    int
	reverse bool
}

func (it *RangeLimitedIterator) Valid() bool {
	if it.l.Offset < 0 {
		return false
	}
	if it.l.Count >= 0 && it.step >= it.l.Count {
		return false
	}
	if !it.Iterator.Valid() {
		return false
	}

	if !it.reverse {
		if it.r.Max != nil {
			r := bytes.Compare(it.Iterator.RefKey(), it.r.Max)
			if it.r.Type&common.RangeROpen > 0 {
				return !(r >= 0)
			} else {
				return !(r > 0)
			}
		}
	} else {
		if it.r.Min != nil {
			r := bytes.Compare(it.Iterator.RefKey(), it.r.Min)
			if it.r.Type&common.RangeLOpen > 0 {
				return !(r <= 0)
			} else {
				return !(r < 0)
			}
		}
	}
	return true
}

func (it *RangeLimitedIterator) Next() {
	it.step++
	if !it.reverse {
		it.Iterator.Next()
	} else {
		it.Iterator.Prev()
	}
}

func NewRangeLimitIterator(i Iterator, r *Range, l *Limit) *RangeLimitedIterator {
	return rangeLimitIterator(i, r, l, false)
}
func NewRevRangeLimitIterator(i Iterator, r *Range, l *Limit) *RangeLimitedIterator {
	return rangeLimitIterator(i, r, l, true)
}
func NewRangeIterator(i Iterator, r *Range) *RangeLimitedIterator {
	return rangeLimitIterator(i, r, &Limit{0, -1}, false)
}
func NewRevRangeIterator(i Iterator, r *Range) *RangeLimitedIterator {
	return rangeLimitIterator(i, r, &Limit{0, -1}, true)
}
func rangeLimitIterator(i Iterator, r *Range, l *Limit, reverse bool) *RangeLimitedIterator {
	it := &RangeLimitedIterator{
		Iterator: i,
		l:        *l,
		r:        *r,
		reverse:  reverse,
		step:     0,
	}
	if l.Offset < 0 {
		return it
	}
	if !reverse {
		if r.Min == nil {
			it.Iterator.SeekToFirst()
		} else {
			it.Iterator.Seek(r.Min)
			if r.Type&common.RangeLOpen > 0 {
				if it.Iterator.Valid() && bytes.Equal(it.Iterator.RefKey(), r.Min) {
					it.Iterator.Next()
				}
			}
		}
	} else {
		if r.Max == nil {
			it.Iterator.SeekToLast()
		} else {
			it.Iterator.SeekForPrev(r.Max)
			if !it.Iterator.Valid() {
				it.Iterator.SeekToLast()
				if it.Iterator.Valid() && bytes.Compare(it.Iterator.RefKey(), r.Max) == 1 {
					dbLog.Infof("iterator seek to last key %v should not great than seek to max %v", it.Iterator.RefKey(), r.Max)
				}
			}
			if r.Type&common.RangeROpen > 0 {
				if it.Iterator.Valid() && bytes.Equal(it.Iterator.RefKey(), r.Max) {
					it.Iterator.Prev()
				}
			}
		}
	}
	for i := 0; i < l.Offset; i++ {
		if it.Iterator.Valid() {
			if !it.reverse {
				it.Iterator.Next()
			} else {
				it.Iterator.Prev()
			}
		}
	}
	return it
}
