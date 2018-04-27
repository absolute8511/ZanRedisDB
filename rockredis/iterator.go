package rockredis

import (
	"bytes"

	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/absolute8511/gorocksdb"
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
	removeTsType byte
}

// low_bound is inclusive
// upper bound is exclusive
func NewDBIterator(db *gorocksdb.DB, withSnap bool, prefixSame bool, lowbound []byte, upbound []byte, ignoreDel bool) (*DBIterator, error) {
	db.RLock()
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
		readOpts.SetIterLowerBound(lowbound)
	}
	if upbound != nil {
		readOpts.SetIterUpperBound(upbound)
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
	it.db.RUnlock()
}

// note: all the iterator use the prefix iterator flag. Which means it may skip the keys for different table
// prefix.
func NewDBRangeLimitIterator(db *gorocksdb.DB, min []byte, max []byte, rtype uint8,
	offset int, count int, reverse bool) (*RangeLimitedIterator, error) {
	upperBound := max
	lowerBound := min
	if rtype&common.RangeROpen <= 0 && upperBound != nil {
		// range right not open, we need inclusive the max,
		// however upperBound is exclusive
		upperBound = append(upperBound, 0)
	}

	dbit, err := NewDBIterator(db, false, true, lowerBound, upperBound, false)
	if err != nil {
		return nil, err
	}
	if !reverse {
		return NewRangeLimitIterator(dbit, &Range{Min: min, Max: max, Type: rtype},
			&Limit{Offset: offset, Count: count}), nil
	} else {
		return NewRevRangeLimitIterator(dbit, &Range{Min: min, Max: max, Type: rtype},
			&Limit{Offset: offset, Count: count}), nil
	}
}

func NewSnapshotDBRangeLimitIterator(db *gorocksdb.DB, min []byte, max []byte, rtype uint8,
	offset int, count int, reverse bool) (*RangeLimitedIterator, error) {
	upperBound := max
	lowerBound := min
	if rtype&common.RangeROpen <= 0 && upperBound != nil {
		// range right not open, we need inclusive the max,
		// however upperBound is exclusive
		upperBound = append(upperBound, 0)
	}
	dbit, err := NewDBIterator(db, true, true, lowerBound, upperBound, false)
	if err != nil {
		return nil, err
	}
	if !reverse {
		return NewRangeLimitIterator(dbit, &Range{Min: min, Max: max, Type: rtype},
			&Limit{Offset: offset, Count: count}), nil
	} else {
		return NewRevRangeLimitIterator(dbit, &Range{Min: min, Max: max, Type: rtype},
			&Limit{Offset: offset, Count: count}), nil
	}
}

func NewDBRangeIterator(db *gorocksdb.DB, min []byte, max []byte, rtype uint8,
	reverse bool) (*RangeLimitedIterator, error) {
	upperBound := max
	lowerBound := min
	if rtype&common.RangeROpen <= 0 && upperBound != nil {
		// range right not open, we need inclusive the max,
		// however upperBound is exclusive
		upperBound = append(upperBound, 0)
	}
	dbit, err := NewDBIterator(db, false, true, lowerBound, upperBound, false)
	if err != nil {
		return nil, err
	}
	if !reverse {
		return NewRangeIterator(dbit, &Range{Min: min, Max: max, Type: rtype}), nil
	} else {
		return NewRevRangeIterator(dbit, &Range{Min: min, Max: max, Type: rtype}), nil
	}
}

func NewSnapshotDBRangeIterator(db *gorocksdb.DB, min []byte, max []byte, rtype uint8,
	reverse bool) (*RangeLimitedIterator, error) {
	upperBound := max
	lowerBound := min
	if rtype&common.RangeROpen <= 0 && upperBound != nil {
		// range right not open, we need inclusive the max,
		// however upperBound is exclusive
		upperBound = append(upperBound, 0)
	}
	dbit, err := NewDBIterator(db, true, true, lowerBound, upperBound, false)
	if err != nil {
		return nil, err
	}
	if !reverse {
		return NewRangeIterator(dbit, &Range{Min: min, Max: max, Type: rtype}), nil
	} else {
		return NewRevRangeIterator(dbit, &Range{Min: min, Max: max, Type: rtype}), nil
	}
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
