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
	SeekToFirst()
	SeekToLast()
	Close()
	RefKey() []byte
	Key() []byte
	RefValue() []byte
	Value() []byte
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
	snap *gorocksdb.Snapshot
	ro   *gorocksdb.ReadOptions
	db   *gorocksdb.DB
}

func NewDBIterator(db *gorocksdb.DB, withSnap bool) (*DBIterator, error) {
	db.RLock()
	dbit := &DBIterator{
		db: db,
	}
	readOpts := gorocksdb.NewDefaultReadOptions()
	readOpts.SetFillCache(false)
	readOpts.SetVerifyChecksums(false)
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
	return it.Iterator.Value().Data()
}

func (it *DBIterator) Value() []byte {
	return it.Iterator.Value().Bytes()
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

func NewDBRangeLimitIterator(db *gorocksdb.DB, min []byte, max []byte, rtype uint8,
	offset int, count int, reverse bool) (*RangeLimitedIterator, error) {
	dbit, err := NewDBIterator(db, false)
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
	dbit, err := NewDBIterator(db, true)
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
	dbit, err := NewDBIterator(db, false)
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
	dbit, err := NewDBIterator(db, true)
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
	if !it.Iterator.Valid() {
		return false
	}
	if it.l.Offset < 0 {
		return false
	}
	if it.l.Count >= 0 && it.step >= it.l.Count {
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
			it.Iterator.Seek(r.Max)
			if !it.Iterator.Valid() {
				it.Iterator.SeekToLast()
			} else {
				if !bytes.Equal(it.Iterator.RefKey(), r.Max) {
					it.Iterator.Prev()
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
