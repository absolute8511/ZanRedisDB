package engine

import (
	"github.com/youzan/ZanRedisDB/common"
)

type memIter interface {
	Next()
	Prev()
	Valid() bool
	Seek([]byte)
	SeekForPrev([]byte)
	First()
	Last()
	Close()
	Key() []byte
	Value() []byte
}

type memIterator struct {
	db           *memEng
	memit        memIter
	opts         IteratorOpts
	upperBound   []byte
	lowerBound   []byte
	removeTsType byte
}

// low_bound is inclusive
// upper bound is exclusive
func newMemIterator(db *memEng, opts IteratorOpts) (*memIterator, error) {
	db.rwmutex.RLock()
	if db.IsClosed() {
		db.rwmutex.RUnlock()
		return nil, errDBEngClosed
	}
	upperBound := opts.Max
	lowerBound := opts.Min
	if opts.Type&common.RangeROpen <= 0 && upperBound != nil {
		// range right not open, we need inclusive the max,
		// however upperBound is exclusive
		upperBound = append(upperBound, 0)
	}

	dbit := &memIterator{
		db:         db,
		lowerBound: lowerBound,
		upperBound: upperBound,
		opts:       opts,
	}
	if useMemType == memTypeBtree {
		bit := db.eng.MakeIter()
		dbit.memit = &bit
	} else if useMemType == memTypeRadix {
		var err error
		dbit.memit, err = db.radixMemI.NewIterator()
		if err != nil {
			return nil, err
		}
	} else {
		dbit.memit = db.slEng.NewIterator()
	}

	return dbit, nil
}

func (it *memIterator) Next() {
	it.memit.Next()
}

func (it *memIterator) Prev() {
	it.memit.Prev()
}

func (it *memIterator) Seek(key []byte) {
	it.memit.Seek(key)
}

func (it *memIterator) SeekForPrev(key []byte) {
	it.memit.SeekForPrev(key)
}

func (it *memIterator) SeekToFirst() {
	it.memit.First()
}

func (it *memIterator) SeekToLast() {
	it.memit.Last()
}

func (it *memIterator) Valid() bool {
	return it.memit.Valid()
}

// the bytes returned will be freed after next
func (it *memIterator) RefKey() []byte {
	return it.memit.Key()
}

func (it *memIterator) Key() []byte {
	d := it.RefKey()
	if useMemType == memTypeSkiplist || useMemType == memTypeRadix {
		return d
	}
	c := make([]byte, len(d))
	copy(c, d)
	return c
}

// the bytes returned will be freed after next
func (it *memIterator) RefValue() []byte {
	v := it.memit.Value()
	if (it.removeTsType == KVType || it.removeTsType == HashType) && len(v) >= tsLen {
		v = v[:len(v)-tsLen]
	}
	return v
}

func (it *memIterator) Value() []byte {
	d := it.RefValue()
	if useMemType == memTypeSkiplist {
		return d
	}
	c := make([]byte, len(d))
	copy(c, d)
	return c
}

func (it *memIterator) NoTimestamp(vt byte) {
	it.removeTsType = vt
}

func (it *memIterator) Close() {
	it.memit.Close()
	it.db.rwmutex.RUnlock()
}
