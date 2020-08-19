package engine

import (
	"github.com/youzan/ZanRedisDB/common"
)

type memIterator struct {
	db           *memEng
	bit          biterator
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
		bit:        db.eng.MakeIter(),
		lowerBound: lowerBound,
		upperBound: upperBound,
		opts:       opts,
	}

	return dbit, nil
}

func (it *memIterator) Next() {
	it.bit.Next()
}

func (it *memIterator) Prev() {
	it.bit.Prev()
}

func (it *memIterator) Seek(key []byte) {
	it.bit.SeekGE(&kvitem{key: key})
}

func (it *memIterator) SeekForPrev(key []byte) {
	it.bit.SeekLT(&kvitem{key: key})
}

func (it *memIterator) SeekToFirst() {
	it.bit.First()
}

func (it *memIterator) SeekToLast() {
	it.bit.Last()
}

func (it *memIterator) Valid() bool {
	return it.bit.Valid()
}

// the bytes returned will be freed after next
func (it *memIterator) RefKey() []byte {
	return it.bit.Cur().key
}

func (it *memIterator) Key() []byte {
	d := it.RefKey()
	c := make([]byte, len(d))
	copy(c, d)
	return c
}

// the bytes returned will be freed after next
func (it *memIterator) RefValue() []byte {
	v := it.bit.Cur().value
	if (it.removeTsType == KVType || it.removeTsType == HashType) && len(v) >= tsLen {
		v = v[:len(v)-tsLen]
	}
	return v
}

func (it *memIterator) Value() []byte {
	d := it.RefValue()
	c := make([]byte, len(d))
	copy(c, d)
	return c
}

func (it *memIterator) NoTimestamp(vt byte) {
	it.removeTsType = vt
}

func (it *memIterator) Close() {
	it.db.rwmutex.RUnlock()
}
