package engine

import (
	"errors"

	"github.com/hashicorp/go-memdb"
)

var (
	errReverseInvalid = errors.New("invalid reservse iterator in radix")
)

type radixIterator struct {
	miTxn      *memdb.Txn
	cursor     interface{}
	resIter    memdb.ResultIterator
	isReverser bool
	err        error
}

// Valid returns false only when an Iterator has iterated past either the
// first or the last key in the database.
func (iter *radixIterator) Valid() bool {
	if iter.err != nil {
		return false
	}
	return iter.cursor != nil
}

// Key returns the key the iterator currently holds.
func (iter *radixIterator) Key() []byte {
	return []byte(iter.cursor.(*ritem).Key)
}

// Value returns the value in the database the iterator currently holds.
func (iter *radixIterator) Value() []byte {
	return iter.cursor.(*ritem).Value
}

// Next moves the iterator to the next sequential key in the database.
func (iter *radixIterator) Next() {
	if iter.resIter == nil {
		return
	}
	if iter.isReverser {
		//iter.err = errReverseInvalid
		//return
		// we convert iterator to non reverse
		iter.Seek(iter.Key())
		if iter.err != nil {
			return
		}
	}
	iter.cursor = iter.resIter.Next()
}

// Prev moves the iterator to the previous sequential key in the database.
func (iter *radixIterator) Prev() {
	if iter.resIter == nil {
		return
	}
	if !iter.isReverser {
		//iter.err = errReverseInvalid
		//return
		iter.SeekForPrev(iter.Key())
		if iter.err != nil {
			return
		}
	}
	// for reverse iterator, prev is just next
	iter.cursor = iter.resIter.Next()
}

// SeekToFirst moves the iterator to the first key in the database.
func (iter *radixIterator) First() {
	resIter, err := iter.miTxn.Get(defaultTableName, "id")
	if err != nil {
		iter.err = err
		return
	}
	iter.resIter = resIter
	iter.isReverser = false
	iter.Next()
}

// SeekToLast moves the iterator to the last key in the database.
func (iter *radixIterator) Last() {
	resIter, err := iter.miTxn.GetReverse(defaultTableName, "id")
	if err != nil {
		iter.err = err
		return
	}
	iter.resIter = resIter
	iter.isReverser = true
	iter.Prev()
}

// Seek moves the iterator to the position greater than or equal to the key.
func (iter *radixIterator) Seek(key []byte) {
	resIter, err := iter.miTxn.LowerBound(defaultTableName, "id", string(key))
	if err != nil {
		iter.err = err
		return
	}
	iter.resIter = resIter
	iter.isReverser = false
	iter.Next()
}

// seek to the last key that less than or equal to the target key
// while enable prefix_extractor, use seek() and prev() doesn't work if seek to the end
// of the prefix range. use this seekforprev instead
func (iter *radixIterator) SeekForPrev(key []byte) {
	resIter, err := iter.miTxn.ReverseLowerBound(defaultTableName, "id", string(key))
	if err != nil {
		iter.err = err
		return
	}
	iter.resIter = resIter
	iter.isReverser = true
	iter.Prev()
}

// Close closes the iterator.
func (iter *radixIterator) Close() {
	iter.miTxn.Abort()
}
