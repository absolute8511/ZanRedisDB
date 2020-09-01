package engine

// #include <stdlib.h>
// #include "kv_skiplist.h"
import "C"
import "unsafe"

type SkipListIterator struct {
	sl     *skipList
	cursor *C.skiplist_node
}

// Valid returns false only when an Iterator has iterated past either the
// first or the last key in the database.
func (iter *SkipListIterator) Valid() bool {
	if iter.cursor == nil {
		return false
	}
	return C.skiplist_is_valid_node(iter.cursor) != 0
}

// Key returns the key the iterator currently holds.
func (iter *SkipListIterator) Key() []byte {
	var cLen C.size_t
	cKey := C.kv_skiplist_get_node_key(iter.cursor, &cLen)
	if cKey == nil {
		return nil
	}
	defer C.free(unsafe.Pointer(cKey))
	return C.GoBytes(unsafe.Pointer(cKey), C.int(cLen))
}

// Value returns the value in the database the iterator currently holds.
func (iter *SkipListIterator) Value() []byte {
	var cLen C.size_t
	cVal := C.kv_skiplist_get_node_value(iter.cursor, &cLen)
	if cVal == nil {
		return nil
	}
	defer C.free(unsafe.Pointer(cVal))
	return C.GoBytes(unsafe.Pointer(cVal), C.int(cLen))
}

// Next moves the iterator to the next sequential key in the database.
func (iter *SkipListIterator) Next() {
	old := iter.cursor
	iter.cursor = C.skiplist_next(iter.sl.csl, iter.cursor)
	if old != nil {
		C.skiplist_release_node(old)
	}
}

// Prev moves the iterator to the previous sequential key in the database.
func (iter *SkipListIterator) Prev() {
	old := iter.cursor
	iter.cursor = C.skiplist_prev(iter.sl.csl, iter.cursor)
	if old != nil {
		C.skiplist_release_node(old)
	}
}

// SeekToFirst moves the iterator to the first key in the database.
func (iter *SkipListIterator) SeekToFirst() {
	if iter.cursor != nil {
		C.skiplist_release_node(iter.cursor)
	}
	iter.cursor = C.skiplist_begin(iter.sl.csl)
}

// SeekToLast moves the iterator to the last key in the database.
func (iter *SkipListIterator) SeekToLast() {
	if iter.cursor != nil {
		C.skiplist_release_node(iter.cursor)
	}
	iter.cursor = C.skiplist_end(iter.sl.csl)
}

// Seek moves the iterator to the position greater than or equal to the key.
func (iter *SkipListIterator) Seek(key []byte) {
	if iter.cursor != nil {
		C.skiplist_release_node(iter.cursor)
	}
	cKey := byteToChar(key)
	iter.cursor = C.kv_skiplist_find_ge(iter.sl.csl, cKey, C.size_t(len(key)))
}

// seek to the last key that less than or equal to the target key
// while enable prefix_extractor, use seek() and prev() doesn't work if seek to the end
// of the prefix range. use this seekforprev instead
func (iter *SkipListIterator) SeekForPrev(key []byte) {
	if iter.cursor != nil {
		C.skiplist_release_node(iter.cursor)
	}
	cKey := byteToChar(key)
	iter.cursor = C.kv_skiplist_find_le(iter.sl.csl, cKey, C.size_t(len(key)))
}

// Close closes the iterator.
func (iter *SkipListIterator) Close() {
	iter.sl.mutex.RUnlock()
	if iter.cursor == nil {
		return
	}
	C.skiplist_release_node(iter.cursor)
	iter.cursor = nil
}
