package engine

// #include <stdlib.h>
// #include "skiplist.h"
// #include "kv_skiplist.h"
import "C"
import (
	"sync"
	"sync/atomic"
	"unsafe"
)

// byteToChar returns *C.char from byte slice.
func byteToChar(b []byte) *C.char {
	var c *C.char
	if len(b) > 0 {
		c = (*C.char)(unsafe.Pointer(&b[0]))
	}
	return c
}

type skipList struct {
	csl    *C.skiplist_raw
	closed int32
	mutex  sync.RWMutex
}

func NewSkipList() *skipList {
	return &skipList{
		csl: C.kv_skiplist_create(),
	}
}

func (sl *skipList) Destroy() {
	if sl.IsClosed() {
		return
	}
	sl.mutex.Lock()
	defer sl.mutex.Unlock()
	atomic.StoreInt32(&sl.closed, 1)
	C.kv_skiplist_destroy(sl.csl)
}

func (sl *skipList) IsClosed() bool {
	return atomic.LoadInt32(&sl.closed) == 1
}

func (sl *skipList) Size() int64 {
	sl.mutex.RLock()
	defer sl.mutex.RUnlock()
	if sl.IsClosed() {
		return 0
	}
	cs := C.skiplist_get_size(sl.csl)
	return int64(cs)
}

func (sl *skipList) NewIterator() (*SkipListIterator, error) {
	sl.mutex.RLock()
	if sl.IsClosed() {
		sl.mutex.RUnlock()
		return nil, errDBEngClosed
	}
	// the mutex RUnlock must be called while iterator is closed
	return &SkipListIterator{
		sl:     sl,
		cursor: nil,
	}, nil
}

func (sl *skipList) Get(key []byte) ([]byte, error) {
	var (
		cvsz C.size_t
		cKey = byteToChar(key)
	)

	sl.mutex.RLock()
	defer sl.mutex.RUnlock()
	if sl.IsClosed() {
		return nil, errDBEngClosed
	}
	cv := C.kv_skiplist_get(sl.csl, cKey, C.size_t(len(key)), &cvsz)
	if cv == nil {
		return nil, nil
	}
	defer C.free(unsafe.Pointer(cv))
	return C.GoBytes(unsafe.Pointer(cv), C.int(cvsz)), nil
}

func (sl *skipList) Insert(key []byte, value []byte) error {
	if sl.IsClosed() {
		return errDBEngClosed
	}
	var (
		cKey   = byteToChar(key)
		cValue = byteToChar(value)
	)
	C.kv_skiplist_insert(sl.csl, cKey, C.size_t(len(key)), cValue, C.size_t(len(value)))
	return nil
}

func (sl *skipList) Delete(key []byte) error {
	if sl.IsClosed() {
		return errDBEngClosed
	}
	cKey := byteToChar(key)
	C.kv_skiplist_del(sl.csl, cKey, C.size_t(len(key)))
	return nil
}
