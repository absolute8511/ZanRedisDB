package engine

import (
	"bytes"
	"encoding/binary"
)

type memWriteBatch struct {
	db *memEng
}

func newMemWriteBatch(db *memEng) (*memWriteBatch, error) {
	b := &memWriteBatch{
		db: db,
	}
	return b, nil
}

func (wb *memWriteBatch) Destroy() {
}

func (wb *memWriteBatch) Commit() error {
	return nil
}

func (wb *memWriteBatch) Clear() {
	// TODO: reuse it
	//wb.wb.Reset()
}

func (wb *memWriteBatch) DeleteRange(start, end []byte) {
	wb.db.rwmutex.Lock()
	bit := wb.db.eng.MakeIter()
	bit.SeekGE(&kvitem{key: start})
	keys := make([][]byte, 0, 100)
	for ; bit.Valid(); bit.Next() {
		if end != nil && bytes.Compare(bit.Cur().key, end) >= 0 {
			break
		}
		keys = append(keys, bit.Cur().key)
	}
	wb.db.rwmutex.Unlock()
	for _, k := range keys {
		wb.Delete(k)
	}
}

func (wb *memWriteBatch) Delete(key []byte) {
	wb.db.rwmutex.Lock()
	wb.db.eng.Delete(&kvitem{key: key})
	wb.db.rwmutex.Unlock()
}

func (wb *memWriteBatch) Put(key []byte, value []byte) {
	item := &kvitem{}
	item.key = make([]byte, len(key))
	item.value = make([]byte, len(value))
	copy(item.key, key)
	copy(item.value, value)
	wb.db.rwmutex.Lock()
	wb.db.eng.Set(item)
	wb.db.rwmutex.Unlock()
}

func (wb *memWriteBatch) Merge(key []byte, value []byte) {
	wb.db.rwmutex.Lock()
	defer wb.db.rwmutex.Unlock()
	v, err := wb.db.GetBytesNoLock(key)
	cur, err := GetRocksdbUint64(v, err)
	if err != nil {
		return
	}
	vint, err := GetRocksdbUint64(value, nil)
	if err != nil {
		return
	}
	nv := cur + vint
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, nv)
	item := &kvitem{}
	item.key = make([]byte, len(key))
	copy(item.key, key)
	item.value = buf
	wb.db.eng.Set(item)
	return
}
