package engine

import (
	"bytes"
	"encoding/binary"
	"errors"
)

type wop int

const (
	NotOp wop = iota
	DeleteOp
	PutOp
	MergeOp
	DeleteRangeOp
)

type writeOp struct {
	op    wop
	key   []byte
	value []byte
}

type memWriteBatch struct {
	db  *memEng
	ops []writeOp
}

func newMemWriteBatch(db *memEng) (*memWriteBatch, error) {
	b := &memWriteBatch{
		db:  db,
		ops: make([]writeOp, 0, 10),
	}
	return b, nil
}

func (wb *memWriteBatch) Destroy() {
	wb.ops = wb.ops[:0]
}

func (wb *memWriteBatch) Commit() error {
	wb.db.rwmutex.Lock()
	defer wb.db.rwmutex.Unlock()
	defer wb.Clear()
	for _, w := range wb.ops {
		item := &kvitem{key: w.key, value: w.value}
		switch w.op {
		case DeleteOp:
			wb.db.eng.Delete(item)
		case PutOp:
			wb.db.eng.Set(item)
		case DeleteRangeOp:
			bit := wb.db.eng.MakeIter()
			end := item.value
			item.value = nil
			bit.SeekGE(item)
			keys := make([][]byte, 0, 100)
			for ; bit.Valid(); bit.Next() {
				if end != nil && bytes.Compare(bit.Cur().key, end) >= 0 {
					break
				}
				keys = append(keys, bit.Cur().key)
			}
			for _, k := range keys {
				wb.db.eng.Delete(&kvitem{key: k})
			}
		case MergeOp:
			v, err := wb.db.GetBytesNoLock(item.key)
			cur, err := GetRocksdbUint64(v, err)
			if err != nil {
				return err
			}
			vint, err := GetRocksdbUint64(item.value, nil)
			if err != nil {
				return err
			}
			nv := cur + vint
			buf := make([]byte, 8)
			binary.LittleEndian.PutUint64(buf, nv)
			item.value = buf
			wb.db.eng.Set(item)
		default:
			return errors.New("unknown write operation")
		}
	}
	return nil
}

func (wb *memWriteBatch) Clear() {
	wb.ops = wb.ops[:0]
}

func (wb *memWriteBatch) DeleteRange(start, end []byte) {
	wb.ops = append(wb.ops, writeOp{
		op:    DeleteRangeOp,
		key:   start,
		value: end,
	})
}

func (wb *memWriteBatch) Delete(key []byte) {
	wb.ops = append(wb.ops, writeOp{
		op:    DeleteOp,
		key:   key,
		value: nil,
	})
}

func (wb *memWriteBatch) Put(key []byte, value []byte) {
	item := &kvitem{}
	item.key = make([]byte, len(key))
	item.value = make([]byte, len(value))
	copy(item.key, key)
	copy(item.value, value)
	wb.ops = append(wb.ops, writeOp{
		op:    PutOp,
		key:   item.key,
		value: item.value,
	})
}

func (wb *memWriteBatch) Merge(key []byte, value []byte) {
	item := &kvitem{}
	item.key = make([]byte, len(key))
	item.value = make([]byte, len(value))
	copy(item.key, key)
	copy(item.value, value)
	wb.ops = append(wb.ops, writeOp{
		op:    MergeOp,
		key:   item.key,
		value: item.value,
	})
}
