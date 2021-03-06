package engine

import (
	"bytes"
	"encoding/binary"
	"errors"

	memdb "github.com/youzan/ZanRedisDB/engine/radixdb"
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
	db             *memEng
	ops            []writeOp
	writer         *memdb.Txn
	hasErr         error
	cachedForMerge map[string][]byte
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
	wb.hasErr = nil
	wb.cachedForMerge = nil
	if useMemType == memTypeRadix {
		if wb.writer != nil {
			wb.writer.Abort()
		}
	}
}

func (wb *memWriteBatch) commitSkiplist() error {
	defer wb.Clear()
	var err error
	for _, w := range wb.ops {
		switch w.op {
		case DeleteOp:
			err = wb.db.slEng.Delete(w.key)
		case PutOp:
			err = wb.db.slEng.Set(w.key, w.value)
		case DeleteRangeOp:
			it := wb.db.slEng.NewIterator()
			it.Seek(w.key)
			keys := make([][]byte, 0, 100)
			for ; it.Valid(); it.Next() {
				k := it.Key()
				if w.value != nil && bytes.Compare(k, w.value) >= 0 {
					break
				}
				keys = append(keys, k)
			}
			it.Close()
			for _, k := range keys {
				wb.db.slEng.Delete(k)
			}
		case MergeOp:
			cur, err := GetRocksdbUint64(wb.db.GetBytesNoLock(w.key))
			if err != nil {
				return err
			}
			vint, err := GetRocksdbUint64(w.value, nil)
			if err != nil {
				return err
			}
			nv := cur + vint
			buf := make([]byte, 8)
			binary.LittleEndian.PutUint64(buf, nv)
			err = wb.db.slEng.Set(w.key, buf)
		default:
			return errors.New("unknown write operation")
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (wb *memWriteBatch) Commit() error {
	switch useMemType {
	case memTypeBtree:
		return wb.commitBtree()
	case memTypeRadix:
		return wb.commitRadixMem()
	default:
		return wb.commitSkiplist()
	}
}

func (wb *memWriteBatch) commitBtree() error {
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

func (wb *memWriteBatch) commitRadixMem() error {
	defer wb.Clear()
	if wb.hasErr != nil {
		wb.writer.Abort()
		return wb.hasErr
	}
	if wb.writer != nil {
		wb.writer.Commit()
	}
	return nil
}

func (wb *memWriteBatch) Clear() {
	wb.ops = wb.ops[:0]
	wb.hasErr = nil
	wb.cachedForMerge = nil
	if useMemType == memTypeRadix {
		if wb.writer != nil {
			wb.writer.Abort()
			// TODO: maybe reset for reuse if possible
			wb.writer = nil
		}
	}
}

func (wb *memWriteBatch) DeleteRange(start, end []byte) {
	if useMemType == memTypeRadix {
		if wb.writer == nil {
			wb.writer = wb.db.radixMemI.memkv.Txn(true)
		}
		it, err := wb.db.radixMemI.NewIterator()
		if err != nil {
			wb.hasErr = err
			return
		}
		it.Seek(start)
		for ; it.Valid(); it.Next() {
			k := it.Key()
			if end != nil && bytes.Compare(k, end) >= 0 {
				break
			}
			err = wb.db.radixMemI.Delete(wb.writer, k)
			if err != nil {
				wb.hasErr = err
				break
			}
			if wb.cachedForMerge != nil {
				delete(wb.cachedForMerge, string(k))
			}
		}
		it.Close()
	}
	wb.ops = append(wb.ops, writeOp{
		op:    DeleteRangeOp,
		key:   start,
		value: end,
	})
}

func (wb *memWriteBatch) Delete(key []byte) {
	if useMemType == memTypeRadix {
		if wb.writer == nil {
			wb.writer = wb.db.radixMemI.memkv.Txn(true)
		}
		err := wb.db.radixMemI.Delete(wb.writer, key)
		if err != nil {
			wb.hasErr = err
		}
		if wb.cachedForMerge != nil {
			delete(wb.cachedForMerge, string(key))
		}
		return
	}
	wb.ops = append(wb.ops, writeOp{
		op:    DeleteOp,
		key:   key,
		value: nil,
	})
}

func (wb *memWriteBatch) Put(key []byte, value []byte) {
	if useMemType == memTypeRadix {
		if wb.writer == nil {
			wb.writer = wb.db.radixMemI.memkv.Txn(true)
		}
		err := wb.db.radixMemI.Put(wb.writer, key, value)
		if err != nil {
			wb.hasErr = err
		} else {
			if wb.cachedForMerge == nil {
				wb.cachedForMerge = make(map[string][]byte, 4)
			}
			wb.cachedForMerge[string(key)] = value
		}
		return
	}
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
	if useMemType == memTypeRadix {
		if wb.writer == nil {
			wb.writer = wb.db.radixMemI.memkv.Txn(true)
		}
		var oldV []byte
		if wb.cachedForMerge != nil {
			oldV = wb.cachedForMerge[string(key)]
		}
		var err error
		if oldV == nil {
			oldV, err = wb.db.GetBytesNoLock(key)
		}
		cur, err := GetRocksdbUint64(oldV, err)
		if err != nil {
			wb.hasErr = err
			return
		}
		vint, err := GetRocksdbUint64(value, nil)
		if err != nil {
			wb.hasErr = err
			return
		}
		nv := cur + vint
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, nv)
		err = wb.db.radixMemI.Put(wb.writer, key, buf)
		if err != nil {
			wb.hasErr = err
		} else {
			if wb.cachedForMerge == nil {
				wb.cachedForMerge = make(map[string][]byte, 4)
			}
			wb.cachedForMerge[string(key)] = buf
		}
		return
	}
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
