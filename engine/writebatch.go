package engine

import (
	"github.com/youzan/gorocksdb"
)

type WriteBatch interface {
	Destroy()
	Clear()
	DeleteRange(start, end []byte)
	Delete(key []byte)
	Put(key []byte, value []byte)
	Merge(key []byte, value []byte)
}

type RocksWriteBatch struct {
	wb *gorocksdb.WriteBatch
}

func NewRocksWriteBatch() *RocksWriteBatch {
	return &RocksWriteBatch{
		wb: gorocksdb.NewWriteBatch(),
	}
}

func (wb *RocksWriteBatch) Destroy() {
	wb.wb.Destroy()
}

func (wb *RocksWriteBatch) Clear() {
	wb.wb.Clear()
}

func (wb *RocksWriteBatch) DeleteRange(start, end []byte) {
	wb.wb.DeleteRange(start, end)
}

func (wb *RocksWriteBatch) Delete(key []byte) {
	wb.wb.Delete(key)
}

func (wb *RocksWriteBatch) Put(key []byte, value []byte) {
	wb.wb.Put(key, value)
}

func (wb *RocksWriteBatch) Merge(key []byte, value []byte) {
	wb.wb.Merge(key, value)
}
