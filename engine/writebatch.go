package engine

import (
	"errors"

	"github.com/cockroachdb/pebble"
	"github.com/youzan/gorocksdb"
)

type WriteBatch interface {
	Destroy()
	Clear()
	DeleteRange(start, end []byte)
	Delete(key []byte)
	Put(key []byte, value []byte)
	Merge(key []byte, value []byte)
	Commit() error
}

type rocksWriteBatch struct {
	wb *gorocksdb.WriteBatch
	wo *gorocksdb.WriteOptions
	db *gorocksdb.DB
}

func newRocksWriteBatch(db *gorocksdb.DB, wo *gorocksdb.WriteOptions) *rocksWriteBatch {
	return &rocksWriteBatch{
		wb: gorocksdb.NewWriteBatch(),
		wo: wo,
		db: db,
	}
}

func (wb *rocksWriteBatch) Destroy() {
	wb.wb.Destroy()
}

func (wb *rocksWriteBatch) Clear() {
	wb.wb.Clear()
}

func (wb *rocksWriteBatch) DeleteRange(start, end []byte) {
	wb.wb.DeleteRange(start, end)
}

func (wb *rocksWriteBatch) Delete(key []byte) {
	wb.wb.Delete(key)
}

func (wb *rocksWriteBatch) Put(key []byte, value []byte) {
	wb.wb.Put(key, value)
}

func (wb *rocksWriteBatch) Merge(key []byte, value []byte) {
	wb.wb.Merge(key, value)
}

func (wb *rocksWriteBatch) Commit() error {
	if wb.db == nil || wb.wo == nil {
		return errors.New("nil db or options")
	}
	return wb.db.Write(wb.wo, wb.wb)
}

type pebbleWriteBatch struct {
	wb *pebble.Batch
	wo *pebble.WriteOptions
	db *pebble.DB
}

func newPebbleWriteBatch(db *pebble.DB, wo *pebble.WriteOptions) *pebbleWriteBatch {
	return &pebbleWriteBatch{
		wb: db.NewBatch(),
		wo: wo,
		db: db,
	}
}

func (wb *pebbleWriteBatch) Destroy() {
	wb.wb.Close()
}

func (wb *pebbleWriteBatch) Clear() {
	wb.wb.Close()
	wb.wb = wb.db.NewBatch()
	// TODO: reuse it
	//wb.wb.Reset()
}

func (wb *pebbleWriteBatch) DeleteRange(start, end []byte) {
	wb.wb.DeleteRange(start, end, wb.wo)
}

func (wb *pebbleWriteBatch) Delete(key []byte) {
	wb.wb.Delete(key, wb.wo)
}

func (wb *pebbleWriteBatch) Put(key []byte, value []byte) {
	wb.wb.Set(key, value, wb.wo)
}

func (wb *pebbleWriteBatch) Merge(key []byte, value []byte) {
	wb.wb.Merge(key, value, wb.wo)
}

func (wb *pebbleWriteBatch) Commit() error {
	if wb.db == nil || wb.wo == nil {
		return errors.New("nil db or options")
	}
	return wb.db.Apply(wb.wb, wb.wo)
}
