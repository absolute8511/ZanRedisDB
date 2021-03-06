package engine

import (
	"bytes"
	"sync/atomic"

	memdb "github.com/youzan/ZanRedisDB/engine/radixdb"
)

const (
	defaultTableName = "default"
)

type radixMemIndex struct {
	memkv  *memdb.MemDB
	closed int32
}

func NewRadix() (*radixMemIndex, error) {
	memkv, err := memdb.NewMemDB()
	return &radixMemIndex{
		memkv: memkv,
	}, err
}

func (mi *radixMemIndex) Destroy() {
	atomic.StoreInt32(&mi.closed, 1)
}

func (mi *radixMemIndex) IsClosed() bool {
	return atomic.LoadInt32(&mi.closed) == 1
}

func (mi *radixMemIndex) Len() int64 {
	cs := 0
	return int64(cs)
}

func (mi *radixMemIndex) NewIterator() (*radixIterator, error) {
	txn := mi.memkv.Snapshot().Txn(false)
	return &radixIterator{
		miTxn: txn.Snapshot(),
	}, nil
}

func (mi *radixMemIndex) Get(key []byte) ([]byte, error) {
	sn := mi.memkv.Snapshot()
	txn := sn.Txn(false)
	defer txn.Abort()

	v, err := txn.First(key)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, nil
	}
	dbk, dbv, err := memdb.KVFromObject(v)
	if err != nil {
		return nil, err
	}
	if bytes.Equal(key, dbk) {
		return dbv, nil
	}
	return nil, nil
}

func (mi *radixMemIndex) Put(txn *memdb.Txn, key []byte, value []byte) error {
	nk := make([]byte, len(key))
	nv := make([]byte, len(value))
	copy(nk, key)
	copy(nv, value)
	return txn.Insert(nk, nv)
}

func (mi *radixMemIndex) Delete(txn *memdb.Txn, key []byte) error {
	err := txn.Delete(key)
	if err == memdb.ErrNotFound {
		return nil
	}
	return err
}
