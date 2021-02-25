package engine

import (
	"sync/atomic"

	"github.com/hashicorp/go-memdb"
)

const (
	defaultTableName = "default"
)

type ritem struct {
	Key   string
	Value []byte
}

type radixMemIndex struct {
	memkv  *memdb.MemDB
	closed int32
}

func NewRadix() (*radixMemIndex, error) {
	schema := &memdb.DBSchema{
		Tables: map[string]*memdb.TableSchema{
			defaultTableName: &memdb.TableSchema{
				Name: defaultTableName,
				Indexes: map[string]*memdb.IndexSchema{
					"id": &memdb.IndexSchema{
						Name:    "id",
						Unique:  true,
						Indexer: &memdb.StringFieldIndex{Field: "Key"},
					},
				},
			},
		},
	}
	memkv, err := memdb.NewMemDB(schema)
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

	v, err := txn.First(defaultTableName, "id", string(key))
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, nil
	}
	item := v.(*ritem)
	if string(key) == item.Key {
		return item.Value, nil
	}
	return nil, nil
}

func (mi *radixMemIndex) Put(txn *memdb.Txn, key []byte, value []byte) error {
	nv := make([]byte, len(value))
	copy(nv, value)
	return txn.Insert(defaultTableName, &ritem{Key: string(key), Value: nv})
}

func (mi *radixMemIndex) Delete(txn *memdb.Txn, key []byte) error {
	err := txn.Delete(defaultTableName, &ritem{Key: string(key), Value: nil})
	if err == memdb.ErrNotFound {
		return nil
	}
	return err
}
