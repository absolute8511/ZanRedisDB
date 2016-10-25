package main

import (
	"bytes"
	"fmt"
	"github.com/tecbot/gorocksdb"
	"io/ioutil"
	"time"
)

func main() {
	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
	//bbto.SetBlockCache(gorocksdb.NewLRUCache(3 << 30))
	filter := gorocksdb.NewBloomFilter(10)
	bbto.SetFilterPolicy(filter)
	opts := gorocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(bbto)
	opts.SetCreateIfMissing(true)

	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("rocksdb-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	fmt.Printf("open dir:%v\n", tmpDir)

	db, err := gorocksdb.OpenDb(opts, tmpDir)
	if err != nil {
		fmt.Printf("open db error: %v\n", err)
		return
	}
	defer db.Close()
	ro := gorocksdb.NewDefaultReadOptions()
	wo := gorocksdb.NewDefaultWriteOptions()
	// if ro and wo are not used again, be sure to Close them.
	err = db.Put(wo, []byte("foo"), []byte("bar"))
	value, err := db.Get(ro, []byte("foo"))
	defer value.Free()
	if bytes.Compare(value.Data(), []byte("bar")) != 0 {
		fmt.Printf("read not equal to write: %v\n", value)
	}
	fmt.Printf("Value: %v\n", string(value.Data()))

	err = db.Delete(wo, []byte("foo"))

	ro.Destroy()
	ro = gorocksdb.NewDefaultReadOptions()
	ro.SetFillCache(false)
	it := db.NewIterator(ro)
	defer it.Close()
	it.Seek([]byte("foo"))
	for it = it; it.Valid(); it.Next() {
		key := it.Key()
		value := it.Value()
		fmt.Printf("Key: %v Value: %v\n", key.Data(), value.Data())
		key.Free()
		value.Free()
	}
	if err := it.Err(); err != nil {
	}
	wb := gorocksdb.NewWriteBatch()
	// defer wb.Close or use wb.Clear and reuse.
	wb.Delete([]byte("foo"))
	wb.Put([]byte("foo"), []byte("bar"))
	wb.Put([]byte("bar"), []byte("foo"))
	err = db.Write(wo, wb)
	if err != nil {
		fmt.Printf("batch write failed\n")
	}
}
