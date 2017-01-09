package rockredis

import (
	"bytes"
	"errors"
	"github.com/absolute8511/gorocksdb"
)

// Note: since different data structure has different prefix,
// the table with same data type can be scanned, but for different data type,
// we need scan all data types to get all the data in the same table

var (
	errTableNameLen = errors.New("invalid table name length")
	errTableName    = errors.New("invalid table name")
	errTableMetaKey = errors.New("invalid table meta key")
)

const (
	tableStartSep byte = ':'
	tableStopSep  byte = tableStartSep + 1
	colStartSep   byte = ':'
	colStopSep    byte = colStartSep + 1
)

// only KV data type need a table name as prefix to allow scan by table
func checkTableName(table []byte) error {
	if len(table) >= MaxTableNameLen || len(table) == 0 {
		return errTableNameLen
	}
	return nil
}

func extractTableFromRedisKey(key []byte) []byte {
	index := bytes.IndexByte(key, tableStartSep)
	if index == -1 {
		return nil
	}
	return key[:index]
}

func encodeTableMetaKey(table []byte) []byte {
	tmkey := make([]byte, 1+len(table))
	pos := 0
	tmkey[pos] = TableMetaType
	pos++
	copy(tmkey[pos:], table)
	return tmkey
}

func decodeTableMetaKey(tk []byte) ([]byte, error) {
	pos := 0
	if len(tk) < pos+1 || tk[pos] != TableMetaType {
		return nil, errTableMetaKey
	}
	pos++
	return tk[pos:], nil
}

func encodeTableMetaStartKey() []byte {
	return encodeTableMetaKey(nil)
}

func encodeTableMetaStopKey() []byte {
	t := encodeTableMetaKey(nil)
	t[len(t)-1] = TableMetaType + 1
	return t
}

func (db *RockDB) GetTables() chan []byte {
	ch := make(chan []byte, 10)
	go func() {
		s := encodeTableMetaStartKey()
		e := encodeTableMetaStopKey()
		it := NewDBRangeIterator(db.eng, s, e, RangeOpen, false)
		defer it.Close()
		for ; it.Valid(); it.Next() {
			rk := it.Key()
			table, err := decodeTableMetaKey(rk)
			if err != nil {
				continue
			}
			ch <- table
		}
		close(ch)
	}()
	return ch
}

func (db *RockDB) IncrTableKeyCount(table []byte, delta int64, wb *gorocksdb.WriteBatch) (int64, error) {
	tm := encodeTableMetaKey(table)
	var size int64
	v, err := db.eng.GetBytes(db.defaultReadOpts, tm)
	if err != nil {
		return 0, err
	}
	if size, err = Int64(v, err); err != nil {
		dbLog.Infof("convert table size error: %v, set size to init: %v", err, delta)
		size = delta
	} else {
		size += delta
	}
	if size < 0 {
		size = 0
	}

	wb.Put(tm, PutInt64(size))
	return size, nil
}

func (db *RockDB) GetTableKeyCount(table []byte) (int64, error) {
	tm := encodeTableMetaKey(table)
	var err error
	var size int64
	if size, err = Int64(db.eng.GetBytes(db.defaultReadOpts, tm)); err != nil {
	}
	return size, err
}
