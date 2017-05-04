package rockredis

import (
	"bytes"
	"errors"
	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/absolute8511/gorocksdb"
)

// Note: since different data structure has different prefix,
// the table with same data type can be scanned, but for different data type,
// we need scan all data types to get all the data in the same table

var (
	errTableNameLen      = errors.New("invalid table name length")
	errTableName         = errors.New("invalid table name")
	errTableMetaKey      = errors.New("invalid table meta key")
	errTableIndexMetaKey = errors.New("invalid table index meta key")
)

const (
	tableStartSep          byte = ':'
	colStartSep            byte = ':'
	tableIndexMetaStartSep byte = ':'
	metaSep                byte = ':'
)

var metaPrefix = []byte("meta" + string(metaSep))

const (
	hsetIndexMeta     byte = 1
	jsonIndexMeta     byte = 2
	hsetIndexDataType byte = 1
	jsonIndexDataType byte = 2
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
	tmkey := make([]byte, 1+len(metaPrefix)+len(table))
	pos := 0
	tmkey[pos] = TableMetaType
	pos++
	copy(tmkey[pos:], metaPrefix)
	pos += len(metaPrefix)
	copy(tmkey[pos:], table)
	return tmkey
}

func decodeTableMetaKey(tk []byte) ([]byte, error) {
	pos := 0
	if len(tk) < pos+1+len(metaPrefix) || tk[pos] != TableMetaType {
		return nil, errTableMetaKey
	}
	pos++
	pos += len(metaPrefix)
	return tk[pos:], nil
}

func encodeTableMetaStartKey() []byte {
	return encodeTableMetaKey(nil)
}

func encodeTableMetaStopKey() []byte {
	t := encodeTableMetaKey(nil)
	t[len(t)-1] = t[len(t)-1] + 1
	return t
}

func (db *RockDB) GetTables() chan []byte {
	ch := make(chan []byte, 10)
	go func() {
		s := encodeTableMetaStartKey()
		e := encodeTableMetaStopKey()
		it, err := NewDBRangeIterator(db.eng, s, e, common.RangeOpen, false)
		if err != nil {
			close(ch)
			return
		}
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

func (db *RockDB) IncrTableKeyCount(table []byte, delta int64, wb *gorocksdb.WriteBatch) error {
	if !db.cfg.EnableTableCounter {
		return nil
	}
	tm := encodeTableMetaKey(table)
	wb.Merge(tm, PutRocksdbUint64(uint64(delta)))
	return nil
}

func (db *RockDB) GetTableKeyCount(table []byte) (int64, error) {
	tm := encodeTableMetaKey(table)
	var err error
	var size uint64
	if size, err = GetRocksdbUint64(db.eng.GetBytes(db.defaultReadOpts, tm)); err != nil {
	}
	return int64(size), err
}
