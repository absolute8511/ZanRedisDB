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
	db.wg.Add(1)
	go func() {
		defer db.wg.Done()
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

func encodeTableIndexMetaKey(table []byte, itype byte) []byte {
	tmkey := make([]byte, 2+len(metaPrefix)+len(table))
	pos := 0
	tmkey[pos] = TableIndexMetaType
	pos++
	copy(tmkey[pos:], metaPrefix)
	pos += len(metaPrefix)
	tmkey[pos] = itype
	pos++
	copy(tmkey[pos:], table)
	return tmkey
}

func decodeTableIndexMetaKey(tk []byte) (byte, []byte, error) {
	pos := 0
	if len(tk) < pos+2+len(metaPrefix) || tk[pos] != TableIndexMetaType {
		return 0, nil, errTableIndexMetaKey
	}
	pos++
	pos += len(metaPrefix)
	itype := tk[pos]
	pos++
	return itype, tk[pos:], nil
}

func encodeTableIndexMetaStartKey(itype byte) []byte {
	return encodeTableIndexMetaKey(nil, itype)
}

func encodeTableIndexMetaStopKey(itype byte) []byte {
	t := encodeTableIndexMetaKey(nil, itype)
	t[len(t)-1] = t[len(t)-1] + 1
	return t
}

func (db *RockDB) GetHsetIndexTables() chan []byte {
	// TODO: use total_order_seek options for rocksdb to seek with prefix less than prefix extractor
	ch := make(chan []byte, 10)
	db.wg.Add(1)
	go func() {
		defer db.wg.Done()
		s := encodeTableIndexMetaStartKey(hsetIndexMeta)
		e := encodeTableIndexMetaStopKey(hsetIndexMeta)
		it, err := NewDBRangeIterator(db.eng, s, e, common.RangeOpen, false)
		if err != nil {
			close(ch)
			return
		}
		defer it.Close()
		defer close(ch)

		for ; it.Valid(); it.Next() {
			rk := it.Key()
			_, table, err := decodeTableIndexMetaKey(rk)
			if err != nil {
				dbLog.Infof("decode index table name %v failed: %v", rk, err)
				continue
			}
			ch <- table
		}
	}()
	return ch
}

func (db *RockDB) GetTableHsetIndexValue(table []byte) ([]byte, error) {
	key := encodeTableIndexMetaKey(table, hsetIndexMeta)
	return db.eng.GetBytes(db.defaultReadOpts, key)
}

func (db *RockDB) SetTableHsetIndexValue(table []byte, value []byte) error {
	key := encodeTableIndexMetaKey(table, hsetIndexMeta)
	db.wb.Clear()
	db.wb.Put(key, value)
	return db.eng.Write(db.defaultWriteOpts, db.wb)
}
