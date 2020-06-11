package rockredis

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/youzan/ZanRedisDB/common"
	"github.com/youzan/ZanRedisDB/engine"
)

// Note: since different data structure has different prefix,
// the table with same data type can be scanned, but for different data type,
// we need scan all data types to get all the data in the same table

var (
	errTableNameLen       = errors.New("invalid table name length")
	errTableName          = errors.New("invalid table name")
	errTableMetaKey       = errors.New("invalid table meta key")
	errTableIndexMetaKey  = errors.New("invalid table index meta key")
	errTableDataKeyPrefix = errors.New("invalid table data key prefix")
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

func extractTableFromRedisKey(key []byte) ([]byte, []byte, error) {
	index := bytes.IndexByte(key, tableStartSep)
	if index == -1 {
		return nil, nil, errTableName
	}
	return key[:index], key[index+1:], nil
}

func packRedisKey(table, key []byte) []byte {
	var newKey []byte

	newKey = append(newKey, table...)
	newKey = append(newKey, tableStartSep)
	newKey = append(newKey, key...)
	return newKey
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

func getDataTablePrefixBufLen(dataType byte, table []byte) int {
	bufLen := len(table) + 2 + 1 + 1
	if dataType == KVType {
		// kv has no table length prefix
		bufLen = len(table) + 1 + 1
	}
	return bufLen
}

func encodeDataTablePrefixToBuf(buf []byte, dataType byte, table []byte) int {
	pos := 0
	buf[pos] = dataType
	pos++

	// in order to make sure all the table data are in the same range
	// we need make sure we has the same table prefix
	if dataType != KVType {
		// for compatible, kv has no table length prefix
		binary.BigEndian.PutUint16(buf[pos:], uint16(len(table)))
		pos += 2
	}
	copy(buf[pos:], table)
	pos += len(table)
	buf[pos] = tableStartSep
	pos++
	return pos
}

func decodeDataTablePrefixFromBuf(buf []byte, dataType byte) ([]byte, int, error) {
	pos := 0
	if pos+1 > len(buf) || buf[pos] != dataType {
		return nil, 0, errTableDataKeyPrefix
	}
	pos++
	if pos+2 > len(buf) {
		return nil, 0, errTableDataKeyPrefix
	}

	tableLen := int(binary.BigEndian.Uint16(buf[pos:]))
	pos += 2
	if tableLen+pos > len(buf) {
		return nil, 0, errTableDataKeyPrefix
	}
	table := buf[pos : pos+tableLen]
	pos += tableLen
	if buf[pos] != tableStartSep {
		return nil, 0, errTableDataKeyPrefix
	}
	pos++
	return table, pos, nil
}

func encodeDataTableStart(dataType byte, table []byte) []byte {
	bufLen := len(table) + 2 + 1 + 1
	if dataType == KVType {
		// kv has no table length prefix
		bufLen = len(table) + 1 + 1
	}
	bufLen = getDataTablePrefixBufLen(dataType, table)
	buf := make([]byte, bufLen)

	encodeDataTablePrefixToBuf(buf, dataType, table)
	return buf
}

func encodeDataTableEnd(dataType byte, table []byte) []byte {
	k := encodeDataTableStart(dataType, table)
	k[len(k)-1] = k[len(k)-1] + 1
	return k
}

func (db *RockDB) GetTables() [][]byte {
	ch := make([][]byte, 0, 100)
	s := encodeTableMetaStartKey()
	e := encodeTableMetaStopKey()
	it, err := db.NewDBRangeIterator(s, e, common.RangeOpen, false)
	if err != nil {
		return nil
	}
	defer it.Close()
	for ; it.Valid(); it.Next() {
		rk := it.Key()
		table, err := decodeTableMetaKey(rk)
		if err != nil {
			continue
		}
		ch = append(ch, table)
	}
	return ch
}

func (db *RockDB) DelTableKeyCount(table []byte, wb engine.WriteBatch) error {
	if !db.cfg.EnableTableCounter {
		return nil
	}
	tm := encodeTableMetaKey(table)
	wb.Delete(tm)
	return nil
}

func (db *RockDB) IncrTableKeyCount(table []byte, delta int64, wb engine.WriteBatch) error {
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
	if size, err = GetRocksdbUint64(db.GetBytes(tm)); err != nil {
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

func (db *RockDB) GetHsetIndexTables() [][]byte {
	ch := make([][]byte, 0, 100)
	s := encodeTableIndexMetaStartKey(hsetIndexMeta)
	e := encodeTableIndexMetaStopKey(hsetIndexMeta)
	it, err := db.NewDBRangeIterator(s, e, common.RangeOpen, false)
	if err != nil {
		return nil
	}
	defer it.Close()

	for ; it.Valid(); it.Next() {
		rk := it.Key()
		_, table, err := decodeTableIndexMetaKey(rk)
		if err != nil {
			dbLog.Infof("decode index table name %v failed: %v", rk, err)
			continue
		}
		ch = append(ch, table)
	}
	return ch
}

func (db *RockDB) GetTableHsetIndexValue(table []byte) ([]byte, error) {
	key := encodeTableIndexMetaKey(table, hsetIndexMeta)
	return db.GetBytes(key)
}

func (db *RockDB) SetTableHsetIndexValue(table []byte, value []byte) error {
	// this may not run in raft loop
	// so we should use new db write batch here
	key := encodeTableIndexMetaKey(table, hsetIndexMeta)
	wb := db.rockEng.NewWriteBatch()
	defer wb.Destroy()
	wb.Put(key, value)
	return db.rockEng.Write(wb)
}
