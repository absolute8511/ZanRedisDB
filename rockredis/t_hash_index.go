package rockredis

import (
	"encoding/binary"
	"errors"
	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/absolute8511/gorocksdb"
	"strconv"
)

const (
	hindexStartSep byte = ':'
)

var (
	ErrIndexExist          = errors.New("index already exist")
	ErrIndexNotExist       = errors.New("index not exist")
	ErrIndexDeleted        = errors.New("index is deleted")
	ErrIndexValueNotNumber = errors.New("invalid value for number")
	errHsetIndexKey        = errors.New("invalid hset index key")
	emptyValue             = []byte("")
)

func encodeHsetIndexNumberKey(table []byte, indexName []byte, indexValue int64, pk []byte) []byte {
	tmpkey := make([]byte, 2+2+len(table)+1+2+len(indexName)+1+8+1+len(pk))
	pos := 0
	tmpkey[pos] = IndexDataType
	pos++
	tmpkey[pos] = hsetIndexDataType
	pos++

	binary.BigEndian.PutUint16(tmpkey[pos:], uint16(len(table)))
	pos += 2

	copy(tmpkey[pos:], table)
	pos += len(table)
	tmpkey[pos] = hindexStartSep
	pos++

	binary.BigEndian.PutUint16(tmpkey[pos:], uint16(len(indexName)))
	pos += 2
	copy(tmpkey[pos:], indexName)
	pos += len(indexName)
	tmpkey[pos] = hindexStartSep
	pos++
	copy(tmpkey[pos:], PutInt64(indexValue))
	pos += 8
	tmpkey[pos] = hindexStartSep
	pos++
	copy(tmpkey[pos:], pk)

	return tmpkey
}

func encodeHsetIndexStringKey(table []byte, indexName []byte, indexValue []byte, pk []byte) []byte {
	tmpkey := make([]byte, 2+2+len(table)+1+2+len(indexName)+1+2+len(indexValue)+1+len(pk))
	pos := 0
	tmpkey[pos] = IndexDataType
	pos++
	tmpkey[pos] = hsetIndexDataType
	pos++
	binary.BigEndian.PutUint16(tmpkey[pos:], uint16(len(table)))
	pos += 2
	copy(tmpkey[pos:], table)
	pos += len(table)
	tmpkey[pos] = hindexStartSep
	pos++
	binary.BigEndian.PutUint16(tmpkey[pos:], uint16(len(indexName)))
	pos += 2
	copy(tmpkey[pos:], indexName)
	pos += len(indexName)
	tmpkey[pos] = hindexStartSep
	pos++
	binary.BigEndian.PutUint16(tmpkey[pos:], uint16(len(indexValue)))
	pos += 2
	copy(tmpkey[pos:], indexValue)
	pos += len(indexValue)
	tmpkey[pos] = hindexStartSep
	pos++
	copy(tmpkey[pos:], pk)
	return tmpkey
}

func decodeHsetIndexNumberKey(rawKey []byte) ([]byte, []byte, int64, []byte, error) {
	pos := 0
	if len(rawKey) < pos+2+2+1+2+1+8+1 {
		return nil, nil, 0, nil, errHsetIndexKey
	}
	if rawKey[0] != IndexDataType || rawKey[1] != hsetIndexDataType {
		return nil, nil, 0, nil, errHsetIndexKey
	}
	pos += 2
	tableLen := int(binary.BigEndian.Uint16(rawKey[pos:]))
	pos += 2
	if len(rawKey) < tableLen+pos+1+2 {
		return nil, nil, 0, nil, errHsetIndexKey
	}
	table := rawKey[pos : pos+tableLen]
	pos += tableLen
	if rawKey[pos] != hindexStartSep {
		return nil, nil, 0, nil, errHsetIndexKey
	}
	pos++
	indexNameLen := int(binary.BigEndian.Uint16(rawKey[pos:]))
	pos += 2
	if len(rawKey) < indexNameLen+pos+1+2 {
		return nil, nil, 0, nil, errHsetIndexKey
	}

	indexName := rawKey[pos : pos+indexNameLen]
	pos += indexNameLen
	if rawKey[pos] != hindexStartSep {
		return nil, nil, 0, nil, errHsetIndexKey
	}
	pos++
	indexValueLen := 8
	if len(rawKey) < indexValueLen+pos+1 {
		return nil, nil, 0, nil, errHsetIndexKey
	}
	indexValue := rawKey[pos : pos+indexValueLen]
	pos += indexValueLen

	iv, err := Int64(indexValue, nil)

	if rawKey[pos] != hindexStartSep {
		return nil, nil, 0, nil, errHsetIndexKey
	}
	pos++
	pk := rawKey[pos:]

	return table, indexName, iv, pk, err
}

func decodeHsetIndexStringKey(rawKey []byte) ([]byte, []byte, []byte, []byte, error) {
	pos := 0
	if len(rawKey) < pos+2+2+1+2+1 {
		return nil, nil, nil, nil, errHsetIndexKey
	}
	if rawKey[0] != IndexDataType || rawKey[1] != hsetIndexDataType {
		return nil, nil, nil, nil, errHsetIndexKey
	}
	pos += 2
	tableLen := int(binary.BigEndian.Uint16(rawKey[pos:]))
	pos += 2
	if len(rawKey) < tableLen+pos+1+2 {
		return nil, nil, nil, nil, errHsetIndexKey
	}
	table := rawKey[pos : pos+tableLen]
	pos += tableLen
	if rawKey[pos] != hindexStartSep {
		return nil, nil, nil, nil, errHsetIndexKey
	}
	pos++
	indexNameLen := int(binary.BigEndian.Uint16(rawKey[pos:]))
	pos += 2
	if len(rawKey) < indexNameLen+pos+1+2 {
		return nil, nil, nil, nil, errHsetIndexKey
	}

	indexName := rawKey[pos : pos+indexNameLen]
	pos += indexNameLen
	if rawKey[pos] != hindexStartSep {
		return nil, nil, nil, nil, errHsetIndexKey
	}
	pos++
	indexValueLen := int(binary.BigEndian.Uint16(rawKey[pos:]))
	pos += 2
	if len(rawKey) < indexValueLen+pos+1 {
		return nil, nil, nil, nil, errHsetIndexKey
	}
	indexValue := rawKey[pos : pos+indexValueLen]
	pos += indexValueLen

	if rawKey[pos] != hindexStartSep {
		return nil, nil, nil, nil, errHsetIndexKey
	}
	pos++
	pk := rawKey[pos:]

	return table, indexName, indexValue, pk, nil
}

func encodeHsetIndexStartKey(table []byte, indexName []byte) []byte {
	tmpkey := make([]byte, 2+2+len(table)+1+2+len(indexName)+1)
	pos := 0
	tmpkey[pos] = IndexDataType
	pos++
	tmpkey[pos] = hsetIndexDataType
	pos++

	binary.BigEndian.PutUint16(tmpkey[pos:], uint16(len(table)))
	pos += 2
	copy(tmpkey[pos:], table)
	pos += len(table)
	tmpkey[pos] = hindexStartSep
	pos++
	binary.BigEndian.PutUint16(tmpkey[pos:], uint16(len(indexName)))
	pos += 2
	copy(tmpkey[pos:], indexName)
	pos += len(indexName)
	tmpkey[pos] = hindexStartSep
	return tmpkey
}

func encodeHsetIndexStopKey(table []byte, indexName []byte) []byte {
	k := encodeHsetIndexStartKey(table, indexName)
	k[len(k)-1] = k[len(k)-1] + 1
	return k
}

func encodeHsetIndexNumberStartKey(table []byte, indexName []byte, indexValue int64) []byte {
	return encodeHsetIndexNumberKey(table, indexName, indexValue, nil)
}

func encodeHsetIndexNumberStopKey(table []byte, indexName []byte, indexValue int64) []byte {
	k := encodeHsetIndexNumberKey(table, indexName, indexValue, nil)
	k[len(k)-1] = k[len(k)-1] + 1
	return k
}

func encodeHsetIndexStringStartKey(table []byte, indexName []byte, indexValue []byte) []byte {
	return encodeHsetIndexStringKey(table, indexName, indexValue, nil)
}

func encodeHsetIndexStringStopKey(table []byte, indexName []byte, indexValue []byte) []byte {
	k := encodeHsetIndexStringKey(table, indexName, indexValue, nil)
	k[len(k)-1] = k[len(k)-1] + 1
	return k
}

func hsetIndexAddNumberRec(table []byte, indexName []byte, indexValue int64, pk []byte, pkvalue []byte, wb *gorocksdb.WriteBatch) {
	dbkey := encodeHsetIndexNumberKey(table, indexName, indexValue, pk)
	wb.Put(dbkey, pkvalue)
}

func hsetIndexRemoveNumberRec(table []byte, indexName []byte, indexValue int64, pk []byte, wb *gorocksdb.WriteBatch) {
	dbkey := encodeHsetIndexNumberKey(table, indexName, indexValue, pk)
	wb.Delete(dbkey)
}

func hsetIndexAddStringRec(table []byte, indexName []byte, indexValue []byte, pk []byte, pkvalue []byte, wb *gorocksdb.WriteBatch) {
	dbkey := encodeHsetIndexStringKey(table, indexName, indexValue, pk)
	wb.Put(dbkey, pkvalue)
}

func hsetIndexRemoveStringRec(table []byte, indexName []byte, indexValue []byte, pk []byte, wb *gorocksdb.WriteBatch) {
	dbkey := encodeHsetIndexStringKey(table, indexName, indexValue, pk)
	wb.Delete(dbkey)
}

func (db *RockDB) hsetIndexAddFieldRecs(pk []byte, fieldList [][]byte, valueList [][]byte, wb *gorocksdb.WriteBatch) error {
	table := extractTableFromRedisKey(pk)
	if len(table) == 0 {
		return errTableName
	}
	if len(fieldList) != len(valueList) {
		return errors.New("invalid args")
	}
	for i, field := range fieldList {
		hindex, err := db.indexMgr.GetHsetIndex(string(table), string(field))
		if err != nil {
			continue
		}

		err = hindex.UpdateRec(nil, valueList[i], pk, wb)
		if err != nil {
			return err
		}
	}
	return nil
}

func (db *RockDB) hsetIndexUpdateFieldRecs(pk []byte, fieldList [][]byte, valueList [][]byte, wb *gorocksdb.WriteBatch) error {
	table := extractTableFromRedisKey(pk)
	if len(table) == 0 {
		return errTableName
	}
	if len(fieldList) != len(valueList) {
		return errors.New("invalid args")
	}
	oldvalues, err := db.HMget(pk, fieldList...)
	if err != nil {
		return err
	}

	for i, field := range fieldList {
		hindex, err := db.indexMgr.GetHsetIndex(string(table), string(field))
		if err != nil {
			continue
		}
		err = hindex.UpdateRec(oldvalues[i], valueList[i], pk, wb)
		if err != nil {
			return err
		}
	}
	return nil
}

func (db *RockDB) hsetIndexAddRec(pk []byte, field []byte, value []byte, wb *gorocksdb.WriteBatch) error {
	table := extractTableFromRedisKey(pk)
	if len(table) == 0 {
		return errTableName
	}

	hindex, err := db.indexMgr.GetHsetIndex(string(table), string(field))
	if err != nil {
		return err
	}

	return hindex.UpdateRec(nil, value, pk, wb)
}

func (db *RockDB) hsetIndexUpdateRec(pk []byte, field []byte, value []byte, wb *gorocksdb.WriteBatch) error {
	table := extractTableFromRedisKey(pk)
	if len(table) == 0 {
		return errTableName
	}

	hindex, err := db.indexMgr.GetHsetIndex(string(table), string(field))
	if err != nil {
		return err
	}

	oldvalue, err := db.HGet(pk, field)
	if err != nil {
		return err
	}

	return hindex.UpdateRec(oldvalue, value, pk, wb)
}

func (self *RockDB) hsetIndexRemoveRec(pk []byte, field []byte, value []byte, wb *gorocksdb.WriteBatch) error {
	table := extractTableFromRedisKey(pk)
	if len(table) == 0 {
		return errTableName
	}

	hindex, err := self.indexMgr.GetHsetIndex(string(table), string(field))
	if err != nil {
		return err
	}
	hindex.RemoveRec(value, pk, wb)
	return nil
}

// search return the hash keys for matching field value
func (db *RockDB) HsetIndexSearch(table []byte, field []byte, cond *IndexCondition, countOnly bool) (int64, [][]byte, error) {
	hindex, err := db.indexMgr.GetHsetIndex(string(table), string(field))
	if err != nil {
		return 0, nil, err
	}
	if hindex.State == DeletedIndex {
		return 0, nil, ErrIndexDeleted
	}

	return hindex.SearchRec(db, cond, countOnly)
}

// TODO: handle IN, LIKE, NOT equal condition in future
// handle index condition combine (AND, OR)
type IndexCondition struct {
	StartKey     []byte
	IncludeStart bool
	EndKey       []byte
	IncludeEnd   bool
	Offset       int
	Limit        int
}

type HsetIndex struct {
	Table []byte
	HsetIndexInfo
}

func (self *HsetIndex) SearchRec(db *RockDB, cond *IndexCondition, countOnly bool) (int64, [][]byte, error) {
	var n int64 = 0
	pkList := make([][]byte, 0, 32)
	var min []byte
	var max []byte
	rt := common.RangeClose
	if cond.StartKey == nil {
		min = encodeHsetIndexStartKey(self.Table, self.Name)
	}
	if cond.EndKey == nil {
		max = encodeHsetIndexStopKey(self.Table, self.Name)
	}
	if self.ValueType == Int64V {
		if cond.StartKey != nil {
			sn, err := strconv.ParseInt(string(cond.StartKey), 10, 64)
			if err != nil {
				return n, nil, err
			}
			if !cond.IncludeStart {
				sn++
			}
			min = encodeHsetIndexNumberStartKey(self.Table, self.Name, sn)
		}
		if cond.EndKey != nil {
			en, err := strconv.ParseInt(string(cond.EndKey), 10, 64)
			if err != nil {
				return n, nil, err
			}
			if !cond.IncludeEnd {
				en--
			}
			max = encodeHsetIndexNumberStopKey(self.Table, self.Name, en)
		}
	} else if self.ValueType == StringV {
		isLOpen := cond.StartKey != nil && !cond.IncludeStart
		isROpen := cond.EndKey != nil && !cond.IncludeEnd
		if isLOpen && isROpen {
			rt = common.RangeOpen
		} else if isROpen {
			rt = common.RangeROpen
		} else if isLOpen {
			rt = common.RangeLOpen
		}
		if cond.StartKey != nil {
			if (rt & common.RangeLOpen) > 0 {
				min = encodeHsetIndexStringStopKey(self.Table, self.Name, cond.StartKey)
			} else {
				min = encodeHsetIndexStringStartKey(self.Table, self.Name, cond.StartKey)
			}
		}
		if cond.EndKey != nil {
			if (rt & common.RangeROpen) > 0 {
				max = encodeHsetIndexStringStartKey(self.Table, self.Name, cond.EndKey)
			} else {
				max = encodeHsetIndexStringStopKey(self.Table, self.Name, cond.EndKey)
			}
		}
	}
	it, err := NewDBRangeLimitIterator(db.eng, min, max, rt, cond.Offset, cond.Limit, false)
	if err != nil {
		return n, nil, err
	}
	for ; it.Valid(); it.Next() {
		n++
		if countOnly {
			continue
		}
		var pk []byte
		if self.Unique == 1 {
			pk = it.Value()
		} else {
			if self.ValueType == Int64V {
				_, _, _, pk, err = decodeHsetIndexNumberKey(it.Key())
			} else if self.ValueType == StringV {
				_, _, _, pk, err = decodeHsetIndexStringKey(it.Key())
			} else {
				continue
			}
			if err != nil {
				continue
			}
		}
		pkList = append(pkList, pk)
	}
	it.Close()
	return n, pkList, nil
}

func (self *HsetIndex) UpdateRec(oldvalue []byte, value []byte, pk []byte, wb *gorocksdb.WriteBatch) error {
	if self.State == DeletedIndex {
		return nil
	}
	pkkey := pk
	pkvalue := emptyValue
	if self.Unique == 1 {
		pkkey = nil
		pkvalue = pk
	}
	if oldvalue != nil {
		self.RemoveRec(oldvalue, pkkey, wb)
	}
	if self.ValueType == Int64V {
		n, err := strconv.ParseInt(string(value), 10, 64)
		if err != nil {
			return err
		}
		hsetIndexAddNumberRec(self.Table, self.Name, n, pkkey, pkvalue, wb)
	} else if self.ValueType == StringV {
		if self.PrefixLen > 0 && int32(len(value)) > self.PrefixLen {
			value = value[:self.PrefixLen]
		}
		hsetIndexAddStringRec(self.Table, self.Name, value, pkkey, pkvalue, wb)
	}
	return nil
}

func (self *HsetIndex) RemoveRec(value []byte, pk []byte, wb *gorocksdb.WriteBatch) {
	if value == nil {
		return
	}
	if self.Unique == 1 {
		pk = nil
	}
	if self.ValueType == Int64V {
		n, err := strconv.ParseInt(string(value), 10, 64)
		if err != nil {
			return
		}

		hsetIndexRemoveNumberRec(self.Table, self.Name, n, pk, wb)
	} else if self.ValueType == StringV {
		if self.PrefixLen > 0 && int32(len(value)) > self.PrefixLen {
			value = value[:self.PrefixLen]
		}

		hsetIndexRemoveStringRec(self.Table, self.Name, value, pk, wb)
	}
}

func (self *HsetIndex) cleanAll(db *RockDB, stopChan chan struct{}) error {
	rt := common.RangeClose
	min := encodeHsetIndexStartKey(self.Table, self.Name)
	max := encodeHsetIndexStopKey(self.Table, self.Name)
	var r gorocksdb.Range
	r.Start = min
	r.Limit = max

	n := 0
	dbLog.Infof("begin clean index: %v-%v-%v", string(self.Table), string(self.Name), string(self.IndexField))

	db.eng.DeleteFilesInRange(r)
	db.eng.CompactRange(r)

	it, err := NewDBRangeIterator(db.eng, min, max, rt, false)
	if err != nil {
		return err
	}
	defer it.Close()
	wb := gorocksdb.NewWriteBatch()
	defer wb.Destroy()
	for ; it.Valid(); it.Next() {
		n++
		if n%1000 == 0 {
			select {
			case <-stopChan:
				return errDBClosed
			default:
			}
		}
		wb.Delete(it.RefKey())
	}
	err = db.eng.Write(db.defaultWriteOpts, wb)
	if err != nil {
		dbLog.Infof("clean index %v, %v error: %v", string(self.Table), string(self.Name), err)
	} else {
		dbLog.Infof("clean index: %v-%v-%v done, scan number: %v", string(self.Table),
			string(self.Name), string(self.IndexField), n)
	}
	return nil
}
