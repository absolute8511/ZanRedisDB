package rockredis

import (
	"errors"
	"github.com/absolute8511/gorocksdb"
	"strconv"
)

const (
	hindexStartSep byte = ':'
	hindexStopSep  byte = hindexStartSep + 1
)

type TableIndexType byte
type TableIndexValueType byte
type TableIndexState int

const (
	Unique TableIndexType = iota
	NonUnique
)

const (
	// int, int64 (double is not supported currently)
	Number TableIndexValueType = iota
	String
)

const (
	InitIndex TableIndexState = iota
	BuildingIndex
	ReadyIndex
)

var (
	ErrIndexExist          = errors.New("index already exist")
	ErrIndexNotExist       = errors.New("index not exist")
	ErrIndexValueNotNumber = errors.New("invalid value for number")
	emptyValue             = []byte("")
)

func encodeHsetIndexNumberKey(table []byte, indexName []byte, indexValue int64, pk []byte) []byte {
	tmpkey := make([]byte, 2+len(table)+1+len(indexName)+1+8+1+len(pk))
	pos := 0
	tmpkey[pos] = IndexDataType
	pos++
	tmpkey[pos] = hsetIndexDataType
	pos++
	copy(tmpkey[pos:], table)
	pos += len(table)
	tmpkey[pos] = hindexStartSep
	pos++
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
	tmpkey := make([]byte, 2+len(table)+1+len(indexName)+1+len(indexValue)+1+len(pk))
	pos := 0
	tmpkey[pos] = IndexDataType
	pos++
	tmpkey[pos] = hsetIndexDataType
	pos++
	copy(tmpkey[pos:], table)
	pos += len(table)
	tmpkey[pos] = hindexStartSep
	pos++
	copy(tmpkey[pos:], indexName)
	pos += len(indexName)
	tmpkey[pos] = hindexStartSep
	pos++
	copy(tmpkey[pos:], indexValue)
	pos += len(indexValue)
	tmpkey[pos] = hindexStartSep
	pos++
	copy(tmpkey[pos:], pk)
	return tmpkey
}

func decodeHsetIndexNumberKey(rawKey []byte) ([]byte, error) {
	return nil, nil
}

func encodeHsetIndexStartKey(table []byte, indexName []byte) []byte {
	tmpkey := make([]byte, 2+len(table)+1+len(indexName)+1)
	pos := 0
	tmpkey[pos] = IndexDataType
	pos++
	tmpkey[pos] = hsetIndexDataType
	pos++
	copy(tmpkey[pos:], table)
	pos += len(table)
	tmpkey[pos] = hindexStartSep
	pos++
	copy(tmpkey[pos:], indexName)
	pos += len(indexName)
	tmpkey[pos] = hindexStartSep
	return tmpkey
}

func encodeHsetIndexStopKey(table []byte, indexName []byte) []byte {
	k := encodeHsetIndexStartKey(table, indexName)
	k[len(k)-1] = hindexStopSep
	return k
}

func encodeHsetIndexNumberStartKey(table []byte, indexName []byte, indexValue int64) []byte {
	return encodeHsetIndexNumberKey(table, indexName, indexValue, nil)
}

func encodeHsetIndexNumberStopKey(table []byte, indexName []byte, indexValue int64) []byte {
	k := encodeHsetIndexNumberKey(table, indexName, indexValue, nil)
	k[len(k)-1] = hindexStopSep
	return k
}

func encodeHsetIndexStringStartKey(table []byte, indexName []byte, indexValue []byte) []byte {
	return encodeHsetIndexStringKey(table, indexName, indexValue, nil)
}

func encodeHsetIndexStringStopKey(table []byte, indexName []byte, indexValue []byte) []byte {
	k := encodeHsetIndexStringKey(table, indexName, indexValue, nil)
	k[len(k)-1] = hindexStopSep
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

func (db *RockDB) HsetIndexAddRec(pk []byte, field []byte, value []byte, wb *gorocksdb.WriteBatch) error {
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

func (self *RockDB) HsetIndexRemoveRec(pk []byte, field []byte, value []byte, wb *gorocksdb.WriteBatch) error {
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

type IndexCondition struct {
	StartKey     []byte
	IncludeStart bool
	EndKey       []byte
	IncludeEnd   bool
	Limit        int
}

type HsetIndex struct {
	Table      []byte
	Name       []byte
	IndexField []byte
	// used for string type
	PrefixLen int
	IndexType TableIndexType
	ValueType TableIndexValueType
	State     TableIndexState
}

func (self *HsetIndex) SearchRec(cond IndexCondition) ([][]byte, error) {
	if self.ValueType == Number {
		sn, err := strconv.ParseInt(string(cond.StartKey), 10, 64)
		if err != nil {
			return nil, err
		}
		en, err := strconv.ParseInt(string(cond.EndKey), 10, 64)
		if err != nil {
			return nil, err
		}

		sk := encodeHsetIndexNumberStartKey(self.Table, self.Name, sn)
		ek := encodeHsetIndexNumberStopKey(self.Table, self.Name, en)
	} else if self.ValueType == String {
		sk := encodeHsetIndexStringStartKey(self.Table, self.Name, cond.StartKey)
		ek := encodeHsetIndexStringStopKey(self.Table, self.Name, cond.EndKey)
	}
	return nil, nil
}

func (self *HsetIndex) UpdateRec(oldvalue []byte, value []byte, pk []byte, wb *gorocksdb.WriteBatch) error {
	pkkey := pk
	pkvalue := emptyValue
	if self.IndexType == Unique {
		pkkey = nil
		pkvalue = pk
	}
	if oldvalue != nil {
		self.RemoveRec(oldvalue, pkkey, wb)
	}
	if self.ValueType == Number {
		n, err := strconv.ParseInt(string(value), 10, 64)
		if err != nil {
			return err
		}
		hsetIndexAddNumberRec(self.Table, self.Name, n, pkkey, pkvalue, wb)
	} else if self.ValueType == String {

		if self.PrefixLen > 0 && len(value) > self.PrefixLen {
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
	if self.IndexType == Unique {
		pk = nil
	}
	if self.ValueType == Number {
		n, err := strconv.ParseInt(string(value), 10, 64)
		if err != nil {
			return
		}

		hsetIndexRemoveNumberRec(self.Table, self.Name, n, pk, wb)
	} else if self.ValueType == String {
		if self.PrefixLen > 0 && len(value) > self.PrefixLen {
			value = value[:self.PrefixLen]
		}

		hsetIndexRemoveStringRec(self.Table, self.Name, value, pk, wb)
	}
}
