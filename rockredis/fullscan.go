package rockredis

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"

	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/gobwas/glob"
)

type ItemContainer struct {
	table  []byte
	key    []byte
	item   interface{}
	cursor []byte
}

type itemFunc func(*RangeLimitedIterator, glob.Glob) (*ItemContainer, error)

func buildErrFullScanResult(err error, dataType common.DataType) *common.FullScanResult {
	return &common.FullScanResult{
		Results:    nil,
		Type:       dataType,
		NextCursor: nil,
		PartionId:  "",
		Error:      err,
	}
}

func getFullScanDataStoreType(dataType common.DataType) (byte, error) {
	var storeDataType byte
	switch dataType {
	case common.KV:
		storeDataType = KVType
	case common.LIST:
		storeDataType = ListType
	case common.HASH:
		storeDataType = HashType
	case common.SET:
		storeDataType = SetType
	case common.ZSET:
		storeDataType = ZSetType
	default:
		return 0, errDataType
	}
	return storeDataType, nil
}

func (db *RockDB) FullScan(dataType common.DataType, cursor []byte, count int, match string) *common.FullScanResult {
	return db.fullScanGeneric(dataType, cursor, count, match)
}

func (db *RockDB) fullScanGeneric(dataType common.DataType, key []byte, count int,
	match string) *common.FullScanResult {
	return db.fullScanGenericUseBuffer(dataType, key, count, match, nil)
}

func (db *RockDB) fullScanGenericUseBuffer(dataType common.DataType, key []byte, count int,
	match string, inputBuffer []interface{}) *common.FullScanResult {
	storeDataType, err := getFullScanDataStoreType(dataType)
	if err != nil {
		return buildErrFullScanResult(err, dataType)
	}

	var result *common.FullScanResult
	switch storeDataType {
	case KVType:
		result = db.kvFullScan(key, count, match, inputBuffer)
	case HashType:
		result = db.hashFullScan(key, count, match, inputBuffer)
	case ListType:
		result = db.listFullScan(key, count, match, inputBuffer)
	case SetType:
		result = db.setFullScan(key, count, match, inputBuffer)
	case ZSetType:
		result = db.zsetFullScan(key, count, match, inputBuffer)
	default:
		result = buildErrFullScanResult(errUnsuportType, dataType)
	}
	result.Type = dataType
	return result
}

func (db *RockDB) kvFullScan(key []byte, count int,
	match string, inputBuffer []interface{}) *common.FullScanResult {

	return db.fullScanCommon(KVType, key, count, match,
		func(it *RangeLimitedIterator, r glob.Glob) (*ItemContainer, error) {
			if t, k, _, err := decodeFullScanKey(KVType, it.Key()); err != nil {
				return nil, err
			} else if r != nil && !r.Match(string(k)) {
				return nil, errNotMatch
			} else {
				v := it.Value()
				return &ItemContainer{t, k, v, nil}, nil
			}
		})
}

func (db *RockDB) hashFullScan(key []byte, count int,
	match string, inputBuffer []interface{}) *common.FullScanResult {

	return db.fullScanCommon(HashType, key, count, match,
		func(it *RangeLimitedIterator, r glob.Glob) (*ItemContainer, error) {
			var t, k, f []byte
			var err error
			if t, k, f, err = decodeFullScanKey(HashType, it.Key()); err != nil {
				return nil, err
			} else if r != nil && !r.Match(string(k)) {
				return nil, errNotMatch
			} else {
				v := common.FieldPair{
					Field: f,
					Value: it.Value(),
				}
				return &ItemContainer{t, k, v, f}, nil
			}
		})
}

func (db *RockDB) listFullScan(key []byte, count int,
	match string, inputBuffer []interface{}) *common.FullScanResult {

	return db.fullScanCommon(ListType, key, count, match,
		func(it *RangeLimitedIterator, r glob.Glob) (*ItemContainer, error) {
			var t, k, seq []byte
			var err error
			if t, k, seq, err = decodeFullScanKey(ListType, it.Key()); err != nil {
				return nil, err
			} else if r != nil && !r.Match(string(k)) {
				return nil, errNotMatch
			} else {
				v := it.Value()
				return &ItemContainer{t, k, v, seq}, nil
			}
		})
}

func (db *RockDB) setFullScan(key []byte, count int,
	match string, inputBuffer []interface{}) *common.FullScanResult {

	return db.fullScanCommon(SetType, key, count, match,
		func(it *RangeLimitedIterator, r glob.Glob) (*ItemContainer, error) {
			var t, k, m []byte
			var err error
			if t, k, m, err = decodeFullScanKey(SetType, it.Key()); err != nil {
				return nil, err
			} else if r != nil && !r.Match(string(k)) {
				return nil, errNotMatch
			} else {
				return &ItemContainer{t, k, m, m}, nil
			}
		})
}

func (db *RockDB) zsetFullScan(key []byte, count int,
	match string, inputBuffer []interface{}) *common.FullScanResult {

	return db.fullScanCommon(ZSetType, key, count, match,
		func(it *RangeLimitedIterator, r glob.Glob) (*ItemContainer, error) {
			var t, k, m []byte
			var err error
			var s float64
			if t, k, m, err = zDecodeSetKey(it.Key()); err != nil {
				return nil, err
			} else if r != nil && !r.Match(string(k)) {
				return nil, errNotMatch
			} else {
				s, err = Float64(it.Value(), nil)
				if err != nil {
					return nil, err
				}

				v := common.ScorePair{Member: m, Score: s}
				return &ItemContainer{t, k, v, m}, nil
			}
		})
}

func (db *RockDB) fullScanCommon(tp byte, key []byte, count int, match string,
	f itemFunc) *common.FullScanResult {
	r, err := buildMatchRegexp(match)
	if err != nil {
		return buildErrFullScanResult(err, common.NONE)
	}

	table, rk, err := extractTableFromRedisKey(key)
	if err != nil {
		return buildErrFullScanResult(err, common.NONE)
	}

	count = checkScanCount(count)
	it, err := db.buildFullScanIterator(tp, table, rk, count)
	if err != nil {
		return buildErrFullScanResult(err, common.NONE)
	}
	defer it.Close()

	var result []interface{}
	var item []interface{}

	var container *ItemContainer
	var prevKey []byte
	var length int
	for length = 0; it.Valid() && length < count; it.Next() {
		container, err = f(it, r)
		if err != nil {
			if err == errNotMatch {
				continue
			} else {
				return buildErrFullScanResult(err, common.NONE)
			}
		}

		if !bytes.Equal(prevKey, container.key) {
			if len(item) > 0 {
				result = append(result, item)
			}
			item = nil
			item = append(item, container.key)
			prevKey = container.key
		}
		item = append(item, container.item)
		length++
	}
	if len(item) > 0 {
		result = append(result, item)
	}

	var nextCursor []byte
	if length < count || (count == 0 && length == 0) {
		nextCursor = []byte("")
	} else {
		if tp == KVType {
			_, rk, err := common.ExtractTable(container.key)
			if err != nil {
				nextCursor, _ = encodeFullScanCursor(container.key, container.cursor)
			} else {
				nextCursor, _ = encodeFullScanCursor(rk, container.cursor)
			}
			//cross table
			//filter the cross table data
			resLen := len(result)
			if resLen > 0 {
				item := result[resLen-1].([]interface{})
				tab, _, err := common.ExtractTable(item[0].([]byte))
				if err == nil && !bytes.Equal(tab, table) {
					nextCursor, _ = encodeFullScanCursor([]byte(""), container.cursor)
					for idx, v := range result {
						tab, _, err := common.ExtractTable(v.([]interface{})[0].([]byte))
						if err != nil || !bytes.Equal(tab, table) {
							result = result[:idx]
							break
						}
					}
				}
			}
		} else {
			nextCursor, _ = encodeFullScanCursor(container.key, container.cursor)
		}
	}
	return &common.FullScanResult{
		Results:    result,
		Type:       common.NONE,
		NextCursor: nextCursor,
		PartionId:  "",
		Error:      nil,
	}
}

func (db *RockDB) buildFullScanIterator(storeDataType byte, table,
	key []byte, count int) (*RangeLimitedIterator, error) {
	k, c, err := decodeFullScanCursor(key)
	if err != nil {
		return nil, err
	}

	//	if err := checkKeySize(k); err != nil {
	//		return nil, err
	//	}

	minKey, maxKey, err := buildFullScanKeyRange(storeDataType, table, k, c)
	if err != nil {
		return nil, err
	}

	dbLog.Debugf("full scan range: %v, %v, %v, %v", minKey, maxKey, string(minKey), string(maxKey))
	//	minKey = minKey[:0]
	it, err := NewDBRangeLimitIterator(db.eng, minKey, maxKey, common.RangeOpen, 0, count+1, false)
	if err != nil {
		return nil, err
	}
	it.NoTimestamp(storeDataType)
	return it, nil
}

// the range is start from table:key:cursor to the end of the table. (data only, no meta included)
func buildFullScanKeyRange(storeDataType byte, table, key, cursor []byte) (minKey, maxKey []byte, err error) {
	if minKey, err = encodeFullScanMinKey(storeDataType, table, key, cursor); err != nil {
		return
	}
	maxKey = encodeDataTableEnd(storeDataType, table)
	return
}

func encodeFullScanMinKey(storeDataType byte, table, key, cursor []byte) ([]byte, error) {
	if len(cursor) > 0 {
		return encodeFullScanKey(storeDataType, table, key, cursor)
	} else {
		return encodeFullScanKey(storeDataType, table, key, nil)
	}
}

func encodeFullScanKey(storeDataType byte, table, key, cursor []byte) ([]byte, error) {
	switch storeDataType {
	case KVType:
		var newKey []byte
		newKey = append(newKey, table...)
		newKey = append(newKey, []byte(":")...)
		newKey = append(newKey, key...)
		return encodeKVKey(newKey), nil
	case ListType:
		var seq int64
		var err error
		if cursor == nil {
			seq = listMinSeq
		} else {
			seq, err = Int64(cursor, nil)
			if err != nil {
				return nil, err
			}
		}
		return lEncodeListKey(table, key, seq), nil
	case HashType:
		return hEncodeHashKey(table, key, cursor), nil
	case ZSetType:
		return zEncodeSetKey(table, key, cursor), nil
	case SetType:
		return sEncodeSetKey(table, key, cursor), nil
	default:
		return nil, errDataType
	}
}

func decodeFullScanKey(storeDataType byte, ek []byte) (table, key, cursor []byte, err error) {
	switch storeDataType {
	case KVType:
		key, err = decodeKVKey(ek)
		return nil, key, nil, err
	case ListType:
		var seq int64
		table, key, seq, err = lDecodeListKey(ek)
		cursor = make([]byte, 8)
		binary.BigEndian.PutUint64(cursor, uint64(seq))
	case HashType:
		var field []byte
		table, key, field, err = hDecodeHashKey(ek)
		cursor = append(cursor, field...)
	case SetType:
		var member []byte
		table, key, member, err = sDecodeSetKey(ek)
		cursor = append(cursor, member...)
	case ZSetType:
		var member []byte
		table, key, member, err = zDecodeSetKey(ek)
		cursor = append(cursor, member...)
	default:
		err = errDataType
	}

	return
}

func encodeFullScanCursor(key, cursor []byte) ([]byte, error) {
	encodedKey := make([]byte, base64.StdEncoding.EncodedLen(len(key)))
	base64.StdEncoding.Encode(encodedKey, key)
	encodedCursor := make([]byte, base64.StdEncoding.EncodedLen(len(cursor)))
	base64.StdEncoding.Encode(encodedCursor, cursor)
	length := len(encodedKey) + len(encodedCursor) + 1
	pos := 0
	newKey := make([]byte, length)
	copy(newKey[pos:], encodedKey)
	pos += len(encodedKey)
	copy(newKey[pos:], ":")
	pos++
	copy(newKey[pos:], encodedCursor)
	return newKey, nil
}

func decodeFullScanCursor(key []byte) ([]byte, []byte, error) {
	index := bytes.IndexByte(key, ':')
	if index <= 0 {
		return key, nil, nil
	}

	newKey := key[:index]
	cursor := key[index+1:]

	decodedKey := make([]byte, base64.StdEncoding.DecodedLen(len(newKey)))
	n, err := base64.StdEncoding.Decode(decodedKey, newKey)
	if err != nil {
		return nil, nil, err
	}
	decodedKey = decodedKey[:n]
	decodedCursor := make([]byte, base64.StdEncoding.DecodedLen(len(cursor)))
	n, err = base64.StdEncoding.Decode(decodedCursor, cursor)
	if err != nil {
		return nil, nil, err
	}
	decodedCursor = decodedCursor[:n]
	return decodedKey, decodedCursor, nil
}
