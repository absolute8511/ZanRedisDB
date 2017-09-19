package rockredis

import (
	"errors"

	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/gobwas/glob"
)

var errDataType = errors.New("error data type")
var errMetaKey = errors.New("error meta key")

func (db *RockDB) Scan(dataType common.DataType, cursor []byte, count int, match string) ([][]byte, error) {
	storeDataType, err := getDataStoreType(dataType)
	if err != nil {
		return nil, err
	}
	return db.scanGeneric(storeDataType, cursor, count, match)
}

func (db *RockDB) ScanWithBuffer(dataType common.DataType, cursor []byte, count int, match string, buffer [][]byte) ([][]byte, error) {
	storeDataType, err := getDataStoreType(dataType)
	if err != nil {
		return nil, err
	}
	return db.scanGenericUseBuffer(storeDataType, cursor, count, match, buffer)
}

func getDataStoreType(dataType common.DataType) (byte, error) {
	var storeDataType byte
	// for list, hash, set, zset, we can scan all keys from meta ,
	// because meta key is the key of these structure
	switch dataType {
	case common.KV:
		storeDataType = KVType
	case common.LIST:
		storeDataType = LMetaType
	case common.HASH:
		storeDataType = HSizeType
	case common.SET:
		storeDataType = SSizeType
	case common.ZSET:
		storeDataType = ZSizeType
	default:
		return 0, errDataType
	}
	return storeDataType, nil
}

func getCommonDataType(dataType byte) (common.DataType, error) {
	var commonDataType common.DataType
	switch dataType {
	case KVType:
		commonDataType = common.KV
	case LMetaType:
		commonDataType = common.LIST
	case HSizeType:
		commonDataType = common.HASH
	case SSizeType:
		commonDataType = common.SET
	case ZSizeType:
		commonDataType = common.ZSET
	default:
		return 0, errDataType
	}
	return commonDataType, nil
}
func buildMatchRegexp(match string) (glob.Glob, error) {
	var err error
	var r glob.Glob

	if len(match) > 0 {
		if r, err = glob.Compile(match); err != nil {
			return nil, err
		}
	}

	return r, nil
}

func (db *RockDB) buildScanIterator(minKey []byte, maxKey []byte) (*RangeLimitedIterator, error) {
	tp := common.RangeOpen
	return NewDBRangeIterator(db.eng, minKey, maxKey, tp, false)
}

func buildScanKeyRange(storeDataType byte, key []byte) (minKey []byte, maxKey []byte, err error) {
	if minKey, err = encodeScanMinKey(storeDataType, key); err != nil {
		return
	}
	if maxKey, err = encodeScanMaxKey(storeDataType, nil); err != nil {
		return
	}
	return
}

func encodeScanMinKey(storeDataType byte, key []byte) ([]byte, error) {
	return encodeScanKey(storeDataType, key)
}

func encodeScanMaxKey(storeDataType byte, key []byte) ([]byte, error) {
	if len(key) > 0 {
		return encodeScanKey(storeDataType, key)
	}

	k, err := encodeScanKey(storeDataType, nil)
	if err != nil {
		return nil, err
	}
	k[len(k)-1] = k[len(k)-1] + 1
	return k, nil
}

func encodeScanKey(storeDataType byte, key []byte) ([]byte, error) {
	switch storeDataType {
	case KVType:
		return encodeKVKey(key), nil
	case LMetaType:
		return lEncodeMetaKey(key), nil
	case HSizeType:
		return hEncodeSizeKey(key), nil
	case ZSizeType:
		return zEncodeSizeKey(key), nil
	case SSizeType:
		return sEncodeSizeKey(key), nil
	default:
		return nil, errDataType
	}
}

func decodeScanKey(storeDataType byte, ek []byte) (key []byte, err error) {
	switch storeDataType {
	case KVType:
		key, err = decodeKVKey(ek)
	case LMetaType:
		key, err = lDecodeMetaKey(ek)
	case HSizeType:
		key, err = hDecodeSizeKey(ek)
	case ZSizeType:
		key, err = zDecodeSizeKey(ek)
	case SSizeType:
		key, err = sDecodeSizeKey(ek)
	default:
		err = errDataType
	}
	return
}

func checkScanCount(count int) int {
	if count <= 0 {
		count = defaultScanCount
	}
	if count > MAX_BATCH_NUM {
		count = MAX_BATCH_NUM
	}
	return count
}

func (db *RockDB) scanGenericUseBuffer(storeDataType byte, key []byte, count int,
	match string, inputBuffer [][]byte) ([][]byte, error) {
	r, err := buildMatchRegexp(match)
	if err != nil {
		return nil, err
	}

	minKey, maxKey, err := buildScanKeyRange(storeDataType, key)
	if err != nil {
		return nil, err
	}
	count = checkScanCount(count)

	it, err := db.buildScanIterator(minKey, maxKey)
	if err != nil {
		return nil, err
	}

	var v [][]byte
	if inputBuffer != nil {
		v = inputBuffer
	} else {
		v = make([][]byte, 0, count)
	}

	for i := 0; it.Valid() && i < count; it.Next() {
		if k, err := decodeScanKey(storeDataType, it.Key()); err != nil {
			continue
		} else if r != nil && !r.Match(string(k)) {
			continue
		} else {
			v = append(v, k)
			i++
		}
	}
	it.Close()
	return v, nil

}

func (db *RockDB) scanGeneric(storeDataType byte, key []byte, count int,
	match string) ([][]byte, error) {

	return db.scanGenericUseBuffer(storeDataType, key, count, match, nil)
}

// for special data scan
func buildSpecificDataScanKeyRange(storeDataType byte, key []byte, cursor []byte) (minKey []byte, maxKey []byte, err error) {
	if minKey, err = encodeSpecificDataScanMinKey(storeDataType, key, cursor); err != nil {
		return
	}
	if maxKey, err = encodeSpecificDataScanMaxKey(storeDataType, key, nil); err != nil {
		return
	}
	return
}

func encodeSpecificDataScanMinKey(storeDataType byte, key []byte, cursor []byte) ([]byte, error) {
	return encodeSpecificDataScanKey(storeDataType, key, cursor)
}

func encodeSpecificDataScanMaxKey(storeDataType byte, key []byte, cursor []byte) ([]byte, error) {
	if len(cursor) > 0 {
		return encodeSpecificDataScanKey(storeDataType, key, cursor)
	}

	k, err := encodeSpecificDataScanKey(storeDataType, key, nil)
	if err != nil {
		return nil, err
	}
	// here, the last byte is the start separator, set it to stop separator
	k[len(k)-1] = k[len(k)-1] + 1
	return k, nil
}

func encodeSpecificDataScanKey(storeDataType byte, key []byte, cursor []byte) ([]byte, error) {
	table, rk, err := extractTableFromRedisKey(key)
	if err != nil {
		return nil, err
	}

	switch storeDataType {
	case HashType:
		return hEncodeHashKey(table, rk, cursor), nil
	case ZSetType:
		return zEncodeSetKey(table, rk, cursor), nil
	case SetType:
		return sEncodeSetKey(table, rk, cursor), nil
	default:
		return nil, errDataType
	}
}

func (db *RockDB) buildSpecificDataScanIterator(storeDataType byte,
	key []byte, cursor []byte,
	count int) (*RangeLimitedIterator, error) {

	if err := checkKeySize(key); err != nil {
		return nil, err
	}

	minKey, maxKey, err := buildSpecificDataScanKeyRange(storeDataType, key, cursor)
	if err != nil {
		return nil, err
	}

	it, err := db.buildScanIterator(minKey, maxKey)

	if err != nil {
		return nil, err
	}
	return it, nil
}

func (db *RockDB) hScanGeneric(key []byte, cursor []byte, count int, match string) ([]common.KVRecord, error) {
	count = checkScanCount(count)
	r, err := buildMatchRegexp(match)
	if err != nil {
		return nil, err
	}

	v := make([]common.KVRecord, 0, count)

	it, err := db.buildSpecificDataScanIterator(HashType, key, cursor, count)
	if err != nil {
		return nil, err
	}
	defer it.Close()

	for i := 0; it.Valid() && i < count; it.Next() {
		_, _, f, err := hDecodeHashKey(it.Key())
		if err != nil {
			return nil, err
		} else if r != nil && !r.Match(string(f)) {
			continue
		}
		v = append(v, common.KVRecord{Key: f, Value: it.Value()})
		i++
	}
	return v, nil
}

func (db *RockDB) HScan(key []byte, cursor []byte, count int, match string) ([]common.KVRecord, error) {
	return db.hScanGeneric(key, cursor, count, match)
}

func (db *RockDB) sScanGeneric(key []byte, cursor []byte, count int, match string) ([][]byte, error) {
	count = checkScanCount(count)
	r, err := buildMatchRegexp(match)
	if err != nil {
		return nil, err
	}
	v := make([][]byte, 0, count)

	it, err := db.buildSpecificDataScanIterator(SetType, key, cursor, count)
	if err != nil {
		return nil, err
	}
	defer it.Close()

	for i := 0; it.Valid() && i < count; it.Next() {
		_, _, m, err := sDecodeSetKey(it.Key())
		if err != nil {
			return nil, err
		} else if r != nil && !r.Match(string(m)) {
			continue
		}

		v = append(v, m)
		i++
	}
	return v, nil
}

func (db *RockDB) SScan(key []byte, cursor []byte, count int, match string) ([][]byte, error) {
	return db.sScanGeneric(key, cursor, count, match)
}

func (db *RockDB) zScanGeneric(key []byte, cursor []byte, count int, match string) ([]common.ScorePair, error) {
	count = checkScanCount(count)

	r, err := buildMatchRegexp(match)
	if err != nil {
		return nil, err
	}

	v := make([]common.ScorePair, 0, count)

	it, err := db.buildSpecificDataScanIterator(ZSetType, key, cursor, count)
	if err != nil {
		return nil, err
	}
	defer it.Close()

	for i := 0; it.Valid() && i < count; it.Next() {
		_, _, m, err := zDecodeSetKey(it.Key())
		if err != nil {
			return nil, err
		} else if r != nil && !r.Match(string(m)) {
			continue
		}

		score, err := Float64(it.Value(), nil)
		if err != nil {
			return nil, err
		}

		v = append(v, common.ScorePair{Score: score, Member: m})
		i++
	}
	return v, nil
}

func (db *RockDB) ZScan(key []byte, cursor []byte, count int, match string) ([]common.ScorePair, error) {
	return db.zScanGeneric(key, cursor, count, match)
}
