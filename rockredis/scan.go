package rockredis

import (
	"errors"
	"time"

	"github.com/gobwas/glob"
	"github.com/youzan/ZanRedisDB/common"
	"github.com/youzan/ZanRedisDB/engine"
)

var errDataType = errors.New("error data type")
var errMetaKey = errors.New("error meta key")

func (db *RockDB) Scan(dataType common.DataType, cursor []byte, count int, match string, reverse bool) ([][]byte, error) {
	storeDataType, err := getDataStoreType(dataType)
	if err != nil {
		return nil, err
	}
	return db.scanGeneric(storeDataType, cursor, count, match, reverse)
}

func (db *RockDB) ScanWithBuffer(dataType common.DataType, cursor []byte, count int, match string, buffer [][]byte, reverse bool) ([][]byte, error) {
	storeDataType, err := getDataStoreType(dataType)
	if err != nil {
		return nil, err
	}
	return db.scanGenericUseBuffer(storeDataType, cursor, count, match, buffer, reverse)
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

func (db *RockDB) buildScanIterator(minKey []byte, maxKey []byte, reverse bool) (*engine.RangeLimitedIterator, error) {
	tp := common.RangeOpen
	return db.NewDBRangeIterator(minKey, maxKey, tp, reverse)
}

func buildScanKeyRange(storeDataType byte, key []byte, reverse bool) (minKey []byte, maxKey []byte, err error) {
	if reverse {
		// reverse we should make current key as end if key is nil
		if maxKey, err = encodeScanKey(storeDataType, key); err != nil {
			return
		}
		if minKey, err = encodeScanMinKey(storeDataType, nil); err != nil {
			return
		}
	} else {
		if minKey, err = encodeScanMinKey(storeDataType, key); err != nil {
			return
		}
		if maxKey, err = encodeScanMaxKey(storeDataType, nil); err != nil {
			return
		}
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

func encodeScanKeyTableEnd(storeDataType byte, key []byte) ([]byte, error) {
	table, _, err := common.ExtractTable(key)
	if err != nil {
		return nil, err
	}
	table = append(table, tableStartSep+1)
	return encodeScanKey(storeDataType, table)
}

func encodeScanKey(storeDataType byte, key []byte) ([]byte, error) {
	return encodeMetaKey(storeDataType, key)
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

// note: this scan will not stop while cross table, it will scan begin from key until count or no more in db.
func (db *RockDB) scanGenericUseBuffer(storeDataType byte, key []byte, count int,
	match string, inputBuffer [][]byte, reverse bool) ([][]byte, error) {
	r, err := buildMatchRegexp(match)
	if err != nil {
		return nil, err
	}

	minKey, maxKey, err := buildScanKeyRange(storeDataType, key, reverse)
	if err != nil {
		return nil, err
	}
	dbLog.Debugf("scan range: %v, %v", minKey, maxKey)
	count = checkScanCount(count)

	it, err := db.buildScanIterator(minKey, maxKey, reverse)
	if err != nil {
		return nil, err
	}
	defer it.Close()

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
	return v, nil

}

func (db *RockDB) scanGeneric(storeDataType byte, key []byte, count int,
	match string, reverse bool) ([][]byte, error) {

	return db.scanGenericUseBuffer(storeDataType, key, count, match, nil, reverse)
}

// for special data scan
func buildSpecificDataScanKeyRange(storeDataType byte, table []byte, key []byte, cursor []byte, reverse bool) (minKey []byte, maxKey []byte, err error) {
	if reverse {
		// for reverse, we need use current cursor as the end if cursor is nil
		if maxKey, err = encodeSpecificDataScanKey(storeDataType, table, key, cursor); err != nil {
			return
		}
		if minKey, err = encodeSpecificDataScanMinKey(storeDataType, table, key, nil); err != nil {
			return
		}
	} else {
		if minKey, err = encodeSpecificDataScanMinKey(storeDataType, table, key, cursor); err != nil {
			return
		}
		if maxKey, err = encodeSpecificDataScanMaxKey(storeDataType, table, key, nil); err != nil {
			return
		}
	}
	return
}

func encodeSpecificDataScanMinKey(storeDataType byte, table []byte, key []byte, cursor []byte) ([]byte, error) {
	return encodeSpecificDataScanKey(storeDataType, table, key, cursor)
}

func encodeSpecificDataScanMaxKey(storeDataType byte, table []byte, key []byte, cursor []byte) ([]byte, error) {
	if len(cursor) > 0 {
		return encodeSpecificDataScanKey(storeDataType, table, key, cursor)
	}

	k, err := encodeSpecificDataScanKey(storeDataType, table, key, nil)
	if err != nil {
		return nil, err
	}
	// here, the last byte is the start separator, set it to stop separator
	k[len(k)-1] = k[len(k)-1] + 1
	return k, nil
}

func encodeSpecificDataScanKey(storeDataType byte, table []byte, rk []byte, cursor []byte) ([]byte, error) {

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
	table []byte, key []byte, cursor []byte,
	count int, reverse bool) (*engine.RangeLimitedIterator, error) {

	if err := checkKeySize(key); err != nil {
		return nil, err
	}

	minKey, maxKey, err := buildSpecificDataScanKeyRange(storeDataType, table, key, cursor, reverse)
	if err != nil {
		return nil, err
	}

	dbLog.Debugf("scan data %v range: %v, %v, %v", storeDataType, minKey, maxKey, reverse)
	it, err := db.buildScanIterator(minKey, maxKey, reverse)

	if err != nil {
		return nil, err
	}
	return it, nil
}

func (db *RockDB) hScanGeneric(key []byte, cursor []byte, count int, match string, reverse bool) ([]common.KVRecord, error) {
	count = checkScanCount(count)
	r, err := buildMatchRegexp(match)
	if err != nil {
		return nil, err
	}
	v := make([]common.KVRecord, 0, count)

	tn := time.Now().UnixNano()
	keyInfo, err := db.GetCollVersionKey(tn, HashType, key, true)
	if err != nil {
		return nil, err
	}
	if keyInfo.IsNotExistOrExpired() {
		return v, nil
	}

	it, err := db.buildSpecificDataScanIterator(HashType, keyInfo.Table, keyInfo.VerKey, cursor, count, reverse)
	if err != nil {
		return nil, err
	}
	it.NoTimestamp(HashType)
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

func (db *RockDB) HScan(key []byte, cursor []byte, count int, match string, reverse bool) ([]common.KVRecord, error) {
	return db.hScanGeneric(key, cursor, count, match, reverse)
}

func (db *RockDB) sScanGeneric(key []byte, cursor []byte, count int, match string, reverse bool) ([][]byte, error) {
	count = checkScanCount(count)
	r, err := buildMatchRegexp(match)
	if err != nil {
		return nil, err
	}

	// TODO: use pool for large alloc
	v := make([][]byte, 0, count)
	tn := time.Now().UnixNano()
	keyInfo, err := db.GetCollVersionKey(tn, SetType, key, true)
	if err != nil {
		return nil, err
	}
	if keyInfo.IsNotExistOrExpired() {
		return v, nil
	}

	it, err := db.buildSpecificDataScanIterator(SetType, keyInfo.Table, keyInfo.VerKey, cursor, count, reverse)
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

func (db *RockDB) SScan(key []byte, cursor []byte, count int, match string, reverse bool) ([][]byte, error) {
	return db.sScanGeneric(key, cursor, count, match, reverse)
}

func (db *RockDB) zScanGeneric(key []byte, cursor []byte, count int, match string, reverse bool) ([]common.ScorePair, error) {
	count = checkScanCount(count)

	r, err := buildMatchRegexp(match)
	if err != nil {
		return nil, err
	}

	tn := time.Now().UnixNano()
	keyInfo, err := db.GetCollVersionKey(tn, ZSetType, key, true)
	if err != nil {
		return nil, err
	}
	// TODO: use pool for large alloc
	v := make([]common.ScorePair, 0, count)
	if keyInfo.IsNotExistOrExpired() {
		return v, nil
	}

	it, err := db.buildSpecificDataScanIterator(ZSetType, keyInfo.Table, keyInfo.VerKey, cursor, count, reverse)
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

		score, err := Float64(it.RefValue(), nil)
		if err != nil {
			return nil, err
		}

		v = append(v, common.ScorePair{Score: score, Member: m})
		i++
	}
	return v, nil
}

func (db *RockDB) ZScan(key []byte, cursor []byte, count int, match string, reverse bool) ([]common.ScorePair, error) {
	return db.zScanGeneric(key, cursor, count, match, reverse)
}
