package rockredis

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"
	"time"

	"github.com/youzan/ZanRedisDB/common"
	"github.com/youzan/gorocksdb"
)

// kv new format as below:
// KVType | key  ->  |value header| value | modify time|
const (
	tsLen = 8
	// 2MB
	MaxBitOffset = 2 * 8 * 1024 * 1024
)

const (
	defaultSep byte = ':'
)

var errKVKey = errors.New("invalid encode kv key")
var errInvalidDBValue = errors.New("invalide db value")
var ErrBitOverflow = errors.New("bit offset overflowed")
var errInvalidTTL = errors.New("invalid expire time")

func convertRedisKeyToDBKVKey(key []byte) ([]byte, []byte, error) {
	table, _, _ := extractTableFromRedisKey(key)
	if len(table) == 0 {
		return nil, nil, errTableName
	}
	if err := checkKeySize(key); err != nil {
		return nil, nil, err
	}
	key = encodeKVKey(key)
	return table, key, nil
}

func checkKeySize(key []byte) error {
	if len(key) > MaxKeySize || len(key) == 0 {
		return errKeySize
	}
	return nil
}

func checkValueSize(value []byte) error {
	if len(value) > MaxValueSize {
		return errValueSize
	}

	return nil
}

func encodeKVKey(key []byte) []byte {
	ek := make([]byte, len(key)+1)
	pos := 0
	ek[pos] = KVType
	pos++
	copy(ek[pos:], key)
	return ek
}

func decodeKVKey(ek []byte) ([]byte, error) {
	pos := 0
	if pos+1 > len(ek) || ek[pos] != KVType {
		return nil, errKVKey
	}

	pos++

	return ek[pos:], nil
}

type verKeyInfo struct {
	OldHeader *headerMetaValue
	Expired   bool
	Table     []byte
	VerKey    []byte
}

// decoded meta for compatible with old format
func (info verKeyInfo) MetaData() []byte {
	return info.OldHeader.UserData
}

// use for kv write operation to do some init on header
func (db *RockDB) prepareKVValueForWrite(ts int64, rawKey []byte, reset bool) (verKeyInfo, []byte, error) {
	var keyInfo verKeyInfo
	var err error
	var v []byte
	keyInfo.Table, keyInfo.VerKey, v, keyInfo.Expired, err = db.getRawDBKVValue(ts, rawKey, false)
	if err != nil {
		return keyInfo, nil, err
	}
	// expired is true only if the value is exist and expired time is reached.
	// we need decode old data on expired to left the caller to check the expired state
	var realV []byte
	realV, keyInfo.OldHeader, err = db.decodeDBRawValueToRealValue(v)
	if err != nil {
		return keyInfo, nil, err
	}
	if keyInfo.Expired || reset {
		// since renew may not remove the old expire meta under consistence policy,
		// we need delete expire meta for kv type to avoid expire the new rewrite data
		db.expiration.renewOnExpired(ts, KVType, rawKey, keyInfo.OldHeader)
	}
	return keyInfo, realV, nil
}

// this will reset the expire meta on old and rewrite the value with new ttl and new header
func (db *RockDB) resetWithNewKVValue(ts int64, rawKey []byte, value []byte, ttl int64, wb *gorocksdb.WriteBatch) ([]byte, error) {
	oldHeader, err := db.expiration.decodeRawValue(KVType, nil)
	if err != nil {
		return nil, err
	}
	oldHeader.UserData = value
	value = db.expiration.encodeToRawValue(KVType, oldHeader)
	if ttl <= 0 {
		value, err = db.expiration.delExpire(KVType, rawKey, value, true, wb)
	} else {
		value, err = db.expiration.rawExpireAt(KVType, rawKey, value, ttl+ts/int64(time.Second), wb)
	}
	if err != nil {
		return nil, err
	}

	var nvalue []byte
	if len(value)+8 > len(db.writeTmpBuf) {
		nvalue = make([]byte, len(value)+8)
	} else {
		nvalue = db.writeTmpBuf[:len(value)+8]
	}
	copy(nvalue, value)
	PutInt64ToBuf(ts, nvalue[len(value):])
	return nvalue, nil
}

// encode the user kv data (no header and no modify time) to the db value (include header and modify time)
func (db *RockDB) encodeRealValueToDBRawValue(ts int64, oldh *headerMetaValue, value []byte) []byte {
	oldh.UserData = value
	buf := db.expiration.encodeToRawValue(KVType, oldh)
	tsBuf := PutInt64(ts)
	buf = append(buf, tsBuf...)
	return buf
}

// decode the db value (include header and modify time) to the real user kv data + header
func (db *RockDB) decodeDBRawValueToRealValue(value []byte) ([]byte, *headerMetaValue, error) {
	if len(value) >= tsLen {
		value = value[:len(value)-tsLen]
	}
	oldh, err := db.expiration.decodeRawValue(KVType, value)
	if err != nil {
		return nil, oldh, err
	}
	return oldh.UserData, oldh, nil
}

// read from db and header and modify time will be removed for returned real value
func (db *RockDB) getDBKVRealValueAndHeader(ts int64, rawKey []byte, useLock bool) (verKeyInfo, []byte, error) {
	var keyInfo verKeyInfo
	var err error
	var v []byte
	keyInfo.Table, keyInfo.VerKey, v, keyInfo.Expired, err = db.getRawDBKVValue(ts, rawKey, useLock)
	if err != nil {
		return keyInfo, nil, err
	}
	// expired is true only if the value is exist and expired time is reached.
	// we need decode old data on expired to left the caller to check the expired state
	var realV []byte
	realV, keyInfo.OldHeader, err = db.decodeDBRawValueToRealValue(v)
	if err != nil {
		return keyInfo, nil, err
	}
	return keyInfo, realV, nil
}

func (db *RockDB) getAndCheckExpRealValue(ts int64, rawKey []byte, rawValue []byte, useLock bool) (bool, []byte, error) {
	if rawValue == nil {
		return false, nil, nil
	}
	expired, err := db.expiration.isExpired(ts, KVType, rawKey, rawValue, useLock)
	if err != nil {
		return false, nil, err
	}
	realV, _, err := db.decodeDBRawValueToRealValue(rawValue)
	if err != nil {
		return expired, nil, err
	}
	return expired, realV, nil
}

// should use only in read operation
func (db *RockDB) isKVExistOrExpired(ts int64, rawKey []byte) (int64, error) {
	_, kk, err := convertRedisKeyToDBKVKey(rawKey)
	if err != nil {
		return 0, err
	}
	vref, err := db.eng.Get(db.defaultReadOpts, kk)
	if err != nil {
		return 0, err
	}
	defer vref.Free()
	v := vref.Data()
	if v == nil {
		return 0, nil
	}
	expired, err := db.expiration.isExpired(ts, KVType, rawKey, v, true)
	if expired {
		return 0, err
	}
	return 1, err
}

// Get the kv value including the header meta and modify time
func (db *RockDB) getRawDBKVValue(ts int64, rawKey []byte, useLock bool) ([]byte, []byte, []byte, bool, error) {
	table, key, err := convertRedisKeyToDBKVKey(rawKey)
	if err != nil {
		return table, key, nil, false, err
	}

	var v []byte
	if useLock {
		v, err = db.eng.GetBytes(db.defaultReadOpts, key)
	} else {
		v, err = db.eng.GetBytesNoLock(db.defaultReadOpts, key)
	}
	if err != nil {
		return table, key, nil, false, err
	}
	if v == nil {
		return table, key, nil, false, nil
	}
	expired, err := db.expiration.isExpired(ts, KVType, rawKey, v, useLock)
	if err != nil {
		return table, key, v, expired, err
	}
	return table, key, v, expired, nil
}

func (db *RockDB) incr(ts int64, key []byte, delta int64) (int64, error) {
	keyInfo, realV, err := db.prepareKVValueForWrite(ts, key, false)
	if err != nil {
		return 0, err
	}
	created := false
	n := int64(0)
	if realV == nil || keyInfo.Expired {
		// expired will rewrite old, which should not change table counter
		created = (realV == nil)
	} else {
		n, err = StrInt64(realV, err)
		if err != nil {
			return 0, err
		}
	}
	db.wb.Clear()
	n += delta
	buf := FormatInt64ToSlice(n)
	buf = db.encodeRealValueToDBRawValue(ts, keyInfo.OldHeader, buf)
	db.wb.Put(keyInfo.VerKey, buf)
	if created {
		db.IncrTableKeyCount(keyInfo.Table, 1, db.wb)
	}

	err = db.eng.Write(db.defaultWriteOpts, db.wb)
	return n, err
}

//	ps : here just focus on deleting the key-value data,
//		 any other likes expire is ignore.
func (db *RockDB) KVDel(key []byte) (int64, error) {
	rawKey := key
	table, key, err := convertRedisKeyToDBKVKey(key)
	if err != nil {
		return 0, err
	}
	db.MaybeClearBatch()
	delCnt := int64(1)
	if db.cfg.EnableTableCounter {
		if !db.cfg.EstimateTableCounter {
			vok, _ := db.eng.ExistNoLock(db.defaultReadOpts, key)
			if vok {
				db.IncrTableKeyCount(table, -1, db.wb)
			} else {
				delCnt = int64(0)
			}
		} else {
			db.IncrTableKeyCount(table, -1, db.wb)
		}
	}
	db.wb.Delete(key)
	err = db.MaybeCommitBatch()
	if err != nil {
		return 0, err
	}
	// fixme: if del is batched, the deleted key may be in write batch while removing cache
	// and removed cache may be reload by read before the write batch is committed.
	db.delPFCache(rawKey)
	return delCnt, nil
}

func (db *RockDB) KVDelWithBatch(key []byte, wb *gorocksdb.WriteBatch) error {
	table, key, err := convertRedisKeyToDBKVKey(key)
	if err != nil {
		return err
	}
	if db.cfg.EnableTableCounter {
		if !db.cfg.EstimateTableCounter {
			vok, _ := db.eng.ExistNoLock(db.defaultReadOpts, key)
			if vok {
				db.IncrTableKeyCount(table, -1, wb)
			}
		} else {
			db.IncrTableKeyCount(table, -1, wb)
		}
	}
	wb.Delete(key)
	return nil
}

func (db *RockDB) Decr(ts int64, key []byte) (int64, error) {
	return db.incr(ts, key, -1)
}

func (db *RockDB) DecrBy(ts int64, key []byte, decrement int64) (int64, error) {
	return db.incr(ts, key, -decrement)
}

func (db *RockDB) DelKeys(keys ...[]byte) (int64, error) {
	if len(keys) == 0 {
		return 0, nil
	}

	delCnt := int64(0)
	for _, k := range keys {
		c, _ := db.KVDel(k)
		delCnt += c
	}

	//clear all the expire meta data related to the keys
	db.MaybeClearBatch()
	for _, k := range keys {
		db.delExpire(KVType, k, nil, false, db.wb)
	}
	err := db.MaybeCommitBatch()
	if err != nil {
		return 0, err
	}
	return delCnt, nil
}

func (db *RockDB) KVExists(keys ...[]byte) (int64, error) {
	tn := time.Now().UnixNano()
	if len(keys) == 1 {
		return db.isKVExistOrExpired(tn, keys[0])
	}
	keyList := make([][]byte, len(keys))
	valueList := make([][]byte, len(keys))
	errs := make([]error, len(keys))
	for i, k := range keys {
		_, kk, err := convertRedisKeyToDBKVKey(k)
		if err != nil {
			keyList[i] = nil
			errs[i] = err
		} else {
			keyList[i] = kk
		}
	}
	cnt := int64(0)
	db.eng.MultiGetBytes(db.defaultReadOpts, keyList, valueList, errs)
	for i, v := range valueList {
		if errs[i] == nil && v != nil {
			expired, _ := db.expiration.isExpired(tn, KVType, keys[i], v, true)
			if expired {
				continue
			}
			cnt++
		}
	}
	return cnt, nil
}

func (db *RockDB) KVGetVer(key []byte) (int64, error) {
	_, key, err := convertRedisKeyToDBKVKey(key)
	if err != nil {
		return 0, err
	}
	var ts uint64
	v, err := db.eng.GetBytes(db.defaultReadOpts, key)
	if len(v) >= tsLen {
		ts, err = Uint64(v[len(v)-tsLen:], err)
	}
	return int64(ts), err
}

func (db *RockDB) GetValueWithOpNoLock(rawKey []byte,
	op func([]byte) error) error {
	_, key, err := convertRedisKeyToDBKVKey(rawKey)
	if err != nil {
		return err
	}
	return db.rockEng.GetValueWithOpNoLock(db.defaultReadOpts, key, func(v []byte) error {
		ts := time.Now().UnixNano()
		expired, realV, err := db.getAndCheckExpRealValue(ts, rawKey, v, false)
		if err != nil {
			return err
		}
		if expired {
			realV = nil
		}
		return op(realV)
	})
}

func (db *RockDB) GetValueWithOp(rawKey []byte,
	op func([]byte) error) error {
	_, key, err := convertRedisKeyToDBKVKey(rawKey)
	if err != nil {
		return err
	}
	return db.rockEng.GetValueWithOp(db.defaultReadOpts, key, func(v []byte) error {
		ts := time.Now().UnixNano()
		expired, realV, err := db.getAndCheckExpRealValue(ts, rawKey, v, false)
		if err != nil {
			return err
		}
		if expired {
			realV = nil
		}
		return op(realV)
	})
}

func (db *RockDB) KVGet(key []byte) ([]byte, error) {
	tn := time.Now().UnixNano()
	keyInfo, v, err := db.getDBKVRealValueAndHeader(tn, key, true)
	if err != nil {
		return nil, err
	}
	if keyInfo.Expired || v == nil {
		return nil, nil
	}
	return v, err
}

func (db *RockDB) Incr(ts int64, key []byte) (int64, error) {
	return db.incr(ts, key, 1)
}

func (db *RockDB) IncrBy(ts int64, key []byte, increment int64) (int64, error) {
	return db.incr(ts, key, increment)
}

func (db *RockDB) MGet(keys ...[]byte) ([][]byte, []error) {
	keyList := make([][]byte, len(keys))
	valueList := make([][]byte, len(keys))
	errs := make([]error, len(keys))
	for i, k := range keys {
		_, kk, err := convertRedisKeyToDBKVKey(k)
		if err != nil {
			keyList[i] = nil
			errs[i] = err
		} else {
			keyList[i] = kk
		}
	}
	tn := time.Now().UnixNano()
	db.eng.MultiGetBytes(db.defaultReadOpts, keyList, valueList, errs)
	//log.Printf("mget: %v", keyList)
	for i, v := range valueList {
		if errs[i] == nil {
			expired, realV, err := db.getAndCheckExpRealValue(tn, keys[i], v, true)
			if err != nil {
				errs[i] = err
			} else if expired {
				valueList[i] = nil
			} else {
				valueList[i] = realV
			}
		}
	}
	return valueList, errs
}

func (db *RockDB) MSet(ts int64, args ...common.KVRecord) error {
	if len(args) == 0 {
		return nil
	}
	if len(args) > MAX_BATCH_NUM {
		return errTooMuchBatchSize
	}

	db.MaybeClearBatch()

	var err error
	var key []byte
	var value []byte
	tableCnt := make(map[string]int)
	var table []byte

	for i := 0; i < len(args); i++ {
		table, key, err = convertRedisKeyToDBKVKey(args[i].Key)
		if err != nil {
			return err
		} else if err = checkValueSize(args[i].Value); err != nil {
			return err
		}
		value = value[:0]
		value = append(value, args[i].Value...)
		if db.cfg.EnableTableCounter {
			vok := false
			if !db.cfg.EstimateTableCounter {
				vok, _ = db.eng.ExistNoLock(db.defaultReadOpts, key)
			}
			if !vok {
				n := tableCnt[string(table)]
				n++
				tableCnt[string(table)] = n
			}
		}
		value, err = db.resetWithNewKVValue(ts, args[i].Key, value, 0, db.wb)
		if err != nil {
			return err
		}
		db.wb.Put(key, value)
	}
	for t, num := range tableCnt {
		db.IncrTableKeyCount([]byte(t), int64(num), db.wb)
	}

	err = db.MaybeCommitBatch()
	return err
}

func (db *RockDB) KVSet(ts int64, rawKey []byte, value []byte) error {
	table, key, err := convertRedisKeyToDBKVKey(rawKey)
	if err != nil {
		return err
	} else if err = checkValueSize(value); err != nil {
		return err
	}
	db.MaybeClearBatch()
	if db.cfg.EnableTableCounter {
		found := false
		if !db.cfg.EstimateTableCounter {
			found, _ = db.eng.ExistNoLock(db.defaultReadOpts, key)
		}
		if !found {
			db.IncrTableKeyCount(table, 1, db.wb)
		}
	}
	value, err = db.resetWithNewKVValue(ts, rawKey, value, 0, db.wb)
	if err != nil {
		return err
	}
	db.wb.Put(key, value)
	err = db.MaybeCommitBatch()

	return err
}

func (db *RockDB) KVGetSet(ts int64, rawKey []byte, value []byte) ([]byte, error) {
	if err := checkValueSize(value); err != nil {
		return nil, err
	}
	db.MaybeClearBatch()
	keyInfo, realOldV, err := db.getDBKVRealValueAndHeader(ts, rawKey, false)
	if err != nil {
		return nil, err
	}
	if realOldV == nil && !keyInfo.Expired {
		db.IncrTableKeyCount(keyInfo.Table, 1, db.wb)
	} else if keyInfo.Expired {
		realOldV = nil
	}
	value, err = db.resetWithNewKVValue(ts, rawKey, value, 0, db.wb)
	db.wb.Put(keyInfo.VerKey, value)

	err = db.MaybeCommitBatch()

	return realOldV, err
}

func (db *RockDB) SetEx(ts int64, rawKey []byte, duration int64, value []byte) error {
	if duration <= 0 {
		return errInvalidTTL
	}
	table, key, err := convertRedisKeyToDBKVKey(rawKey)
	if err != nil {
		return err
	} else if err = checkValueSize(value); err != nil {
		return err
	}
	db.MaybeClearBatch()
	if db.cfg.EnableTableCounter {
		vok := false
		if !db.cfg.EstimateTableCounter {
			vok, _ = db.eng.ExistNoLock(db.defaultReadOpts, key)
		}
		if !vok {
			db.IncrTableKeyCount(table, 1, db.wb)
		}
	}
	value, err = db.resetWithNewKVValue(ts, rawKey, value, duration, db.wb)
	if err != nil {
		return err
	}
	db.wb.Put(key, value)
	err = db.MaybeCommitBatch()

	return err
}

func (db *RockDB) SetNX(ts int64, rawKey []byte, value []byte) (int64, error) {
	if err := checkValueSize(value); err != nil {
		return 0, err
	}
	db.wb.Clear()
	keyInfo, realV, err := db.prepareKVValueForWrite(ts, rawKey, false)
	if err != nil {
		return 0, err
	}
	var n int64 = 1

	if realV != nil && !keyInfo.Expired {
		n = 0
	} else {
		if realV == nil && !keyInfo.Expired {
			db.IncrTableKeyCount(keyInfo.Table, 1, db.wb)
		}
		// prepare for write will renew the expire data on expired value,
		// however, we still need del the old expire meta data since it may store the
		// expire meta data in different place under different expire policy.
		value, err = db.resetWithNewKVValue(ts, rawKey, value, 0, db.wb)
		db.wb.Put(keyInfo.VerKey, value)
		err = db.eng.Write(db.defaultWriteOpts, db.wb)
	}
	return n, err
}

func (db *RockDB) SetRange(ts int64, rawKey []byte, offset int, value []byte) (int64, error) {
	if len(value) == 0 {
		return 0, nil
	}
	if len(value)+offset > MaxValueSize {
		return 0, errValueSize
	}
	keyInfo, realV, err := db.prepareKVValueForWrite(ts, rawKey, false)
	if err != nil {
		return 0, err
	}

	db.wb.Clear()
	if realV == nil && !keyInfo.Expired {
		db.IncrTableKeyCount(keyInfo.Table, 1, db.wb)
	}
	extra := offset + len(value) - len(realV)
	if extra > 0 {
		realV = append(realV, make([]byte, extra)...)
	}
	copy(realV[offset:], value)
	retn := len(realV)

	realV = db.encodeRealValueToDBRawValue(ts, keyInfo.OldHeader, realV)
	db.wb.Put(keyInfo.VerKey, realV)

	err = db.eng.Write(db.defaultWriteOpts, db.wb)

	if err != nil {
		return 0, err
	}
	return int64(retn), nil
}

func getRange(start int, end int, valLen int) (int, int) {
	if start < 0 {
		start = valLen + start
	}

	if end < 0 {
		end = valLen + end
	}

	if start < 0 {
		start = 0
	}

	if end < 0 {
		end = 0
	}

	if end >= valLen {
		end = valLen - 1
	}
	return start, end
}

func (db *RockDB) GetRange(key []byte, start int, end int) ([]byte, error) {
	value, err := db.KVGet(key)
	if err != nil {
		return nil, err
	}

	valLen := len(value)

	start, end = getRange(start, end, valLen)

	if start > end {
		return nil, nil
	}
	return value[start : end+1], nil
}

func (db *RockDB) StrLen(key []byte) (int64, error) {
	v, err := db.KVGet(key)
	if err != nil {
		return 0, err
	}

	n := len(v)
	return int64(n), nil
}

func (db *RockDB) Append(ts int64, rawKey []byte, value []byte) (int64, error) {
	if len(value) == 0 {
		return 0, nil
	}

	keyInfo, realV, err := db.prepareKVValueForWrite(ts, rawKey, false)
	if err != nil {
		return 0, err
	}
	if len(realV)+len(value) > MaxValueSize {
		return 0, errValueSize
	}
	db.wb.Clear()
	if realV == nil && !keyInfo.Expired {
		db.IncrTableKeyCount(keyInfo.Table, 1, db.wb)
	}

	newLen := len(realV) + len(value)
	realV = append(realV, value...)
	dbv := db.encodeRealValueToDBRawValue(ts, keyInfo.OldHeader, realV)
	// TODO: do we need make sure delete the old expire meta to avoid expire the rewritten new data?

	db.wb.Put(keyInfo.VerKey, dbv)
	err = db.eng.Write(db.defaultWriteOpts, db.wb)
	if err != nil {
		return 0, err
	}

	return int64(newLen), nil
}

// BitSet set the bitmap data with format as below:
// key -> 0(first bit) 0 0 0 0 0 0 0 (last bit) | (second byte with 8 bits) | .... | (last byte with 8bits) at most MaxBitOffset/8 bytes for each bitmap
func (db *RockDB) BitSetOld(ts int64, key []byte, offset int64, on int) (int64, error) {
	if offset > MaxBitOffset || offset < 0 {
		return 0, ErrBitOverflow
	}

	if (on & ^1) != 0 {
		return 0, fmt.Errorf("bit should be 0 or 1, got %d", on)
	}
	keyInfo, realV, err := db.prepareKVValueForWrite(ts, key, false)
	if err != nil {
		return 0, err
	}
	db.wb.Clear()
	if realV == nil && !keyInfo.Expired {
		db.IncrTableKeyCount(keyInfo.Table, 1, db.wb)
	}
	if keyInfo.Expired {
		realV = nil
	}

	byteOffset := int(uint32(offset) >> 3)
	expandLen := byteOffset + 1 - len(realV)
	if expandLen > 0 {
		if on == 0 {
			// not changed
			return 0, nil
		}
		realV = append(realV, make([]byte, expandLen)...)
	}
	byteVal := realV[byteOffset]
	bit := 7 - uint8(uint32(offset)&0x7)
	oldBit := byteVal & (1 << bit)

	byteVal &= ^(1 << bit)
	byteVal |= (uint8(on&0x1) << bit)
	realV[byteOffset] = byteVal

	realV = db.encodeRealValueToDBRawValue(ts, keyInfo.OldHeader, realV)
	db.wb.Put(keyInfo.VerKey, realV)
	err = db.eng.Write(db.defaultWriteOpts, db.wb)
	if err != nil {
		return 0, err
	}
	if oldBit > 0 {
		return 1, nil
	}
	return 0, nil
}

func popcountBytes(s []byte) (count int64) {
	for i := 0; i+8 <= len(s); i += 8 {
		x := binary.LittleEndian.Uint64(s[i:])
		count += int64(bits.OnesCount64(x))
	}

	s = s[len(s)&^7:]

	if len(s) >= 4 {
		count += int64(bits.OnesCount32(binary.LittleEndian.Uint32(s)))
		s = s[4:]
	}

	if len(s) >= 2 {
		count += int64(bits.OnesCount16(binary.LittleEndian.Uint16(s)))
		s = s[2:]
	}

	if len(s) == 1 {
		count += int64(bits.OnesCount8(s[0]))
	}
	return
}

func (db *RockDB) bitGetOld(key []byte, offset int64) (int64, error) {
	v, err := db.KVGet(key)
	if err != nil {
		return 0, err
	}

	byteOffset := (uint32(offset) >> 3)
	if byteOffset >= uint32(len(v)) {
		return 0, nil
	}
	byteVal := v[byteOffset]
	bit := 7 - uint8(uint32(offset)&0x7)
	oldBit := byteVal & (1 << bit)
	if oldBit > 0 {
		return 1, nil
	}

	return 0, nil
}

func (db *RockDB) bitCountOld(key []byte, start, end int) (int64, error) {
	v, err := db.KVGet(key)
	if err != nil {
		return 0, err
	}
	start, end = getRange(start, end, len(v))
	if start > end {
		return 0, nil
	}
	v = v[start : end+1]
	return popcountBytes(v), nil
}

func (db *RockDB) Expire(ts int64, rawKey []byte, duration int64) (int64, error) {
	_, _, v, expired, err := db.getRawDBKVValue(ts, rawKey, false)
	if err != nil || v == nil || expired {
		return 0, err
	}
	return db.ExpireAt(KVType, rawKey, v, ts/int64(time.Second)+duration)
}

func (db *RockDB) Persist(ts int64, rawKey []byte) (int64, error) {
	_, _, v, expired, err := db.getRawDBKVValue(ts, rawKey, false)
	if err != nil {
		return 0, err
	}
	if v == nil || expired {
		return 0, nil
	}

	return db.ExpireAt(KVType, rawKey, v, 0)
}
