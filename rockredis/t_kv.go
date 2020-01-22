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

const (
	tsLen = 8
	// 2MB
	MaxBitOffset = 2 * 8 * 1024 * 1024
)

var errKVKey = errors.New("invalid encode kv key")
var errInvalidDBValue = errors.New("invalide db value")
var ErrBitOverflow = errors.New("bit offset overflowed")

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

func (db *RockDB) incr(ts int64, key []byte, delta int64) (int64, error) {
	table, key, err := convertRedisKeyToDBKVKey(key)
	if err != nil {
		return 0, err
	}
	refV, err := db.eng.GetNoLock(db.defaultReadOpts, key)
	defer refV.Free()
	v := refV.Data()
	created := false
	n := int64(0)
	if v == nil {
		created = true
	} else {
		if len(v) < tsLen {
			return 0, errIntNumber
		}
		n, err = StrInt64(v[:len(v)-tsLen], err)
		if err != nil {
			return 0, err
		}
	}
	db.wb.Clear()
	n += delta
	buf := FormatInt64ToSlice(n)
	tsBuf := PutInt64(ts)
	buf = append(buf, tsBuf...)
	db.wb.Put(key, buf)
	if created {
		db.IncrTableKeyCount(table, 1, db.wb)
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
		db.delExpire(KVType, k, db.wb)
	}
	err := db.MaybeCommitBatch()
	if err != nil {
		return 0, err
	}
	return delCnt, nil
}

func (db *RockDB) KVExists(keys ...[]byte) (int64, error) {
	if len(keys) == 1 {
		_, kk, err := convertRedisKeyToDBKVKey(keys[0])
		if err != nil {
			return 0, err
		}
		vok, err := db.eng.Exist(db.defaultReadOpts, kk)
		if err != nil {
			return 0, err
		}
		if vok {
			return 1, nil
		}
		return 0, nil
	}
	keyList := make([][]byte, len(keys))
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
	cnt, err := db.eng.ExistCnt(db.defaultReadOpts, keyList)
	return cnt, err
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

func (db *RockDB) GetValueWithOpNoLock(key []byte,
	op func([]byte) error) error {
	_, key, err := convertRedisKeyToDBKVKey(key)
	if err != nil {
		return err
	}
	return db.rockEng.GetValueWithOpNoLock(db.defaultReadOpts, key, func(v []byte) error {
		if len(v) >= tsLen {
			v = v[:len(v)-tsLen]
		}
		return op(v)
	})
}

func (db *RockDB) GetValueWithOp(key []byte,
	op func([]byte) error) error {
	_, key, err := convertRedisKeyToDBKVKey(key)
	if err != nil {
		return err
	}
	return db.rockEng.GetValueWithOp(db.defaultReadOpts, key, func(v []byte) error {
		if len(v) >= tsLen {
			v = v[:len(v)-tsLen]
		}
		return op(v)
	})
}

func (db *RockDB) KVGet(key []byte) ([]byte, error) {
	_, key, err := convertRedisKeyToDBKVKey(key)
	if err != nil {
		return nil, err
	}

	v, err := db.eng.GetBytes(db.defaultReadOpts, key)
	if len(v) >= tsLen {
		v = v[:len(v)-tsLen]
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
	db.eng.MultiGetBytes(db.defaultReadOpts, keyList, keyList, errs)
	//log.Printf("mget: %v", keyList)
	for i, v := range keyList {
		if errs[i] == nil && len(v) >= tsLen {
			keyList[i] = keyList[i][:len(v)-tsLen]
		}
	}
	return keyList, errs
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

	tsBuf := PutInt64(ts)
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
		value = append(value, tsBuf...)
		db.wb.Put(key, value)
		//the expire meta data related to the key should be cleared as the key-value has been reset
		db.delExpire(KVType, args[i].Key, db.wb)
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
	tsBuf := PutInt64(ts)
	value = append(value, tsBuf...)
	db.wb.Put(key, value)

	//db.delExpire(KVType, rawKey, db.wb)
	err = db.MaybeCommitBatch()

	return err
}

func (db *RockDB) KVGetSet(ts int64, rawKey []byte, value []byte) ([]byte, error) {
	table, key, err := convertRedisKeyToDBKVKey(rawKey)
	if err != nil {
		return nil, err
	} else if err = checkValueSize(value); err != nil {
		return nil, err
	}
	db.MaybeClearBatch()
	v, err := db.eng.GetBytesNoLock(db.defaultReadOpts, key)
	if err != nil {
		return nil, err
	}
	if v == nil {
		db.IncrTableKeyCount(table, 1, db.wb)
	} else if len(v) >= tsLen {
		v = v[:len(v)-tsLen]
	}
	tsBuf := PutInt64(ts)
	value = append(value, tsBuf...)
	db.wb.Put(key, value)

	err = db.MaybeCommitBatch()

	return v, err
}

func (db *RockDB) SetEx(ts int64, rawKey []byte, duration int64, value []byte) error {
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
	tsBuf := PutInt64(ts)
	value = append(value, tsBuf...)
	db.wb.Put(key, value)

	if err := db.rawExpireAt(KVType, rawKey, duration+time.Now().Unix(), db.wb); err != nil {
		return err
	}

	err = db.MaybeCommitBatch()

	return err

}

func (db *RockDB) SetNX(ts int64, key []byte, value []byte) (int64, error) {
	table, key, err := convertRedisKeyToDBKVKey(key)
	if err != nil {
		return 0, err
	} else if err := checkValueSize(value); err != nil {
		return 0, err
	}

	vok := false
	var n int64 = 1

	if vok, err = db.eng.ExistNoLock(db.defaultReadOpts, key); err != nil {
		return 0, err
	} else if vok {
		n = 0
	} else {
		db.wb.Clear()
		db.IncrTableKeyCount(table, 1, db.wb)
		value = append(value, PutInt64(ts)...)
		db.wb.Put(key, value)
		err = db.eng.Write(db.defaultWriteOpts, db.wb)
	}
	return n, err
}

func (db *RockDB) SetRange(ts int64, key []byte, offset int, value []byte) (int64, error) {
	if len(value) == 0 {
		return 0, nil
	}

	table, key, err := convertRedisKeyToDBKVKey(key)
	if err != nil {
		return 0, err
	} else if len(value)+offset > MaxValueSize {
		return 0, errValueSize
	}

	oldValue, err := db.eng.GetBytesNoLock(db.defaultReadOpts, key)
	if err != nil {
		return 0, err
	}
	db.wb.Clear()
	if oldValue == nil {
		db.IncrTableKeyCount(table, 1, db.wb)
	} else if len(oldValue) < tsLen {
		return 0, errInvalidDBValue
	} else {
		oldValue = oldValue[:len(oldValue)-tsLen]
	}

	extra := offset + len(value) - len(oldValue)
	if extra > 0 {
		oldValue = append(oldValue, make([]byte, extra)...)
	}
	copy(oldValue[offset:], value)
	oldValue = append(oldValue, PutInt64(ts)...)
	db.wb.Put(key, oldValue)

	err = db.eng.Write(db.defaultWriteOpts, db.wb)

	if err != nil {
		return 0, err
	}
	return int64(len(oldValue) - tsLen), nil
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

func (db *RockDB) Append(ts int64, key []byte, value []byte) (int64, error) {
	if len(value) == 0 {
		return 0, nil
	}

	table, key, err := convertRedisKeyToDBKVKey(key)
	if err != nil {
		return 0, err
	}

	oldValue, err := db.eng.GetBytesNoLock(db.defaultReadOpts, key)
	if err != nil {
		return 0, err
	}

	if len(oldValue)+len(value) > MaxValueSize {
		return 0, errValueSize
	}
	db.wb.Clear()
	if oldValue == nil {
		db.IncrTableKeyCount(table, 1, db.wb)
	} else if len(oldValue) < tsLen {
		return 0, errInvalidDBValue
	} else {
		oldValue = oldValue[:len(oldValue)-tsLen]
	}

	oldValue = append(oldValue, value...)
	oldValue = append(oldValue, PutInt64(ts)...)

	db.wb.Put(key, oldValue)
	err = db.eng.Write(db.defaultWriteOpts, db.wb)
	if err != nil {
		return 0, err
	}

	return int64(len(oldValue) - tsLen), nil
}

// BitSet set the bitmap data with format as below:
// key -> 0(first bit) 0 0 0 0 0 0 0 (last bit) | (second byte with 8 bits) | .... | (last byte with 8bits) at most MaxBitOffset/8 bytes for each bitmap
func (db *RockDB) bitSetOld(ts int64, key []byte, offset int64, on int) (int64, error) {
	table, key, err := convertRedisKeyToDBKVKey(key)
	if err != nil {
		return 0, err
	}
	if offset > MaxBitOffset || offset < 0 {
		return 0, ErrBitOverflow
	}

	if (on & ^1) != 0 {
		return 0, fmt.Errorf("bit should be 0 or 1, got %d", on)
	}
	var v []byte
	if v, err = db.eng.GetBytesNoLock(db.defaultReadOpts, key); err != nil {
		return 0, err
	}
	db.wb.Clear()
	if v == nil {
		db.IncrTableKeyCount(table, 1, db.wb)
	} else if len(v) >= tsLen {
		v = v[:len(v)-tsLen]
	}

	byteOffset := int(uint32(offset) >> 3)
	expandLen := byteOffset + 1 - len(v)
	if expandLen > 0 {
		if on == 0 {
			// not changed
			return 0, nil
		}
		v = append(v, make([]byte, expandLen)...)
	}
	byteVal := v[byteOffset]
	bit := 7 - uint8(uint32(offset)&0x7)
	oldBit := byteVal & (1 << bit)

	byteVal &= ^(1 << bit)
	byteVal |= (uint8(on&0x1) << bit)
	v[byteOffset] = byteVal
	v = append(v, PutInt64(ts)...)
	db.wb.Put(key, v)
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

func (db *RockDB) Expire(key []byte, duration int64) (int64, error) {
	if exists, err := db.KVExists(key); err != nil || exists != 1 {
		return 0, err
	} else {
		if err2 := db.expire(KVType, key, duration); err2 != nil {
			return 0, err2
		} else {
			return 1, nil
		}
	}
}

func (db *RockDB) Persist(key []byte) (int64, error) {
	if exists, err := db.KVExists(key); err != nil || exists != 1 {
		return 0, err
	}

	if ttl, err := db.ttl(KVType, key); err != nil || ttl < 0 {
		return 0, err
	}

	db.wb.Clear()
	if err := db.delExpire(KVType, key, db.wb); err != nil {
		return 0, err
	} else {
		if err2 := db.eng.Write(db.defaultWriteOpts, db.wb); err2 != nil {
			return 0, err2
		} else {
			return 1, nil
		}
	}
}
