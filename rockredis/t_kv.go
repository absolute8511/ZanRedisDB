package rockredis

import (
	"errors"
	"time"

	"github.com/absolute8511/ZanRedisDB/common"
)

const (
	tsLen = 8
)

var errKVKey = errors.New("invalid encode kv key")
var errInvalidDBValue = errors.New("invalide db value")

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
	v, err := db.eng.GetBytesNoLock(db.defaultReadOpts, key)
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
func (db *RockDB) KVDel(key []byte) error {
	table, key, err := convertRedisKeyToDBKVKey(key)
	if err != nil {
		return err
	}
	db.MaybeClearBatch()
	if db.cfg.EnableTableCounter {
		v, _ := db.eng.GetBytesNoLock(db.defaultReadOpts, key)
		if v != nil {
			db.IncrTableKeyCount(table, -1, db.wb)
		}
	}
	db.wb.Delete(key)
	return db.MaybeCommitBatch()
}

func (db *RockDB) Decr(ts int64, key []byte) (int64, error) {
	return db.incr(ts, key, -1)
}

func (db *RockDB) DecrBy(ts int64, key []byte, decrement int64) (int64, error) {
	return db.incr(ts, key, -decrement)
}

func (db *RockDB) DelKeys(keys ...[]byte) {
	if len(keys) == 0 {
		return
	}

	for _, k := range keys {
		db.KVDel(k)
	}

	//clear all the expire meta data related to the keys
	db.MaybeClearBatch()
	for _, k := range keys {
		db.delExpire(KVType, k, db.wb)
	}
	db.MaybeCommitBatch()
}

func (db *RockDB) KVExists(key []byte) (int64, error) {
	_, key, err := convertRedisKeyToDBKVKey(key)
	if err != nil {
		return 0, err
	}

	var v []byte
	v, err = db.eng.GetBytes(db.defaultReadOpts, key)
	if v != nil && err == nil {
		return 1, nil
	}
	return 0, err
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
			v, _ := db.eng.GetBytesNoLock(db.defaultReadOpts, key)
			if v == nil {
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
		v, _ := db.eng.GetBytesNoLock(db.defaultReadOpts, key)
		if v == nil {
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

func (db *RockDB) SetEx(ts int64, rawKey []byte, duration int64, value []byte) error {
	table, key, err := convertRedisKeyToDBKVKey(rawKey)
	if err != nil {
		return err
	} else if err = checkValueSize(value); err != nil {
		return err
	}
	db.MaybeClearBatch()
	if db.cfg.EnableTableCounter {
		v, _ := db.eng.GetBytesNoLock(db.defaultReadOpts, key)
		if v == nil {
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

	var v []byte
	var n int64 = 1

	if v, err = db.eng.GetBytesNoLock(db.defaultReadOpts, key); err != nil {
		return 0, err
	} else if v != nil {
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
