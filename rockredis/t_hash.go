package rockredis

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/absolute8511/gorocksdb"
)

var (
	errHashKey       = errors.New("invalid hash key")
	errHSizeKey      = errors.New("invalid hash size key")
	errHashFieldSize = errors.New("invalid hash field size")
)

const (
	hashStartSep byte = ':'
)

func convertRedisKeyToDBHKey(key []byte, field []byte) ([]byte, error) {
	table, rk, err := extractTableFromRedisKey(key)
	if err != nil {
		return nil, err
	}

	if err := checkHashKFSize(rk, field); err != nil {
		return nil, err
	}
	key = hEncodeHashKey(table, key[len(table)+1:], field)
	return key, nil
}

func checkHashKFSize(key []byte, field []byte) error {
	if len(key) > MaxKeySize || len(key) == 0 {
		return errKeySize
	} else if len(field) > MaxHashFieldSize {
		return errHashFieldSize
	}
	return nil
}

func hEncodeSizeKey(key []byte) []byte {
	buf := make([]byte, len(key)+1+len(metaPrefix))

	pos := 0
	buf[pos] = HSizeType

	pos++
	copy(buf[pos:], metaPrefix)
	pos += len(metaPrefix)
	copy(buf[pos:], key)
	return buf
}

func hDecodeSizeKey(ek []byte) ([]byte, error) {
	pos := 0

	if pos+1+len(metaPrefix) > len(ek) || ek[pos] != HSizeType {
		return nil, errHSizeKey
	}
	pos++
	pos += len(metaPrefix)

	return ek[pos:], nil
}

func hEncodeHashKey(table []byte, key []byte, field []byte) []byte {
	buf := make([]byte, len(table)+2+1+len(key)+len(field)+1+1+2)

	pos := 0
	buf[pos] = HashType
	pos++

	// in order to make sure all the table data are in the same range
	// we need make sure we has the same table prefix
	binary.BigEndian.PutUint16(buf[pos:], uint16(len(table)))
	pos += 2
	copy(buf[pos:], table)
	pos += len(table)
	buf[pos] = tableStartSep
	pos++

	binary.BigEndian.PutUint16(buf[pos:], uint16(len(key)))
	pos += 2

	copy(buf[pos:], key)
	pos += len(key)

	buf[pos] = hashStartSep
	pos++
	copy(buf[pos:], field)
	return buf
}

func hDecodeHashKey(ek []byte) ([]byte, []byte, []byte, error) {
	pos := 0
	if pos+1 > len(ek) || ek[pos] != HashType {
		return nil, nil, nil, errHashKey
	}
	pos++

	if pos+2 > len(ek) {
		return nil, nil, nil, errHashKey
	}

	tableLen := int(binary.BigEndian.Uint16(ek[pos:]))
	pos += 2
	if tableLen+pos > len(ek) {
		return nil, nil, nil, errHashKey
	}
	table := ek[pos : pos+tableLen]
	pos += tableLen
	if ek[pos] != tableStartSep {
		return nil, nil, nil, errHashKey
	}
	pos++
	if pos+2 > len(ek) {
		return nil, nil, nil, errHashKey
	}

	keyLen := int(binary.BigEndian.Uint16(ek[pos:]))
	pos += 2

	if keyLen+pos > len(ek) {
		return nil, nil, nil, errHashKey
	}

	key := ek[pos : pos+keyLen]
	pos += keyLen

	if ek[pos] != hashStartSep {
		return nil, nil, nil, errHashKey
	}

	pos++
	field := ek[pos:]
	return table, key, field, nil
}

func hEncodeStartKey(table []byte, key []byte) []byte {
	return hEncodeHashKey(table, key, nil)
}

func hEncodeStopKey(table []byte, key []byte) []byte {
	k := hEncodeHashKey(table, key, nil)
	k[len(k)-1] = k[len(k)-1] + 1
	return k
}

// return if we create the new field or override it
func (db *RockDB) hSetField(ts int64, checkNX bool, hkey []byte, field []byte, value []byte,
	wb *gorocksdb.WriteBatch, hindex *HsetIndex) (int64, error) {
	table, rk, err := extractTableFromRedisKey(hkey)

	if err != nil {
		return 0, err
	}
	if err := checkHashKFSize(rk, field); err != nil {
		return 0, err
	}
	ek := hEncodeHashKey(table, rk, field)

	created := int64(1)
	tsBuf := PutInt64(ts)
	value = append(value, tsBuf...)
	var oldV []byte
	if oldV, _ = db.eng.GetBytesNoLock(db.defaultReadOpts, ek); oldV != nil {
		created = 0
		if checkNX || bytes.Equal(oldV, value) {
			return created, nil
		}
	} else {
		if n, err := db.hIncrSize(hkey, 1, wb); err != nil {
			return 0, err
		} else if n == 1 {
			db.IncrTableKeyCount(table, 1, wb)
		}
	}
	wb.Put(ek, value)

	if hindex != nil {
		if len(oldV) >= tsLen {
			oldV = oldV[:len(oldV)-tsLen]
		}
		err = hindex.UpdateRec(oldV, value[:len(value)-tsLen], hkey, wb)
		if err != nil {
			return created, err
		}
	}

	return created, nil
}

func (db *RockDB) HLen(hkey []byte) (int64, error) {
	if err := checkKeySize(hkey); err != nil {
		return 0, err
	}
	sizeKey := hEncodeSizeKey(hkey)
	v, err := db.eng.GetBytes(db.defaultReadOpts, sizeKey)
	return Int64(v, err)
}

func (db *RockDB) hIncrSize(hkey []byte, delta int64, wb *gorocksdb.WriteBatch) (int64, error) {
	sk := hEncodeSizeKey(hkey)

	var err error
	var size int64
	if size, err = Int64(db.eng.GetBytesNoLock(db.defaultReadOpts, sk)); err != nil {
		return 0, err
	} else {
		size += delta
		if size <= 0 {
			size = 0
			wb.Delete(sk)
		} else {
			wb.Put(sk, PutInt64(size))
		}
	}
	return size, nil
}

func (db *RockDB) HSet(ts int64, checkNX bool, key []byte, field []byte, value []byte) (int64, error) {
	if err := checkValueSize(value); err != nil {
		return 0, err
	}
	table, _, err := extractTableFromRedisKey(key)

	if err != nil {
		return 0, err
	}

	tableIndexes := db.indexMgr.GetTableIndexes(string(table))
	var hindex *HsetIndex
	if tableIndexes != nil {
		tableIndexes.Lock()
		defer tableIndexes.Unlock()
		hindex = tableIndexes.GetHIndexNoLock(string(field))
	}
	db.wb.Clear()

	created, err := db.hSetField(ts, checkNX, key, field, value, db.wb, hindex)
	if err != nil {
		return 0, err
	}

	err = db.eng.Write(db.defaultWriteOpts, db.wb)
	return created, err
}

func (db *RockDB) HMset(ts int64, key []byte, args ...common.KVRecord) error {
	if len(args) >= MAX_BATCH_NUM {
		return errTooMuchBatchSize
	}
	if len(args) == 0 {
		return nil
	}
	table, rk, err := extractTableFromRedisKey(key)
	if err != nil {
		return err
	}
	tableIndexes := db.indexMgr.GetTableIndexes(string(table))
	if tableIndexes != nil {
		tableIndexes.Lock()
		defer tableIndexes.Unlock()
	}
	db.wb.Clear()

	var num int64
	var value []byte
	tsBuf := PutInt64(ts)
	for i := 0; i < len(args); i++ {
		if err = checkHashKFSize(rk, args[i].Key); err != nil {
			return err
		} else if err = checkValueSize(args[i].Value); err != nil {
			return err
		}
		ek := hEncodeHashKey(table, rk, args[i].Key)

		var oldV []byte
		if oldV, err = db.eng.GetBytesNoLock(db.defaultReadOpts, ek); err != nil {
			return err
		} else if oldV == nil {
			num++
		}
		value = value[:0]
		value = append(value, args[i].Value...)
		value = append(value, tsBuf...)
		db.wb.Put(ek, value)

		if tableIndexes != nil {
			if hindex := tableIndexes.GetHIndexNoLock(string(args[i].Key)); hindex != nil {
				if len(oldV) >= tsLen {
					oldV = oldV[:len(oldV)-tsLen]
				}
				err = hindex.UpdateRec(oldV, value[:len(value)-tsLen], key, db.wb)
				if err != nil {
					return err
				}
			}
		}
	}
	if newNum, err := db.hIncrSize(key, num, db.wb); err != nil {
		return err
	} else if newNum > 0 && newNum == num {
		db.IncrTableKeyCount(table, 1, db.wb)
	}

	err = db.eng.Write(db.defaultWriteOpts, db.wb)
	return err
}

func (db *RockDB) HGet(key []byte, field []byte) ([]byte, error) {
	if err := checkHashKFSize(key, field); err != nil {
		return nil, err
	}

	dbKey, err := convertRedisKeyToDBHKey(key, field)
	if err != nil {
		return nil, err
	}
	v, err := db.eng.GetBytes(db.defaultReadOpts, dbKey)
	if len(v) >= tsLen {
		v = v[:len(v)-tsLen]
	}
	return v, err
}

func (db *RockDB) HKeyExists(key []byte) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}
	sk := hEncodeSizeKey(key)
	v, err := db.eng.GetBytes(db.defaultReadOpts, sk)
	if v != nil && err == nil {
		return 1, nil
	}
	return 0, err
}

func (db *RockDB) HMget(key []byte, args ...[]byte) ([][]byte, error) {
	if len(args) >= MAX_BATCH_NUM {
		return nil, errTooMuchBatchSize
	}
	var err error
	r := make([][]byte, len(args))
	for i := 0; i < len(args); i++ {
		r[i], err = db.HGet(key, args[i])
		if err != nil {
			return nil, err
		}
	}
	return r, nil
}

func (db *RockDB) HDel(key []byte, args ...[]byte) (int64, error) {
	if len(args) >= MAX_BATCH_NUM {
		return 0, errTooMuchBatchSize
	}
	if len(args) == 0 {
		return 0, nil
	}
	table, rk, err := extractTableFromRedisKey(key)
	if err != nil {
		return 0, err
	}

	tableIndexes := db.indexMgr.GetTableIndexes(string(table))
	if tableIndexes != nil {
		tableIndexes.Lock()
		defer tableIndexes.Unlock()
	}

	db.wb.Clear()
	wb := db.wb
	var ek []byte
	var oldV []byte

	var num int64 = 0
	var newNum int64 = -1
	for i := 0; i < len(args); i++ {
		if err := checkHashKFSize(rk, args[i]); err != nil {
			return 0, err
		}

		ek = hEncodeHashKey(table, rk, args[i])
		oldV, err = db.eng.GetBytesNoLock(db.defaultReadOpts, ek)
		if oldV == nil {
			continue
		} else {
			num++
			wb.Delete(ek)

			if tableIndexes != nil {
				if hindex := tableIndexes.GetHIndexNoLock(string(args[i])); hindex != nil {
					if len(oldV) >= tsLen {
						oldV = oldV[:len(oldV)-tsLen]
					}
					hindex.RemoveRec(oldV, key, wb)
				}
			}
		}
	}

	if newNum, err = db.hIncrSize(key, -num, wb); err != nil {
		return 0, err
	} else if num > 0 && newNum == 0 {
		db.IncrTableKeyCount(table, -1, wb)
		db.delExpire(HashType, key, wb)
	}

	err = db.eng.Write(db.defaultWriteOpts, wb)
	return num, err
}

func (db *RockDB) hDeleteAll(hkey []byte, wb *gorocksdb.WriteBatch, tableIndexes *TableIndexContainer) error {
	sk := hEncodeSizeKey(hkey)
	table, rk, err := extractTableFromRedisKey(hkey)
	if err != nil {
		return err
	}
	start := hEncodeStartKey(table, rk)
	stop := hEncodeStopKey(table, rk)

	if tableIndexes != nil {
		it, err := NewDBRangeIterator(db.eng, start, stop, common.RangeROpen, false)
		if err != nil {
			return err
		}
		defer it.Close()

		for ; it.Valid(); it.Next() {
			rawk := it.RefKey()
			_, _, field, _ := hDecodeHashKey(rawk)
			if hindex := tableIndexes.GetHIndexNoLock(string(field)); hindex != nil {
				oldV := it.RefValue()
				if len(oldV) >= tsLen {
					oldV = oldV[:len(oldV)-tsLen]
				}
				hindex.RemoveRec(oldV, hkey, wb)
			}
		}
	}
	wb.DeleteRange(start, stop)
	wb.Delete(sk)
	return nil
}

func (db *RockDB) HClear(hkey []byte) (int64, error) {
	if err := checkKeySize(hkey); err != nil {
		return 0, err
	}
	table, _, err := extractTableFromRedisKey(hkey)
	if len(table) == 0 {
		return 0, errTableName
	}

	tableIndexes := db.indexMgr.GetTableIndexes(string(table))
	if tableIndexes != nil {
		tableIndexes.Lock()
		defer tableIndexes.Unlock()
	}

	hlen, err := db.HLen(hkey)
	if err != nil {
		return 0, err
	}

	wb := db.wb
	wb.Clear()
	err = db.hDeleteAll(hkey, wb, tableIndexes)
	if err != nil {
		return 0, err
	}
	if hlen > 0 {
		db.IncrTableKeyCount(table, -1, wb)
		db.delExpire(HashType, hkey, wb)
	}

	err = db.eng.Write(db.defaultWriteOpts, wb)
	return hlen, err
}

func (db *RockDB) hClearWithBatch(hkey []byte, wb *gorocksdb.WriteBatch) error {
	if err := checkKeySize(hkey); err != nil {
		return err
	}

	hlen, err := db.HLen(hkey)
	if err != nil {
		return err
	}
	table, _, err := extractTableFromRedisKey(hkey)
	if len(table) == 0 {
		return errTableName
	}
	tableIndexes := db.indexMgr.GetTableIndexes(string(table))
	if tableIndexes != nil {
		tableIndexes.Lock()
		defer tableIndexes.Unlock()
	}

	err = db.hDeleteAll(hkey, wb, tableIndexes)
	if err != nil {
		return err
	}
	if hlen > 0 {
		db.IncrTableKeyCount(table, -1, wb)
		db.delExpire(HashType, hkey, wb)
	}

	return err
}

func (db *RockDB) HMclear(keys ...[]byte) {
	for _, key := range keys {
		db.HClear(key)
	}
}

func (db *RockDB) HIncrBy(ts int64, key []byte, field []byte, delta int64) (int64, error) {
	if err := checkHashKFSize(key, field); err != nil {
		return 0, err
	}
	table, _, err := extractTableFromRedisKey(key)
	if err != nil {
		return 0, err
	}

	var ek []byte

	ek, err = convertRedisKeyToDBHKey(key, field)
	if err != nil {
		return 0, err
	}

	tableIndexes := db.indexMgr.GetTableIndexes(string(table))
	var hindex *HsetIndex
	if tableIndexes != nil {
		tableIndexes.Lock()
		defer tableIndexes.Unlock()
		hindex = tableIndexes.GetHIndexNoLock(string(field))
	}
	wb := db.wb
	wb.Clear()

	var n int64
	oldV, err := db.eng.GetBytesNoLock(db.defaultReadOpts, ek)
	if len(oldV) >= tsLen {
		oldV = oldV[:len(oldV)-tsLen]
	}
	if n, err = StrInt64(oldV, err); err != nil {
		return 0, err
	}

	n += delta

	_, err = db.hSetField(ts, false, key, field, FormatInt64ToSlice(n), wb, hindex)
	if err != nil {
		return 0, err
	}

	err = db.eng.Write(db.defaultWriteOpts, wb)
	return n, err
}

func (db *RockDB) HGetAll(key []byte) (int64, chan common.KVRecordRet, error) {
	if err := checkKeySize(key); err != nil {
		return 0, nil, err
	}

	table, rk, err := extractTableFromRedisKey(key)
	if err != nil {
		return 0, nil, err
	}

	length, err := db.HLen(key)
	if err != nil {
		return 0, nil, err
	}
	if length >= MAX_BATCH_NUM {
		return length, nil, errTooMuchBatchSize
	}

	start := hEncodeStartKey(table, rk)
	stop := hEncodeStopKey(table, rk)
	it, err := NewDBRangeIterator(db.eng, start, stop, common.RangeROpen, false)
	if err != nil {
		return 0, nil, err
	}
	it.NoTimestamp(HashType)

	valCh := make(chan common.KVRecordRet, 16)
	doScan := func() {
		defer it.Close()
		defer close(valCh)
		for ; it.Valid(); it.Next() {
			_, _, f, err := hDecodeHashKey(it.Key())
			v := it.Value()
			select {
			case valCh <- common.KVRecordRet{
				Rec: common.KVRecord{Key: f, Value: v},
				Err: err,
			}:
			case <-db.quit:
				break
			}
		}
	}
	if length < int64(len(valCh)) {
		doScan()
	} else {
		go doScan()
	}

	return length, valCh, nil
}

func (db *RockDB) HKeys(key []byte) (int64, chan common.KVRecordRet, error) {
	if err := checkKeySize(key); err != nil {
		return 0, nil, err
	}
	table, rk, err := extractTableFromRedisKey(key)

	length, err := db.HLen(key)
	if err != nil {
		return 0, nil, err
	}
	if length >= MAX_BATCH_NUM {
		return length, nil, errTooMuchBatchSize
	}
	start := hEncodeStartKey(table, rk)
	stop := hEncodeStopKey(table, rk)
	it, err := NewDBRangeIterator(db.eng, start, stop, common.RangeROpen, false)
	if err != nil {
		return 0, nil, err
	}

	valCh := make(chan common.KVRecordRet, 16)
	doScan := func() {
		defer it.Close()
		defer close(valCh)
		for ; it.Valid(); it.Next() {
			_, _, f, err := hDecodeHashKey(it.Key())
			valCh <- common.KVRecordRet{
				Rec: common.KVRecord{Key: f, Value: nil},
				Err: err,
			}
		}
	}
	if length < int64(len(valCh)) {
		doScan()
	} else {
		go doScan()
	}
	return length, valCh, nil
}

func (db *RockDB) HValues(key []byte) (int64, chan common.KVRecordRet, error) {
	if err := checkKeySize(key); err != nil {
		return 0, nil, err
	}

	table, rk, err := extractTableFromRedisKey(key)
	if err != nil {
		return 0, nil, err
	}

	length, err := db.HLen(key)
	if err != nil {
		return 0, nil, err
	}
	if length >= MAX_BATCH_NUM {
		return length, nil, errTooMuchBatchSize
	}

	start := hEncodeStartKey(table, rk)
	stop := hEncodeStopKey(table, rk)
	it, err := NewDBRangeIterator(db.eng, start, stop, common.RangeROpen, false)
	if err != nil {
		return 0, nil, err
	}
	it.NoTimestamp(HashType)
	valCh := make(chan common.KVRecordRet, 16)
	go func() {
		defer it.Close()
		defer close(valCh)
		for ; it.Valid(); it.Next() {
			va := it.Value()
			valCh <- common.KVRecordRet{
				Rec: common.KVRecord{Key: nil, Value: va},
				Err: nil,
			}
		}
	}()

	return length, valCh, nil
}

func (db *RockDB) HExpire(key []byte, duration int64) (int64, error) {
	if exists, err := db.HKeyExists(key); err != nil || exists != 1 {
		return 0, err
	} else {
		if err2 := db.expire(HashType, key, duration); err2 != nil {
			return 0, err2
		} else {
			return 1, nil
		}
	}
}

func (db *RockDB) HPersist(key []byte) (int64, error) {
	if exists, err := db.HKeyExists(key); err != nil || exists != 1 {
		return 0, err
	}

	if ttl, err := db.ttl(HashType, key); err != nil || ttl < 0 {
		return 0, err
	}

	db.wb.Clear()
	if err := db.delExpire(HashType, key, db.wb); err != nil {
		return 0, err
	} else {
		if err2 := db.eng.Write(db.defaultWriteOpts, db.wb); err2 != nil {
			return 0, err2
		} else {
			return 1, nil
		}
	}
}
