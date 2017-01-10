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
	hashStopSep  byte = hashStartSep + 1
)

func convertRedisKeyToDBHKey(key []byte, field []byte) ([]byte, []byte, error) {
	table := extractTableFromRedisKey(key)
	if len(table) == 0 {
		return nil, nil, errTableName
	}

	if err := checkHashKFSize(key, field); err != nil {
		return nil, nil, err
	}
	key = hEncodeHashKey(key, field)
	return table, key, nil
}

func checkHashKFSize(key []byte, field []byte) error {
	if len(key) > MaxKeySize || len(key) == 0 {
		return errKeySize
	} else if len(field) > MaxHashFieldSize || len(field) == 0 {
		return errHashFieldSize
	}
	return nil
}

func hEncodeSizeKey(key []byte) []byte {
	buf := make([]byte, len(key)+1)

	pos := 0
	buf[pos] = HSizeType

	pos++
	copy(buf[pos:], key)
	return buf
}

func hDecodeSizeKey(ek []byte) ([]byte, error) {
	pos := 0

	if pos+1 > len(ek) || ek[pos] != HSizeType {
		return nil, errHSizeKey
	}
	pos++

	return ek[pos:], nil
}

func hEncodeHashKey(key []byte, field []byte) []byte {
	buf := make([]byte, len(key)+len(field)+1+1+2)

	pos := 0
	buf[pos] = HashType
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

func hDecodeHashKey(ek []byte) ([]byte, []byte, error) {
	pos := 0
	if pos+1 > len(ek) || ek[pos] != HashType {
		return nil, nil, errHashKey
	}
	pos++

	if pos+2 > len(ek) {
		return nil, nil, errHashKey
	}

	keyLen := int(binary.BigEndian.Uint16(ek[pos:]))
	pos += 2

	if keyLen+pos > len(ek) {
		return nil, nil, errHashKey
	}

	key := ek[pos : pos+keyLen]
	pos += keyLen

	if ek[pos] != hashStartSep {
		return nil, nil, errHashKey
	}

	pos++
	field := ek[pos:]
	return key, field, nil
}

func hEncodeStartKey(key []byte) []byte {
	return hEncodeHashKey(key, nil)
}

func hEncodeStopKey(key []byte) []byte {
	k := hEncodeHashKey(key, nil)
	k[len(k)-1] = hashStopSep
	return k
}

// return if we create the new field or override it
func (db *RockDB) hSetField(hkey []byte, field []byte, value []byte, wb *gorocksdb.WriteBatch) (int64, error) {
	table, ek, err := convertRedisKeyToDBHKey(hkey, field)
	if err != nil {
		return 0, err
	}

	created := int64(1)
	if v, _ := db.eng.GetBytes(db.defaultReadOpts, ek); v != nil {
		created = 0
		if bytes.Equal(v, value) {
			return created, nil
		}
	} else {
		if n, err := db.hIncrSize(hkey, 1, wb); err != nil {
			return 0, err
		} else if n == 1 {
			_, err = db.IncrTableKeyCount(table, 1, wb)
			if err != nil {
				return 0, err
			}
		}
	}

	wb.Put(ek, value)
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
	var size int64 = 0
	if size, err = Int64(db.eng.GetBytes(db.defaultReadOpts, sk)); err != nil {
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

func (db *RockDB) HSet(key []byte, field []byte, value []byte) (int64, error) {
	if err := checkValueSize(value); err != nil {
		return 0, err
	}

	db.wb.Clear()

	created, err := db.hSetField(key, field, value, db.wb)
	if err != nil {
		return 0, err
	}

	err = db.eng.Write(db.defaultWriteOpts, db.wb)
	return created, err
}

func (db *RockDB) HMset(key []byte, args ...common.KVRecord) error {
	if len(args) >= MAX_BATCH_NUM {
		return errTooMuchBatchSize
	}
	if len(args) == 0 {
		return nil
	}
	table, _, err := convertRedisKeyToDBHKey(key, args[0].Key)
	if err != nil {
		return err
	}
	db.wb.Clear()

	var num int64 = 0
	for i := 0; i < len(args); i++ {
		if err = checkHashKFSize(key, args[i].Key); err != nil {
			return err
		} else if err = checkValueSize(args[i].Value); err != nil {
			return err
		}
		ek := hEncodeHashKey(key, args[i].Key)

		if v, err := db.eng.GetBytes(db.defaultReadOpts, ek); err != nil {
			return err
		} else if v == nil {
			num++
		}
		db.wb.Put(ek, args[i].Value)
	}
	if newNum, err := db.hIncrSize(key, num, db.wb); err != nil {
		return err
	} else if newNum > 0 && newNum == num {
		_, err = db.IncrTableKeyCount(table, 1, db.wb)
		if err != nil {
			return err
		}
	}

	err = db.eng.Write(db.defaultWriteOpts, db.wb)
	return err
}

func (db *RockDB) HGet(key []byte, field []byte) ([]byte, error) {
	if err := checkHashKFSize(key, field); err != nil {
		return nil, err
	}

	return db.eng.GetBytes(db.defaultReadOpts, hEncodeHashKey(key, field))
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
	table, _, err := convertRedisKeyToDBHKey(key, args[0])
	if err != nil {
		return 0, err
	}

	db.wb.Clear()
	wb := db.wb

	var ek []byte
	var v []byte

	var num int64 = 0
	var newNum int64 = -1
	for i := 0; i < len(args); i++ {
		if err := checkHashKFSize(key, args[i]); err != nil {
			return 0, err
		}

		ek = hEncodeHashKey(key, args[i])
		v, err = db.eng.GetBytes(db.defaultReadOpts, ek)
		if v == nil {
			continue
		} else {
			num++
			wb.Delete(ek)
		}
	}

	if newNum, err = db.hIncrSize(key, -num, wb); err != nil {
		return 0, err
	} else if num > 0 && newNum == 0 {
		_, err = db.IncrTableKeyCount(table, -1, wb)
		if err != nil {
			return 0, err
		}
	}

	err = db.eng.Write(db.defaultWriteOpts, wb)
	return num, err
}

func (db *RockDB) hDeleteAll(hkey []byte, wb *gorocksdb.WriteBatch) int64 {
	sk := hEncodeSizeKey(hkey)
	start := hEncodeStartKey(hkey)
	stop := hEncodeStopKey(hkey)

	it := NewDBRangeIterator(db.eng, start, stop, common.RangeROpen, false)
	defer it.Close()
	var num int64 = 0
	for ; it.Valid(); it.Next() {
		rawk := it.RefKey()
		wb.Delete(rawk)
		num++
	}

	wb.Delete(sk)
	return num
}

func (db *RockDB) HClear(hkey []byte) (int64, error) {
	if err := checkKeySize(hkey); err != nil {
		return 0, err
	}

	hlen, err := db.HLen(hkey)
	if err != nil {
		return 0, err
	}
	table := extractTableFromRedisKey(hkey)
	if len(table) == 0 {
		return 0, errTableName
	}

	if hlen > RANGE_DELETE_NUM {
		var r gorocksdb.Range
		sk := hEncodeSizeKey(hkey)
		r.Start = hEncodeStartKey(hkey)
		r.Limit = hEncodeStopKey(hkey)
		db.eng.DeleteFilesInRange(r)
		db.eng.CompactRange(r)
		db.eng.Delete(db.defaultWriteOpts, sk)
	}
	wb := db.wb
	wb.Clear()
	db.hDeleteAll(hkey, wb)
	if hlen > 0 {
		_, err = db.IncrTableKeyCount(table, -1, wb)
		if err != nil {
			return 0, err
		}
	}

	err = db.eng.Write(db.defaultWriteOpts, wb)
	return hlen, err
}

func (db *RockDB) HMclear(keys ...[]byte) {
	for _, key := range keys {
		db.HClear(key)
	}
}

func (db *RockDB) HIncrBy(key []byte, field []byte, delta int64) (int64, error) {
	if err := checkHashKFSize(key, field); err != nil {
		return 0, err
	}

	wb := db.wb
	wb.Clear()
	var ek []byte
	var err error

	ek = hEncodeHashKey(key, field)
	var n int64 = 0
	if n, err = StrInt64(db.eng.GetBytes(db.defaultReadOpts, ek)); err != nil {
		return 0, err
	}

	n += delta

	_, err = db.hSetField(key, field, FormatInt64ToSlice(n), wb)
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

	v := make(chan common.KVRecordRet, 16)
	len, err := db.HLen(key)
	if err != nil {
		return 0, nil, err
	}
	if len >= MAX_BATCH_NUM {
		return len, nil, errTooMuchBatchSize
	}

	go func() {
		start := hEncodeStartKey(key)
		stop := hEncodeStopKey(key)
		it := NewDBRangeIterator(db.eng, start, stop, common.RangeROpen, false)
		defer it.Close()
		defer close(v)
		for ; it.Valid(); it.Next() {
			_, f, err := hDecodeHashKey(it.Key())
			v <- common.KVRecordRet{
				common.KVRecord{Key: f, Value: it.Value()},
				err,
			}
		}
	}()

	return len, v, nil
}

func (db *RockDB) HKeys(key []byte) (int64, chan common.KVRecordRet, error) {
	if err := checkKeySize(key); err != nil {
		return 0, nil, err
	}

	len, err := db.HLen(key)
	if err != nil {
		return 0, nil, err
	}
	if len >= MAX_BATCH_NUM {
		return len, nil, errTooMuchBatchSize
	}
	v := make(chan common.KVRecordRet, 16)

	go func() {
		start := hEncodeStartKey(key)
		stop := hEncodeStopKey(key)
		it := NewDBRangeIterator(db.eng, start, stop, common.RangeROpen, false)
		defer it.Close()
		defer close(v)
		for ; it.Valid(); it.Next() {
			_, f, err := hDecodeHashKey(it.Key())
			v <- common.KVRecordRet{
				common.KVRecord{Key: f, Value: nil},
				err,
			}
		}
	}()

	return len, v, nil
}

func (db *RockDB) HValues(key []byte) (int64, chan common.KVRecordRet, error) {
	if err := checkKeySize(key); err != nil {
		return 0, nil, err
	}

	len, err := db.HLen(key)
	if err != nil {
		return 0, nil, err
	}
	if len >= MAX_BATCH_NUM {
		return len, nil, errTooMuchBatchSize
	}

	v := make(chan common.KVRecordRet, 16)

	go func() {
		start := hEncodeStartKey(key)
		stop := hEncodeStopKey(key)
		it := NewDBRangeIterator(db.eng, start, stop, common.RangeROpen, false)
		defer it.Close()
		defer close(v)
		for ; it.Valid(); it.Next() {
			v <- common.KVRecordRet{
				common.KVRecord{Key: nil, Value: it.Value()},
				nil,
			}
		}
	}()

	return len, v, nil
}
