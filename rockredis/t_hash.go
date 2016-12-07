package rockredis

import (
	"bytes"
	"encoding/binary"
	"errors"
	"github.com/absolute8511/gorocksdb"
)

type FVPair struct {
	Field []byte
	Value []byte
}

type FVPairRec struct {
	Rec FVPair
	Err error
}

var (
	errHashKey       = errors.New("invalid hash key")
	errHSizeKey      = errors.New("invalid hash size key")
	errHashFieldSize = errors.New("invalid hash field size")
)

const (
	hashStartSep byte = ':'
	hashStopSep  byte = hashStartSep + 1
)

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
	ek := hEncodeHashKey(hkey, field)

	created := int64(1)
	if v, _ := db.eng.GetBytes(db.defaultReadOpts, ek); v != nil {
		created = 0
		if bytes.Equal(v, value) {
			return created, nil
		}
	} else {
		if _, err := db.hIncrSize(hkey, 1, wb); err != nil {
			return 0, err
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
	if err := checkHashKFSize(key, field); err != nil {
		return 0, err
	} else if err := checkValueSize(value); err != nil {
		return 0, err
	}

	wb := gorocksdb.NewWriteBatch()
	defer wb.Destroy()

	created, err := db.hSetField(key, field, value, wb)
	if err != nil {
		return 0, err
	}

	err = db.eng.Write(db.defaultWriteOpts, wb)
	return created, err
}

func (db *RockDB) HMset(key []byte, args ...FVPair) error {
	if len(args) >= MAX_BATCH_NUM {
		return errTooMuchBatchSize
	}
	wb := gorocksdb.NewWriteBatch()
	defer wb.Destroy()

	var err error
	var num int64 = 0
	for i := 0; i < len(args); i++ {
		if err = checkHashKFSize(key, args[i].Field); err != nil {
			return err
		} else if err = checkValueSize(args[i].Value); err != nil {
			return err
		}
		ek := hEncodeHashKey(key, args[i].Field)

		if v, err := db.eng.GetBytes(db.defaultReadOpts, ek); err != nil {
			return err
		} else if v == nil {
			num++
		}
		wb.Put(ek, args[i].Value)
	}
	if _, err := db.hIncrSize(key, num, wb); err != nil {
		return err
	}

	err = db.eng.Write(db.defaultWriteOpts, wb)
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
	wb := gorocksdb.NewWriteBatch()
	defer wb.Destroy()

	var ek []byte
	var v []byte
	var err error

	var num int64 = 0
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

	if _, err = db.hIncrSize(key, -num, wb); err != nil {
		return 0, err
	}

	err = db.eng.Write(db.defaultWriteOpts, wb)
	return num, err
}

func (db *RockDB) hDeleteAll(hkey []byte, wb *gorocksdb.WriteBatch) int64 {
	sk := hEncodeSizeKey(hkey)
	start := hEncodeStartKey(hkey)
	stop := hEncodeStopKey(hkey)

	it := NewDBRangeIterator(db.eng, start, stop, RangeROpen, false)
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

func (db *RockDB) HClear(hkey []byte) error {
	if err := checkKeySize(hkey); err != nil {
		return err
	}

	len, err := db.HLen(hkey)
	if err != nil {
		return err
	}
	if len > RANGE_DELETE_NUM {
		var r gorocksdb.Range
		sk := hEncodeSizeKey(hkey)
		r.Start = hEncodeStartKey(hkey)
		r.Limit = hEncodeStopKey(hkey)
		db.eng.DeleteFilesInRange(r)
		db.eng.CompactRange(r)
		db.eng.Delete(db.defaultWriteOpts, sk)
	}
	wb := gorocksdb.NewWriteBatch()
	defer wb.Destroy()
	db.hDeleteAll(hkey, wb)
	err = db.eng.Write(db.defaultWriteOpts, wb)
	return err
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

	wb := gorocksdb.NewWriteBatch()
	defer wb.Destroy()
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

func (db *RockDB) HGetAll(key []byte) (int64, chan FVPairRec, error) {
	if err := checkKeySize(key); err != nil {
		return 0, nil, err
	}

	v := make(chan FVPairRec, 16)
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
		it := NewDBRangeIterator(db.eng, start, stop, RangeROpen, false)
		defer it.Close()
		defer close(v)
		for ; it.Valid(); it.Next() {
			_, f, err := hDecodeHashKey(it.Key())
			v <- FVPairRec{
				FVPair{Field: f, Value: it.Value()},
				err,
			}
		}
	}()

	return len, v, nil
}

func (db *RockDB) HKeys(key []byte) (int64, chan FVPairRec, error) {
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
	v := make(chan FVPairRec, 16)

	go func() {
		start := hEncodeStartKey(key)
		stop := hEncodeStopKey(key)
		it := NewDBRangeIterator(db.eng, start, stop, RangeROpen, false)
		defer it.Close()
		defer close(v)
		for ; it.Valid(); it.Next() {
			_, f, err := hDecodeHashKey(it.Key())
			v <- FVPairRec{
				FVPair{Field: f, Value: nil},
				err,
			}
		}
	}()

	return len, v, nil
}

func (db *RockDB) HValues(key []byte) (int64, chan FVPairRec, error) {
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

	v := make(chan FVPairRec, 16)

	go func() {
		start := hEncodeStartKey(key)
		stop := hEncodeStopKey(key)
		it := NewDBRangeIterator(db.eng, start, stop, RangeROpen, false)
		defer it.Close()
		defer close(v)
		for ; it.Valid(); it.Next() {
			v <- FVPairRec{
				FVPair{Field: nil, Value: it.Value()},
				nil,
			}
		}
	}()

	return len, v, nil
}
