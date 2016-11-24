package rockredis

import (
	"errors"
	"github.com/absolute8511/gorocksdb"
)

type KVPair struct {
	Key   []byte
	Value []byte
}

var errKVKey = errors.New("invalid encode kv key")

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

func encodeKVMinKey() []byte {
	ek := encodeKVKey(nil)
	return ek
}

func encodeKVMaxKey() []byte {
	ek := encodeKVKey(nil)
	ek[len(ek)-1] = KVType + 1
	return ek
}

func (db *RockDB) incr(key []byte, delta int64) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}

	var err error
	key = encodeKVKey(key)

	var n int64
	n, err = StrInt64(db.eng.GetBytes(db.defaultReadOpts, key))
	if err != nil {
		return 0, err
	}
	n += delta

	err = db.eng.Put(db.defaultWriteOpts, key, FormatInt64ToSlice(n))
	return n, err
}

//	ps : here just focus on deleting the key-value data,
//		 any other likes expire is ignore.
func (db *RockDB) KVDel(key []byte) error {
	key = encodeKVKey(key)
	return db.eng.Delete(db.defaultWriteOpts, key)
}

func (db *RockDB) Decr(key []byte) (int64, error) {
	return db.incr(key, -1)
}

func (db *RockDB) DecrBy(key []byte, decrement int64) (int64, error) {
	return db.incr(key, -decrement)
}

func (db *RockDB) DelKeys(keys ...[]byte) {
	if len(keys) == 0 {
		return
	}

	for _, k := range keys {
		db.KVDel(k)
	}
}

func (db *RockDB) KVExists(key []byte) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}

	var err error
	key = encodeKVKey(key)

	var v []byte
	v, err = db.eng.GetBytes(db.defaultReadOpts, key)
	if v != nil && err == nil {
		return 1, nil
	}
	return 0, err
}

func (db *RockDB) KVGet(key []byte) ([]byte, error) {
	if err := checkKeySize(key); err != nil {
		return nil, err
	}

	key = encodeKVKey(key)

	return db.eng.GetBytes(db.defaultReadOpts, key)
}

func (db *RockDB) Incr(key []byte) (int64, error) {
	return db.incr(key, 1)
}

func (db *RockDB) IncrBy(key []byte, increment int64) (int64, error) {
	return db.incr(key, increment)
}

func (db *RockDB) MGet(keys ...[]byte) ([][]byte, []error) {
	keyList := make([][]byte, len(keys))
	errs := make([]error, len(keys))
	for i, k := range keys {
		if err := checkKeySize(k); err != nil {
			keyList[i] = nil
			errs[i] = err
		} else {
			kk := encodeKVKey(k)
			keyList[i] = kk
		}
	}
	db.eng.MultiGetBytes(db.defaultReadOpts, keyList, keyList, errs)
	return keyList, errs
}

func (db *RockDB) MSet(args ...KVPair) error {
	if len(args) == 0 {
		return nil
	}
	if len(args) > MAX_BATCH_NUM {
		return errTooMuchBatchSize
	}

	wb := gorocksdb.NewWriteBatch()
	defer wb.Destroy()

	var err error
	var key []byte
	var value []byte
	for i := 0; i < len(args); i++ {
		if err := checkKeySize(args[i].Key); err != nil {
			return err
		} else if err := checkValueSize(args[i].Value); err != nil {
			return err
		}
		key = encodeKVKey(args[i].Key)
		value = args[i].Value
		wb.Put(key, value)
	}

	err = db.eng.Write(db.defaultWriteOpts, wb)
	return err
}

func (db *RockDB) KVSet(key []byte, value []byte) error {
	if err := checkKeySize(key); err != nil {
		return err
	} else if err := checkValueSize(value); err != nil {
		return err
	}
	key = encodeKVKey(key)
	err := db.eng.Put(db.defaultWriteOpts, key, value)
	return err
}

func (db *RockDB) SetNX(key []byte, value []byte) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	} else if err := checkValueSize(value); err != nil {
		return 0, err
	}

	var err error
	key = encodeKVKey(key)
	var n int64 = 1

	if v, err := db.eng.GetBytes(db.defaultReadOpts, key); err != nil {
		return 0, err
	} else if v != nil {
		n = 0
	} else {
		db.eng.Put(db.defaultWriteOpts, key, value)
	}
	return n, err
}

func (db *RockDB) SetRange(key []byte, offset int, value []byte) (int64, error) {
	if len(value) == 0 {
		return 0, nil
	}

	if err := checkKeySize(key); err != nil {
		return 0, err
	} else if len(value)+offset > MaxValueSize {
		return 0, errValueSize
	}

	key = encodeKVKey(key)

	oldValue, err := db.eng.GetBytes(db.defaultReadOpts, key)
	if err != nil {
		return 0, err
	}

	extra := offset + len(value) - len(oldValue)
	if extra > 0 {
		oldValue = append(oldValue, make([]byte, extra)...)
	}
	copy(oldValue[offset:], value)

	err = db.eng.Put(db.defaultWriteOpts, key, oldValue)

	if err != nil {
		return 0, err
	}
	return int64(len(oldValue)), nil
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

func (db *RockDB) Append(key []byte, value []byte) (int64, error) {
	if len(value) == 0 {
		return 0, nil
	}

	if err := checkKeySize(key); err != nil {
		return 0, err
	}
	key = encodeKVKey(key)

	oldValue, err := db.eng.GetBytes(db.defaultReadOpts, key)
	if err != nil {
		return 0, err
	}

	if len(oldValue)+len(value) > MaxValueSize {
		return 0, errValueSize
	}

	oldValue = append(oldValue, value...)

	err = db.eng.Put(db.defaultWriteOpts, key, oldValue)
	if err != nil {
		return 0, err
	}

	return int64(len(oldValue)), nil
}
