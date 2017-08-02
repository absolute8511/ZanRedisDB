package rockredis

import (
	"encoding/binary"
	"errors"

	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/absolute8511/gorocksdb"
)

var (
	errSetKey        = errors.New("invalid set key")
	errSSizeKey      = errors.New("invalid ssize key")
	errSetMemberSize = errors.New("invalid set member size")
)

const (
	setStartSep byte = ':'
	setStopSep  byte = setStartSep + 1
)

func checkSetKMSize(key []byte, member []byte) error {
	if len(key) > MaxKeySize || len(key) == 0 {
		return errKeySize
	} else if len(member) > MaxSetMemberSize || len(member) == 0 {
		return errSetMemberSize
	}
	return nil
}

func sEncodeSizeKey(key []byte) []byte {
	buf := make([]byte, len(key)+1+len(metaPrefix))

	pos := 0
	buf[pos] = SSizeType
	pos++
	copy(buf[pos:], metaPrefix)
	pos += len(metaPrefix)

	copy(buf[pos:], key)
	return buf
}

func sDecodeSizeKey(ek []byte) ([]byte, error) {
	pos := 0
	if pos+1+len(metaPrefix) > len(ek) || ek[pos] != SSizeType {
		return nil, errSSizeKey
	}
	pos++
	pos += len(metaPrefix)

	return ek[pos:], nil
}

func convertRedisKeyToDBSKey(key []byte, member []byte) ([]byte, error) {
	table, rk, err := extractTableFromRedisKey(key)
	if err != nil {
		return nil, err
	}
	if err := checkSetKMSize(rk, member); err != nil {
		return nil, err
	}
	dbKey := sEncodeSetKey(table, rk, member)
	return dbKey, nil
}

func sEncodeSetKey(table []byte, key []byte, member []byte) []byte {
	buf := make([]byte, len(table)+2+1+len(key)+len(member)+1+1+2)

	pos := 0

	buf[pos] = SetType
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

	buf[pos] = setStartSep
	pos++
	copy(buf[pos:], member)

	return buf
}

func sDecodeSetKey(ek []byte) ([]byte, []byte, []byte, error) {
	pos := 0

	if pos+1 > len(ek) || ek[pos] != SetType {
		return nil, nil, nil, errSetKey
	}

	pos++

	if pos+2 > len(ek) {
		return nil, nil, nil, errSetKey
	}

	tableLen := int(binary.BigEndian.Uint16(ek[pos:]))
	pos += 2
	if tableLen+pos > len(ek) {
		return nil, nil, nil, errSetKey
	}
	table := ek[pos : pos+tableLen]
	pos += tableLen
	if ek[pos] != tableStartSep {
		return nil, nil, nil, errSetKey
	}
	pos++
	if pos+2 > len(ek) {
		return nil, nil, nil, errSetKey
	}

	keyLen := int(binary.BigEndian.Uint16(ek[pos:]))
	pos += 2

	if keyLen+pos > len(ek) {
		return table, nil, nil, errSetKey
	}

	key := ek[pos : pos+keyLen]
	pos += keyLen

	if ek[pos] != hashStartSep {
		return table, nil, nil, errSetKey
	}

	pos++
	member := ek[pos:]
	return table, key, member, nil
}

func sEncodeStartKey(table []byte, key []byte) []byte {
	return sEncodeSetKey(table, key, nil)
}

func sEncodeStopKey(table []byte, key []byte) []byte {
	k := sEncodeSetKey(table, key, nil)

	k[len(k)-1] = setStopSep

	return k
}

func (db *RockDB) sDelete(key []byte, wb *gorocksdb.WriteBatch) int64 {
	table, rk, err := extractTableFromRedisKey(key)
	if len(table) == 0 {
		return 0
	}

	sk := sEncodeSizeKey(key)
	start := sEncodeStartKey(table, rk)
	stop := sEncodeStopKey(table, rk)

	var num int64 = 0
	it, err := NewDBRangeIterator(db.eng, start, stop, common.RangeROpen, false)
	if err != nil {
		return 0
	}
	for ; it.Valid(); it.Next() {
		wb.Delete(it.RefKey())
		num++
	}
	it.Close()
	if num > 0 {
		db.IncrTableKeyCount(table, -1, wb)
		db.delExpire(SetType, key, wb)
	}

	wb.Delete(sk)
	return num
}

func (db *RockDB) sIncrSize(key []byte, delta int64, wb *gorocksdb.WriteBatch) (int64, error) {
	sk := sEncodeSizeKey(key)

	var err error
	var size int64 = 0
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

func (db *RockDB) sSetItem(key []byte, member []byte, wb *gorocksdb.WriteBatch) (int64, error) {
	table, _, err := extractTableFromRedisKey(key)
	if err != nil {
		return 0, err
	}

	ek, err := convertRedisKeyToDBSKey(key, member)
	if err != nil {
		return 0, err
	}

	var n int64 = 1
	if v, _ := db.eng.GetBytesNoLock(db.defaultReadOpts, ek); v != nil {
		n = 0
	} else {
		if newNum, err := db.sIncrSize(key, 1, wb); err != nil {
			return 0, err
		} else if newNum == 1 {
			db.IncrTableKeyCount(table, 1, wb)
		}
	}

	wb.Put(ek, nil)
	return n, nil
}

func (db *RockDB) SAdd(key []byte, args ...[]byte) (int64, error) {
	if len(args) >= MAX_BATCH_NUM {
		return 0, errTooMuchBatchSize
	}
	table, rk, _ := extractTableFromRedisKey(key)
	if len(table) == 0 {
		return 0, errTableName
	}

	wb := db.wb
	wb.Clear()

	var err error
	var ek []byte
	var num int64 = 0
	for i := 0; i < len(args); i++ {
		if err := checkSetKMSize(key, args[i]); err != nil {
			return 0, err
		}
		ek = sEncodeSetKey(table, rk, args[i])

		// TODO: how to tell not found and nil value (member value is also nil)
		if v, err := db.eng.GetBytesNoLock(db.defaultReadOpts, ek); err != nil {
			return 0, err
		} else if v == nil {
			num++
		}
		wb.Put(ek, nil)
	}

	if newNum, err := db.sIncrSize(key, num, wb); err != nil {
		return 0, err
	} else if newNum > 0 && newNum == num {
		db.IncrTableKeyCount(table, 1, wb)
	}

	err = db.eng.Write(db.defaultWriteOpts, wb)
	return num, err

}

func (db *RockDB) SCard(key []byte) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}

	sk := sEncodeSizeKey(key)
	return Int64(db.eng.GetBytes(db.defaultReadOpts, sk))
}

func (db *RockDB) SKeyExists(key []byte) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}
	sk := sEncodeSizeKey(key)
	v, err := db.eng.GetBytes(db.defaultReadOpts, sk)
	if v != nil && err == nil {
		return 1, nil
	}
	return 0, err
}

func (db *RockDB) SIsMember(key []byte, member []byte) (int64, error) {
	ek, err := convertRedisKeyToDBSKey(key, member)
	if err != nil {
		return 0, err
	}

	var n int64 = 1
	if v, err := db.eng.GetBytes(db.defaultReadOpts, ek); err != nil {
		return 0, err
	} else if v == nil {
		n = 0
	}
	return n, nil
}

func (db *RockDB) SMembers(key []byte) ([][]byte, error) {
	if err := checkKeySize(key); err != nil {
		return nil, err
	}

	table, rk, err := extractTableFromRedisKey(key)
	if err != nil {
		return nil, err
	}
	start := sEncodeStartKey(table, rk)
	stop := sEncodeStopKey(table, rk)

	v := make([][]byte, 0, 16)

	it, err := NewDBRangeIterator(db.eng, start, stop, common.RangeROpen, false)
	if err != nil {
		return nil, err
	}
	defer it.Close()
	for ; it.Valid(); it.Next() {
		_, _, m, err := sDecodeSetKey(it.Key())
		if err != nil {
			return nil, err
		}
		v = append(v, m)
	}

	return v, nil
}

func (db *RockDB) SRem(key []byte, args ...[]byte) (int64, error) {
	table, rk, _ := extractTableFromRedisKey(key)
	if len(table) == 0 {
		return 0, errTableName
	}

	wb := db.wb
	wb.Clear()

	var ek []byte
	var v []byte
	var err error

	var num int64 = 0
	for i := 0; i < len(args); i++ {
		if err := checkSetKMSize(key, args[i]); err != nil {
			return 0, err
		}

		ek = sEncodeSetKey(table, rk, args[i])
		v, err = db.eng.GetBytesNoLock(db.defaultReadOpts, ek)
		if v == nil {
			continue
		} else {
			num++
			wb.Delete(ek)
		}
	}

	if newNum, err := db.sIncrSize(key, -num, wb); err != nil {
		return 0, err
	} else if num > 0 && newNum == 0 {
		db.IncrTableKeyCount(table, -1, wb)
		db.delExpire(SetType, key, wb)
	}

	err = db.eng.Write(db.defaultWriteOpts, wb)
	return num, err
}

func (db *RockDB) SClear(key []byte) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}

	wb := gorocksdb.NewWriteBatch()
	num := db.sDelete(key, wb)
	err := db.eng.Write(db.defaultWriteOpts, wb)
	return num, err
}

func (db *RockDB) SMclear(keys ...[]byte) (int64, error) {
	if len(keys) >= MAX_BATCH_NUM {
		return 0, errTooMuchBatchSize
	}
	wb := gorocksdb.NewWriteBatch()
	for _, key := range keys {
		if err := checkKeySize(key); err != nil {
			return 0, err
		}
		db.sDelete(key, wb)
	}

	err := db.eng.Write(db.defaultWriteOpts, wb)
	return int64(len(keys)), err
}

func (db *RockDB) SExpire(key []byte, duration int64) (int64, error) {
	if exists, err := db.SKeyExists(key); err != nil || exists != 1 {
		return 0, err
	} else {
		if err2 := db.expire(SetType, key, duration); err2 != nil {
			return 0, err2
		} else {
			return 1, nil
		}
	}
}

func (db *RockDB) SPersist(key []byte) (int64, error) {
	if exists, err := db.SKeyExists(key); err != nil || exists != 1 {
		return 0, err
	}

	if ttl, err := db.ttl(SetType, key); err != nil || ttl < 0 {
		return 0, err
	}

	db.wb.Clear()
	if err := db.delExpire(SetType, key, db.wb); err != nil {
		return 0, err
	} else {
		if err2 := db.eng.Write(db.defaultWriteOpts, db.wb); err2 != nil {
			return 0, err2
		} else {
			return 1, nil
		}
	}
}
