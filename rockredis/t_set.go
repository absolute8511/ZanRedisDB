package rockredis

import (
	"encoding/binary"
	"errors"
	"time"

	"github.com/youzan/ZanRedisDB/common"
	"github.com/youzan/ZanRedisDB/engine"
	"github.com/youzan/gorocksdb"
)

var (
	errSetKey   = errors.New("invalid set key")
	errSSizeKey = errors.New("invalid ssize key")
)

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

func sEncodeSetKey(table []byte, key []byte, member []byte) []byte {
	buf := make([]byte, getDataTablePrefixBufLen(SetType, table)+len(key)+len(member)+1+2)

	pos := encodeDataTablePrefixToBuf(buf, SetType, table)

	binary.BigEndian.PutUint16(buf[pos:], uint16(len(key)))
	pos += 2

	copy(buf[pos:], key)
	pos += len(key)

	buf[pos] = collStartSep
	pos++
	copy(buf[pos:], member)

	return buf
}

func sDecodeSetKey(ek []byte) ([]byte, []byte, []byte, error) {
	table, pos, err := decodeDataTablePrefixFromBuf(ek, SetType)

	if err != nil {
		return nil, nil, nil, err
	}

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

	if ek[pos] != collStartSep {
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
	k[len(k)-1] = collStopSep
	return k
}

func (db *RockDB) sDelete(key []byte, wb *gorocksdb.WriteBatch) int64 {
	sk := sEncodeSizeKey(key)
	keyInfo, err := db.getCollVerKeyForRange(0, SetType, key, false)
	if err != nil {
		return 0
	}

	start := keyInfo.RangeStart
	stop := keyInfo.RangeEnd

	num, err := db.sGetSize(key, false)
	if err != nil {
		return 0
	}
	if num > RangeDeleteNum {
		wb.DeleteRange(start, stop)
	} else {
		opts := engine.IteratorOpts{
			Range:     engine.Range{Min: start, Max: stop, Type: common.RangeROpen},
			Reverse:   false,
			IgnoreDel: true,
		}
		it, err := engine.NewDBRangeIteratorWithOpts(db.eng, opts)
		if err != nil {
			return 0
		}
		for ; it.Valid(); it.Next() {
			wb.Delete(it.RefKey())
		}
		it.Close()
	}
	if num > 0 {
		db.IncrTableKeyCount(keyInfo.Table, -1, wb)
	}
	db.delExpire(SetType, key, nil, false, wb)

	wb.Delete(sk)
	return num
}

// size key include set size and set modify timestamp
func (db *RockDB) sIncrSize(ts int64, key []byte, oldh *headerMetaValue, delta int64, wb *gorocksdb.WriteBatch) (int64, error) {
	sk := sEncodeSizeKey(key)
	meta := oldh.UserData

	var size int64
	var err error
	if len(meta) == 0 {
		size = 0
	} else if len(meta) < 8 {
		return 0, errIntNumber
	} else {
		if size, err = Int64(meta[:8], err); err != nil {
			return 0, err
		}
	}
	size += delta
	if size <= 0 {
		size = 0
		wb.Delete(sk)
	} else {
		buf := make([]byte, 16)
		binary.BigEndian.PutUint64(buf[0:8], uint64(size))
		binary.BigEndian.PutUint64(buf[8:16], uint64(ts))
		oldh.UserData = buf
		nv := oldh.encodeWithData()
		wb.Put(sk, nv)
	}

	return size, nil
}

func (db *RockDB) sGetSize(key []byte, useLock bool) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}
	oldh, expired, err := db.collHeaderMeta(time.Now().UnixNano(), SetType, key, useLock)
	if err != nil {
		return 0, err
	}
	if len(oldh.UserData) == 0 || expired {
		return 0, nil
	}
	if len(oldh.UserData) < 8 {
		return 0, errIntNumber
	}
	return Int64(oldh.UserData[:8], err)
}

func (db *RockDB) sGetVer(key []byte) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}
	oldh, _, err := db.collHeaderMeta(time.Now().UnixNano(), SetType, key, true)
	if err != nil {
		return 0, err
	}
	if len(oldh.UserData) == 0 {
		return 0, nil
	}
	if len(oldh.UserData) < 16 {
		return 0, errIntNumber
	}
	return Int64(oldh.UserData[8:16], err)
}

func (db *RockDB) sSetItem(ts int64, key []byte, member []byte, wb *gorocksdb.WriteBatch) (int64, error) {
	keyInfo, err := db.prepareCollKeyForWrite(ts, SetType, key, member)
	if err != nil {
		return 0, err
	}

	ek := sEncodeSetKey(keyInfo.Table, keyInfo.VerKey, member)

	var n int64 = 1
	if vok, _ := db.eng.ExistNoLock(db.defaultReadOpts, ek); vok {
		n = 0
	} else {
		if newNum, err := db.sIncrSize(ts, key, keyInfo.OldHeader, 1, wb); err != nil {
			return 0, err
		} else if newNum == 1 && !keyInfo.Expired {
			db.IncrTableKeyCount(keyInfo.Table, 1, wb)
		}
		wb.Put(ek, nil)
	}

	return n, nil
}

func (db *RockDB) SAdd(ts int64, key []byte, args ...[]byte) (int64, error) {
	if len(args) > MAX_BATCH_NUM {
		return 0, errTooMuchBatchSize
	}

	wb := db.wb
	wb.Clear()

	keyInfo, err := db.prepareCollKeyForWrite(ts, SetType, key, nil)
	if err != nil {
		return 0, err
	}
	table := keyInfo.Table
	rk := keyInfo.VerKey
	oldh := keyInfo.OldHeader

	var ek []byte
	var num int64 = 0
	for i := 0; i < len(args); i++ {
		if err := checkCollKFSize(key, args[i]); err != nil {
			return 0, err
		}
		ek = sEncodeSetKey(table, rk, args[i])

		// must use exist to tell the different of not found and nil value (member value is also nil)
		if vok, err := db.eng.ExistNoLock(db.defaultReadOpts, ek); err != nil {
			return 0, err
		} else if !vok {
			num++
			wb.Put(ek, nil)
		}
	}

	if newNum, err := db.sIncrSize(ts, key, oldh, num, wb); err != nil {
		return 0, err
	} else if newNum > 0 && newNum == num && !keyInfo.Expired {
		db.IncrTableKeyCount(table, 1, wb)
	}

	err = db.eng.Write(db.defaultWriteOpts, wb)
	return num, err
}

func (db *RockDB) SGetVer(key []byte) (int64, error) {
	return db.sGetVer(key)
}

func (db *RockDB) SCard(key []byte) (int64, error) {
	return db.sGetSize(key, true)
}

func (db *RockDB) SIsMember(key []byte, member []byte) (int64, error) {
	tn := time.Now().UnixNano()
	keyInfo, err := db.GetCollVersionKey(tn, SetType, key, true)
	if err != nil {
		return 0, err
	}
	if keyInfo.IsNotExistOrExpired() {
		return 0, nil
	}
	if err := checkSubKey(member); err != nil {
		return 0, err
	}
	table := keyInfo.Table
	rk := keyInfo.VerKey
	ek := sEncodeSetKey(table, rk, member)

	var n int64 = 1
	if vok, err := db.eng.Exist(db.defaultReadOpts, ek); err != nil {
		return 0, err
	} else if !vok {
		n = 0
	}
	return n, nil
}

func (db *RockDB) SMembers(key []byte) ([][]byte, error) {
	num, err := db.sGetSize(key, true)
	if err != nil {
		return nil, err
	}

	return db.sMembersN(key, int(num))
}

func (db *RockDB) sMembersN(key []byte, num int) ([][]byte, error) {
	if num > MAX_BATCH_NUM {
		return nil, errTooMuchBatchSize
	}

	tn := time.Now().UnixNano()
	keyInfo, err := db.getCollVerKeyForRange(tn, SetType, key, true)
	if err != nil {
		return nil, err
	}
	v := make([][]byte, 0, num)
	if keyInfo.IsNotExistOrExpired() {
		return v, nil
	}

	start := keyInfo.RangeStart
	stop := keyInfo.RangeEnd

	it, err := engine.NewDBRangeIterator(db.eng, start, stop, common.RangeROpen, false)
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
		if len(v) >= num {
			break
		}
	}
	return v, nil
}

func (db *RockDB) SPop(ts int64, key []byte, count int) ([][]byte, error) {
	vals, err := db.sMembersN(key, count)
	if err != nil {
		return nil, err
	}

	_, err = db.SRem(ts, key, vals...)
	return vals, err
}

func (db *RockDB) SRem(ts int64, key []byte, args ...[]byte) (int64, error) {
	wb := db.wb
	wb.Clear()
	keyInfo, err := db.GetCollVersionKey(ts, SetType, key, false)
	if err != nil {
		return 0, err
	}
	table := keyInfo.Table
	rk := keyInfo.VerKey
	oldh := keyInfo.OldHeader

	var ek []byte

	var num int64 = 0
	for i := 0; i < len(args); i++ {
		if err := checkCollKFSize(key, args[i]); err != nil {
			return 0, err
		}

		ek = sEncodeSetKey(table, rk, args[i])
		vok, _ := db.eng.ExistNoLock(db.defaultReadOpts, ek)
		if !vok {
			continue
		} else {
			num++
			wb.Delete(ek)
		}
	}

	newNum, err := db.sIncrSize(ts, key, oldh, -num, wb)
	if err != nil {
		return 0, err
	}
	if num > 0 && newNum == 0 {
		db.IncrTableKeyCount(table, -1, wb)
	}
	if newNum == 0 {
		db.delExpire(SetType, key, nil, false, wb)
	}

	err = db.eng.Write(db.defaultWriteOpts, wb)
	return num, err
}

func (db *RockDB) SClear(key []byte) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}

	wb := db.wb
	wb.Clear()
	num := db.sDelete(key, wb)
	err := db.eng.Write(db.defaultWriteOpts, wb)
	return num, err
}

func (db *RockDB) SMclear(keys ...[]byte) (int64, error) {
	if len(keys) > MAX_BATCH_NUM {
		return 0, errTooMuchBatchSize
	}
	wb := gorocksdb.NewWriteBatch()
	defer wb.Destroy()
	for _, key := range keys {
		if err := checkKeySize(key); err != nil {
			return 0, err
		}
		db.sDelete(key, wb)
	}

	err := db.eng.Write(db.defaultWriteOpts, wb)
	return int64(len(keys)), err
}

func (db *RockDB) sMclearWithBatch(wb *gorocksdb.WriteBatch, keys ...[]byte) error {
	if len(keys) > MAX_BATCH_NUM {
		return errTooMuchBatchSize
	}
	for _, key := range keys {
		if err := checkKeySize(key); err != nil {
			return err
		}
		db.sDelete(key, wb)
	}
	return nil
}

func (db *RockDB) SKeyExists(key []byte) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}

	return db.collKeyExists(SetType, key)
}

func (db *RockDB) SExpire(ts int64, key []byte, duration int64) (int64, error) {
	return db.collExpire(ts, SetType, key, duration)
}

func (db *RockDB) SPersist(ts int64, key []byte) (int64, error) {
	return db.collPersist(ts, SetType, key)
}
