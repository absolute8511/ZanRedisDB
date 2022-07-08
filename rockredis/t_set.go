package rockredis

import (
	"encoding/binary"
	"errors"
	"time"

	ps "github.com/prometheus/client_golang/prometheus"
	"github.com/youzan/ZanRedisDB/common"
	"github.com/youzan/ZanRedisDB/engine"
	"github.com/youzan/ZanRedisDB/metric"
	"github.com/youzan/ZanRedisDB/slow"
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
	return encodeCollSubKey(SetType, table, key, member)
}

func sDecodeSetKey(ek []byte) ([]byte, []byte, []byte, error) {
	dt, table, key, member, err := decodeCollSubKey(ek)
	if err != nil {
		return nil, nil, nil, err
	}
	if dt != SetType {
		return table, key, member, errCollTypeMismatch
	}
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

func (db *RockDB) sDelete(tn int64, key []byte, wb engine.WriteBatch) (int64, error) {
	sk := sEncodeSizeKey(key)
	keyInfo, err := db.getCollVerKeyForRange(tn, SetType, key, false)
	if err != nil {
		return 0, err
	}
	// no need delete if expired
	if keyInfo.IsNotExistOrExpired() {
		return 0, nil
	}
	num, err := db.sGetSize(tn, key, false)
	if err != nil {
		return 0, err
	}
	if num == 0 {
		return 0, nil
	}
	if num > 0 {
		db.IncrTableKeyCount(keyInfo.Table, -1, wb)
	}
	wb.Delete(sk)

	db.topLargeCollKeys.Update(key, int(0))
	if db.cfg.ExpirationPolicy == common.WaitCompact {
		// for compact ttl , we can just delete the meta
		return num, nil
	}
	start := keyInfo.RangeStart
	stop := keyInfo.RangeEnd

	if num > RangeDeleteNum {
		wb.DeleteRange(start, stop)
	} else {
		opts := engine.IteratorOpts{
			Range: engine.Range{Min: start, Max: stop, Type: common.RangeROpen},
		}
		it, err := db.NewDBRangeIteratorWithOpts(opts)
		if err != nil {
			return 0, err
		}
		for ; it.Valid(); it.Next() {
			wb.Delete(it.RefKey())
		}
		it.Close()
	}

	_, err = db.delExpire(SetType, key, nil, false, wb)
	if err != nil {
		return 0, err
	}
	return num, nil
}

// size key include set size and set modify timestamp
func (db *RockDB) sIncrSize(ts int64, key []byte, oldh *headerMetaValue, delta int64, wb engine.WriteBatch) (int64, error) {
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

func (db *RockDB) sGetSize(tn int64, key []byte, useLock bool) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}
	oldh, expired, err := db.collHeaderMeta(tn, SetType, key, useLock)
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

func (db *RockDB) SAdd(ts int64, key []byte, args ...[]byte) (int64, error) {
	if len(args) > MAX_BATCH_NUM {
		return 0, errTooMuchBatchSize
	}

	wb := db.wb
	defer wb.Clear()

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
		if vok, err := db.ExistNoLock(ek); err != nil {
			return 0, err
		} else if !vok {
			num++
			wb.Put(ek, nil)
		}
	}

	newNum, err := db.sIncrSize(ts, key, oldh, num, wb)
	if err != nil {
		return 0, err
	} else if newNum > 0 && newNum == num && !keyInfo.Expired {
		db.IncrTableKeyCount(table, 1, wb)
	}
	db.topLargeCollKeys.Update(key, int(newNum))
	slow.LogLargeCollection(int(newNum), slow.NewSlowLogInfo(string(table), string(key), "set"))
	if newNum > collectionLengthForMetric {
		metric.CollectionLenDist.With(ps.Labels{
			"table": string(table),
		}).Observe(float64(newNum))
	}

	err = db.rockEng.Write(wb)
	return num, err
}

func (db *RockDB) SGetVer(key []byte) (int64, error) {
	return db.sGetVer(key)
}

func (db *RockDB) SCard(key []byte) (int64, error) {
	tn := time.Now().UnixNano()
	return db.sGetSize(tn, key, true)
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
	if err := common.CheckSubKey(member); err != nil {
		return 0, err
	}
	table := keyInfo.Table
	rk := keyInfo.VerKey
	ek := sEncodeSetKey(table, rk, member)

	var n int64 = 1
	if vok, err := db.Exist(ek); err != nil {
		return 0, err
	} else if !vok {
		n = 0
	}
	return n, nil
}

func (db *RockDB) SMembers(key []byte) ([][]byte, error) {
	tn := time.Now().UnixNano()
	num, err := db.sGetSize(tn, key, true)
	if err != nil {
		return nil, err
	}

	if num == 0 {
		return nil, nil
	}
	return db.sMembersN(tn, key, int(num))
}

// we do not use rand here
func (db *RockDB) SRandMembers(key []byte, count int64) ([][]byte, error) {
	tn := time.Now().UnixNano()
	return db.sMembersN(tn, key, int(count))
}

func (db *RockDB) sMembersN(tn int64, key []byte, num int) ([][]byte, error) {
	if num > MAX_BATCH_NUM {
		return nil, errTooMuchBatchSize
	}
	if num <= 0 {
		return nil, common.ErrInvalidArgs
	}

	keyInfo, err := db.getCollVerKeyForRange(tn, SetType, key, true)
	if err != nil {
		return nil, err
	}
	if keyInfo.IsNotExistOrExpired() {
		return [][]byte{}, nil
	}
	preAlloc := num
	oldh := keyInfo.OldHeader
	if len(oldh.UserData) < 8 {
		return nil, errIntNumber
	}
	n, err := Int64(oldh.UserData[:8], nil)
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return [][]byte{}, nil
	}
	if n > 0 && n < int64(preAlloc) {
		preAlloc = int(n)
	}
	// TODO: use pool for large alloc
	v := make([][]byte, 0, preAlloc)

	start := keyInfo.RangeStart
	stop := keyInfo.RangeEnd

	it, err := db.NewDBRangeIterator(start, stop, common.RangeROpen, false)
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
	vals, err := db.sMembersN(ts, key, count)
	if err != nil {
		return nil, err
	}

	_, err = db.SRem(ts, key, vals...)
	return vals, err
}

func (db *RockDB) SRem(ts int64, key []byte, args ...[]byte) (int64, error) {
	if len(args) == 0 {
		return 0, nil
	}
	if len(args) > MAX_BATCH_NUM {
		return 0, errTooMuchBatchSize
	}
	wb := db.wb
	defer wb.Clear()
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
		vok, err := db.ExistNoLock(ek)
		if err != nil {
			return 0, err
		}
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
		_, err := db.delExpire(SetType, key, nil, false, wb)
		if err != nil {
			return 0, err
		}
	}
	db.topLargeCollKeys.Update(key, int(newNum))

	err = db.rockEng.Write(wb)
	return num, err
}

func (db *RockDB) SClear(ts int64, key []byte) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}

	num, err := db.sDelete(ts, key, db.wb)
	if err != nil {
		return 0, err
	}
	err = db.CommitBatchWrite()
	if num > 0 {
		return 1, err
	}
	return 0, err
}

func (db *RockDB) SMclear(keys ...[]byte) (int64, error) {
	if len(keys) > MAX_BATCH_NUM {
		return 0, errTooMuchBatchSize
	}
	wb := db.rockEng.NewWriteBatch()
	defer wb.Destroy()
	cnt := 0
	for _, key := range keys {
		if err := checkKeySize(key); err != nil {
			return 0, err
		}
		n, err := db.sDelete(0, key, wb)
		if err != nil {
			return 0, err
		}
		if n > 0 {
			cnt++
		}
	}

	err := db.rockEng.Write(wb)
	return int64(cnt), err
}

func (db *RockDB) sMclearWithBatch(wb engine.WriteBatch, keys ...[]byte) error {
	if len(keys) > MAX_BATCH_NUM {
		return errTooMuchBatchSize
	}
	for _, key := range keys {
		if err := checkKeySize(key); err != nil {
			return err
		}
		_, err := db.sDelete(0, key, wb)
		if err != nil {
			return err
		}
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
