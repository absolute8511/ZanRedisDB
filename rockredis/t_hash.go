package rockredis

import (
	"bytes"
	"errors"
	"time"

	ps "github.com/prometheus/client_golang/prometheus"
	"github.com/youzan/ZanRedisDB/common"
	"github.com/youzan/ZanRedisDB/engine"
	"github.com/youzan/ZanRedisDB/metric"
	"github.com/youzan/ZanRedisDB/slow"
)

var (
	errHashKey  = errors.New("invalid hash key")
	errHSizeKey = errors.New("invalid hash size key")
)

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
	return encodeCollSubKey(HashType, table, key, field)
}

func hDecodeHashKey(ek []byte) ([]byte, []byte, []byte, error) {
	dt, table, key, field, err := decodeCollSubKey(ek)
	if err != nil {
		return nil, nil, nil, err
	}
	if dt != HashType {
		return table, key, field, errCollTypeMismatch
	}
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
	wb engine.WriteBatch, hindex *HsetIndex) (int64, error) {
	created := int64(1)
	keyInfo, err := db.prepareHashKeyForWrite(ts, hkey, field)
	if err != nil {
		return 0, err
	}
	table := keyInfo.Table
	rk := keyInfo.VerKey
	ek := hEncodeHashKey(table, rk, field)

	tsBuf := PutInt64(ts)
	value = append(value, tsBuf...)
	var oldV []byte
	if oldV, _ = db.GetBytesNoLock(ek); oldV != nil {
		created = 0
		if checkNX || bytes.Equal(oldV, value) {
			return created, nil
		}
	} else {
		newNum, err := db.hIncrSize(hkey, keyInfo.OldHeader, 1, wb)
		if err != nil {
			return 0, err
		} else if newNum == 1 && !keyInfo.Expired {
			db.IncrTableKeyCount(table, 1, wb)
		}
		db.topLargeCollKeys.Update(hkey, int(newNum))
		slow.LogLargeCollection(int(newNum), slow.NewSlowLogInfo(string(table), string(hkey), "hash"))
		if newNum > collectionLengthForMetric {
			metric.CollectionLenDist.With(ps.Labels{
				"table": string(table),
			}).Observe(float64(newNum))
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
	tn := time.Now().UnixNano()
	oldh, expired, err := db.hHeaderMeta(tn, hkey, true)
	if err != nil {
		return 0, err
	}
	if expired {
		return 0, nil
	}
	return Int64(oldh.UserData, err)
}

func (db *RockDB) hIncrSize(hkey []byte, oldh *headerMetaValue, delta int64, wb engine.WriteBatch) (int64, error) {
	sk := hEncodeSizeKey(hkey)
	metaV := oldh.UserData
	size, err := Int64(metaV, nil)
	if err != nil {
		return 0, err
	}
	size += delta
	if size <= 0 {
		size = 0
		wb.Delete(sk)
	} else {
		oldh.UserData = PutInt64(size)
		nv := oldh.encodeWithData()
		wb.Put(sk, nv)
	}
	return size, nil
}

func (db *RockDB) HSet(ts int64, checkNX bool, key []byte, field []byte, ovalue []byte) (int64, error) {
	if err := checkValueSize(ovalue); err != nil {
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

	var value []byte
	if len(ovalue) > len(db.writeTmpBuf) {
		value = make([]byte, len(ovalue))
	} else {
		value = db.writeTmpBuf[:len(ovalue)]
	}
	copy(value, ovalue)
	created, err := db.hSetField(ts, checkNX, key, field, value, db.wb, hindex)
	if err != nil {
		return 0, err
	}

	err = db.MaybeCommitBatch()
	return created, err
}

func (db *RockDB) HMset(ts int64, key []byte, args ...common.KVRecord) error {
	if len(args) > MAX_BATCH_NUM {
		return errTooMuchBatchSize
	}
	if len(args) == 0 {
		return nil
	}

	// get old header for this hash key
	keyInfo, err := db.prepareHashKeyForWrite(ts, key, nil)
	if err != nil {
		return err
	}
	table := keyInfo.Table
	verKey := keyInfo.VerKey

	tableIndexes := db.indexMgr.GetTableIndexes(string(table))
	if tableIndexes != nil {
		tableIndexes.Lock()
		defer tableIndexes.Unlock()
	}

	var num int64
	var value []byte
	tsBuf := PutInt64(ts)
	for i := 0; i < len(args); i++ {
		if err = checkCollKFSize(verKey, args[i].Key); err != nil {
			return err
		} else if err = checkValueSize(args[i].Value); err != nil {
			return err
		}
		ek := hEncodeHashKey(table, verKey, args[i].Key)

		var oldV []byte
		if oldV, err = db.GetBytesNoLock(ek); err != nil {
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
	newNum, err := db.hIncrSize(key, keyInfo.OldHeader, num, db.wb)
	if err != nil {
		return err
	} else if newNum > 0 && newNum == num && !keyInfo.Expired {
		db.IncrTableKeyCount(table, 1, db.wb)
	}
	db.topLargeCollKeys.Update(key, int(newNum))
	slow.LogLargeCollection(int(newNum), slow.NewSlowLogInfo(string(table), string(key), "hash"))
	if newNum > collectionLengthForMetric {
		metric.CollectionLenDist.With(ps.Labels{
			"table": string(table),
		}).Observe(float64(newNum))
	}

	err = db.MaybeCommitBatch()
	return err
}

func (db *RockDB) hGetRawFieldValue(ts int64, key []byte, field []byte, checkExpired bool, useLock bool) ([]byte, error) {
	if err := checkCollKFSize(key, field); err != nil {
		return nil, err
	}
	keyInfo, err := db.GetCollVersionKey(ts, HashType, key, useLock)
	if err != nil {
		return nil, err
	}
	if checkExpired && keyInfo.IsNotExistOrExpired() {
		return nil, nil
	}
	table := keyInfo.Table
	rk := keyInfo.VerKey
	ek := hEncodeHashKey(table, rk, field)

	if useLock {
		return db.GetBytes(ek)
	} else {
		return db.GetBytesNoLock(ek)
	}
}

func (db *RockDB) hExistRawField(ts int64, key []byte, field []byte, checkExpired bool, useLock bool) (bool, error) {
	if err := checkCollKFSize(key, field); err != nil {
		return false, err
	}
	keyInfo, err := db.GetCollVersionKey(ts, HashType, key, useLock)
	if err != nil {
		return false, err
	}
	if checkExpired && keyInfo.IsNotExistOrExpired() {
		return false, nil
	}
	table := keyInfo.Table
	rk := keyInfo.VerKey
	ek := hEncodeHashKey(table, rk, field)

	if useLock {
		v, err := db.Exist(ek)
		return v, err
	} else {
		v, err := db.ExistNoLock(ek)
		return v, err
	}
}

func (db *RockDB) HGetVer(key []byte, field []byte) (int64, error) {
	v, err := db.hGetRawFieldValue(0, key, field, false, true)
	var ts uint64
	if len(v) >= tsLen {
		ts, err = Uint64(v[len(v)-tsLen:], err)
	}
	return int64(ts), err
}

func (db *RockDB) HGetWithOp(key []byte, field []byte, op func([]byte) error) error {
	tn := time.Now().UnixNano()
	if err := checkCollKFSize(key, field); err != nil {
		return err
	}
	keyInfo, err := db.GetCollVersionKey(tn, HashType, key, true)
	if err != nil {
		return err
	}
	if keyInfo.IsNotExistOrExpired() {
		// we must call the callback if no error returned
		return op(nil)
	}
	table := keyInfo.Table
	rk := keyInfo.VerKey
	ek := hEncodeHashKey(table, rk, field)

	return db.rockEng.GetValueWithOp(ek, func(v []byte) error {
		if len(v) >= tsLen {
			v = v[:len(v)-tsLen]
		}
		return op(v)
	})
}

func (db *RockDB) HGetExpired(key []byte, field []byte) ([]byte, error) {
	return db.hgetWithFlag(key, field, true)
}

func (db *RockDB) HGet(key []byte, field []byte) ([]byte, error) {
	return db.hgetWithFlag(key, field, false)
}

func (db *RockDB) hgetWithFlag(key []byte, field []byte, getExpired bool) ([]byte, error) {
	tn := time.Now().UnixNano()
	v, err := db.hGetRawFieldValue(tn, key, field, !getExpired, true)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, nil
	}
	if len(v) >= tsLen {
		v = v[:len(v)-tsLen]
	}
	return v, nil
}

func (db *RockDB) HExist(key []byte, field []byte) (bool, error) {
	tn := time.Now().UnixNano()
	vok, err := db.hExistRawField(tn, key, field, true, true)
	return vok, err
}

func (db *RockDB) HMgetExpired(key []byte, args ...[]byte) ([][]byte, error) {
	if len(args) > MAX_BATCH_NUM {
		return nil, errTooMuchBatchSize
	}
	var err error
	r := make([][]byte, len(args))
	for i := 0; i < len(args); i++ {
		r[i], err = db.HGetExpired(key, args[i])
		if err != nil {
			return nil, err
		}
	}
	return r, nil
}

func (db *RockDB) HMget(key []byte, args ...[]byte) ([][]byte, error) {
	if len(args) > MAX_BATCH_NUM {
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

func (db *RockDB) HDel(ts int64, key []byte, args ...[]byte) (int64, error) {
	if len(args) > MAX_BATCH_NUM {
		return 0, errTooMuchBatchSize
	}
	if len(args) == 0 {
		return 0, nil
	}
	keyInfo, err := db.GetCollVersionKey(ts, HashType, key, false)
	if err != nil {
		return 0, err
	}
	table := keyInfo.Table
	rk := keyInfo.VerKey
	oldh := keyInfo.OldHeader

	tableIndexes := db.indexMgr.GetTableIndexes(string(table))
	if tableIndexes != nil {
		tableIndexes.Lock()
		defer tableIndexes.Unlock()
	}

	wb := db.wb
	var ek []byte
	var oldV []byte

	var num int64 = 0
	var newNum int64 = -1
	for i := 0; i < len(args); i++ {
		if err := checkKeySubKey(rk, args[i]); err != nil {
			return 0, err
		}

		ek = hEncodeHashKey(table, rk, args[i])
		oldV, err = db.GetBytesNoLock(ek)
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

	if newNum, err = db.hIncrSize(key, oldh, -num, wb); err != nil {
		return 0, err
	}
	if num > 0 && newNum == 0 {
		db.IncrTableKeyCount(table, -1, wb)
	}
	if newNum == 0 {
		db.delExpire(HashType, key, nil, false, wb)
	}
	db.topLargeCollKeys.Update(key, int(newNum))

	err = db.MaybeCommitBatch()
	return num, err
}

func (db *RockDB) hDeleteAll(ts int64, hkey []byte, hlen int64, wb engine.WriteBatch, tableIndexes *TableIndexContainer) error {
	keyInfo, err := db.getCollVerKeyForRange(ts, HashType, hkey, false)
	if err != nil {
		return err
	}
	// no need delete if expired
	if keyInfo.IsNotExistOrExpired() {
		return nil
	}
	sk := hEncodeSizeKey(hkey)
	wb.Delete(sk)
	db.topLargeCollKeys.Update(hkey, int(0))
	if db.cfg.ExpirationPolicy == common.WaitCompact && tableIndexes == nil {
		// for compact ttl , we can just delete the meta
		return nil
	}
	start := keyInfo.RangeStart
	stop := keyInfo.RangeEnd

	if tableIndexes != nil || hlen <= RangeDeleteNum {
		it, err := db.NewDBRangeIterator(start, stop, common.RangeROpen, false)
		if err != nil {
			return err
		}
		defer it.Close()

		for ; it.Valid(); it.Next() {
			rawk := it.Key()
			if hlen <= RangeDeleteNum {
				wb.Delete(rawk)
			}
			if tableIndexes != nil {
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
	}
	if hlen > RangeDeleteNum {
		wb.DeleteRange(start, stop)
	}
	return nil
}

func (db *RockDB) HClear(ts int64, hkey []byte) (int64, error) {
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
	if hlen == 0 {
		return 0, nil
	}

	wb := db.wb
	err = db.hDeleteAll(ts, hkey, hlen, wb, tableIndexes)
	if err != nil {
		return 0, err
	}
	if hlen > 0 {
		db.IncrTableKeyCount(table, -1, wb)
	}
	db.delExpire(HashType, hkey, nil, false, wb)

	err = db.MaybeCommitBatch()
	if hlen > 0 {
		return 1, err
	}
	return 0, err
}

func (db *RockDB) hClearWithBatch(hkey []byte, wb engine.WriteBatch) error {
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

	err = db.hDeleteAll(0, hkey, hlen, wb, tableIndexes)
	if err != nil {
		return err
	}
	if hlen > 0 {
		db.IncrTableKeyCount(table, -1, wb)
	}
	db.delExpire(HashType, hkey, nil, false, wb)

	return err
}

func (db *RockDB) HMclear(keys ...[]byte) {
	for _, key := range keys {
		db.HClear(0, key)
	}
}

func (db *RockDB) HIncrBy(ts int64, key []byte, field []byte, delta int64) (int64, error) {
	if err := checkCollKFSize(key, field); err != nil {
		return 0, err
	}
	table, _, err := extractTableFromRedisKey(key)
	if err != nil {
		return 0, err
	}

	fv, err := db.hGetRawFieldValue(ts, key, field, true, false)
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

	var n int64
	if fv != nil {
		if len(fv) >= tsLen {
			fv = fv[:len(fv)-tsLen]
		}
		if n, err = StrInt64(fv, err); err != nil {
			return 0, err
		}
	}

	n += delta

	_, err = db.hSetField(ts, false, key, field, FormatInt64ToSlice(n), wb, hindex)
	if err != nil {
		return 0, err
	}

	err = db.MaybeCommitBatch()
	return n, err
}

func (db *RockDB) HGetAll(key []byte) (int64, []common.KVRecordRet, error) {
	return db.hGetAll(key, false)
}

func (db *RockDB) HGetAllExpired(key []byte) (int64, []common.KVRecordRet, error) {
	return db.hGetAll(key, true)
}

func (db *RockDB) hGetAll(key []byte, getExpired bool) (int64, []common.KVRecordRet, error) {
	if err := checkKeySize(key); err != nil {
		return 0, nil, err
	}

	tn := time.Now().UnixNano()
	keyInfo, err := db.getCollVerKeyForRange(tn, HashType, key, true)
	if err != nil {
		return 0, nil, err
	}
	if keyInfo.IsNotExistOrExpired() && !getExpired {
		return 0, nil, nil
	}
	start := keyInfo.RangeStart
	stop := keyInfo.RangeEnd

	length, err := Int64(keyInfo.MetaData(), err)
	if length > MAX_BATCH_NUM {
		return length, nil, errTooMuchBatchSize
	}

	it, err := db.NewDBRangeIterator(start, stop, common.RangeROpen, false)
	if err != nil {
		return 0, nil, err
	}
	it.NoTimestamp(HashType)

	vals := make([]common.KVRecordRet, 0, length)
	doScan := func() {
		defer it.Close()
		for ; it.Valid(); it.Next() {
			_, _, f, err := hDecodeHashKey(it.Key())
			v := it.Value()
			vals = append(vals, common.KVRecordRet{
				Rec: common.KVRecord{Key: f, Value: v},
				Err: err,
			})
		}
	}
	doScan()
	return length, vals, nil
}

func (db *RockDB) HKeys(key []byte) (int64, []common.KVRecordRet, error) {
	if err := checkKeySize(key); err != nil {
		return 0, nil, err
	}
	tn := time.Now().UnixNano()
	keyInfo, err := db.getCollVerKeyForRange(tn, HashType, key, true)
	if err != nil {
		return 0, nil, err
	}
	if keyInfo.IsNotExistOrExpired() {
		return 0, nil, nil
	}
	start := keyInfo.RangeStart
	stop := keyInfo.RangeEnd
	length, err := Int64(keyInfo.MetaData(), err)
	if err != nil {
		return 0, nil, err
	}
	if length > MAX_BATCH_NUM {
		return length, nil, errTooMuchBatchSize
	}

	it, err := db.NewDBRangeIterator(start, stop, common.RangeROpen, false)
	if err != nil {
		return 0, nil, err
	}

	vals := make([]common.KVRecordRet, 0, length)
	doScan := func() {
		defer it.Close()
		for ; it.Valid(); it.Next() {
			_, _, f, _ := hDecodeHashKey(it.Key())
			if f == nil {
				continue
			}
			vals = append(vals, common.KVRecordRet{
				Rec: common.KVRecord{Key: f, Value: nil},
				Err: nil,
			})
		}
	}
	doScan()
	return length, vals, nil
}

func (db *RockDB) HValues(key []byte) (int64, []common.KVRecordRet, error) {
	if err := checkKeySize(key); err != nil {
		return 0, nil, err
	}

	tn := time.Now().UnixNano()
	keyInfo, err := db.getCollVerKeyForRange(tn, HashType, key, true)
	if err != nil {
		return 0, nil, err
	}
	if keyInfo.IsNotExistOrExpired() {
		return 0, nil, nil
	}
	start := keyInfo.RangeStart
	stop := keyInfo.RangeEnd
	length, err := Int64(keyInfo.MetaData(), err)
	if err != nil {
		return 0, nil, err
	}
	if length > MAX_BATCH_NUM {
		return length, nil, errTooMuchBatchSize
	}

	it, err := db.NewDBRangeIterator(start, stop, common.RangeROpen, false)
	if err != nil {
		return 0, nil, err
	}
	it.NoTimestamp(HashType)
	// TODO: use pool for large alloc
	vals := make([]common.KVRecordRet, 0, length)
	defer it.Close()
	for ; it.Valid(); it.Next() {
		va := it.Value()
		if va == nil {
			continue
		}
		vals = append(vals, common.KVRecordRet{
			Rec: common.KVRecord{Key: nil, Value: va},
			Err: nil,
		})
	}

	return length, vals, nil
}

func (db *RockDB) HKeyExists(key []byte) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}

	return db.collKeyExists(HashType, key)
}

func (db *RockDB) HExpire(ts int64, key []byte, duration int64) (int64, error) {
	return db.collExpire(ts, HashType, key, duration)
}

func (db *RockDB) HPersist(ts int64, key []byte) (int64, error) {
	return db.collPersist(ts, HashType, key)
}

func (db *RockDB) prepareHashKeyForWrite(ts int64, key []byte, field []byte) (collVerKeyInfo, error) {
	return db.prepareCollKeyForWrite(ts, HashType, key, field)
}

func (db *RockDB) hHeaderMeta(ts int64, hkey []byte, useLock bool) (*headerMetaValue, bool, error) {
	return db.collHeaderMeta(ts, HashType, hkey, useLock)
}
