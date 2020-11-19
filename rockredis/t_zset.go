package rockredis

import (
	"bytes"
	"encoding/binary"
	"errors"
	"time"

	ps "github.com/prometheus/client_golang/prometheus"
	"github.com/youzan/ZanRedisDB/common"
	"github.com/youzan/ZanRedisDB/engine"
	"github.com/youzan/ZanRedisDB/metric"
	"github.com/youzan/ZanRedisDB/slow"
)

const (
	MinScore     int64 = -1<<63 + 1
	MaxScore     int64 = 1<<63 - 1
	InvalidScore int64 = -1 << 63

	AggregateSum byte = 0
	AggregateMin byte = 1
	AggregateMax byte = 2
)

var errZSetInvalidEncode = errors.New("invalid zset encoded data")
var errZSizeKey = errors.New("invalid zsize key")
var errZSetKey = errors.New("invalid zset key")
var errZScoreKey = errors.New("invalid zscore key")
var errScoreOverflow = errors.New("zset score overflow")
var errInvalidAggregate = errors.New("invalid aggregate")
var errInvalidWeightNum = errors.New("invalid weight number")
var errInvalidSrcKeyNum = errors.New("invalid src key number")
var errScoreMiss = errors.New("missing score for zset")

const (
	zsetKeySep   byte = ':'
	zsetScoreSep byte = ':'
	zsetMemSep   byte = ':'
)

func zEncodeSizeKey(key []byte) []byte {
	buf := make([]byte, len(key)+1+len(metaPrefix))
	pos := 0
	buf[pos] = ZSizeType
	pos++
	copy(buf[pos:], metaPrefix)
	pos += len(metaPrefix)
	copy(buf[pos:], key)
	return buf
}

func zDecodeSizeKey(ek []byte) ([]byte, error) {
	pos := 0
	if pos+1+len(metaPrefix) > len(ek) || ek[pos] != ZSizeType {
		return nil, errZSizeKey
	}
	pos++
	pos += len(metaPrefix)
	return ek[pos:], nil
}

func zEncodeSetKey(table []byte, key []byte, member []byte) []byte {
	buf := make([]byte, getDataTablePrefixBufLen(ZSetType, table)+len(key)+len(member)+3)
	pos := 0
	pos = encodeDataTablePrefixToBuf(buf, ZSetType, table)

	binary.BigEndian.PutUint16(buf[pos:], uint16(len(key)))
	pos += 2

	copy(buf[pos:], key)
	pos += len(key)

	buf[pos] = zsetMemSep
	pos++

	copy(buf[pos:], member)
	return buf
}

func zDecodeSetKey(ek []byte) ([]byte, []byte, []byte, error) {
	table, pos, err := decodeDataTablePrefixFromBuf(ek, ZSetType)
	if err != nil {
		return nil, nil, nil, err
	}

	if pos+2 > len(ek) {
		return table, nil, nil, errZSetKey
	}

	keyLen := int(binary.BigEndian.Uint16(ek[pos:]))
	if keyLen+pos > len(ek) {
		return table, nil, nil, errZSetKey
	}

	pos += 2
	key := ek[pos : pos+keyLen]

	if ek[pos+keyLen] != zsetMemSep {
		return table, nil, nil, errZSetKey
	}
	pos++

	member := ek[pos+keyLen:]
	return table, key, member, nil
}

func zEncodeStartSetKey(table []byte, key []byte) []byte {
	k := zEncodeSetKey(table, key, nil)
	return k
}

func zEncodeStopSetKey(table []byte, key []byte) []byte {
	k := zEncodeSetKey(table, key, nil)
	k[len(k)-1] = k[len(k)-1] + 1
	return k
}

func zEncodeScoreKeyInternal(minScore bool,
	stopKey bool, stopMember bool, table []byte, key []byte, member []byte, score float64) []byte {
	buf := make([]byte, getDataTablePrefixBufLen(ZScoreType, table))
	pos := 0
	// in order to make sure all the table data are in the same range
	// we need make sure we has the same table prefix
	pos = encodeDataTablePrefixToBuf(buf, ZScoreType, table)

	sep := int32(zsetKeySep)
	if stopKey {
		sep = int32(zsetKeySep + 1)
	}
	if minScore {
		sep = int32(zsetKeySep - 1)
	}
	scoreSep := int32(zsetScoreSep)
	if stopMember {
		scoreSep = int32(zsetScoreSep + 1)
	}
	buf, _ = EncodeMemCmpKey(buf[:pos], key, sep, score, scoreSep, member)
	return buf
}

func zEncodeScoreKey(stopKey bool, stopMember bool, table []byte, key []byte, member []byte, score float64) []byte {
	return zEncodeScoreKeyInternal(false, stopKey, stopMember, table, key, member, score)
}

func zEncodeStartScoreKey(table []byte, key []byte, score float64) []byte {
	return zEncodeScoreKey(false, false, table, key, nil, score)
}

func zEncodeStopScoreKey(table []byte, key []byte, score float64) []byte {
	return zEncodeScoreKey(false, true, table, key, nil, score)
}

func zEncodeStartKey(table []byte, key []byte) []byte {
	return zEncodeScoreKeyInternal(true, false, false, table, key, nil, 0.0)
}

func zEncodeStopKey(table []byte, key []byte) []byte {
	return zEncodeScoreKey(true, false, table, key, nil, 0.0)
}

func zDecodeScoreKey(ek []byte) (table []byte, key []byte, member []byte, score float64, err error) {
	table, pos, derr := decodeDataTablePrefixFromBuf(ek, ZScoreType)
	if derr != nil {
		err = derr
		return
	}

	// key, sep, score, sep, member
	var rets []interface{}
	rets, err = Decode(ek[pos:], 5)
	if err != nil {
		return
	}
	if len(rets) != 5 {
		err = errZSetInvalidEncode
		return
	}
	var ok bool
	key, ok = rets[0].([]byte)
	if !ok {
		err = errZSetInvalidEncode
		return
	}
	score, ok = rets[2].(float64)
	if !ok {
		err = errZSetInvalidEncode
		return
	}
	member, ok = rets[4].([]byte)
	if !ok {
		err = errZSetInvalidEncode
		return
	}
	return
}

func parseZMeta(meta []byte) (int64, int64, error) {
	num, err := parseZMetaSize(meta)
	if err != nil {
		return 0, 0, err
	}
	if len(meta) < 16 {
		return num, 0, nil
	}
	ver, err := Int64(meta[8:16], nil)
	return num, ver, err
}

func parseZMetaSize(meta []byte) (int64, error) {
	if len(meta) == 0 {
		return 0, nil
	}
	if len(meta) < 8 {
		return 0, errIntNumber
	}
	num, err := Int64(meta[:8], nil)
	if err != nil {
		return 0, err
	}
	return num, nil
}

func encodeZMetaData(size int64, ts int64, oldh *headerMetaValue) []byte {
	buf := make([]byte, 16)
	binary.BigEndian.PutUint64(buf[0:8], uint64(size))
	binary.BigEndian.PutUint64(buf[8:16], uint64(ts))
	oldh.UserData = buf
	nv := oldh.encodeWithData()
	return nv
}

func (db *RockDB) getZSetForRangeWithMinMax(ts int64, key []byte, min []byte, max []byte, useLock bool) (collVerKeyInfo, error) {
	info, err := db.GetCollVersionKey(ts, ZSetType, key, useLock)
	if err != nil {
		return info, err
	}
	rk := info.VerKey
	table := info.Table
	if min == nil {
		info.RangeStart = zEncodeStartSetKey(table, rk)
	} else {
		info.RangeStart = zEncodeSetKey(table, rk, min)
	}
	if max == nil {
		info.RangeEnd = zEncodeStopSetKey(table, rk)
	} else {
		info.RangeEnd = zEncodeSetKey(table, rk, max)
	}
	return info, err
}

func (db *RockDB) getZSetForRangeWithNum(ts int64, key []byte, min float64, max float64, useLock bool) (collVerKeyInfo, error) {
	info, err := db.GetCollVersionKey(ts, ZSetType, key, useLock)
	if err != nil {
		return info, err
	}
	info.RangeStart = zEncodeStartScoreKey(info.Table, info.VerKey, min)
	info.RangeEnd = zEncodeStopScoreKey(info.Table, info.VerKey, max)
	return info, err
}

func (db *RockDB) zSetItem(table []byte, rk []byte, score float64, member []byte, wb engine.WriteBatch) (int64, error) {
	// if score <= MinScore || score >= MaxScore {
	// 	return 0, errScoreOverflow
	// }
	var exists int64
	ek := zEncodeSetKey(table, rk, member)

	if v, err := db.GetBytesNoLock(ek); err != nil {
		return 0, err
	} else if v != nil {
		exists = 1
		if s, err := Float64(v, err); err != nil {
			return 0, err
		} else {
			if s == score {
				return exists, nil
			}
			// delete old score key
			sk := zEncodeScoreKey(false, false, table, rk, member, s)
			if err != nil {
				return 0, err
			}
			wb.Delete(sk)
		}
	}

	wb.Put(ek, PutFloat64(score))

	sk := zEncodeScoreKey(false, false, table, rk, member, score)
	wb.Put(sk, []byte{})
	return exists, nil
}

func (db *RockDB) zDelItem(table, rk, member []byte,
	wb engine.WriteBatch) (int64, error) {
	ek := zEncodeSetKey(table, rk, member)
	if v, err := db.GetBytesNoLock(ek); err != nil {
		return 0, err
	} else if v == nil {
		//not exists
		return 0, nil
	} else {
		//exists
		//we must del score
		if s, err := Float64(v, err); err != nil {
			return 0, err
		} else {
			sk := zEncodeScoreKey(false, false, table, rk, member, s)
			wb.Delete(sk)
		}
	}
	wb.Delete(ek)
	return 1, nil
}

func (db *RockDB) ZAdd(ts int64, key []byte, args ...common.ScorePair) (int64, error) {
	if len(args) == 0 {
		return 0, nil
	}
	if len(args) > MAX_BATCH_NUM {
		return 0, errTooMuchBatchSize
	}
	keyInfo, err := db.prepareCollKeyForWrite(ts, ZSetType, key, nil)
	if err != nil {
		return 0, err
	}
	table := keyInfo.Table

	wb := db.wb
	defer wb.Clear()

	var num int64
	for i := 0; i < len(args); i++ {
		score := args[i].Score
		member := args[i].Member

		if err := checkKeySubKey(key, member); err != nil {
			return 0, err
		}
		if n, err := db.zSetItem(table, keyInfo.VerKey, score, member, wb); err != nil {
			return 0, err
		} else if n == 0 {
			//add new
			num++
		}
	}

	newNum, err := db.zIncrSize(ts, key, keyInfo.OldHeader, num, wb)
	if err != nil {
		return 0, err
	} else if newNum > 0 && newNum == num && !keyInfo.Expired {
		db.IncrTableKeyCount(table, 1, wb)
	}
	db.topLargeCollKeys.Update(key, int(newNum))

	slow.LogLargeCollection(int(newNum), slow.NewSlowLogInfo(string(table), string(key), "zset"))
	if newNum > collectionLengthForMetric {
		metric.CollectionLenDist.With(ps.Labels{
			"table": string(table),
		}).Observe(float64(newNum))
	}
	err = db.rockEng.Write(wb)
	return num, err
}

func (db *RockDB) ZFixKey(ts int64, key []byte) error {
	oldh, n, err := db.zGetSize(ts, key, false)
	if err != nil {
		dbLog.Infof("get zset card failed: %v", err.Error())
		return err
	}
	elems, err := db.ZRange(key, 0, -1)
	if err != nil {
		dbLog.Infof("get zset range failed: %v", err.Error())
		return err
	}
	if len(elems) != int(n) {
		dbLog.Infof("unmatched length : %v, %v, detail: %v", n, len(elems), elems)
		db.zSetSize(ts, key, oldh, int64(len(elems)), db.wb)
		err = db.CommitBatchWrite()
		if err != nil {
			return err
		}
	}
	return nil
}

// note: we should not batch incrsize, because we read the old and put the new.
func (db *RockDB) zIncrSize(ts int64, key []byte, oldh *headerMetaValue, delta int64, wb engine.WriteBatch) (int64, error) {
	meta := oldh.UserData
	size, err := parseZMetaSize(meta)
	if err != nil {
		return 0, err
	}
	size += delta
	sk := zEncodeSizeKey(key)
	if size <= 0 {
		size = 0
		wb.Delete(sk)
	} else {
		wb.Put(sk, encodeZMetaData(size, ts, oldh))
	}
	return size, nil
}

func (db *RockDB) zGetSize(tn int64, key []byte, useLock bool) (*headerMetaValue, int64, error) {
	oldh, expired, err := db.collHeaderMeta(tn, ZSetType, key, useLock)
	if err != nil {
		return oldh, 0, err
	}
	if expired {
		return oldh, 0, nil
	}
	s, err := parseZMetaSize(oldh.UserData)
	return oldh, s, err
}

func (db *RockDB) zGetVer(key []byte) (int64, error) {
	oldh, _, err := db.collHeaderMeta(time.Now().UnixNano(), ZSetType, key, true)
	if err != nil {
		return 0, err
	}
	_, ver, err := parseZMeta(oldh.UserData)
	return ver, err
}

func (db *RockDB) zSetSize(ts int64, key []byte, oldh *headerMetaValue, newSize int64, wb engine.WriteBatch) {
	sk := zEncodeSizeKey(key)
	if newSize <= 0 {
		wb.Delete(sk)
	} else {
		wb.Put(sk, encodeZMetaData(newSize, ts, oldh))
	}
}

func (db *RockDB) ZGetVer(key []byte) (int64, error) {
	return db.zGetVer(key)
}

func (db *RockDB) ZCard(key []byte) (int64, error) {
	ts := time.Now().UnixNano()
	_, s, err := db.zGetSize(ts, key, true)
	return s, err
}

func (db *RockDB) ZScore(key []byte, member []byte) (float64, error) {
	ts := time.Now().UnixNano()
	keyInfo, err := db.GetCollVersionKey(ts, ZSetType, key, true)
	var score float64
	if err != nil {
		return score, err
	}
	if keyInfo.IsNotExistOrExpired() {
		return score, errScoreMiss
	}
	k := zEncodeSetKey(keyInfo.Table, keyInfo.VerKey, member)
	refv, err := db.rockEng.GetRef(k)
	if err != nil {
		return score, err
	}
	if refv == nil {
		return score, errScoreMiss
	}
	defer refv.Free()
	if refv.Data() == nil {
		return score, errScoreMiss
	}
	if score, err = Float64(refv.Data(), nil); err != nil {
		return score, err
	}
	return score, nil
}

func (db *RockDB) ZRem(ts int64, key []byte, members ...[]byte) (int64, error) {
	if len(members) == 0 {
		return 0, nil
	}
	if len(members) > MAX_BATCH_NUM {
		return 0, errTooMuchBatchSize
	}
	keyInfo, err := db.GetCollVersionKey(ts, ZSetType, key, false)
	if err != nil {
		return 0, err
	}
	table := keyInfo.Table

	wb := db.wb
	defer wb.Clear()

	var num int64 = 0
	for i := 0; i < len(members); i++ {
		if err := checkKeySubKey(key, members[i]); err != nil {
			return 0, err
		}
		if n, err := db.zDelItem(table, keyInfo.VerKey, members[i], wb); err != nil {
			return 0, err
		} else if n == 1 {
			num++
		}
	}

	newNum, err := db.zIncrSize(ts, key, keyInfo.OldHeader, -num, wb)
	if err != nil {
		return 0, err
	} else if num > 0 && newNum == 0 {
		db.IncrTableKeyCount(table, -1, wb)
	}
	if newNum == 0 {
		db.delExpire(ZSetType, key, nil, false, wb)
	}
	db.topLargeCollKeys.Update(key, int(newNum))

	err = db.rockEng.Write(wb)
	return num, err
}

func (db *RockDB) ZIncrBy(ts int64, key []byte, delta float64, member []byte) (float64, error) {
	var score float64
	if err := checkKeySubKey(key, member); err != nil {
		return score, err
	}

	keyInfo, err := db.prepareCollKeyForWrite(ts, ZSetType, key, nil)
	if err != nil {
		return score, err
	}
	table := keyInfo.Table
	rk := keyInfo.VerKey

	wb := db.wb
	defer wb.Clear()

	ek := zEncodeSetKey(table, rk, member)

	var oldScore float64
	v, err := db.GetBytesNoLock(ek)
	if err != nil {
		return score, err
	} else if v == nil {
		newNum, err := db.zIncrSize(ts, key, keyInfo.OldHeader, 1, wb)
		if err != nil {
			return score, err
		} else if newNum == 1 && !keyInfo.Expired {
			db.IncrTableKeyCount(table, 1, wb)
		}
		db.topLargeCollKeys.Update(key, int(newNum))
	} else {
		if oldScore, err = Float64(v, err); err != nil {
			return score, err
		}
	}

	score = oldScore + delta

	sk := zEncodeScoreKey(false, false, table, rk, member, score)
	wb.Put(sk, []byte{})
	wb.Put(ek, PutFloat64(score))

	if v != nil {
		// so as to update score, we must delete the old one
		oldSk := zEncodeScoreKey(false, false, table, rk, member, oldScore)
		wb.Delete(oldSk)
	}

	err = db.rockEng.Write(wb)
	return score, err
}

func (db *RockDB) ZCount(key []byte, min float64, max float64) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}
	tn := time.Now().UnixNano()
	keyInfo, err := db.getZSetForRangeWithNum(tn, key, min, max, true)
	if err != nil {
		return 0, err
	}
	if keyInfo.IsNotExistOrExpired() {
		return 0, nil
	}
	minKey := keyInfo.RangeStart
	maxKey := keyInfo.RangeEnd
	it, err := db.NewDBRangeIterator(minKey, maxKey, common.RangeClose, false)
	if err != nil {
		return 0, err
	}

	var n int64
	for ; it.Valid(); it.Next() {
		n++
	}

	it.Close()
	return n, nil
}

func (db *RockDB) zrank(key []byte, member []byte, reverse bool) (int64, error) {
	if err := checkKeySubKey(key, member); err != nil {
		return 0, err
	}

	tn := time.Now().UnixNano()
	keyInfo, err := db.GetCollVersionKey(tn, ZSetType, key, true)
	if err != nil {
		return 0, err
	}
	if keyInfo.IsNotExistOrExpired() {
		return -1, nil
	}
	table := keyInfo.Table
	rk := keyInfo.VerKey

	k := zEncodeSetKey(table, rk, member)

	v, _ := db.GetBytes(k)
	if v == nil {
		return -1, nil
	} else {
		if s, err := Float64(v, nil); err != nil {
			return 0, err
		} else {
			sk := zEncodeScoreKey(false, false, table, rk, member, s)
			var rit *engine.RangeLimitedIterator
			if !reverse {
				minKey := zEncodeStartKey(table, rk)
				rit, err = db.NewDBRangeIterator(minKey, sk, common.RangeClose, reverse)
				if err != nil {
					return 0, err
				}
			} else {
				maxKey := zEncodeStopKey(table, rk)
				rit, err = db.NewDBRangeIterator(sk, maxKey, common.RangeClose, reverse)
				if err != nil {
					return 0, err
				}
			}
			defer rit.Close()

			var lastKey []byte
			var n int64 = 0

			for ; rit.Valid(); rit.Next() {
				rawk := rit.Key()
				n++
				lastKey = lastKey[0:0]
				lastKey = append(lastKey, rawk...)
			}

			if _, _, m, _, err := zDecodeScoreKey(lastKey); err == nil && bytes.Equal(m, member) {
				n--
				return n, nil
			} else {
				dbLog.Infof("last key decode error: %v, %v, %v\n", lastKey, m, member)
			}
		}
	}
	return -1, nil
}

func (db *RockDB) zRemAll(ts int64, key []byte, wb engine.WriteBatch) (int64, error) {
	keyInfo, err := db.getCollVerKeyForRange(ts, ZSetType, key, false)
	if err != nil {
		return 0, err
	}
	table := keyInfo.Table
	rk := keyInfo.VerKey
	num, err := parseZMetaSize(keyInfo.OldHeader.UserData)
	if err != nil {
		return 0, err
	}
	if num == 0 {
		return 0, nil
	}
	// no need delete if expired
	if keyInfo.IsNotExistOrExpired() {
		return 0, nil
	}
	db.topLargeCollKeys.Update(key, int(0))
	if db.cfg.ExpirationPolicy == common.WaitCompact {
		// for compact ttl , we can just delete the meta
		sk := zEncodeSizeKey(key)
		wb.Delete(sk)
		if num > 0 {
			db.IncrTableKeyCount(table, -1, wb)
		}
		return num, nil
	}

	minKey := keyInfo.RangeStart
	maxKey := keyInfo.RangeEnd
	if num > RangeDeleteNum {
		sk := zEncodeSizeKey(key)
		wb.DeleteRange(minKey, maxKey)

		minSetKey := zEncodeStartSetKey(table, rk)
		maxSetKey := zEncodeStopSetKey(table, rk)
		wb.DeleteRange(minSetKey, maxSetKey)
		if num > 0 {
			db.IncrTableKeyCount(table, -1, wb)
		}
		db.delExpire(ZSetType, key, nil, false, wb)
		wb.Delete(sk)
	} else {
		// remove all scan can ignore deleted to speed up scan.
		// update: no ignore deleted is needed, and it may costly if too much deleted
		rmCnt, err := db.zRemRangeBytes(ts, key, keyInfo, 0, -1, wb)
		return rmCnt, err
	}
	return num, nil
}

func (db *RockDB) zRemRangeBytes(ts int64, key []byte, keyInfo collVerKeyInfo, offset int,
	count int, wb engine.WriteBatch) (int64, error) {
	if len(key) > MaxKeySize {
		return 0, errKeySize
	}
	total, err := parseZMetaSize(keyInfo.OldHeader.UserData)
	if err != nil {
		return 0, err
	}
	if total == 0 {
		// no data to be deleted, avoid iterator data
		return 0, nil
	}
	// if count >= total size , remove all
	if offset == 0 {
		if err == nil && int64(count) >= total {
			return db.zRemAll(ts, key, wb)
		}
	}
	if count > MAX_BATCH_NUM {
		return 0, errTooMuchBatchSize
	}
	table := keyInfo.Table
	minKey := keyInfo.RangeStart
	maxKey := keyInfo.RangeEnd

	opts := engine.IteratorOpts{
		Range:   engine.Range{Min: minKey, Max: maxKey, Type: common.RangeClose},
		Limit:   engine.Limit{Offset: offset, Count: count},
		Reverse: false,
	}
	it, err := db.NewDBRangeLimitIteratorWithOpts(opts)
	if err != nil {
		return 0, err
	}
	defer it.Close()
	num := int64(0)
	for ; it.Valid(); it.Next() {
		sk := it.RefKey()
		_, _, m, _, err := zDecodeScoreKey(sk)
		if err != nil {
			continue
		}

		if n, err := db.zDelItem(table, keyInfo.VerKey, m, wb); err != nil {
			return 0, err
		} else if n == 1 {
			num++
		}
	}

	newNum, err := db.zIncrSize(ts, key, keyInfo.OldHeader, -num, wb)
	if err != nil {
		return 0, err
	} else if num > 0 && newNum == 0 {
		db.IncrTableKeyCount(table, -1, wb)
	}
	if newNum == 0 {
		db.delExpire(ZSetType, key, nil, false, wb)
	}
	db.topLargeCollKeys.Update(key, int(newNum))

	return num, nil
}

func (db *RockDB) zRemRange(ts int64, key []byte, min float64, max float64, offset int,
	count int, wb engine.WriteBatch) (int64, error) {

	keyInfo, err := db.getZSetForRangeWithNum(ts, key, min, max, false)
	if err != nil {
		return 0, err
	}

	return db.zRemRangeBytes(ts, key, keyInfo, offset, count, wb)
}

func (db *RockDB) zRangeBytes(ts int64, preCheckCnt bool, key []byte, minKey []byte, maxKey []byte, offset int, count int, reverse bool) ([]common.ScorePair, error) {
	if len(key) > MaxKeySize {
		return nil, errKeySize
	}
	if offset < 0 {
		return []common.ScorePair{}, nil
	}

	if count > MAX_BATCH_NUM {
		return nil, errTooMuchBatchSize
	}
	// if count == -1, check if we may get too much data
	if count < 0 && preCheckCnt {
		_, total, _ := db.zGetSize(ts, key, true)
		if total-int64(offset) > MAX_BATCH_NUM {
			return nil, errTooMuchBatchSize
		}
	}

	nv := count
	// count may be very large, so we must limit it for below mem make.
	if nv <= 0 || nv > MAX_BATCH_NUM {
		nv = MAX_BATCH_NUM
	}

	// TODO: use buf pool
	// we can not use count for prealloc since it may large than we have for (total - offset),
	// since it have minkey~maxkey range, it may much smaller than count
	preAlloc := 16
	if count > 0 && count < preAlloc {
		preAlloc = count
	}
	// TODO: use pool for large alloc
	v := make([]common.ScorePair, 0, preAlloc)

	var err error
	var it *engine.RangeLimitedIterator
	//if reverse and offset is 0, count < 0, we may use forward iterator then reverse
	//because store iterator prev is slower than next
	if !reverse || (offset == 0 && count < 0) {
		it, err = db.NewDBRangeLimitIterator(minKey, maxKey, common.RangeClose, offset, count, false)
	} else {
		it, err = db.NewDBRangeLimitIterator(minKey, maxKey, common.RangeClose, offset, count, true)
	}
	if err != nil {
		return nil, err
	}
	tooMuch := false
	for ; it.Valid(); it.Next() {
		rawk := it.Key()
		_, _, m, s, err := zDecodeScoreKey(rawk)
		if err != nil {
			continue
		}
		v = append(v, common.ScorePair{Member: m, Score: s})

		if count < 0 && len(v) > MAX_BATCH_NUM {
			tooMuch = true
			break
		}
	}
	it.Close()
	if tooMuch {
		dbLog.Infof("key %v huge range in result: %v", string(key), len(v))
		return nil, errTooMuchBatchSize
	}
	if reverse && (offset == 0 && count < 0) {
		for i, j := 0, len(v)-1; i < j; i, j = i+1, j-1 {
			v[i], v[j] = v[j], v[i]
		}
	}

	return v, nil
}

func (db *RockDB) zRange(key []byte, min float64, max float64, offset int, count int, reverse bool) ([]common.ScorePair, error) {
	tn := time.Now().UnixNano()
	keyInfo, err := db.getZSetForRangeWithNum(tn, key, min, max, true)
	if err != nil {
		return nil, err
	}
	if keyInfo.IsNotExistOrExpired() {
		return nil, nil
	}

	minKey := keyInfo.RangeStart
	maxKey := keyInfo.RangeEnd
	preCheckCnt := false
	if min == common.MinScore && max == common.MaxScore {
		preCheckCnt = true
	}
	return db.zRangeBytes(tn, preCheckCnt, key, minKey, maxKey, offset, count, reverse)
}

func (db *RockDB) zParseLimit(total int64, start int, stop int) (offset int, count int, err error) {
	if start < 0 || stop < 0 {
		//refer redis implementation
		var size int64
		size = total
		llen := int(size)

		if start < 0 {
			start = llen + start
		}
		if stop < 0 {
			stop = llen + stop
		}

		if start < 0 {
			start = 0
		}

		if start >= llen {
			offset = -1
			return
		}
	}

	if start > stop {
		offset = -1
		return
	}

	offset = start
	count = (stop - start) + 1
	return
}

func (db *RockDB) ZClear(ts int64, key []byte) (int64, error) {
	defer db.wb.Clear()

	rmCnt, err := db.zRemAll(ts, key, db.wb)
	if err == nil {
		err = db.rockEng.Write(db.wb)
	}
	if rmCnt > 0 {
		return 1, err
	}
	return 0, err
}

func (db *RockDB) ZMclear(keys ...[]byte) (int64, error) {
	if len(keys) > MAX_BATCH_NUM {
		return 0, errTooMuchBatchSize
	}
	deleted := int64(0)
	for _, key := range keys {
		// note: the zRemAll can not be batched, so we need clear and commit
		// after each key.
		if _, err := db.zRemAll(0, key, db.wb); err != nil {
			db.wb.Clear()
			return deleted, err
		}
		err := db.CommitBatchWrite()
		if err != nil {
			return deleted, err
		}
		deleted++
	}

	return int64(len(keys)), nil
}

func (db *RockDB) zMclearWithBatch(wb engine.WriteBatch, keys ...[]byte) error {
	if len(keys) > MAX_BATCH_NUM {
		return errTooMuchBatchSize
	}
	for _, key := range keys {
		if _, err := db.zRemAll(0, key, wb); err != nil {
			return err
		}
	}

	return nil
}

func (db *RockDB) ZRange(key []byte, start int, stop int) ([]common.ScorePair, error) {
	return db.ZRangeGeneric(key, start, stop, false)
}

//min and max must be inclusive
//if no limit, set offset = 0 and count = -1
func (db *RockDB) ZRangeByScore(key []byte, min float64, max float64,
	offset int, count int) ([]common.ScorePair, error) {
	return db.ZRangeByScoreGeneric(key, min, max, offset, count, false)
}

func (db *RockDB) ZRank(key []byte, member []byte) (int64, error) {
	return db.zrank(key, member, false)
}

func (db *RockDB) ZRemRangeByRank(ts int64, key []byte, start int, stop int) (int64, error) {
	keyInfo, err := db.getCollVerKeyForRange(ts, ZSetType, key, false)
	if err != nil {
		return 0, err
	}
	num, err := parseZMetaSize(keyInfo.OldHeader.UserData)
	if err != nil {
		return 0, err
	}

	offset, count, err := db.zParseLimit(num, start, stop)
	if err != nil {
		return 0, err
	}

	var rmCnt int64
	defer db.wb.Clear()

	rmCnt, err = db.zRemRangeBytes(ts, key, keyInfo, offset, count, db.wb)
	if err == nil {
		err = db.rockEng.Write(db.wb)
	}
	return rmCnt, err
}

//min and max must be inclusive
func (db *RockDB) ZRemRangeByScore(ts int64, key []byte, min float64, max float64) (int64, error) {
	defer db.wb.Clear()

	rmCnt, err := db.zRemRange(ts, key, min, max, 0, -1, db.wb)
	if err == nil {
		err = db.rockEng.Write(db.wb)
	}

	return rmCnt, err
}

func (db *RockDB) ZRevRange(key []byte, start int, stop int) ([]common.ScorePair, error) {
	return db.ZRangeGeneric(key, start, stop, true)
}

func (db *RockDB) ZRevRank(key []byte, member []byte) (int64, error) {
	return db.zrank(key, member, true)
}

//min and max must be inclusive
//if no limit, set offset = 0 and count = -1
func (db *RockDB) ZRevRangeByScore(key []byte, min float64, max float64, offset int, count int) ([]common.ScorePair, error) {
	return db.ZRangeByScoreGeneric(key, min, max, offset, count, true)
}

func (db *RockDB) ZRangeGeneric(key []byte, start int, stop int, reverse bool) ([]common.ScorePair, error) {
	tn := time.Now().UnixNano()
	keyInfo, err := db.getCollVerKeyForRange(tn, ZSetType, key, true)
	if err != nil {
		return nil, err
	}
	if keyInfo.IsNotExistOrExpired() {
		return nil, nil
	}
	num, err := parseZMetaSize(keyInfo.OldHeader.UserData)
	if err != nil {
		return nil, err
	}
	offset, count, err := db.zParseLimit(num, start, stop)
	if err != nil {
		return nil, err
	}
	return db.zRangeBytes(tn, true, key, keyInfo.RangeStart, keyInfo.RangeEnd, offset, count, reverse)
}

//min and max must be inclusive
//if no limit, set offset = 0 and count = -1
func (db *RockDB) ZRangeByScoreGeneric(key []byte, min float64, max float64,
	offset int, count int, reverse bool) ([]common.ScorePair, error) {

	return db.zRange(key, min, max, offset, count, reverse)
}

func getAggregateFunc(aggregate byte) func(int64, int64) int64 {
	switch aggregate {
	case AggregateSum:
		return func(a int64, b int64) int64 {
			return a + b
		}
	case AggregateMax:
		return func(a int64, b int64) int64 {
			if a > b {
				return a
			}
			return b
		}
	case AggregateMin:
		return func(a int64, b int64) int64 {
			if a > b {
				return b
			}
			return a
		}
	}
	return nil
}

func (db *RockDB) ZRangeByLex(key []byte, min []byte, max []byte, rangeType uint8, offset int, count int) ([][]byte, error) {
	if count > MAX_BATCH_NUM {
		return nil, errTooMuchBatchSize
	}

	tn := time.Now().UnixNano()
	keyInfo, err := db.getZSetForRangeWithMinMax(tn, key, min, max, true)
	if err != nil {
		return nil, err
	}
	if keyInfo.IsNotExistOrExpired() {
		return nil, nil
	}
	min = keyInfo.RangeStart
	max = keyInfo.RangeEnd

	if count < 0 && min == nil && max == nil {
		total, _ := parseZMetaSize(keyInfo.OldHeader.UserData)
		if total-int64(offset) > MAX_BATCH_NUM {
			return nil, errTooMuchBatchSize
		}
	}
	it, err := db.NewDBRangeLimitIterator(min, max, rangeType, offset, count, false)
	if err != nil {
		return nil, err
	}
	defer it.Close()

	preAlloc := 16
	if count > 0 && count < preAlloc {
		preAlloc = count
	}
	// TODO: use pool for large alloc
	ay := make([][]byte, 0, preAlloc)
	for ; it.Valid(); it.Next() {
		rawk := it.Key()
		if _, _, m, err := zDecodeSetKey(rawk); err == nil {
			ay = append(ay, m)
			//dbLog.Infof("key %v : %v", rawk)
		} else {
			dbLog.Infof("key %v : error %v", rawk, err)
		}
		// TODO: err for iterator step would match the final count?
		if count >= 0 && len(ay) >= count {
			break
		}
		if count < 0 && len(ay) > MAX_BATCH_NUM {
			dbLog.Infof("key %v huge range in result: %v", string(key), len(ay))
			return nil, errTooMuchBatchSize
		}
	}

	return ay, nil
}

func (db *RockDB) internalZRemRangeByLex(ts int64, key []byte, min []byte, max []byte, rangeType uint8, wb engine.WriteBatch) (int64, error) {
	keyInfo, err := db.getZSetForRangeWithMinMax(ts, key, min, max, false)
	if err != nil {
		return 0, err
	}

	it, err := db.NewDBRangeIterator(keyInfo.RangeStart, keyInfo.RangeEnd, rangeType, false)
	if err != nil {
		return 0, err
	}
	defer it.Close()
	var num int64 = 0
	for ; it.Valid(); it.Next() {
		sk := it.RefKey()
		_, _, m, err := zDecodeSetKey(sk)
		if err != nil {
			continue
		}
		if n, err := db.zDelItem(keyInfo.Table, keyInfo.VerKey, m, wb); err != nil {
			return 0, err
		} else if n == 1 {
			num++
		}
	}

	newNum, err := db.zIncrSize(ts, key, keyInfo.OldHeader, -num, wb)
	if err != nil {
		return 0, err
	} else if num > 0 && newNum == 0 {
		db.IncrTableKeyCount(keyInfo.Table, -1, wb)
	}
	if newNum == 0 {
		db.delExpire(ZSetType, key, nil, false, wb)
	}

	db.topLargeCollKeys.Update(key, int(newNum))
	return num, nil
}

func (db *RockDB) ZRemRangeByLex(ts int64, key []byte, min []byte, max []byte, rangeType uint8) (int64, error) {
	wb := db.wb
	defer wb.Clear()
	if min == nil && max == nil {
		cnt, err := db.zRemAll(ts, key, wb)
		if err != nil {
			return 0, err
		}
		if err := db.rockEng.Write(wb); err != nil {
			return 0, err
		}
		return cnt, nil
	}

	num, err := db.internalZRemRangeByLex(ts, key, min, max, rangeType, wb)
	if err != nil {
		return 0, err
	}

	if err := db.rockEng.Write(wb); err != nil {
		return 0, err
	}

	return num, nil
}

func (db *RockDB) ZLexCount(key []byte, min []byte, max []byte, rangeType uint8) (int64, error) {
	tn := time.Now().UnixNano()
	keyInfo, err := db.getZSetForRangeWithMinMax(tn, key, min, max, true)
	if err != nil {
		return 0, err
	}
	if keyInfo.IsNotExistOrExpired() {
		return 0, nil
	}

	it, err := db.NewDBRangeIterator(keyInfo.RangeStart, keyInfo.RangeEnd, rangeType, false)
	if err != nil {
		return 0, err
	}
	var n int64 = 0
	for ; it.Valid(); it.Next() {
		n++
	}
	it.Close()
	return n, nil
}

func (db *RockDB) ZKeyExists(key []byte) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}

	return db.collKeyExists(ZSetType, key)
}

func (db *RockDB) ZExpire(ts int64, key []byte, duration int64) (int64, error) {
	return db.collExpire(ts, ZSetType, key, duration)
}

func (db *RockDB) ZPersist(ts int64, key []byte) (int64, error) {
	return db.collPersist(ts, ZSetType, key)
}
