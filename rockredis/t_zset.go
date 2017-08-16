package rockredis

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/absolute8511/gorocksdb"
)

const (
	MinScore     int64 = -1<<63 + 1
	MaxScore     int64 = 1<<63 - 1
	InvalidScore int64 = -1 << 63

	AggregateSum byte = 0
	AggregateMin byte = 1
	AggregateMax byte = 2
)

var errZSizeKey = errors.New("invalid zsize key")
var errZSetKey = errors.New("invalid zset key")
var errZScoreKey = errors.New("invalid zscore key")
var errScoreOverflow = errors.New("zset score overflow")
var errInvalidAggregate = errors.New("invalid aggregate")
var errInvalidWeightNum = errors.New("invalid weight number")
var errInvalidSrcKeyNum = errors.New("invalid src key number")
var errScoreMiss = errors.New("missing score for zset")

const (
	zsetNScoreSep    byte = '<'
	zsetPScoreSep    byte = zsetNScoreSep + 1
	zsetStopScoreSep byte = zsetPScoreSep + 1

	zsetStartMemSep byte = ':'
)

func checkZSetKMSize(key []byte, member []byte) error {
	if len(key) > MaxKeySize || len(key) == 0 {
		return errKeySize
	} else if len(member) > MaxZSetMemberSize || len(member) == 0 {
		return errZSetMemberSize
	}
	return nil
}

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

func convertRedisKeyToDBZSetKey(key []byte, member []byte) ([]byte, error) {
	table, rk, err := extractTableFromRedisKey(key)
	if err != nil {
		return nil, err
	}
	if err := checkZSetKMSize(rk, member); err != nil {
		return nil, err
	}
	return zEncodeSetKey(table, rk, member), nil
}

func convertRedisKeyToDBZScoreKey(key []byte, member []byte, score int64) ([]byte, error) {
	table, rk, err := extractTableFromRedisKey(key)
	if err != nil {
		return nil, err
	}
	if err := checkZSetKMSize(rk, member); err != nil {
		return nil, err
	}
	if score <= MinScore || score >= MaxScore {
		return nil, errScoreOverflow
	}
	return zEncodeScoreKey(table, rk, member, score), nil
}

func zEncodeSetKey(table []byte, key []byte, member []byte) []byte {
	buf := make([]byte, len(table)+2+1+len(key)+len(member)+4)
	pos := 0
	buf[pos] = ZSetType
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

	buf[pos] = zsetStartMemSep
	pos++

	copy(buf[pos:], member)
	return buf
}

func zDecodeSetKey(ek []byte) ([]byte, []byte, []byte, error) {
	pos := 0
	if pos+1 > len(ek) || ek[pos] != ZSetType {
		return nil, nil, nil, errZSetKey
	}

	pos++
	if pos+2 > len(ek) {
		return nil, nil, nil, errZSetKey
	}

	tableLen := int(binary.BigEndian.Uint16(ek[pos:]))
	pos += 2
	if tableLen+pos > len(ek) {
		return nil, nil, nil, errZSetKey
	}
	table := ek[pos : pos+tableLen]
	pos += tableLen
	if ek[pos] != tableStartSep {
		return nil, nil, nil, errZSetKey
	}
	pos++

	if pos+2 > len(ek) {
		return table, nil, nil, errZSetKey
	}

	keyLen := int(binary.BigEndian.Uint16(ek[pos:]))
	if keyLen+pos > len(ek) {
		return table, nil, nil, errZSetKey
	}

	pos += 2
	key := ek[pos : pos+keyLen]

	if ek[pos+keyLen] != zsetStartMemSep {
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

func zEncodeScoreKey(table []byte, key []byte, member []byte, score int64) []byte {
	buf := make([]byte, len(table)+2+1+len(key)+len(member)+13)
	pos := 0

	buf[pos] = ZScoreType
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

	if score < 0 {
		buf[pos] = zsetNScoreSep
	} else {
		buf[pos] = zsetPScoreSep
	}

	pos++
	binary.BigEndian.PutUint64(buf[pos:], uint64(score))
	pos += 8

	buf[pos] = zsetStartMemSep
	pos++

	copy(buf[pos:], member)
	return buf
}

func zEncodeStartScoreKey(table []byte, key []byte, score int64) []byte {
	return zEncodeScoreKey(table, key, nil, score)
}

func zEncodeStopScoreKey(table []byte, key []byte, score int64) []byte {
	k := zEncodeScoreKey(table, key, nil, score)
	k[len(k)-1] = k[len(k)-1] + 1
	return k
}

func zDecodeScoreKey(ek []byte) (table []byte, key []byte, member []byte, score int64, err error) {
	pos := 0
	if pos+1 > len(ek) || ek[pos] != ZScoreType {
		err = errZScoreKey
		return
	}
	pos++

	if pos+2 > len(ek) {
		err = errZScoreKey
		return
	}

	tableLen := int(binary.BigEndian.Uint16(ek[pos:]))
	pos += 2
	if tableLen+pos > len(ek) {
		err = errZScoreKey
		return
	}
	table = ek[pos : pos+tableLen]
	pos += tableLen
	if ek[pos] != tableStartSep {
		err = errZScoreKey
		return
	}
	pos++

	if pos+2 > len(ek) {
		err = errZScoreKey
		return
	}

	keyLen := int(binary.BigEndian.Uint16(ek[pos:]))
	pos += 2

	if keyLen+pos > len(ek) {
		err = errZScoreKey
		return
	}

	key = ek[pos : pos+keyLen]
	pos += keyLen

	if pos+10 > len(ek) {
		err = errZScoreKey
		return
	}

	if (ek[pos] != zsetNScoreSep) && (ek[pos] != zsetPScoreSep) {
		err = errZScoreKey
		return
	}
	pos++

	score = int64(binary.BigEndian.Uint64(ek[pos:]))
	pos += 8

	if ek[pos] != zsetStartMemSep {
		err = errZScoreKey
		return
	}

	pos++

	member = ek[pos:]
	return
}

func (db *RockDB) zSetItem(key []byte, score int64, member []byte, wb *gorocksdb.WriteBatch) (int64, error) {
	if score <= MinScore || score >= MaxScore {
		return 0, errScoreOverflow
	}
	var exists int64
	ek, err := convertRedisKeyToDBZSetKey(key, member)
	if err != nil {
		return 0, err
	}

	if v, err := db.eng.GetBytesNoLock(db.defaultReadOpts, ek); err != nil {
		return 0, err
	} else if v != nil {
		exists = 1
		if s, err := Int64(v, err); err != nil {
			return 0, err
		} else {
			sk, err := convertRedisKeyToDBZScoreKey(key, member, s)
			if err != nil {
				return 0, err
			}
			wb.Delete(sk)
		}
	}

	wb.Put(ek, PutInt64(score))

	sk, err := convertRedisKeyToDBZScoreKey(key, member, score)
	if err != nil {
		return 0, err
	}
	wb.Put(sk, []byte{})
	return exists, nil
}

func (db *RockDB) zDelItem(key []byte, member []byte,
	wb *gorocksdb.WriteBatch) (int64, error) {
	ek, err := convertRedisKeyToDBZSetKey(key, member)
	if err != nil {
		return 0, err
	}
	if v, err := db.eng.GetBytesNoLock(db.defaultReadOpts, ek); err != nil {
		return 0, err
	} else if v == nil {
		//not exists
		return 0, nil
	} else {
		//exists
		//we must del score
		if s, err := Int64(v, err); err != nil {
			return 0, err
		} else {
			sk, err := convertRedisKeyToDBZScoreKey(key, member, s)
			if err != nil {
				return 0, err
			}
			wb.Delete(sk)
		}
	}
	wb.Delete(ek)
	return 1, nil
}

func (db *RockDB) zDelete(key []byte, wb *gorocksdb.WriteBatch) (int64, error) {
	delMembCnt, err := db.zRemRange(key, MinScore, MaxScore, 0, -1, wb)
	//	TODO : log err
	return delMembCnt, err
}

func (db *RockDB) ZAdd(key []byte, args ...common.ScorePair) (int64, error) {
	if len(args) == 0 {
		return 0, nil
	}
	if len(args) >= MAX_BATCH_NUM {
		return 0, errTooMuchBatchSize
	}
	table, _, err := extractTableFromRedisKey(key)
	if err != nil {
		return 0, err
	}

	wb := db.wb
	wb.Clear()

	var num int64
	for i := 0; i < len(args); i++ {
		score := args[i].Score
		member := args[i].Member

		if err := checkZSetKMSize(key, member); err != nil {
			return 0, err
		}
		if n, err := db.zSetItem(key, score, member, wb); err != nil {
			return 0, err
		} else if n == 0 {
			//add new
			num++
		}
	}

	if newNum, err := db.zIncrSize(key, num, wb); err != nil {
		return 0, err
	} else if newNum > 0 && newNum == num {
		db.IncrTableKeyCount(table, 1, wb)
	}

	err = db.eng.Write(db.defaultWriteOpts, wb)
	return num, err
}

func (db *RockDB) zIncrSize(key []byte, delta int64, wb *gorocksdb.WriteBatch) (int64, error) {
	sk := zEncodeSizeKey(key)

	size, err := Int64(db.eng.GetBytesNoLock(db.defaultReadOpts, sk))
	if err != nil {
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

func (db *RockDB) ZCard(key []byte) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}

	sk := zEncodeSizeKey(key)
	return Int64(db.eng.GetBytes(db.defaultReadOpts, sk))
}

func (db *RockDB) ZScore(key []byte, member []byte) (int64, error) {
	score := InvalidScore

	k, err := convertRedisKeyToDBZSetKey(key, member)
	if err != nil {
		return InvalidScore, err
	}
	if v, err := db.eng.GetBytes(db.defaultReadOpts, k); err != nil {
		return InvalidScore, err
	} else if v == nil {
		return InvalidScore, errScoreMiss
	} else {
		if score, err = Int64(v, nil); err != nil {
			return InvalidScore, err
		}
	}
	return score, nil
}

func (db *RockDB) ZRem(key []byte, members ...[]byte) (int64, error) {
	if len(members) == 0 {
		return 0, nil
	}
	if len(members) >= MAX_BATCH_NUM {
		return 0, errTooMuchBatchSize
	}
	table, _, err := extractTableFromRedisKey(key)
	if err != nil {
		return 0, err
	}

	wb := db.wb
	wb.Clear()

	var num int64 = 0
	for i := 0; i < len(members); i++ {
		if err := checkZSetKMSize(key, members[i]); err != nil {
			return 0, err
		}
		if n, err := db.zDelItem(key, members[i], wb); err != nil {
			return 0, err
		} else if n == 1 {
			num++
		}
	}

	if newNum, err := db.zIncrSize(key, -num, wb); err != nil {
		return 0, err
	} else if num > 0 && newNum == 0 {
		db.IncrTableKeyCount(table, -1, wb)
		db.delExpire(ZSetType, key, wb)
	}

	err = db.eng.Write(db.defaultWriteOpts, wb)
	return num, err
}

func (db *RockDB) ZIncrBy(key []byte, delta int64, member []byte) (int64, error) {
	if err := checkZSetKMSize(key, member); err != nil {
		return InvalidScore, err
	}
	table, rk, err := extractTableFromRedisKey(key)
	if err != nil {
		return 0, err
	}

	wb := db.wb
	wb.Clear()

	ek := zEncodeSetKey(table, rk, member)

	var oldScore int64
	v, err := db.eng.GetBytesNoLock(db.defaultReadOpts, ek)
	if err != nil {
		return InvalidScore, err
	} else if v == nil {
		newNum, err := db.zIncrSize(key, 1, wb)
		if err != nil {
			return InvalidScore, err
		} else if newNum == 1 {
			db.IncrTableKeyCount(table, 1, wb)
		}
	} else {
		if oldScore, err = Int64(v, err); err != nil {
			return InvalidScore, err
		}
	}

	newScore := oldScore + delta
	if newScore >= MaxScore || newScore <= MinScore {
		return InvalidScore, errScoreOverflow
	}

	sk := zEncodeScoreKey(table, rk, member, newScore)
	wb.Put(sk, []byte{})
	wb.Put(ek, PutInt64(newScore))

	if v != nil {
		// so as to update score, we must delete the old one
		oldSk := zEncodeScoreKey(table, rk, member, oldScore)
		wb.Delete(oldSk)
	}

	err = db.eng.Write(db.defaultWriteOpts, wb)
	return newScore, err
}

func (db *RockDB) ZCount(key []byte, min int64, max int64) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}
	table, rk, err := extractTableFromRedisKey(key)
	if err != nil {
		return 0, err
	}
	minKey := zEncodeStartScoreKey(table, rk, min)
	maxKey := zEncodeStopScoreKey(table, rk, max)
	it, err := NewDBRangeIterator(db.eng, minKey, maxKey, common.RangeClose, false)
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
	if err := checkZSetKMSize(key, member); err != nil {
		return 0, err
	}

	table, rk, err := extractTableFromRedisKey(key)
	if err != nil {
		return 0, err
	}
	k := zEncodeSetKey(table, rk, member)

	v, _ := db.eng.GetBytes(db.defaultReadOpts, k)
	if v == nil {
		return -1, nil
	} else {
		if s, err := Int64(v, nil); err != nil {
			return 0, err
		} else {
			sk := zEncodeScoreKey(table, rk, member, s)
			var rit *RangeLimitedIterator
			if !reverse {
				minKey := zEncodeStartScoreKey(table, rk, MinScore)
				rit, err = NewDBRangeIterator(db.eng, minKey, sk, common.RangeClose, reverse)
				if err != nil {
					return 0, err
				}
			} else {
				maxKey := zEncodeStopScoreKey(table, rk, MaxScore)
				rit, err = NewDBRangeIterator(db.eng, sk, maxKey, common.RangeClose, reverse)
				if err != nil {
					return 0, err
				}
			}
			defer rit.Close()

			var lastKey []byte
			var n int64 = 0

			for ; rit.Valid(); rit.Next() {
				rawk := rit.RefKey()
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

func (db *RockDB) zRemRange(key []byte, min int64, max int64, offset int,
	count int, wb *gorocksdb.WriteBatch) (int64, error) {
	if len(key) > MaxKeySize {
		return 0, errKeySize
	}
	// TODO: if count <=0 , maybe remove all?
	if count >= MAX_BATCH_NUM {
		return 0, errTooMuchBatchSize
	}
	table, rk, err := extractTableFromRedisKey(key)
	if err != nil {
		return 0, err
	}

	minKey := zEncodeStartScoreKey(table, rk, min)
	maxKey := zEncodeStopScoreKey(table, rk, max)
	it, err := NewDBRangeLimitIterator(db.eng, minKey, maxKey, common.RangeClose, offset, count, false)
	if err != nil {
		return 0, err
	}
	num := int64(0)
	for ; it.Valid(); it.Next() {
		sk := it.RefKey()
		_, _, m, _, err := zDecodeScoreKey(sk)
		if err != nil {
			continue
		}

		if n, err := db.zDelItem(key, m, wb); err != nil {
			return 0, err
		} else if n == 1 {
			num++
		}
	}
	it.Close()

	if newNum, err := db.zIncrSize(key, -num, wb); err != nil {
		return 0, err
	} else if num > 0 && newNum == 0 {
		db.IncrTableKeyCount(table, -1, wb)
		db.delExpire(ZSetType, key, wb)
	}

	return num, nil
}

func (db *RockDB) zRange(key []byte, min int64, max int64, offset int, count int, reverse bool) ([]common.ScorePair, error) {
	if len(key) > MaxKeySize {
		return nil, errKeySize
	}

	if offset < 0 {
		return []common.ScorePair{}, nil
	}

	if count >= MAX_BATCH_NUM {
		return nil, errTooMuchBatchSize
	}
	nv := count
	// count may be very large, so we must limit it for below mem make.
	if nv <= 0 || nv > 1024 {
		nv = 64
	}

	v := make([]common.ScorePair, 0, nv)

	table, rk, err := extractTableFromRedisKey(key)
	if err != nil {
		return nil, err
	}
	var it *RangeLimitedIterator
	minKey := zEncodeStartScoreKey(table, rk, min)
	maxKey := zEncodeStopScoreKey(table, rk, max)
	//if reverse and offset is 0, count < 0, we may use forward iterator then reverse
	//because store iterator prev is slower than next
	if !reverse || (offset == 0 && count < 0) {
		it, err = NewDBRangeLimitIterator(db.eng, minKey, maxKey, common.RangeClose, offset, count, false)
	} else {
		it, err = NewDBRangeLimitIterator(db.eng, minKey, maxKey, common.RangeClose, offset, count, true)
	}
	if err != nil {
		return nil, err
	}
	for ; it.Valid(); it.Next() {
		rawk := it.Key()
		_, _, m, s, err := zDecodeScoreKey(rawk)
		if err != nil {
			continue
		}
		v = append(v, common.ScorePair{Member: m, Score: s})
	}
	it.Close()

	if reverse && (offset == 0 && count < 0) {
		for i, j := 0, len(v)-1; i < j; i, j = i+1, j-1 {
			v[i], v[j] = v[j], v[i]
		}
	}

	return v, nil
}

func (db *RockDB) zParseLimit(key []byte, start int, stop int) (offset int, count int, err error) {
	if start < 0 || stop < 0 {
		//refer redis implementation
		var size int64
		size, err = db.ZCard(key)
		if err != nil {
			return
		}

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

func (db *RockDB) ZClear(key []byte) (int64, error) {
	db.wb.Clear()
	rmCnt, err := db.zRemRange(key, MinScore, MaxScore, 0, -1, db.wb)
	if err == nil {
		err = db.eng.Write(db.defaultWriteOpts, db.wb)
	}
	return rmCnt, err
}

func (db *RockDB) ZMclear(keys ...[]byte) (int64, error) {
	if len(keys) > MAX_BATCH_NUM {
		return 0, errTooMuchBatchSize
	}
	db.wb.Clear()
	for _, key := range keys {
		if _, err := db.zRemRange(key, MinScore, MaxScore, 0, -1, db.wb); err != nil {
			return 0, err
		}
	}

	err := db.eng.Write(db.defaultWriteOpts, db.wb)
	return int64(len(keys)), err
}

func (db *RockDB) zMclearWithBatch(wb *gorocksdb.WriteBatch, keys ...[]byte) error {
	if len(keys) > MAX_BATCH_NUM {
		return errTooMuchBatchSize
	}
	for _, key := range keys {
		if _, err := db.zRemRange(key, MinScore, MaxScore, 0, -1, wb); err != nil {
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
func (db *RockDB) ZRangeByScore(key []byte, min int64, max int64,
	offset int, count int) ([]common.ScorePair, error) {
	return db.ZRangeByScoreGeneric(key, min, max, offset, count, false)
}

func (db *RockDB) ZRank(key []byte, member []byte) (int64, error) {
	return db.zrank(key, member, false)
}

func (db *RockDB) ZRemRangeByRank(key []byte, start int, stop int) (int64, error) {
	offset, count, err := db.zParseLimit(key, start, stop)
	if err != nil {
		return 0, err
	}

	var rmCnt int64

	db.wb.Clear()
	rmCnt, err = db.zRemRange(key, MinScore, MaxScore, offset, count, db.wb)
	if err == nil {
		err = db.eng.Write(db.defaultWriteOpts, db.wb)
	}
	return rmCnt, err
}

//min and max must be inclusive
func (db *RockDB) ZRemRangeByScore(key []byte, min int64, max int64) (int64, error) {
	db.wb.Clear()

	rmCnt, err := db.zRemRange(key, min, max, 0, -1, db.wb)
	if err == nil {
		err = db.eng.Write(db.defaultWriteOpts, db.wb)
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
func (db *RockDB) ZRevRangeByScore(key []byte, min int64, max int64, offset int, count int) ([]common.ScorePair, error) {
	return db.ZRangeByScoreGeneric(key, min, max, offset, count, true)
}

func (db *RockDB) ZRangeGeneric(key []byte, start int, stop int, reverse bool) ([]common.ScorePair, error) {
	offset, count, err := db.zParseLimit(key, start, stop)
	if err != nil {
		return nil, err
	}

	return db.zRange(key, MinScore, MaxScore, offset, count, reverse)
}

//min and max must be inclusive
//if no limit, set offset = 0 and count = -1
func (db *RockDB) ZRangeByScoreGeneric(key []byte, min int64, max int64,
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
	table, rk, err := extractTableFromRedisKey(key)
	if err != nil {
		return nil, err
	}

	if min == nil {
		min = zEncodeStartSetKey(table, rk)
	} else {
		min = zEncodeSetKey(table, rk, min)
	}
	if max == nil {
		max = zEncodeStopSetKey(table, rk)
	} else {
		max = zEncodeSetKey(table, rk, max)
	}
	if count >= MAX_BATCH_NUM {
		return nil, errTooMuchBatchSize
	}

	it, err := NewDBRangeLimitIterator(db.eng, min, max, rangeType, offset, count, false)
	if err != nil {
		return nil, err
	}
	defer it.Close()

	ay := make([][]byte, 0, 16)
	for ; it.Valid(); it.Next() {
		rawk := it.Key()
		if _, _, m, err := zDecodeSetKey(rawk); err == nil {
			ay = append(ay, m)
		}
		// TODO: err for iterator step would match the final count?
		if count >= 0 && len(ay) >= count {
			break
		}
	}
	return ay, nil
}

func (db *RockDB) ZRemRangeByLex(key []byte, min []byte, max []byte, rangeType uint8) (int64, error) {
	table, rk, err := extractTableFromRedisKey(key)
	if err != nil {
		return 0, err
	}

	if min == nil {
		min = zEncodeStartSetKey(table, rk)
	} else {
		min = zEncodeSetKey(table, rk, min)
	}
	if max == nil {
		max = zEncodeStopSetKey(table, rk)
	} else {
		max = zEncodeSetKey(table, rk, max)
	}

	wb := db.wb
	wb.Clear()
	it, err := NewDBRangeIterator(db.eng, min, max, rangeType, false)
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
		if n, err := db.zDelItem(key, m, wb); err != nil {
			return 0, err
		} else if n == 1 {
			num++
		}
	}

	if newNum, err := db.zIncrSize(key, -num, wb); err != nil {
		return 0, err
	} else if num > 0 && newNum == 0 {
		db.IncrTableKeyCount(table, -1, wb)
		db.delExpire(ZSetType, key, wb)
	}

	if err := db.eng.Write(db.defaultWriteOpts, wb); err != nil {
		return 0, err
	}

	return num, nil
}

func (db *RockDB) ZLexCount(key []byte, min []byte, max []byte, rangeType uint8) (int64, error) {
	table, rk, err := extractTableFromRedisKey(key)
	if err != nil {
		return 0, err
	}

	if min == nil {
		min = zEncodeStartSetKey(table, rk)
	} else {
		min = zEncodeSetKey(table, rk, min)
	}
	if max == nil {
		max = zEncodeStopSetKey(table, rk)
	} else {
		max = zEncodeSetKey(table, rk, max)
	}

	it, err := NewDBRangeIterator(db.eng, min, max, rangeType, false)
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
	sk := zEncodeSizeKey(key)
	v, err := db.eng.GetBytes(db.defaultReadOpts, sk)
	if v != nil && err == nil {
		return 1, nil
	}
	return 0, err
}

func (db *RockDB) ZExpire(key []byte, duration int64) (int64, error) {
	if exists, err := db.ZKeyExists(key); err != nil || exists != 1 {
		return 0, err
	} else {
		if err2 := db.expire(ZSetType, key, duration); err2 != nil {
			return 0, err2
		} else {
			return 1, nil
		}
	}
}

func (db *RockDB) ZPersist(key []byte) (int64, error) {
	if exists, err := db.ZKeyExists(key); err != nil || exists != 1 {
		return 0, err
	}

	if ttl, err := db.ttl(ZSetType, key); err != nil || ttl < 0 {
		return 0, err
	}

	db.wb.Clear()
	if err := db.delExpire(ZSetType, key, db.wb); err != nil {
		return 0, err
	} else {
		if err2 := db.eng.Write(db.defaultWriteOpts, db.wb); err2 != nil {
			return 0, err2
		} else {
			return 1, nil
		}
	}
}
