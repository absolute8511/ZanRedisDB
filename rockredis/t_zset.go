package rockredis

import (
	"bytes"
	"encoding/binary"
	"errors"
	"github.com/absolute8511/gorocksdb"
	"log"
)

const (
	MinScore     int64 = -1<<63 + 1
	MaxScore     int64 = 1<<63 - 1
	InvalidScore int64 = -1 << 63

	AggregateSum byte = 0
	AggregateMin byte = 1
	AggregateMax byte = 2
)

type ScorePair struct {
	Score  int64
	Member []byte
}

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
	zsetStopMemSep  byte = zsetStartMemSep + 1
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
	buf := make([]byte, len(key)+1)
	pos := 0
	buf[pos] = ZSizeType
	pos++
	copy(buf[pos:], key)
	return buf
}

func zDecodeSizeKey(ek []byte) ([]byte, error) {
	pos := 0
	if pos+1 > len(ek) || ek[pos] != ZSizeType {
		return nil, errZSizeKey
	}
	pos++
	return ek[pos:], nil
}

func zEncodeSetKey(key []byte, member []byte) []byte {
	buf := make([]byte, len(key)+len(member)+4)
	pos := 0
	buf[pos] = ZSetType
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

func zDecodeSetKey(ek []byte) ([]byte, []byte, error) {
	pos := 0
	if pos+1 > len(ek) || ek[pos] != ZSetType {
		return nil, nil, errZSetKey
	}

	pos++
	if pos+2 > len(ek) {
		return nil, nil, errZSetKey
	}

	keyLen := int(binary.BigEndian.Uint16(ek[pos:]))
	if keyLen+pos > len(ek) {
		return nil, nil, errZSetKey
	}

	pos += 2
	key := ek[pos : pos+keyLen]

	if ek[pos+keyLen] != zsetStartMemSep {
		return nil, nil, errZSetKey
	}
	pos++

	member := ek[pos+keyLen:]
	return key, member, nil
}

func zEncodeStartSetKey(key []byte) []byte {
	k := zEncodeSetKey(key, nil)
	return k
}

func zEncodeStopSetKey(key []byte) []byte {
	k := zEncodeSetKey(key, nil)
	k[len(k)-1] = zsetStartMemSep + 1
	return k
}

func zEncodeScoreKey(key []byte, member []byte, score int64) []byte {
	buf := make([]byte, len(key)+len(member)+13)
	pos := 0

	buf[pos] = ZScoreType
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

func zEncodeStartScoreKey(key []byte, score int64) []byte {
	return zEncodeScoreKey(key, nil, score)
}

func zEncodeStopScoreKey(key []byte, score int64) []byte {
	k := zEncodeScoreKey(key, nil, score)
	k[len(k)-1] = zsetStopMemSep
	return k
}

func zDecodeScoreKey(ek []byte) (key []byte, member []byte, score int64, err error) {
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

	var exists int64 = 0
	ek := zEncodeSetKey(key, member)

	if v, err := db.eng.GetBytes(db.defaultReadOpts, ek); err != nil {
		return 0, err
	} else if v != nil {
		exists = 1
		if s, err := Int64(v, err); err != nil {
			return 0, err
		} else {
			sk := zEncodeScoreKey(key, member, s)
			wb.Delete(sk)
		}
	}

	wb.Put(ek, PutInt64(score))

	sk := zEncodeScoreKey(key, member, score)
	wb.Put(sk, []byte{})
	return exists, nil
}

func (db *RockDB) zDelItem(key []byte, member []byte,
	skipDelScore bool, wb *gorocksdb.WriteBatch) (int64, error) {
	ek := zEncodeSetKey(key, member)
	if v, err := db.eng.GetBytes(db.defaultReadOpts, ek); err != nil {
		return 0, err
	} else if v == nil {
		//not exists
		return 0, nil
	} else {
		//exists
		if !skipDelScore {
			//we must del score
			if s, err := Int64(v, err); err != nil {
				return 0, err
			} else {
				sk := zEncodeScoreKey(key, member, s)
				wb.Delete(sk)
			}
		}
	}
	wb.Delete(ek)
	return 1, nil
}

func (db *RockDB) zDelete(key []byte, wb *gorocksdb.WriteBatch) (int64, error) {
	delMembCnt, err := db.zRemRange(key, MinScore, MaxScore, 0, -1, wb)
	//	todo : log err
	return delMembCnt, err
}

func (db *RockDB) ZAdd(key []byte, args ...ScorePair) (int64, error) {
	if len(args) == 0 {
		return 0, nil
	}

	wb := gorocksdb.NewWriteBatch()

	var num int64 = 0
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

	if _, err := db.zIncrSize(key, num, wb); err != nil {
		return 0, err
	}

	err := db.eng.Write(db.defaultWriteOpts, wb)
	return num, err
}

func (db *RockDB) zIncrSize(key []byte, delta int64, wb *gorocksdb.WriteBatch) (int64, error) {
	sk := zEncodeSizeKey(key)

	size, err := Int64(db.eng.GetBytes(db.defaultReadOpts, sk))
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
	if err := checkZSetKMSize(key, member); err != nil {
		return InvalidScore, err
	}

	var score int64 = InvalidScore

	k := zEncodeSetKey(key, member)
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

	wb := gorocksdb.NewWriteBatch()
	defer wb.Destroy()

	var num int64 = 0
	for i := 0; i < len(members); i++ {
		if err := checkZSetKMSize(key, members[i]); err != nil {
			return 0, err
		}
		if n, err := db.zDelItem(key, members[i], false, wb); err != nil {
			return 0, err
		} else if n == 1 {
			num++
		}
	}

	if _, err := db.zIncrSize(key, -num, wb); err != nil {
		return 0, err
	}

	err := db.eng.Write(db.defaultWriteOpts, wb)
	return num, err
}

func (db *RockDB) ZIncrBy(key []byte, delta int64, member []byte) (int64, error) {
	if err := checkZSetKMSize(key, member); err != nil {
		return InvalidScore, err
	}

	wb := gorocksdb.NewWriteBatch()
	defer wb.Destroy()

	ek := zEncodeSetKey(key, member)

	var oldScore int64 = 0
	v, err := db.eng.GetBytes(db.defaultReadOpts, ek)
	if err != nil {
		return InvalidScore, err
	} else if v == nil {
		db.zIncrSize(key, 1, wb)
	} else {
		if oldScore, err = Int64(v, err); err != nil {
			return InvalidScore, err
		}
	}

	newScore := oldScore + delta
	if newScore >= MaxScore || newScore <= MinScore {
		return InvalidScore, errScoreOverflow
	}

	sk := zEncodeScoreKey(key, member, newScore)
	wb.Put(sk, []byte{})
	wb.Put(ek, PutInt64(newScore))

	if v != nil {
		// so as to update score, we must delete the old one
		oldSk := zEncodeScoreKey(key, member, oldScore)
		wb.Delete(oldSk)
	}

	err = db.eng.Write(db.defaultWriteOpts, wb)
	return newScore, err
}

func (db *RockDB) ZCount(key []byte, min int64, max int64) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}
	minKey := zEncodeStartScoreKey(key, min)
	maxKey := zEncodeStopScoreKey(key, max)

	snap := gorocksdb.NewSnapshot(db.eng)
	readOpts := gorocksdb.NewDefaultReadOptions()
	readOpts.SetFillCache(false)
	readOpts.SetSnapshot(snap)
	it := db.eng.NewIterator(readOpts)
	it.Seek(minKey)
	var n int64 = 0
	for ; it.Valid(); it.Next() {
		if bytes.Compare(it.Key().Data(), maxKey) >= 0 {
			break
		}
		n++
	}

	it.Close()
	readOpts.Destroy()
	snap.Release()
	return n, nil
}

func (db *RockDB) zrank(key []byte, member []byte, reverse bool) (int64, error) {
	if err := checkZSetKMSize(key, member); err != nil {
		return 0, err
	}

	k := zEncodeSetKey(key, member)

	v, _ := db.eng.GetBytes(db.defaultReadOpts, k)
	if v == nil {
		log.Printf("not found zset member: %v\n", member)
		return -1, nil
	} else {
		if s, err := Int64(v, nil); err != nil {
			return 0, err
		} else {
			sk := zEncodeScoreKey(key, member, s)
			seekEndKey := sk
			snap := db.eng.NewSnapshot()
			readOpts := gorocksdb.NewDefaultReadOptions()
			readOpts.SetFillCache(false)
			readOpts.SetSnapshot(snap)
			defer readOpts.Destroy()
			it := db.eng.NewIterator(readOpts)
			defer it.Close()
			if !reverse {
				minKey := zEncodeStartScoreKey(key, MinScore)
				it.Seek(minKey)
			} else {
				maxKey := zEncodeStopScoreKey(key, MaxScore)
				//it.SeekPrev(maxKey)
				it.Seek(maxKey)
				if !it.Valid() {
					it.SeekToLast()
				} else if !bytes.Equal(it.Key().Data(), maxKey) {
					it.Prev()
				}
			}

			var lastKey []byte = nil
			var n int64 = 0

			for it.Valid() {
				rawk := it.Key().Data()
				if !reverse {
					if bytes.Compare(rawk, seekEndKey) > 0 {
						break
					}
				} else {
					if bytes.Compare(rawk, seekEndKey) < 0 {
						break
					}
				}
				n++
				lastKey = lastKey[0:0]
				lastKey = append(lastKey, rawk...)
				if !reverse {
					it.Next()
				} else {
					it.Prev()
				}
			}

			if _, m, _, err := zDecodeScoreKey(lastKey); err == nil && bytes.Equal(m, member) {
				n--
				return n, nil
			} else {
				log.Printf("last key decode error: %v, %v, %v\n", lastKey, m, member)
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

	minKey := zEncodeStartScoreKey(key, min)
	maxKey := zEncodeStopScoreKey(key, max)
	it := db.eng.NewIterator(db.defaultReadOpts)
	it.Seek(minKey)
	for i := 0; i < offset; i++ {
		if it.Valid() {
			it.Next()
		}
	}
	var num int64 = 0
	for ; it.Valid(); it.Next() {
		sk := it.Key().Data()
		if bytes.Compare(sk, maxKey) >= 0 {
			break
		}
		_, m, _, err := zDecodeScoreKey(sk)
		if err != nil {
			continue
		}

		if n, err := db.zDelItem(key, m, true, wb); err != nil {
			return 0, err
		} else if n == 1 {
			num++
		}
		wb.Delete(sk)
		if count >= 0 && num >= int64(count) {
			break
		}
	}
	it.Close()

	if _, err := db.zIncrSize(key, -num, wb); err != nil {
		return 0, err
	}

	return num, nil
}

func (db *RockDB) zRange(key []byte, min int64, max int64, offset int, count int, reverse bool) ([]ScorePair, error) {
	if len(key) > MaxKeySize {
		return nil, errKeySize
	}

	if offset < 0 {
		return []ScorePair{}, nil
	}

	nv := count
	// count may be very large, so we must limit it for below mem make.
	if nv <= 0 || nv > 1024 {
		nv = 64
	}

	v := make([]ScorePair, 0, nv)

	minKey := zEncodeStartScoreKey(key, min)
	maxKey := zEncodeStopScoreKey(key, max)
	//if reverse and offset is 0, count < 0, we may use forward iterator then reverse
	//because store iterator prev is slower than next
	if !reverse || (offset == 0 && count < 0) {
		reverse = false
	} else {
		reverse = true
	}

	seekEnd := maxKey
	it := db.eng.NewIterator(db.defaultReadOpts)
	if !reverse {
		it.Seek(minKey)
		for i := 0; i < offset; i++ {
			if it.Valid() {
				it.Next()
			}
		}
	} else {
		it.Seek(maxKey)
		if it.Valid() {
			// stop key not considered, we ignore one
			it.Prev()
		}
		for i := 0; i < offset; i++ {
			if it.Valid() {
				it.Prev()
			}
		}
		seekEnd = minKey
	}

	for it.Valid() {
		rawk := it.Key().Bytes()
		if !reverse {
			if bytes.Compare(rawk, seekEnd) >= 0 {
				break
			}
		} else {
			if bytes.Compare(rawk, seekEnd) < 0 {
				break
			}
		}
		_, m, s, err := zDecodeScoreKey(rawk)
		//may be we will check key equal?
		if err != nil {
			continue
		}

		v = append(v, ScorePair{Member: m, Score: s})
		if count >= 0 && len(v) >= count {
			break
		}
		if !reverse {
			it.Next()
		} else {
			it.Prev()
		}
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
	wb := gorocksdb.NewWriteBatch()
	defer wb.Destroy()
	rmCnt, err := db.zRemRange(key, MinScore, MaxScore, 0, -1, wb)
	if err == nil {
		err = db.eng.Write(db.defaultWriteOpts, wb)
	}
	return rmCnt, err
}

func (db *RockDB) ZMclear(keys ...[]byte) (int64, error) {
	if len(keys) > MAX_BATCH_NUM {
		return 0, errTooMuchBatchSize
	}
	wb := gorocksdb.NewWriteBatch()
	defer wb.Destroy()
	for _, key := range keys {
		if _, err := db.zRemRange(key, MinScore, MaxScore, 0, -1, wb); err != nil {
			return 0, err
		}
	}

	err := db.eng.Write(db.defaultWriteOpts, wb)
	return int64(len(keys)), err
}

func (db *RockDB) ZRange(key []byte, start int, stop int) ([]ScorePair, error) {
	return db.ZRangeGeneric(key, start, stop, false)
}

//min and max must be inclusive
//if no limit, set offset = 0 and count = -1
func (db *RockDB) ZRangeByScore(key []byte, min int64, max int64,
	offset int, count int) ([]ScorePair, error) {
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
	wb := gorocksdb.NewWriteBatch()
	defer wb.Destroy()

	rmCnt, err = db.zRemRange(key, MinScore, MaxScore, offset, count, wb)
	if err == nil {
		err = db.eng.Write(db.defaultWriteOpts, wb)
	}
	return rmCnt, err
}

//min and max must be inclusive
func (db *RockDB) ZRemRangeByScore(key []byte, min int64, max int64) (int64, error) {
	wb := gorocksdb.NewWriteBatch()
	defer wb.Destroy()

	rmCnt, err := db.zRemRange(key, min, max, 0, -1, wb)
	if err == nil {
		err = db.eng.Write(db.defaultWriteOpts, wb)
	}

	return rmCnt, err
}

func (db *RockDB) ZRevRange(key []byte, start int, stop int) ([]ScorePair, error) {
	return db.ZRangeGeneric(key, start, stop, true)
}

func (db *RockDB) ZRevRank(key []byte, member []byte) (int64, error) {
	return db.zrank(key, member, true)
}

//min and max must be inclusive
//if no limit, set offset = 0 and count = -1
func (db *RockDB) ZRevRangeByScore(key []byte, min int64, max int64, offset int, count int) ([]ScorePair, error) {
	return db.ZRangeByScoreGeneric(key, min, max, offset, count, true)
}

func (db *RockDB) ZRangeGeneric(key []byte, start int, stop int, reverse bool) ([]ScorePair, error) {
	offset, count, err := db.zParseLimit(key, start, stop)
	if err != nil {
		return nil, err
	}

	return db.zRange(key, MinScore, MaxScore, offset, count, reverse)
}

//min and max must be inclusive
//if no limit, set offset = 0 and count = -1
func (db *RockDB) ZRangeByScoreGeneric(key []byte, min int64, max int64,
	offset int, count int, reverse bool) ([]ScorePair, error) {

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
	if min == nil {
		min = zEncodeStartSetKey(key)
	} else {
		min = zEncodeSetKey(key, min)
	}
	if max == nil {
		max = zEncodeStopSetKey(key)
	} else {
		max = zEncodeSetKey(key, max)
	}
	// TODO: rangeType means the close range or open range for start/end

	it := db.eng.NewIterator(db.defaultReadOpts)
	defer it.Close()
	it.Seek(min)
	// if open range skip to next
	for i := 0; i < offset; i++ {
		if it.Valid() {
			it.Next()
		}
	}

	ay := make([][]byte, 0, 16)
	for ; it.Valid(); it.Next() {
		rawk := it.Key().Bytes()
		// TODO: check open range or close range
		if rangeType == RangeClose {
			if bytes.Compare(rawk, max) > 0 {
				break
			}
		} else {
			if bytes.Compare(rawk, max) >= 0 {
				break
			}
		}
		if _, m, err := zDecodeSetKey(rawk); err == nil {
			ay = append(ay, m)
		}
		if count >= 0 && len(ay) >= count {
			break
		}

	}
	return ay, nil
}

func (db *RockDB) ZRemRangeByLex(key []byte, min []byte, max []byte, rangeType uint8) (int64, error) {
	if min == nil {
		min = zEncodeStartSetKey(key)
	} else {
		min = zEncodeSetKey(key, min)
	}
	if max == nil {
		max = zEncodeStopSetKey(key)
	} else {
		max = zEncodeSetKey(key, max)
	}

	wb := gorocksdb.NewWriteBatch()
	defer wb.Destroy()
	it := db.eng.NewIterator(db.defaultReadOpts)
	defer it.Close()
	it.Seek(min)
	var n int64 = 0
	for ; it.Valid(); it.Next() {
		rawk := it.Key().Data()
		// TODO: check open range or close range
		if bytes.Compare(rawk, max) >= 0 {
			break
		}
		wb.Delete(rawk)
		n++
	}

	if err := db.eng.Write(db.defaultWriteOpts, wb); err != nil {
		return 0, err
	}

	return n, nil
}

func (db *RockDB) ZLexCount(key []byte, min []byte, max []byte, rangeType uint8) (int64, error) {
	if min == nil {
		min = zEncodeStartSetKey(key)
	} else {
		min = zEncodeSetKey(key, min)
	}
	if max == nil {
		max = zEncodeStopSetKey(key)
	} else {
		max = zEncodeSetKey(key, max)
	}

	it := db.eng.NewIterator(db.defaultReadOpts)
	defer it.Close()
	it.Seek(min)
	var n int64 = 0
	for ; it.Valid(); it.Next() {
		if bytes.Compare(it.Key().Data(), max) >= 0 {
			break
		}
		n++
	}
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
