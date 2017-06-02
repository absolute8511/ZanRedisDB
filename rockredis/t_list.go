package rockredis

import (
	"encoding/binary"
	"errors"

	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/absolute8511/gorocksdb"
)

// TODO: we can use ring buffer to allow the list pop and push many times
// when the tail reach the end we roll to the start and check if full.
// Note: to clean the huge list, we can set some meta for each list,
// such as max elements or the max keep time, while insert we auto clean
// the data old than the meta (by number or by keep time)
const (
	listHeadSeq int64 = 1
	listTailSeq int64 = 2

	listMinSeq     int64 = 1000
	listMaxSeq     int64 = 1<<62 - 1000
	listInitialSeq int64 = listMinSeq + (listMaxSeq-listMinSeq)/2
)

var errLMetaKey = errors.New("invalid lmeta key")
var errListKey = errors.New("invalid list key")
var errListSeq = errors.New("invalid list sequence, overflow")
var errListIndex = errors.New("invalid list index")

func lEncodeMetaKey(key []byte) []byte {
	buf := make([]byte, len(key)+1+len(metaPrefix))
	pos := 0
	buf[pos] = LMetaType
	pos++
	copy(buf[pos:], metaPrefix)
	pos += len(metaPrefix)

	copy(buf[pos:], key)
	return buf
}

func lDecodeMetaKey(ek []byte) ([]byte, error) {
	pos := 0
	if pos+1+len(metaPrefix) > len(ek) || ek[pos] != LMetaType {
		return nil, errLMetaKey
	}

	pos++
	pos += len(metaPrefix)
	return ek[pos:], nil
}

func lEncodeMinKey() []byte {
	return lEncodeMetaKey(nil)
}

func lEncodeMaxKey() []byte {
	ek := lEncodeMetaKey(nil)
	ek[len(ek)-1] = ek[len(ek)-1] + 1
	return ek
}

func lEncodeListKey(key []byte, seq int64) []byte {
	buf := make([]byte, len(key)+1+2+8)

	pos := 0
	buf[pos] = ListType
	pos++

	binary.BigEndian.PutUint16(buf[pos:], uint16(len(key)))
	pos += 2

	copy(buf[pos:], key)
	pos += len(key)

	binary.BigEndian.PutUint64(buf[pos:], uint64(seq))

	return buf
}

func lDecodeListKey(ek []byte) (key []byte, seq int64, err error) {
	pos := 0
	if pos+1 > len(ek) || ek[pos] != ListType {
		err = errListKey
		return
	}

	pos++

	if pos+2 > len(ek) {
		err = errListKey
		return
	}

	keyLen := int(binary.BigEndian.Uint16(ek[pos:]))
	pos += 2
	if keyLen+pos+8 != len(ek) {
		err = errListKey
		return
	}

	key = ek[pos : pos+keyLen]
	seq = int64(binary.BigEndian.Uint64(ek[pos+keyLen:]))
	return
}

func (db *RockDB) lpush(key []byte, whereSeq int64, args ...[]byte) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}

	table := extractTableFromRedisKey(key)
	if len(table) == 0 {
		return 0, errTableName
	}

	var headSeq int64
	var tailSeq int64
	var size int64
	var err error

	wb := db.wb
	wb.Clear()
	metaKey := lEncodeMetaKey(key)
	headSeq, tailSeq, size, err = db.lGetMeta(metaKey)
	if err != nil {
		return 0, err
	}

	var pushCnt int = len(args)
	if pushCnt == 0 {
		return int64(size), nil
	}

	var seq int64 = headSeq
	var delta int64 = -1
	if whereSeq == listTailSeq {
		seq = tailSeq
		delta = 1
	}

	//	append elements
	if size > 0 {
		seq += delta
	}

	checkSeq := seq + int64(pushCnt-1)*delta
	if checkSeq <= listMinSeq || checkSeq >= listMaxSeq {
		return 0, errListSeq
	}
	for i := 0; i < pushCnt; i++ {
		ek := lEncodeListKey(key, seq+int64(i)*delta)
		wb.Put(ek, args[i])
	}
	if size == 0 && pushCnt > 0 {
		db.IncrTableKeyCount(table, 1, wb)
	}
	seq += int64(pushCnt-1) * delta
	//	set meta info
	if whereSeq == listHeadSeq {
		headSeq = seq
	} else {
		tailSeq = seq
	}

	db.lSetMeta(metaKey, headSeq, tailSeq, wb)
	err = db.eng.Write(db.defaultWriteOpts, wb)
	return int64(size) + int64(pushCnt), err
}

func (db *RockDB) lpop(key []byte, whereSeq int64) ([]byte, error) {
	if err := checkKeySize(key); err != nil {
		return nil, err
	}
	table := extractTableFromRedisKey(key)
	if len(table) == 0 {
		return nil, errTableName
	}

	wb := db.wb
	wb.Clear()

	var headSeq int64
	var tailSeq int64
	var size int64
	var err error

	metaKey := lEncodeMetaKey(key)
	headSeq, tailSeq, size, err = db.lGetMeta(metaKey)
	if err != nil {
		return nil, err
	} else if size == 0 {
		return nil, nil
	}

	var value []byte

	var seq int64 = headSeq
	if whereSeq == listTailSeq {
		seq = tailSeq
	}

	itemKey := lEncodeListKey(key, seq)
	value, err = db.eng.GetBytes(db.defaultReadOpts, itemKey)
	if err != nil {
		return nil, err
	}

	if whereSeq == listHeadSeq {
		headSeq += 1
	} else {
		tailSeq -= 1
	}

	wb.Delete(itemKey)
	size, err = db.lSetMeta(metaKey, headSeq, tailSeq, wb)
	if err != nil {
		return nil, err
	}
	if size == 0 {
		// list is empty after delete
		db.IncrTableKeyCount(table, -1, wb)
		//delete the expire data related to the list key
		db.delExpire(ListType, key, wb)
	}
	err = db.eng.Write(db.defaultWriteOpts, wb)
	return value, err
}

func (db *RockDB) ltrim2(key []byte, startP, stopP int64) error {
	if err := checkKeySize(key); err != nil {
		return err
	}
	table := extractTableFromRedisKey(key)
	if len(table) == 0 {
		return errTableName
	}

	wb := db.wb
	wb.Clear()

	var headSeq int64
	var llen int64
	var err error
	start := int64(startP)
	stop := int64(stopP)

	ek := lEncodeMetaKey(key)
	if headSeq, _, llen, err = db.lGetMeta(ek); err != nil {
		return err
	} else {
		if start < 0 {
			start = llen + start
		}
		if stop < 0 {
			stop = llen + stop
		}
		if start >= llen || start > stop {
			//db.lDelete(key, wb)
			return errors.New("trim invalid")
		}

		if start < 0 {
			start = 0
		}
		if stop >= llen {
			stop = llen - 1
		}
	}

	if start > 0 {
		for i := int64(0); i < start; i++ {
			wb.Delete(lEncodeListKey(key, headSeq+i))
		}
	}
	if stop < int64(llen-1) {
		for i := int64(stop + 1); i < llen; i++ {
			wb.Delete(lEncodeListKey(key, headSeq+i))
		}
	}

	newLen, _ := db.lSetMeta(ek, headSeq+start, headSeq+stop, wb)
	if llen > 0 && newLen == 0 {
		db.IncrTableKeyCount(table, -1, wb)
		//delete the expire data related to the list key
		db.delExpire(ListType, key, wb)
	}

	return db.eng.Write(db.defaultWriteOpts, wb)
}

func (db *RockDB) ltrim(key []byte, trimSize, whereSeq int64) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}

	if trimSize == 0 {
		return 0, nil
	}
	table := extractTableFromRedisKey(key)
	if len(table) == 0 {
		return 0, errTableName
	}

	wb := db.wb
	wb.Clear()

	var headSeq int64
	var tailSeq int64
	var size int64
	var err error

	metaKey := lEncodeMetaKey(key)
	headSeq, tailSeq, size, err = db.lGetMeta(metaKey)
	if err != nil {
		return 0, err
	} else if size == 0 {
		return 0, nil
	}

	var (
		trimStartSeq int64
		trimEndSeq   int64
	)

	if whereSeq == listHeadSeq {
		trimStartSeq = headSeq
		trimEndSeq = MinInt64(trimStartSeq+trimSize-1, tailSeq)
		headSeq = trimEndSeq + 1
	} else {
		trimEndSeq = tailSeq
		trimStartSeq = MaxInt64(trimEndSeq-trimSize+1, headSeq)
		tailSeq = trimStartSeq - 1
	}

	for trimSeq := trimStartSeq; trimSeq <= trimEndSeq; trimSeq++ {
		itemKey := lEncodeListKey(key, trimSeq)
		wb.Delete(itemKey)
	}

	size, err = db.lSetMeta(metaKey, headSeq, tailSeq, wb)
	if err != nil {
		return 0, err
	}
	if size == 0 {
		// list is empty after trim
		db.IncrTableKeyCount(table, -1, wb)
		//delete the expire data related to the list key
		db.delExpire(ListType, key, wb)
	}

	err = db.eng.Write(db.defaultWriteOpts, wb)
	return trimEndSeq - trimStartSeq + 1, err
}

//	ps : here just focus on deleting the list data,
//		 any other likes expire is ignore.
func (db *RockDB) lDelete(key []byte, wb *gorocksdb.WriteBatch) int64 {
	table := extractTableFromRedisKey(key)
	if len(table) == 0 {
		return 0
	}

	mk := lEncodeMetaKey(key)

	var headSeq int64
	var tailSeq int64
	var size int64
	var err error

	headSeq, tailSeq, size, err = db.lGetMeta(mk)
	if err != nil {
		return 0
	}

	var num int64 = 0
	startKey := lEncodeListKey(key, headSeq)
	stopKey := lEncodeListKey(key, tailSeq)
	if size > RANGE_DELETE_NUM {
		var r gorocksdb.Range
		r.Start = startKey
		r.Limit = stopKey
		db.eng.DeleteFilesInRange(r)
		db.eng.CompactRange(r)
	}

	rit, err := NewDBRangeIterator(db.eng, startKey, stopKey, common.RangeClose, false)
	if err != nil {
		return 0
	}
	for ; rit.Valid(); rit.Next() {
		wb.Delete(rit.RefKey())
		num++
	}
	rit.Close()
	if size > 0 {
		db.IncrTableKeyCount(table, -1, wb)
	}

	wb.Delete(mk)
	return num
}

func (db *RockDB) lGetMeta(ek []byte) (headSeq int64, tailSeq int64, size int64, err error) {
	var v []byte
	v, err = db.eng.GetBytes(db.defaultReadOpts, ek)
	if err != nil {
		return
	} else if v == nil {
		headSeq = listInitialSeq
		tailSeq = listInitialSeq
		size = 0
		return
	} else {
		headSeq = int64(binary.BigEndian.Uint64(v[0:8]))
		tailSeq = int64(binary.BigEndian.Uint64(v[8:16]))
		size = tailSeq - headSeq + 1
	}
	return
}

func (db *RockDB) lSetMeta(ek []byte, headSeq int64, tailSeq int64, wb *gorocksdb.WriteBatch) (int64, error) {
	var size int64 = tailSeq - headSeq + 1
	if size < 0 {
		//	todo : log error + panic
		//log.Fatalf("invalid meta sequence range [%d, %d]", headSeq, tailSeq)
		return 0, errListSeq
	} else if size == 0 {
		wb.Delete(ek)
	} else {
		buf := make([]byte, 16)
		binary.BigEndian.PutUint64(buf[0:8], uint64(headSeq))
		binary.BigEndian.PutUint64(buf[8:16], uint64(tailSeq))
		wb.Put(ek, buf)
	}
	return size, nil
}

func (db *RockDB) LIndex(key []byte, index int64) ([]byte, error) {
	if err := checkKeySize(key); err != nil {
		return nil, err
	}

	var seq int64
	var headSeq int64
	var tailSeq int64
	var err error

	metaKey := lEncodeMetaKey(key)

	headSeq, tailSeq, _, err = db.lGetMeta(metaKey)
	if err != nil {
		return nil, err
	}

	if index >= 0 {
		seq = headSeq + index
	} else {
		seq = tailSeq + index + 1
	}

	sk := lEncodeListKey(key, seq)
	return db.eng.GetBytes(db.defaultReadOpts, sk)
}

func (db *RockDB) LLen(key []byte) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}

	ek := lEncodeMetaKey(key)
	_, _, size, err := db.lGetMeta(ek)
	return int64(size), err
}

func (db *RockDB) LPop(key []byte) ([]byte, error) {
	return db.lpop(key, listHeadSeq)
}

func (db *RockDB) LTrim(key []byte, start, stop int64) error {
	return db.ltrim2(key, start, stop)
}

func (db *RockDB) LTrimFront(key []byte, trimSize int64) (int64, error) {
	return db.ltrim(key, trimSize, listHeadSeq)
}

func (db *RockDB) LTrimBack(key []byte, trimSize int64) (int64, error) {
	return db.ltrim(key, trimSize, listTailSeq)
}

func (db *RockDB) LPush(key []byte, args ...[]byte) (int64, error) {
	if len(args) >= MAX_BATCH_NUM {
		return 0, errTooMuchBatchSize
	}
	return db.lpush(key, listHeadSeq, args...)
}
func (db *RockDB) LSet(key []byte, index int64, value []byte) error {
	if err := checkKeySize(key); err != nil {
		return err
	}

	var seq int64
	var headSeq int64
	var tailSeq int64
	var size int64
	var err error
	metaKey := lEncodeMetaKey(key)

	headSeq, tailSeq, size, err = db.lGetMeta(metaKey)
	if err != nil {
		return err
	}
	if size == 0 {
		return errListIndex
	}

	if index >= 0 {
		seq = headSeq + index
	} else {
		seq = tailSeq + index + 1
	}
	if seq < headSeq || seq > tailSeq {
		return errListIndex
	}
	sk := lEncodeListKey(key, seq)
	err = db.eng.Put(db.defaultWriteOpts, sk, value)
	return err
}

func (db *RockDB) LRange(key []byte, start int64, stop int64) ([][]byte, error) {
	if err := checkKeySize(key); err != nil {
		return nil, err
	}

	var headSeq int64
	var llen int64
	var err error

	metaKey := lEncodeMetaKey(key)

	if headSeq, _, llen, err = db.lGetMeta(metaKey); err != nil {
		return nil, err
	}

	if start < 0 {
		start = llen + start
	}
	if stop < 0 {
		stop = llen + stop
	}
	if start < 0 {
		start = 0
	}

	if start > stop || start >= llen {
		return [][]byte{}, nil
	}

	if stop >= llen {
		stop = llen - 1
	}

	limit := (stop - start) + 1
	if limit >= MAX_BATCH_NUM {
		return nil, errTooMuchBatchSize
	}
	headSeq += start

	v := make([][]byte, 0, limit)

	startKey := lEncodeListKey(key, headSeq)
	rit, err := NewDBRangeLimitIterator(db.eng, startKey, nil, common.RangeClose, 0, int(limit), false)
	if err != nil {
		return nil, err
	}
	for ; rit.Valid(); rit.Next() {
		v = append(v, rit.Value())
	}
	rit.Close()
	return v, nil
}

func (db *RockDB) RPop(key []byte) ([]byte, error) {
	return db.lpop(key, listTailSeq)
}

func (db *RockDB) RPush(key []byte, args ...[]byte) (int64, error) {
	if len(args) >= MAX_BATCH_NUM {
		return 0, errTooMuchBatchSize
	}
	return db.lpush(key, listTailSeq, args...)
}

func (db *RockDB) LClear(key []byte) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}
	db.wb.Clear()
	num := db.lDelete(key, db.wb)
	err := db.eng.Write(db.defaultWriteOpts, db.wb)
	if err != nil {
		// TODO: log here , the list maybe corrupt
	}

	if num > 0 {
		//delete the expire data related to the list key
		db.wb.Clear()
		db.delExpire(ListType, key, db.wb)
		db.eng.Write(db.defaultWriteOpts, db.wb)
	}
	return num, err
}

func (db *RockDB) LMclear(keys ...[]byte) (int64, error) {
	if len(keys) >= MAX_BATCH_NUM {
		return 0, errTooMuchBatchSize
	}

	db.wb.Clear()
	for _, key := range keys {
		if err := checkKeySize(key); err != nil {
			return 0, err
		}
		db.lDelete(key, db.wb)
	}
	err := db.eng.Write(db.defaultWriteOpts, db.wb)
	if err != nil {
		// TODO: log here , the list maybe corrupt
	}

	return int64(len(keys)), err
}

func (db *RockDB) LKeyExists(key []byte) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}
	sk := lEncodeMetaKey(key)
	v, err := db.eng.GetBytes(db.defaultReadOpts, sk)
	if v != nil && err == nil {
		return 1, nil
	}
	return 0, err
}

func (db *RockDB) LExpire(key []byte, duration int64) (int64, error) {
	if exists, err := db.LKeyExists(key); err != nil || exists != 1 {
		return 0, err
	} else {
		if err2 := db.expire(ListType, key, duration); err2 != nil {
			return 0, err2
		} else {
			return 1, nil
		}
	}
}

func (db *RockDB) LPersist(key []byte) (int64, error) {
	if exists, err := db.LKeyExists(key); err != nil || exists != 1 {
		return 0, err
	}

	if ttl, err := db.ttl(ListType, key); err != nil || ttl < 0 {
		return 0, err
	}

	db.wb.Clear()
	if err := db.delExpire(ListType, key, db.wb); err != nil {
		return 0, err
	} else {
		if err2 := db.eng.Write(db.defaultWriteOpts, db.wb); err2 != nil {
			return 0, err2
		} else {
			return 1, nil
		}
	}
}
