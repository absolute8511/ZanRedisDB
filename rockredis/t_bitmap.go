package rockredis

import (
	"encoding/binary"
	"errors"
	"fmt"
	math "math"
	"time"

	"github.com/youzan/ZanRedisDB/common"
	"github.com/youzan/ZanRedisDB/engine"
	"github.com/youzan/gorocksdb"
)

const (
	bitmapSegBits  = 1024 * 8
	bitmapSegBytes = 1024
	MaxBitOffsetV2 = math.MaxUint32 - 1
)

var (
	errBitmapKey     = errors.New("invalid bitmap key")
	errBitmapMetaKey = errors.New("invalid bitmap meta key")
	errBitmapSize    = errors.New("invalid bitmap size")
)

func convertRedisKeyToDBBitmapKey(key []byte, index int64) ([]byte, error) {
	table, rk, err := extractTableFromRedisKey(key)
	if err != nil {
		return nil, err
	}

	if err := checkKeySize(rk); err != nil {
		return nil, err
	}
	return encodeBitmapKey(table, key[len(table)+1:], index)
}

func bitEncodeMetaKey(key []byte) []byte {
	buf := make([]byte, len(key)+1+len(metaPrefix))
	pos := 0
	buf[pos] = BitmapMetaType

	pos++
	copy(buf[pos:], metaPrefix)
	pos += len(metaPrefix)
	copy(buf[pos:], key)
	return buf
}

func bitDecodeMetaKey(ek []byte) ([]byte, error) {
	pos := 0
	if pos+1+len(metaPrefix) > len(ek) || ek[pos] != BitmapMetaType {
		return nil, errBitmapMetaKey
	}
	pos++
	pos += len(metaPrefix)
	return ek[pos:], nil
}

func encodeBitmapKey(table []byte, key []byte, index int64) ([]byte, error) {
	buf := make([]byte, getDataTablePrefixBufLen(BitmapType, table))
	pos := encodeDataTablePrefixToBuf(buf, BitmapType, table)
	var err error
	buf, err = EncodeMemCmpKey(buf[:pos], key, colStartSep, index)
	return buf, err
}

func decodeBitmapKey(ek []byte) ([]byte, []byte, int64, error) {
	table, pos, err := decodeDataTablePrefixFromBuf(ek, BitmapType)
	if err != nil {
		return nil, nil, 0, err
	}

	rets, err := Decode(ek[pos:], 3)
	if err != nil {
		return nil, nil, 0, err
	}
	rk, _ := rets[0].([]byte)
	index, _ := rets[2].(int64)
	return table, rk, index, nil
}

func encodeBitmapStartKey(table []byte, key []byte, index int64) ([]byte, error) {
	return encodeBitmapKey(table, key, index)
}

func encodeBitmapStopKey(table []byte, key []byte) ([]byte, error) {
	buf := make([]byte, getDataTablePrefixBufLen(BitmapType, table))
	pos := encodeDataTablePrefixToBuf(buf, BitmapType, table)
	var err error
	buf, err = EncodeMemCmpKey(buf[:pos], key, colStartSep+1, 0)
	return buf, err
}

func (db *RockDB) bitSetToNew(ts int64, wb *gorocksdb.WriteBatch, bmSize int64, key []byte, offset int64, on int) (int64, error) {
	keyInfo, err := db.prepareCollKeyForWrite(ts, BitmapType, key, nil)
	if err != nil {
		return 0, err
	}
	oldh := keyInfo.OldHeader
	table := keyInfo.Table
	rk := keyInfo.VerKey

	index := (offset / bitmapSegBits) * bitmapSegBytes
	bmk, err := encodeBitmapKey(table, rk, index)
	if err != nil {
		return 0, err
	}
	bmv, err := db.eng.GetBytesNoLock(db.defaultReadOpts, bmk)
	if err != nil {
		return 0, err
	}
	byteOffset := int((offset / 8) % bitmapSegBytes)
	if byteOffset >= len(bmv) {
		expandSize := len(bmv)
		if byteOffset >= 2*len(bmv) {
			expandSize = byteOffset - len(bmv) + 1
		}
		bmv = append(bmv, make([]byte, expandSize)...)
		if int64(len(bmv))+index > bmSize {
			bmSize = int64(len(bmv)) + index
		}
	}
	bit := 7 - uint8(uint32(offset)&0x7)
	byteVal := bmv[byteOffset]
	oldBit := byteVal & (1 << bit)
	byteVal &= ^(1 << bit)
	byteVal |= (uint8(on&0x1) << bit)
	bmv[byteOffset] = byteVal
	wb.Put(bmk, bmv)
	db.updateBitmapMeta(ts, wb, oldh, key, bmSize)
	var ret int64
	if oldBit > 0 {
		ret = 1
	}
	return ret, err
}

// BitSetV2 set the bitmap data with new format as below:
// key:0 -> 0(first bit) 0 0 0 0 0 0 0 (last bit) | (second byte with 8 bits) | .... | (last byte with 8bits) at most bitmapSegBytes bytes for each segment
// key:1024 -> same as key:0
// key:2048 -> same as key:0
// ...
// key:512KB ->
// ...
// key:512MB ->
func (db *RockDB) BitSetV2(ts int64, key []byte, offset int64, on int) (int64, error) {
	if (on & ^1) != 0 {
		return 0, fmt.Errorf("bit should be 0 or 1, got %d", on)
	}
	if offset > MaxBitOffsetV2 || offset < 0 {
		return 0, ErrBitOverflow
	}
	if err := checkKeySize(key); err != nil {
		return 0, err
	}

	wb := db.wb
	// if new v2 is not exist, merge the old data to the new v2 first
	// if new v2 already exist, it means the old data has been merged before, we can ignore old
	// for old data, we can just split them to the 1KB segments
	_, bmSize, _, ok, err := db.getBitmapMeta(ts, key, false)
	if err != nil {
		return 0, err
	}
	if !ok {
		// convert old data to new
		table, oldkey, err := convertRedisKeyToDBKVKey(key)
		if err != nil {
			return 0, err
		}
		var v []byte
		if v, err = db.eng.GetBytesNoLock(db.defaultReadOpts, oldkey); err != nil {
			return 0, err
		}
		if v == nil {
			db.IncrTableKeyCount(table, 1, wb)
		} else if len(v) >= tsLen {
			v = v[:len(v)-tsLen]
			table, rk, _ := extractTableFromRedisKey(key)
			for i := 0; i < len(v); i += bitmapSegBytes {
				index := int64(i)
				bmk, err := encodeBitmapKey(table, rk, index)
				if err != nil {
					return 0, err
				}
				var segv []byte
				if len(v) <= i+bitmapSegBytes {
					segv = v[i:]
				} else {
					segv = v[i : i+bitmapSegBytes]
				}
				bmSize += int64(len(segv))
				wb.Put(bmk, segv)
			}
			if int64(len(v)) != bmSize {
				panic(fmt.Errorf("bitmap size mismatch: %v, %v ", v, bmSize))
			}
			wb.Delete(oldkey)
			// we need flush write batch before we modify new bit
			err = db.MaybeCommitBatch()
			if err != nil {
				return 0, err
			}
		}
	}
	oldBit, err := db.bitSetToNew(ts, wb, bmSize, key, offset, on)
	if err != nil {
		return 0, err
	}
	err = db.MaybeCommitBatch()
	return oldBit, err
}

func (db *RockDB) updateBitmapMeta(ts int64, wb *gorocksdb.WriteBatch, oldh *headerMetaValue, key []byte, bmSize int64) error {
	metaKey := bitEncodeMetaKey(key)
	buf := make([]byte, 16)
	binary.BigEndian.PutUint64(buf[:8], uint64(bmSize))
	binary.BigEndian.PutUint64(buf[8:], uint64(ts))
	oldh.UserData = buf
	nv := oldh.encodeWithData()
	wb.Put(metaKey, nv)
	return nil
}

func (db *RockDB) getBitmapMeta(tn int64, key []byte, lock bool) (*headerMetaValue, int64, int64, bool, error) {
	oldh, expired, err := db.collHeaderMeta(tn, BitmapType, key, lock)
	if err != nil {
		return oldh, 0, 0, false, err
	}
	meta := oldh.UserData
	if len(meta) == 0 {
		return oldh, 0, 0, false, nil
	}
	if len(meta) < 16 {
		return oldh, 0, 0, false, errors.New("invalid bitmap meta value")
	}
	s, err := Int64(meta[:8], nil)
	if err != nil {
		return oldh, s, 0, !expired, err
	}
	timestamp, err := Int64(meta[8:16], err)
	return oldh, s, timestamp, !expired, err
}

func (db *RockDB) BitGetVer(key []byte) (int64, error) {
	_, _, ts, ok, err := db.getBitmapMeta(time.Now().UnixNano(), key, true)
	if err != nil {
		return 0, err
	}
	if !ok && ts == 0 {
		return db.KVGetVer(key)
	}
	return ts, err
}

func (db *RockDB) BitGetV2(key []byte, offset int64) (int64, error) {
	// read new v2 first, if not exist, try old version
	tn := time.Now().UnixNano()
	oldh, _, _, ok, err := db.getBitmapMeta(tn, key, true)
	if err != nil {
		return 0, err
	}
	if !ok {
		return db.bitGetOld(key, offset)
	}
	table, rk, err := extractTableFromRedisKey(key)
	if err != nil {
		return 0, err
	}
	rk = db.expiration.encodeToVersionKey(BitmapType, oldh, rk)
	index := (offset / bitmapSegBits) * bitmapSegBytes
	bitk, err := encodeBitmapKey(table, rk, index)
	if err != nil {
		return 0, err
	}
	v, err := db.eng.GetBytes(db.defaultReadOpts, bitk)
	if err != nil {
		return 0, err
	}
	if v == nil {
		return 0, nil
	}
	byteOffset := uint32(offset/8) % bitmapSegBytes
	if byteOffset >= uint32(len(v)) {
		return 0, nil
	}
	byteVal := v[byteOffset]
	bit := 7 - uint8(uint32(offset)&0x7)
	if (byteVal & (1 << bit)) > 0 {
		return 1, nil
	}
	return 0, nil
}

func (db *RockDB) BitCountV2(key []byte, start, end int) (int64, error) {
	// read new v2 first, if not exist, try old version
	tn := time.Now().UnixNano()
	oldh, bmSize, _, ok, err := db.getBitmapMeta(tn, key, true)
	if err != nil {
		return 0, err
	}
	if !ok {
		return db.bitCountOld(key, start, end)
	}

	start, end = getRange(start, end, int(bmSize))
	if start > end {
		return 0, nil
	}
	table, rk, err := extractTableFromRedisKey(key)
	if err != nil {
		return 0, err
	}
	rk = db.expiration.encodeToVersionKey(BitmapType, oldh, rk)

	total := int64(0)
	startI := start / bitmapSegBytes
	stopI := end / bitmapSegBytes

	iterStart, _ := encodeBitmapStartKey(table, rk, int64(startI)*bitmapSegBytes)
	iterStop, _ := encodeBitmapStopKey(table, rk)
	it, err := engine.NewDBRangeIterator(db.eng, iterStart, iterStop, common.RangeROpen, false)
	if err != nil {
		return 0, err
	}
	defer it.Close()
	for ; it.Valid(); it.Next() {
		rawk := it.RefKey()
		_, _, index, err := decodeBitmapKey(rawk)
		if err != nil {
			return 0, err
		}
		bmv := it.RefValue()
		if bmv == nil {
			continue
		}
		byteStart := 0
		byteEnd := len(bmv)
		if index == int64(startI)*bitmapSegBytes {
			byteStart = start % bitmapSegBytes
		}
		if index == int64(stopI)*bitmapSegBytes {
			byteEnd = end%bitmapSegBytes + 1
			if byteEnd > len(bmv) {
				byteEnd = len(bmv)
			}
		}
		total += popcountBytes(bmv[byteStart:byteEnd])
	}
	return total, nil
}

func (db *RockDB) BitClear(key []byte) (int64, error) {
	oldh, _, err := db.collHeaderMeta(0, BitmapType, key, false)
	if err != nil {
		return 0, err
	}
	meta := oldh.UserData
	bmSize := int64(0)
	if len(meta) >= 8 {
		bmSize, _ = Int64(meta[:8], nil)
	}
	wb := db.wb
	table, rk, err := extractTableFromRedisKey(key)
	if err != nil {
		return 0, err
	}
	rk = db.expiration.encodeToVersionKey(BitmapType, oldh, rk)
	iterStart, _ := encodeBitmapStartKey(table, rk, 0)
	iterStop, _ := encodeBitmapStopKey(table, rk)
	if bmSize/bitmapSegBytes > RangeDeleteNum {
		wb.DeleteRange(iterStart, iterStop)
	} else {
		it, err := engine.NewDBRangeIterator(db.eng, iterStart, iterStop, common.RangeROpen, false)
		if err != nil {
			return 0, err
		}
		defer it.Close()
		for ; it.Valid(); it.Next() {
			rawk := it.RefKey()
			wb.Delete(rawk)
		}
	}

	metaKey := bitEncodeMetaKey(key)
	db.delExpire(BitmapType, key, nil, false, wb)
	wb.Delete(metaKey)
	err = db.MaybeCommitBatch()
	return 1, err
}

func (db *RockDB) BitKeyExist(key []byte) (int64, error) {
	n, err := db.collKeyExists(BitmapType, key)
	if err != nil {
		return 0, err
	}
	if n == 0 {
		return db.KVExists(key)
	}
	return 1, nil
}

func (db *RockDB) BitExpire(ts int64, key []byte, ttlSec int64) (int64, error) {
	return db.collExpire(ts, BitmapType, key, ttlSec)
}

func (db *RockDB) BitPersist(ts int64, key []byte) (int64, error) {
	return db.collPersist(ts, BitmapType, key)
}
