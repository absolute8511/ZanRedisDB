package rockredis

import (
	"encoding/binary"
	"errors"

	hll "github.com/absolute8511/hyperloglog"
	//hll "github.com/axiomhq/hyperloglog"
)

const (
	hllPrecision uint8 = 14
	// to make it compatible for some different hll lib
	// we add a flag to indicate which hll lib we are using
	hllPlusDefault uint8 = 1
)

// pfcount use uint64 to storage the precomputed value, and the most significant bit is used
// to indicate whether the cached value is valid.
// pfadd will modify the pfcount most significant bit to indicate the register is changed
// and pfcount should recompute instead using the cached value.
// since pfadd will be called much more than pfcount, so we compute the lazy while actually pfcount will improve pfadd performance.

// 14 bits precision yield to 16384 registers and standard error is 0.81% (1/sqrt(number of registers))
var errInvalidHLLData = errors.New("invalide hyperloglog data")
var errHLLCountOverflow = errors.New("hyperloglog count overflow")

func (db *RockDB) PFCount(ts int64, keys ...[]byte) (int64, error) {
	keyList := make([][]byte, len(keys))
	errs := make([]error, len(keys))
	var firstKey []byte
	for i, k := range keys {
		_, kk, err := convertRedisKeyToDBKVKey(k)
		if i == 0 {
			firstKey = kk
		}
		if err != nil {
			keyList[i] = nil
			errs[i] = err
		} else {
			keyList[i] = kk
		}
	}
	db.eng.MultiGetBytes(db.defaultReadOpts, keyList, keyList, errs)
	for i, v := range keyList {
		if errs[i] == nil && len(v) >= tsLen {
			keyList[i] = keyList[i][:len(v)-tsLen]
		}
	}
	if len(keyList) == 1 {
		fkv := keyList[0]
		if fkv == nil {
			return 0, nil
		}
		if len(fkv) == 0 {
			return 0, errInvalidHLLData
		}
		hllType := uint8(fkv[0])
		if len(fkv) < 8+1 {
			return 0, errInvalidHLLData
		}
		pos := 1
		cnt := binary.BigEndian.Uint64(fkv[pos : pos+8])
		if cnt&0x8000000000000000 == 0 {
			return int64(cnt), nil
		}
		switch hllType {
		case hllPlusDefault:
			// recompute
			hllp, _ := hll.NewPlus(hllPrecision)
			err := hllp.GobDecode(fkv[pos+8:])
			if err != nil {
				return 0, err
			}
			cnt = hllp.Count()
			if cnt&0x8000000000000000 != 0 {
				return 0, errHLLCountOverflow
			}
			binary.BigEndian.PutUint64(fkv[pos:pos+8], cnt)
			tsBuf := PutInt64(ts)
			fkv = append(fkv, tsBuf...)
			db.wb.Clear()
			db.wb.Put(firstKey, fkv)
			err = db.eng.Write(db.defaultWriteOpts, db.wb)
			return int64(cnt), err
		default:
			// unknown hll type
			return 0, errInvalidHLLData
		}
	} else {
		hllp, _ := hll.NewPlus(hllPrecision)
		// merge count
		for _, v := range keyList {
			if len(v) < 8+1 {
				return 0, errInvalidHLLData
			}
			hllType := uint8(v[0])
			if hllType != hllPlusDefault {
				return 0, errInvalidHLLData
			}
			pos := 1
			hllpv, _ := hll.NewPlus(hllPrecision)
			err := hllpv.GobDecode(v[pos+8:])
			if err != nil {
				return 0, err
			}
			err = hllp.Merge(hllpv)
			if err != nil {
				return 0, err
			}
		}
		return int64(hllp.Count()), nil
	}
}

func (db *RockDB) PFAdd(ts int64, rawKey []byte, elems ...[]byte) (int64, error) {
	if len(elems) > MAX_BATCH_NUM*10 {
		return 0, errTooMuchBatchSize
	}
	table, key, err := convertRedisKeyToDBKVKey(rawKey)

	db.wb.Clear()

	hllp, _ := hll.NewPlus(hllPrecision)
	oldV, _ := db.eng.GetBytesNoLock(db.defaultReadOpts, key)
	if oldV != nil {
		if len(oldV) < 8+1+tsLen {
			return 0, errInvalidHLLData
		}
		if len(oldV) >= tsLen {
			oldV = oldV[:len(oldV)-tsLen]
		}
		if uint8(oldV[0]) != hllPlusDefault {
			return 0, errInvalidHLLData
		}
		err = hllp.GobDecode(oldV[8+1:])
		if err != nil {
			return 0, err
		}
	}
	changed := false
	if oldV == nil {
		// first init always return changed
		changed = true
	}
	for _, elem := range elems {
		db.hasher64.Write(elem)
		if hllp.Add(db.hasher64) {
			changed = true
		}
		db.hasher64.Reset()
	}
	if !changed {
		return 0, nil
	}
	newV, err := hllp.GobEncode()
	if err != nil {
		return 0, err
	}
	if db.cfg.EnableTableCounter && oldV == nil {
		db.IncrTableKeyCount(table, 1, db.wb)
	}

	// modify pfcount bit
	var oldCnt uint64
	if oldV != nil {
		oldCnt = binary.BigEndian.Uint64(oldV[1 : 1+8])
		oldV = oldV[:1+8]
	} else {
		oldV = make([]byte, 8+1)
	}
	oldCnt = oldCnt | 0x8000000000000000
	binary.BigEndian.PutUint64(oldV[1:1+8], oldCnt)
	oldV[0] = hllPlusDefault
	oldV = append(oldV, newV...)
	tsBuf := PutInt64(ts)
	oldV = append(oldV, tsBuf...)
	db.wb.Put(key, oldV)
	err = db.eng.Write(db.defaultWriteOpts, db.wb)
	return 1, err
}
