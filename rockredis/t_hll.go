package rockredis

import (
	"encoding/binary"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/absolute8511/gorocksdb"
	hll "github.com/absolute8511/hyperloglog"
	//hll "github.com/axiomhq/hyperloglog"
	"github.com/hashicorp/golang-lru"
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

type hllCacheItem struct {
	sync.Mutex
	hllp        *hll.HyperLogLogPlus
	cachedCount uint64
	hllType     uint8
	ts          int64
	flushed     bool
}

type hllCache struct {
	lruCache *lru.Cache
	// to avoid pfcount modify the db, we separate the read cache.
	// so we can treat the pfcount as read command, since it only get from the write cache or
	// read from db and cache to read cache.
	readCache *lru.Cache
	db        *RockDB
}

func newHLLCache(size int, db *RockDB) (*hllCache, error) {
	c := &hllCache{
		db: db,
	}
	var err error
	c.readCache, err = lru.NewWithEvict(size, nil)
	if err != nil {
		return nil, err
	}
	c.lruCache, err = lru.NewWithEvict(size, c.onEvicted)
	return c, err
}

// must be called in raft commit loop
func (c *hllCache) Flush() {
	start := time.Now()
	keys := c.lruCache.Keys()
	for _, k := range keys {
		v, ok := c.lruCache.Peek(k)
		if ok {
			item, ok := v.(*hllCacheItem)
			if ok && !item.flushed {
				c.onEvicted(k, v)
			}
		}
	}
	if time.Since(start) > time.Second {
		dbLog.Infof("flush hll cache cost: %v", time.Since(start))
	}
}

// must be called in raft commit loop
func (c *hllCache) onEvicted(rawKey interface{}, value interface{}) {
	wb := gorocksdb.NewWriteBatch()
	defer wb.Destroy()
	item, ok := value.(*hllCacheItem)
	if !ok {
		return
	}
	cachedKey := []byte(rawKey.(string))
	table, key, err := convertRedisKeyToDBKVKey(cachedKey)
	oldV, _ := c.db.eng.GetBytesNoLock(c.db.defaultReadOpts, key)
	hllp := item.hllp
	newV, err := hllp.GobEncode()
	if err != nil {
		dbLog.Warningf("failed to encode %v hll: %v", key, err.Error())
		return
	}
	if c.db.cfg.EnableTableCounter && oldV == nil {
		c.db.IncrTableKeyCount(table, 1, wb)
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
	tsBuf := PutInt64(item.ts)
	oldV = append(oldV, tsBuf...)
	wb.Put(key, oldV)
	c.db.eng.Write(c.db.defaultWriteOpts, wb)
	item.flushed = true
}

func (c *hllCache) Get(key []byte) (*hllCacheItem, bool) {
	v, ok := c.lruCache.Get(string(key))
	if !ok {
		v, ok = c.readCache.Get(string(key))
		if !ok {
			return nil, false
		}
	}

	item, ok := v.(*hllCacheItem)
	if !ok {
		return nil, false
	}
	c.readCache.Remove(string(key))
	return item, true
}

func (c *hllCache) AddToReadCache(key []byte, item *hllCacheItem) {
	c.readCache.Add(string(key), item)
}

func (c *hllCache) Add(key []byte, item *hllCacheItem) {
	c.readCache.Remove(string(key))
	c.lruCache.Add(string(key), item)
}

func (db *RockDB) PFCount(ts int64, keys ...[]byte) (int64, error) {
	if len(keys) == 1 {
		item, ok := db.hllCache.Get(keys[0])
		if ok {
			cnt := atomic.LoadUint64(&item.cachedCount)
			if cnt&0x8000000000000000 == 0 {
				return int64(cnt), nil
			}
			item.Lock()
			cnt = item.hllp.Count()
			item.Unlock()
			if cnt&0x8000000000000000 != 0 {
				return 0, errHLLCountOverflow
			}
			atomic.StoreInt64(&item.ts, ts)
			atomic.StoreUint64(&item.cachedCount, cnt)
			return int64(cnt), nil
		}
	}
	keyList := make([][]byte, len(keys))
	errs := make([]error, len(keys))
	for i, k := range keys {
		_, kk, err := convertRedisKeyToDBKVKey(k)

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
			item := &hllCacheItem{
				hllp:        hllp,
				hllType:     hllPlusDefault,
				ts:          ts,
				cachedCount: cnt,
				flushed:     true,
			}
			db.hllCache.AddToReadCache(keys[0], item)

			return int64(cnt), err
		default:
			// unknown hll type
			return 0, errInvalidHLLData
		}
	} else {
		hllp, _ := hll.NewPlus(hllPrecision)
		// merge count
		for i, v := range keyList {
			item, ok := db.hllCache.Get(keys[i])
			var hllpv *hll.HyperLogLogPlus
			if !ok {
				if len(v) < 8+1 {
					return 0, errInvalidHLLData
				}
				hllType := uint8(v[0])
				if hllType != hllPlusDefault {
					return 0, errInvalidHLLData
				}
				pos := 1
				hllpv, _ = hll.NewPlus(hllPrecision)
				err := hllpv.GobDecode(v[pos+8:])
				if err != nil {
					return 0, err
				}
			} else {
				hllpv = item.hllp
			}
			if item != nil {
				item.Lock()
			}
			err := hllp.Merge(hllpv)
			if item != nil {
				item.Unlock()
			}
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
	_, key, err := convertRedisKeyToDBKVKey(rawKey)

	item, ok := db.hllCache.Get(rawKey)

	db.wb.Clear()
	changed := false
	var hllp *hll.HyperLogLogPlus
	if !ok {
		hllp, _ = hll.NewPlus(hllPrecision)
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
		if oldV == nil {
			// first init always return changed
			changed = true
		}
	} else {
		hllp = item.hllp
	}
	if item == nil {
		item = &hllCacheItem{
			hllp:    hllp,
			hllType: hllPlusDefault,
		}
	}
	item.Lock()
	for _, elem := range elems {
		db.hasher64.Write(elem)
		if hllp.Add(db.hasher64) {
			changed = true
		}
		db.hasher64.Reset()
	}
	item.Unlock()
	if !changed {
		return 0, nil
	}

	cnt := atomic.LoadUint64(&item.cachedCount)
	cnt = cnt | 0x8000000000000000
	atomic.StoreUint64(&item.cachedCount, cnt)
	atomic.StoreInt64(&item.ts, ts)
	item.flushed = false
	db.hllCache.Add(rawKey, item)
	return 1, nil
}
