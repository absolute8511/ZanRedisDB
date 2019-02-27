package rockredis

import (
	"encoding/binary"
	"errors"
	"hash"
	"sync"
	"sync/atomic"
	"time"

	hll2 "github.com/absolute8511/go-hll"
	hll "github.com/absolute8511/hyperloglog"
	hll3 "github.com/absolute8511/hyperloglog2"
	"github.com/golang/snappy"
	"github.com/youzan/gorocksdb"
	//hll "github.com/axiomhq/hyperloglog"
	"github.com/hashicorp/golang-lru"
)

const (
	hllPrecision uint8 = 13
	// to make it compatible for some different hll lib
	// we add a flag to indicate which hll lib we are using
	hllPlusDefault uint8 = 1
	hllType2       uint8 = 2
	hllType3       uint8 = 3
)

//TODO: how to change this through raft and keep it after restart
var initHLLType = hllType3

// pfcount use uint64 to storage the precomputed value, and the most significant bit is used
// to indicate whether the cached value is valid.
// pfadd will modify the pfcount most significant bit to indicate the register is changed
// and pfcount should recompute instead using the cached value.
// since pfadd will be called much more than pfcount, so we compute the lazy while actually pfcount will improve pfadd performance.

// 14 bits precision yield to 16384 registers and standard error is 0.81% (1/sqrt(number of registers))
var errInvalidHLLData = errors.New("invalide hyperloglog data")
var errInvalidHLLType = errors.New("hyperloglog type is wrong")
var errHLLCountOverflow = errors.New("hyperloglog count overflow")

type hllCacheItem struct {
	sync.Mutex
	hllp *hll.HyperLogLogPlus
	hll2 hll2.HLL
	hll3 *hll3.Sketch
	// we use the highest bit to indicated if this cached count is out of date
	cachedCount uint64
	hllType     uint8
	ts          int64
	dirty       bool
	deleting    bool
}

func newHLLItem(init uint8) (*hllCacheItem, error) {
	switch init {
	case hllPlusDefault:
		hllp, _ := hll.NewPlus(hllPrecision)
		item := &hllCacheItem{
			hllp:    hllp,
			hllType: hllPlusDefault,
		}
		return item, nil
	case hllType2:
		sz, err := hll2.SizeByP(int(hllPrecision))
		if err != nil {
			return nil, err
		}
		h2 := make(hll2.HLL, sz)
		item := &hllCacheItem{
			hll2:    h2,
			hllType: hllType2,
		}
		return item, nil
	case hllType3:
		h3, _ := hll3.New(hllPrecision, true)
		item := &hllCacheItem{
			hll3:    h3,
			hllType: hllType3,
		}
		return item, nil
	default:
		return nil, errInvalidHLLType
	}
}

func newHLLItemFromDBBytes(hllType uint8, fkv []byte) (*hllCacheItem, bool, error) {
	pos := 1
	recompute := true
	cnt := binary.BigEndian.Uint64(fkv[pos : pos+8])
	if cnt&0x8000000000000000 == 0 {
		recompute = false
	}
	switch hllType {
	case hllPlusDefault:
		hllp, _ := hll.NewPlus(hllPrecision)
		err := hllp.GobDecode(fkv[pos+8:])
		if err != nil {
			return nil, recompute, err
		}

		item := &hllCacheItem{
			hllp:        hllp,
			hllType:     hllPlusDefault,
			cachedCount: cnt,
			dirty:       false,
		}
		return item, recompute, err
	case hllType3:
		h3, _ := hll3.New(hllPrecision, true)
		err := h3.UnmarshalBinary(fkv[pos+8:])
		if err != nil {
			return nil, recompute, err
		}

		item := &hllCacheItem{
			hll3:        h3,
			hllType:     hllType3,
			cachedCount: cnt,
		}
		return item, recompute, err
	case hllType2:
		sz, err := hll2.SizeByP(int(hllPrecision))
		if err != nil {
			return nil, recompute, err
		}
		h2 := make(hll2.HLL, sz)
		d := fkv[pos+8:]
		if len(d) != len(h2) {
			d, err = snappy.Decode(nil, d)
			if err != nil || len(d) != len(h2) {
				dbLog.Infof("invalid hll data: %v, expect len: %v, %v", d, len(h2), err)
				return nil, recompute, errInvalidHLLData
			}
		}
		copy(h2, d)

		item := &hllCacheItem{
			hll2:        h2,
			hllType:     hllType2,
			cachedCount: cnt,
		}
		return item, recompute, nil
	default:
		// unknown hll type
		return nil, recompute, errInvalidHLLType
	}
}

func (hllItem *hllCacheItem) HLLToBytes() ([]byte, error) {
	if hllItem.hllType == hllPlusDefault {
		return hllItem.hllp.GobEncode()
	} else if hllItem.hllType == hllType2 {
		return snappy.Encode(nil, hllItem.hll2), nil
	} else if hllItem.hllType == hllType3 {
		return hllItem.hll3.MarshalBinary()
	}
	return nil, errInvalidHLLType
}

func (hllItem *hllCacheItem) HLLMerge(item *hllCacheItem) error {
	if hllItem.hllType != item.hllType {
		return errInvalidHLLType
	}
	switch hllItem.hllType {
	case hllPlusDefault:
		return hllItem.hllp.Merge(item.hllp)
	case hllType2:
		return hllItem.hll2.Merge(item.hll2)
	case hllType3:
		return hllItem.hll3.Merge(item.hll3)
	default:
		return errInvalidHLLType
	}
}

func (hllItem *hllCacheItem) computeHLLCount() uint64 {
	if hllItem.hllType == hllPlusDefault {
		return hllItem.hllp.Count()
	} else if hllItem.hllType == hllType2 {
		return hllItem.hll2.EstimateCardinality()
	} else if hllItem.hllType == hllType3 {
		return hllItem.hll3.Estimate()
	}
	return 0
}

func (hllItem *hllCacheItem) getcomputeCount(ts int64) (int64, error) {
	cnt := atomic.LoadUint64(&hllItem.cachedCount)
	if cnt&0x8000000000000000 == 0 {
		return int64(cnt), nil
	}
	hllItem.Lock()
	cnt = hllItem.computeHLLCount()
	if cnt&0x8000000000000000 != 0 {
		hllItem.Unlock()
		return 0, errHLLCountOverflow
	}
	atomic.StoreInt64(&hllItem.ts, ts)
	atomic.StoreUint64(&hllItem.cachedCount, cnt)
	hllItem.Unlock()
	return int64(cnt), nil
}

func (hllItem *hllCacheItem) invalidCachedCnt(ts int64) {
	hllItem.Lock()
	cnt := atomic.LoadUint64(&hllItem.cachedCount)
	cnt = cnt | 0x8000000000000000
	atomic.StoreUint64(&hllItem.cachedCount, cnt)
	atomic.StoreInt64(&hllItem.ts, ts)
	hllItem.dirty = true
	hllItem.Unlock()
}

func (hllItem *hllCacheItem) HLLAdd(hasher hash.Hash64, elems ...[]byte) bool {
	changed := false

	for _, elem := range elems {
		hasher.Write(elem)
		if hllItem.hllType == hllPlusDefault {
			if hllItem.hllp.Add(hasher) {
				changed = true
			}
		} else if hllItem.hllType == hllType2 {
			if hllItem.hll2.Add(hasher.Sum64()) {
				changed = true
			}
		} else if hllItem.hllType == hllType3 {
			if hllItem.hll3.InsertHash(hasher.Sum64()) {
				changed = true
			}
		}
		hasher.Reset()
	}

	return changed
}

func (hllItem *hllCacheItem) addCount(hasher hash.Hash64, elems ...[]byte) (bool, bool) {
	changed := false
	hllItem.Lock()
	isDirty := hllItem.dirty
	if hllItem.HLLAdd(hasher, elems...) {
		changed = true
	}
	hllItem.Unlock()
	return changed, isDirty
}

type hllCache struct {
	dirtyWriteCache *lru.Cache
	// to avoid pfcount modify the db, we separate the read cache.
	// so we can treat the pfcount as read command, since it only get from the write cache or
	// read from db and cache to read cache.
	rl        sync.RWMutex
	readCache *lru.Cache
	db        *RockDB
}

// write cache size should not too much, it may cause flush slow if
// too much dirty write need flush
func newHLLCache(size int, wsize int, db *RockDB) (*hllCache, error) {
	c := &hllCache{
		db: db,
	}
	var err error
	c.readCache, err = lru.NewWithEvict(size, nil)
	if err != nil {
		return nil, err
	}
	c.dirtyWriteCache, err = lru.NewWithEvict(wsize, c.onEvicted)
	return c, err
}

// must be called in raft commit loop
func (c *hllCache) Flush() {
	start := time.Now()
	c.dirtyWriteCache.Purge()
	cost := time.Since(start)
	if cost > time.Millisecond*100 {
		dbLog.Infof("flush hll cache cost: %v", cost)
	}
}

// must be called in raft commit loop
func (c *hllCache) onEvicted(rawKey interface{}, value interface{}) {
	item, ok := value.(*hllCacheItem)
	if !ok {
		return
	}
	item.Lock()
	if item.deleting || !item.dirty {
		item.Unlock()
		return
	}
	tsBuf := PutInt64(item.ts)
	item.dirty = false
	newV, err := item.HLLToBytes()
	oldCnt := atomic.LoadUint64(&item.cachedCount)
	ht := item.hllType
	item.Unlock()
	if err != nil {
		dbLog.Warningf("failed to encode %v hll: %v", rawKey, err.Error())
		return
	}
	cachedKey := []byte(rawKey.(string))
	table, key, err := convertRedisKeyToDBKVKey(cachedKey)
	if err != nil {
		dbLog.Warningf("key invalid %v : %v", cachedKey, err.Error())
		return
	}

	s := time.Now()
	wb := gorocksdb.NewWriteBatch()
	defer wb.Destroy()
	oldV, _ := c.db.eng.GetBytesNoLock(c.db.defaultReadOpts, key)

	if c.db.cfg.EnableTableCounter && oldV == nil {
		c.db.IncrTableKeyCount(table, 1, wb)
	}

	if len(oldV) >= 1+8 {
		oldV = oldV[:1+8]
	} else {
		oldV = make([]byte, 8+1)
	}
	oldV[0] = ht
	binary.BigEndian.PutUint64(oldV[1:1+8], oldCnt)
	oldV = append(oldV, newV...)
	oldV = append(oldV, tsBuf...)
	wb.Put(key, oldV)
	c.db.eng.Write(c.db.defaultWriteOpts, wb)
	cost := time.Since(s)
	if cost > time.Millisecond*200 {
		dbLog.Infof("flush pfadd %v (%v) slow: %v", key, len(oldV), cost)
	}
	c.AddToReadCache(cachedKey, item)
}

func (c *hllCache) Get(key []byte) (*hllCacheItem, bool) {
	v, ok := c.dirtyWriteCache.Get(string(key))
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
	return item, true
}

// make sure dirty write is not added
func (c *hllCache) AddToReadCache(key []byte, item *hllCacheItem) {
	c.readCache.Add(string(key), item)
}

func (c *hllCache) AddDirtyWrite(key []byte, item *hllCacheItem) {
	c.dirtyWriteCache.Add(string(key), item)
	c.readCache.Remove(string(key))
}

func (c *hllCache) Del(key []byte) {
	v, ok := c.dirtyWriteCache.Peek(string(key))
	if ok {
		item, ok := v.(*hllCacheItem)
		if ok {
			item.Lock()
			item.deleting = true
			item.Unlock()
		}
		c.dirtyWriteCache.Remove(string(key))
	}
	c.readCache.Remove(string(key))
}

func cntFromItem(ts int64, item *hllCacheItem) (int64, error) {
	return item.getcomputeCount(ts)
}

func (db *RockDB) PFCount(ts int64, keys ...[]byte) (int64, error) {
	if len(keys) == 1 {
		item, ok := db.hllCache.Get(keys[0])
		if ok {
			return cntFromItem(ts, item)
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
	if len(keys) == 1 {
		db.hllCache.rl.RLock()
		defer db.hllCache.rl.RUnlock()
		item, ok := db.hllCache.Get(keys[0])
		if ok {
			return cntFromItem(ts, item)
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
		s := time.Now()
		item, recompute, err := newHLLItemFromDBBytes(hllType, fkv)
		if err != nil {
			return 0, err
		}
		if cost := time.Since(s); cost > time.Millisecond*200 {
			dbLog.Infof("init item from db slow: %v", cost)
		}
		cnt := item.cachedCount
		if recompute {
			cnt = item.computeHLLCount()
			if cnt&0x8000000000000000 != 0 {
				return 0, errHLLCountOverflow
			}
			atomic.StoreUint64(&item.cachedCount, cnt)
		}
		atomic.StoreInt64(&item.ts, ts)
		db.hllCache.AddToReadCache(keys[0], item)
		return int64(cnt), nil
	} else {
		var mergeItem *hllCacheItem
		// merge count
		for i, v := range keyList {
			item, ok := db.hllCache.Get(keys[i])
			var err error
			if !ok {
				if len(v) < 8+1 {
					return 0, errInvalidHLLData
				}
				hllType := uint8(v[0])
				item, _, err = newHLLItemFromDBBytes(hllType, v)
				if err != nil {
					return 0, err
				}
			}
			if mergeItem == nil {
				mergeItem, err = newHLLItem(item.hllType)
				if err != nil {
					return 0, err
				}
			}
			item.Lock()
			err = mergeItem.HLLMerge(item)
			item.Unlock()
			if err != nil {
				return 0, err
			}
		}
		return int64(mergeItem.computeHLLCount()), nil
	}
}

func (db *RockDB) delPFCache(rawKey []byte) {
	// only use this to delete pf while the key is really deleted
	db.hllCache.Del(rawKey)
}

func (db *RockDB) PFAdd(ts int64, rawKey []byte, elems ...[]byte) (int64, error) {
	if len(elems) > MAX_BATCH_NUM*10 {
		return 0, errTooMuchBatchSize
	}
	_, key, err := convertRedisKeyToDBKVKey(rawKey)

	item, ok := db.hllCache.Get(rawKey)
	s := time.Now()
	changed := false
	if !ok {
		oldV, _ := db.eng.GetBytesNoLock(db.defaultReadOpts, key)
		if oldV != nil {
			if len(oldV) < 8+1+tsLen {
				return 0, errInvalidHLLData
			}
			oldV = oldV[:len(oldV)-tsLen]
			item, _, err = newHLLItemFromDBBytes(uint8(oldV[0]), oldV)
			if err != nil {
				return 0, err
			}
		} else {
			item, err = newHLLItem(initHLLType)
			if err != nil {
				return 0, err
			}
			// first init always return changed
			changed = true
		}
		item.ts = ts
	}

	cost1 := time.Since(s)
	added, isDirty := item.addCount(db.hasher64, elems...)
	if added {
		changed = true
	}
	cost2 := time.Since(s)
	if !changed {
		if cost2 >= time.Millisecond*100 {
			dbLog.Infof("pfadd %v slow: %v, %v", string(rawKey), cost1, cost2)
		}
		if !isDirty {
			db.hllCache.AddToReadCache(rawKey, item)
		}
		return 0, nil
	}

	db.hllCache.rl.Lock()
	defer db.hllCache.rl.Unlock()
	item.invalidCachedCnt(ts)

	db.hllCache.AddDirtyWrite(rawKey, item)
	cost3 := time.Since(s)
	if cost3 >= time.Millisecond*100 {
		dbLog.Infof("pfadd %v slow: %v, %v, %v", string(rawKey), cost1, cost2, cost3)
	}
	return 1, nil
}
