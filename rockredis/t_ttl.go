package rockredis

import (
	"encoding/binary"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/absolute8511/gorocksdb"
)

var (
	errExpMetaKey = errors.New("invalid expire meta key")
	errExpTimeKey = errors.New("invalid expire time key")
)

const (
	expireCheckInterval = 1
	logTimeFormatStr    = "2006-01-02 15:04:05"
	batchedRedDataCount = 1024

	ttlRedundantDataCollectInterval = 5 * 60
)

var errExpType = errors.New("invalid expire type")

/*
the coded format of expire time key:
bytes:  -0-|-1-2-3-4-5-6-7-8-|----9---|-10-11--------x-|
data :  103|       when      |dataType|       key      |
*/
func expEncodeTimeKey(dataType byte, key []byte, when int64) []byte {
	buf := make([]byte, len(key)+1+8+1)

	pos := 0
	buf[pos] = ExpTimeType
	pos++

	binary.BigEndian.PutUint64(buf[pos:], uint64(when))
	pos += 8

	buf[pos] = dataType
	pos++

	copy(buf[pos:], key)

	return buf
}

/*
the coded format of expire meta key:
bytes:  -0-|----1-----|-2-3----------x--|
data :  102| dataType |       key       |
*/
func expEncodeMetaKey(dataType byte, key []byte) []byte {
	buf := make([]byte, len(key)+2)

	pos := 0

	buf[pos] = ExpMetaType
	pos++
	buf[pos] = dataType
	pos++

	copy(buf[pos:], key)

	return buf
}

//decode the expire 'meta key', the return values are: dataType, key, error
func expDecodeMetaKey(mk []byte) (byte, []byte, error) {
	pos := 0

	if pos+2 > len(mk) || mk[pos] != ExpMetaType {
		return 0, nil, errExpMetaKey
	}

	return mk[pos+1], mk[pos+2:], nil
}

//decode the expire 'time key', the return values are: dataType, key, whenToExpire, error
func expDecodeTimeKey(tk []byte) (byte, []byte, int64, error) {
	pos := 0
	if pos+10 > len(tk) || tk[pos] != ExpTimeType {
		return 0, nil, 0, errExpTimeKey
	}

	return tk[pos+9], tk[pos+10:], int64(binary.BigEndian.Uint64(tk[pos+1:])), nil
}

func (db *RockDB) expire(dataType byte, key []byte, duration int64) error {
	return db.expireAt(dataType, key, time.Now().Unix()+duration)
}

func (db *RockDB) rawExpireAt(dataType byte, key []byte, when int64, wb *gorocksdb.WriteBatch) error {
	mk := expEncodeMetaKey(dataType, key)
	tk := expEncodeTimeKey(dataType, key, when)

	wb.Put(tk, mk)
	wb.Put(mk, PutInt64(when))

	db.ttlChecker.setNextCheckTime(when, false)

	return nil
}

func (db *RockDB) expireAt(dataType byte, key []byte, when int64) error {
	mk := expEncodeMetaKey(dataType, key)

	wb := db.wb
	wb.Clear()

	tk := expEncodeTimeKey(dataType, key, when)

	wb.Put(tk, mk)
	wb.Put(mk, PutInt64(when))

	if err := db.eng.Write(db.defaultWriteOpts, wb); err != nil {
		return err
	} else {
		db.ttlChecker.setNextCheckTime(when, false)
		return nil
	}
}

func (db *RockDB) KVTtl(key []byte) (t int64, err error) {
	return db.ttl(KVType, key)
}

func (db *RockDB) HashTtl(key []byte) (t int64, err error) {
	return db.ttl(HashType, key)
}

func (db *RockDB) ListTtl(key []byte) (t int64, err error) {
	return db.ttl(ListType, key)
}

func (db *RockDB) SetTtl(key []byte) (t int64, err error) {
	return db.ttl(SetType, key)
}

func (db *RockDB) ZSetTtl(key []byte) (t int64, err error) {
	return db.ttl(ZSetType, key)
}

func (db *RockDB) ttl(dataType byte, key []byte) (t int64, err error) {
	mk := expEncodeMetaKey(dataType, key)

	if t, err = Int64(db.eng.GetBytes(db.defaultReadOpts, mk)); err != nil || t == 0 {
		t = -1
	} else {
		t -= time.Now().Unix()
		if t <= 0 {
			t = -1
		}

		//TODO, if the key has expired, remove it right now
		// if t == -1 : to remove ????
	}

	return t, err
}

func (db *RockDB) delExpire(dataType byte, key []byte, wb *gorocksdb.WriteBatch) error {
	mk := expEncodeMetaKey(dataType, key)
	wb.Delete(mk)
	return nil
}

type TTLChecker struct {
	sync.Mutex
	db *RockDB

	quitC       chan struct{}
	checking    int32
	wb          *gorocksdb.WriteBatch
	expiredChan chan *common.ExpiredData
	commitC     chan struct{}

	//next check time
	nc int64
}

func NewTTLChecker(db *RockDB) *TTLChecker {
	c := &TTLChecker{
		db:          db,
		nc:          time.Now().Unix(),
		wb:          gorocksdb.NewWriteBatch(),
		expiredChan: make(chan *common.ExpiredData),
		commitC:     make(chan struct{}),
	}
	return c
}

func (c *TTLChecker) GetExpiredDataChan() (chan struct{}, chan *common.ExpiredData) {
	return c.commitC, c.expiredChan
}

func (c *TTLChecker) Start() {
	c.Lock()
	if atomic.CompareAndSwapInt32(&c.checking, 0, 1) {
		dbLog.Infof("ttl checker started")
		defer dbLog.Infof("ttl checker exit")
		c.quitC = make(chan struct{})

		c.Unlock()

		cTicker := time.NewTicker(time.Second * expireCheckInterval)
		defer cTicker.Stop()

		for {
			select {
			case <-cTicker.C:
				c.check(c.quitC)
			case <-c.quitC:
				return
			}
		}
	} else {
		c.Unlock()
	}
}

func (c *TTLChecker) Stop() {
	c.Lock()
	if atomic.CompareAndSwapInt32(&c.checking, 1, 0) {
		close(c.quitC)
	}
	c.Unlock()
}

func (c *TTLChecker) setNextCheckTime(when int64, force bool) {
	c.Lock()
	if force {
		c.nc = when
	} else if c.nc > when {
		c.nc = when
	}
	c.Unlock()
}

func (c *TTLChecker) check(stopChan chan struct{}) {
	defer func() {
		if e := recover(); e != nil {
			buf := make([]byte, 4096)
			n := runtime.Stack(buf, false)
			buf = buf[0:n]
			dbLog.Errorf("check ttl panic: %s:%v", buf, e)
		}
	}()

	now := time.Now().Unix()

	c.Lock()
	nc := c.nc
	c.Unlock()

	if now < nc {
		return
	}

	nc = now + 3600

	minKey := expEncodeTimeKey(NoneType, nil, 0)
	maxKey := expEncodeTimeKey(maxDataType, nil, nc)

	var eCount int64 = 0
	var scanned int64 = 0
	checkStart := time.Now()

	it, err := NewDBRangeLimitIterator(c.db.eng, minKey, maxKey,
		common.RangeROpen, 0, -1, false)
	defer it.Close()
	if err != nil {
		nc = now + 1
		return
	} else if it == nil || !it.Valid() {
		return
	}

	var redundantTimeKey int64 = 0
	c.wb.Clear()

	for ; it.Valid(); it.Next() {

		if scanned%100 == 0 {
			select {
			case <-stopChan:
				break
			default:
			}
		}
		tk := it.Key()
		mk := it.Value()

		if tk == nil {
			continue
		}

		dt, k, nt, err := expDecodeTimeKey(tk)
		if err != nil {
			continue
		}
		scanned += 1

		if scanned == 1 {
			//log the first scanned key
			dbLog.Infof("ttl check start at key:[%s] of type:%d whose expire time is: %s", string(k),
				dt, time.Unix(nt, 0).Format(logTimeFormatStr))
		}

		if nt > now {
			//the next ttl check time is nt!
			nc = nt
			dbLog.Infof("ttl check end at key:[%s] of type:%d whose expire time is: %s", string(k),
				dt, time.Unix(nt, 0).Format(logTimeFormatStr))
			break
		}

		if exp, err := Int64(c.db.eng.GetBytes(c.db.defaultReadOpts, mk)); err == nil {
			if exp != nt {
				//it may happen if ttl of the key has been reset as we do not remove the
				//pre-exists time-key when expire called
				c.wb.Delete(tk)
				redundantTimeKey += 1
			} else {
				select {
				case c.expiredChan <- &common.ExpiredData{DataType: dataType2CommonType(dt), Key: k}:
				case <-stopChan:
					break
				}
				eCount += 1
			}
		}

		if redundantTimeKey%batchedRedDataCount == 0 {
			if err := c.db.eng.Write(c.db.defaultWriteOpts, c.wb); err != nil {
				dbLog.Warningf("delete redundant time keys failed during ttl checking, err:%s", err.Error())
			}
			c.wb.Clear()
		}

	}

	if err := c.db.eng.Write(c.db.defaultWriteOpts, c.wb); err != nil {
		dbLog.Warningf("delete redundant time keys failed during ttl checking, err:%s", err.Error())
	}

	c.wb.Clear()

	c.setNextCheckTime(nc, true)

	select {
	case c.commitC <- struct{}{}:
	case <-stopChan:
	}

	checkCost := time.Since(checkStart).Nanoseconds() / 1000
	dbLog.Infof("[%d/%d] keys have expired and [%d/%d] redundant time keys have been deleted during ttl checking, cost:%d us, the next checking will start at: %s",
		eCount, scanned, redundantTimeKey, scanned, checkCost, time.Unix(nc, 0).Format(logTimeFormatStr))

	return
}

func dataType2CommonType(t byte) common.DataType {
	switch t {
	case KVType:
		return common.KV
	case HashType:
		return common.HASH
	case ListType:
		return common.LIST
	case SetType:
		return common.SET
	case ZSetType:
		return common.ZSET
	default:
		return common.NONE
	}
}

type ttlRedundantDataCollector struct {
	db    *RockDB
	quitC chan struct{}
	wb    *gorocksdb.WriteBatch
	wg    sync.WaitGroup

	sync.Mutex
	stats map[string]int64
}

func NewTTLRedundantDataCollector(db *RockDB) *ttlRedundantDataCollector {
	tc := &ttlRedundantDataCollector{
		db:    db,
		quitC: make(chan struct{}),
		wb:    gorocksdb.NewWriteBatch(),
		stats: make(map[string]int64),
	}
	tc.stats["gc-times"] = 0
	tc.stats["cost"] = 0
	tc.stats["scanned"] = 0
	tc.stats["deleted"] = 0
	return tc

}

func (tc *ttlRedundantDataCollector) Start() {
	tc.wg.Add(1)
	go func() {
		defer tc.wg.Done()
		ticker := time.NewTicker(ttlRedundantDataCollectInterval * time.Second)
		for {
			select {
			case <-ticker.C:
				tc.collect()
			case <-tc.quitC:
				return
			}
		}
	}()
}

func (tc *ttlRedundantDataCollector) Stop() {
	close(tc.quitC)
	tc.wg.Wait()
}

func (tc *ttlRedundantDataCollector) Name() string {
	return "ttl-gc-component"
}

func (tc *ttlRedundantDataCollector) Stats() (interface{}, error) {
	cstats := make(map[string]int64)
	tc.Lock()
	for k, v := range tc.stats {
		cstats[k] = v
	}
	tc.Unlock()
	return cstats, nil
}

func (tc *ttlRedundantDataCollector) collect() {
	defer func() {
		if e := recover(); e != nil {
			buf := make([]byte, 4096)
			n := runtime.Stack(buf, false)
			buf = buf[0:n]
			dbLog.Errorf("collect redundant data of ttl panic: %s:%v", buf, e)
		}
	}()

	start := time.Now()
	dbLog.Infof("gc of redundant data of ttl start at %s", start.Format(logTimeFormatStr))

	minKey := expEncodeTimeKey(NoneType, nil, 0)
	maxKey := expEncodeTimeKey(maxDataType, nil, start.Unix())

	var scanned int64 = 0
	var redundant int64 = 0

	it, err := NewDBRangeLimitIterator(tc.db.eng, minKey, maxKey,
		common.RangeROpen, 0, -1, false)
	defer it.Close()

	defer func() {
		cost := time.Since(start)
		tc.updateStats(scanned, redundant, cost)
		dbLog.Infof("[%d/%d] redundant time keys have been deleted during gc, cost:%dus", redundant, scanned, cost.Nanoseconds()/1000)
	}()

	if err != nil {
		dbLog.Infof("gc of redundant data of ttl, create db range iterator failed as:%s", err.Error())
		return
	} else if it == nil || !it.Valid() {
		return
	}

	tc.wb.Clear()

	for ; it.Valid(); it.Next() {
		if scanned%100 == 0 {
			select {
			case <-tc.quitC:
				break
			default:
			}
		}
		tk := it.Key()
		mk := it.Value()

		if tk == nil {
			continue
		}

		scanned += 1
		_, _, nt, err := expDecodeTimeKey(tk)
		if err != nil {
			dbLog.Warningf("encounter the damaged data of time key during gc")
			continue
		}

		if exp, err := Int64(tc.db.eng.GetBytes(tc.db.defaultReadOpts, mk)); err == nil {
			if exp != nt {
				//it may happen if ttl of the key has been reset as we do not remove the
				//pre-exists time-key when expire called
				tc.wb.Delete(tk)
				redundant += 1
			}
		}

		if redundant%batchedRedDataCount == 0 {
			if err := tc.db.eng.Write(tc.db.defaultWriteOpts, tc.wb); err != nil {
				dbLog.Warningf("delete redundant time keys failed during gc, err:%s", err.Error())
			}
			tc.wb.Clear()
		}

	}

	if err := tc.db.eng.Write(tc.db.defaultWriteOpts, tc.wb); err != nil {
		dbLog.Warningf("delete redundant time keys failed during gc, err:%s", err.Error())
	}

	tc.wb.Clear()

	return
}

func (tc *ttlRedundantDataCollector) updateStats(scanned int64, deleted int64, cost time.Duration) {
	tc.Lock()
	tc.stats["gc-times"] += 1
	tc.stats["cost"] += cost.Nanoseconds() / 1000
	tc.stats["scanned"] += scanned
	tc.stats["deleted"] += deleted
	tc.Unlock()
}
