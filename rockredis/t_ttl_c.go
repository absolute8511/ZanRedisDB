package rockredis

import (
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/absolute8511/gorocksdb"
)

const (
	ttlRDataCollectInterval = 5 * 60
	batchedRedDataCount     = 1024
	consistExpCheckInterval = 1
	consistExpChanBufSize   = 256
	consistBatchedBufSize   = 1000
)

type consistencyExpiration struct {
	*cTTLChecker
	db *RockDB
	tc *ttlRDataCollector
}

func newConsistencyExpiration(db *RockDB) *consistencyExpiration {
	exp := &consistencyExpiration{
		db:          db,
		cTTLChecker: newCTTLChecker(db),
		tc:          newTTLRDataCollector(db),
	}
	return exp
}

func (exp *consistencyExpiration) GetRedundantDataCollector() *ttlRDataCollector {
	return exp.tc
}

func (exp *consistencyExpiration) expireAt(dataType byte, key []byte, when int64) error {
	mk := expEncodeMetaKey(dataType, key)

	wb := exp.db.wb
	wb.Clear()

	tk := expEncodeTimeKey(dataType, key, when)

	wb.Put(tk, mk)
	wb.Put(mk, PutInt64(when))

	if err := exp.db.eng.Write(exp.db.defaultWriteOpts, wb); err != nil {
		return err
	} else {
		exp.setNextCheckTime(when, false)
		return nil
	}
}

func (exp *consistencyExpiration) rawExpireAt(dataType byte, key []byte, when int64, wb *gorocksdb.WriteBatch) error {
	mk := expEncodeMetaKey(dataType, key)
	tk := expEncodeTimeKey(dataType, key, when)

	wb.Put(tk, mk)
	wb.Put(mk, PutInt64(when))

	exp.setNextCheckTime(when, false)
	return nil
}

func (exp *consistencyExpiration) ttl(dataType byte, key []byte) (int64, error) {
	mk := expEncodeMetaKey(dataType, key)

	t, err := Int64(exp.db.eng.GetBytes(exp.db.defaultReadOpts, mk))
	if err != nil || t == 0 {
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

func (exp *consistencyExpiration) delExpire(dataType byte, key []byte, wb *gorocksdb.WriteBatch) error {
	mk := expEncodeMetaKey(dataType, key)
	wb.Delete(mk)
	return nil
}

type batchedBuffer struct {
	sync.Mutex
	stopCh   chan struct{}
	expiredC chan *common.ExpiredData
	keys     [][]byte
}

func (self *batchedBuffer) commit() {
	self.Mutex.Lock()
	if len(self.keys) != 0 {
		select {
		case self.expiredC <- &common.ExpiredData{Keys: self.keys}:
		case <-self.stopCh:
		}
		self.keys = make([][]byte, 0, consistBatchedBufSize)
	}
	self.Mutex.Unlock()
}

func (self *batchedBuffer) propose(key []byte) {
	self.Mutex.Lock()

	self.keys = append(self.keys, key)

	if len(self.keys) >= consistBatchedBufSize {
		select {
		case self.expiredC <- &common.ExpiredData{Keys: self.keys}:
		case <-self.stopCh:
		}
		self.keys = make([][]byte, 0, consistBatchedBufSize)
	}

	self.Mutex.Unlock()
}

func (self *batchedBuffer) setStopChan(stopCh chan struct{}) {
	self.Mutex.Lock()
	self.stopCh = stopCh
	self.Mutex.Unlock()
}

func NewBatchedBuffer() *batchedBuffer {
	return &batchedBuffer{
		expiredC: make(chan *common.ExpiredData, consistExpChanBufSize),
		keys:     make([][]byte, 0, consistBatchedBufSize),
	}
}

type cTTLChecker struct {
	sync.Mutex

	db       *RockDB
	quitC    chan struct{}
	checking int32
	wb       *gorocksdb.WriteBatch
	wg       sync.WaitGroup

	batched [common.ALL - common.NONE]*batchedBuffer

	//next check time
	nc int64
}

func newCTTLChecker(db *RockDB) *cTTLChecker {
	c := &cTTLChecker{
		db: db,
		nc: time.Now().Unix(),
		wb: gorocksdb.NewWriteBatch(),
	}

	for dt, _ := range c.batched {
		c.batched[dt] = NewBatchedBuffer()
	}

	return c
}

func (c *cTTLChecker) GetExpiredDataChan(dt common.DataType) chan *common.ExpiredData {
	return c.batched[dt].expiredC
}

func (c *cTTLChecker) Start() {
	c.Lock()
	if !atomic.CompareAndSwapInt32(&c.checking, 0, 1) {
		c.Unlock()
		return
	}

	c.quitC = make(chan struct{})

	for _, batch := range c.batched {
		batch.setStopChan(c.quitC)
	}

	c.wg.Add(1)
	go func(stopCh chan struct{}) {
		dbLog.Infof("ttl checker of Consistencya-Deletion Policy started")
		t := time.NewTicker(time.Second * consistExpCheckInterval)

		defer func() {
			t.Stop()
			c.wg.Done()
			dbLog.Infof("ttl checker of Consistency-Deletion Policy exit")
		}()

		for {
			select {
			case <-t.C:
				c.check(stopCh)
			case <-stopCh:
				return
			}
		}
	}(c.quitC)

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.applyBatch(c.quitC)
	}()

	c.Unlock()
}

func (c *cTTLChecker) applyBatch(stopCh chan struct{}) {
	commitTicker := time.NewTicker(time.Second)

	defer func() {
		c.commitAllBatched()
		commitTicker.Stop()
	}()

	for {
		select {
		case <-commitTicker.C:
			c.commitAllBatched()
		case <-stopCh:
			return
		}
	}

}

func (c *cTTLChecker) Stop() {
	c.Lock()
	if atomic.LoadInt32(&c.checking) == 1 {
		close(c.quitC)
		c.Unlock()

		//we should unlock the Mutex before Wait to avoid
		//deadlock since the goroutine started requires the lock
		//at running
		c.wg.Wait()

		atomic.StoreInt32(&c.checking, 0)
		return
	}

	c.Unlock()
}

func (c *cTTLChecker) setNextCheckTime(when int64, force bool) {
	c.Lock()
	if force {
		c.nc = when
	} else if c.nc > when {
		c.nc = when
	}
	c.Unlock()
}

func (c *cTTLChecker) commitAllBatched() {
	for _, bat := range c.batched {
		bat.commit()
	}
}

func (c *cTTLChecker) check(stopChan chan struct{}) {
	defer func() {
		if e := recover(); e != nil {
			buf := make([]byte, 4096)
			n := runtime.Stack(buf, false)
			buf = buf[0:n]
			dbLog.Errorf("check ttl panic: %s:%v", buf, e)
		}
		c.commitAllBatched()
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
	if err != nil {
		c.setNextCheckTime(now+1, false)
		return
	} else if it == nil {
		c.setNextCheckTime(nc, false)
		return
	}
	defer it.Close()

	var redundantTimeKey int64 = 0
	c.wb.Clear()

	for ; it.Valid(); it.Next() {
		if scanned%100 == 0 {
			select {
			case <-stopChan:
				nc = now + 1
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
			dbLog.Infof("ttl check start at key:[%s] of type:%s whose expire time is: %s", string(k),
				TypeName[dt], time.Unix(nt, 0).Format(logTimeFormatStr))
		}

		if nt > now {
			//the next ttl check time is nt!
			nc = nt
			dbLog.Infof("ttl check end at key:[%s] of type:%s whose expire time is: %s", string(k),
				TypeName[dt], time.Unix(nt, 0).Format(logTimeFormatStr))
			break
		}

		if exp, err := Int64(c.db.eng.GetBytesNoLock(c.db.defaultReadOpts, mk)); err == nil {
			if exp != nt {
				//this may happen if ttl of the key has been reset as we do not remove the
				//pre-exists time-key when expire called
				c.wb.Delete(tk)
				redundantTimeKey += 1
			} else {
				c.batched[dataType2CommonType(dt)].propose(k)
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

	c.setNextCheckTime(nc, false)

	checkCost := time.Since(checkStart).Nanoseconds() / 1000
	dbLog.Infof("[%d/%d] keys have expired and [%d/%d] redundant time keys have been deleted during ttl checking, cost:%d us, the next checking will start at: %s",
		eCount, scanned, redundantTimeKey, scanned, checkCost, time.Unix(nc, 0).Format(logTimeFormatStr))

	return
}

type ttlRDataCollector struct {
	db      *RockDB
	quitC   chan struct{}
	wb      *gorocksdb.WriteBatch
	wg      sync.WaitGroup
	running int32

	sync.Mutex
	stats map[string]int64
}

func newTTLRDataCollector(db *RockDB) *ttlRDataCollector {
	tc := &ttlRDataCollector{
		db:    db,
		wb:    gorocksdb.NewWriteBatch(),
		stats: make(map[string]int64),
	}
	tc.stats["gc-times"] = 0
	tc.stats["cost"] = 0
	tc.stats["scanned"] = 0
	tc.stats["deleted"] = 0
	return tc

}

func (tc *ttlRDataCollector) Start() {
	tc.Mutex.Lock()

	if !atomic.CompareAndSwapInt32(&tc.running, 0, 1) {
		tc.Mutex.Unlock()
		return
	}

	tc.quitC = make(chan struct{})

	tc.wg.Add(1)
	go func(stopCh chan struct{}) {
		defer tc.wg.Done()
		t := time.NewTicker(ttlRDataCollectInterval * time.Second)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				tc.collect()
			case <-stopCh:
				return
			}
		}
	}(tc.quitC)

	tc.Mutex.Unlock()
}

func (tc *ttlRDataCollector) Stop() {
	tc.Mutex.Lock()

	if atomic.CompareAndSwapInt32(&tc.running, 1, 0) {
		close(tc.quitC)
		tc.Mutex.Unlock()

		tc.wg.Wait()
		return
	}

	tc.Mutex.Unlock()
}

func (tc *ttlRDataCollector) Name() string {
	return "ttl-gc-component"
}

func (tc *ttlRDataCollector) Stats() (interface{}, error) {
	cstats := make(map[string]int64)
	tc.Lock()
	for k, v := range tc.stats {
		cstats[k] = v
	}
	tc.Unlock()
	return cstats, nil
}

func (tc *ttlRDataCollector) collect() {
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

	defer func() {
		cost := time.Since(start)
		tc.updateStats(scanned, redundant, cost)
		dbLog.Infof("[%d/%d] redundant time keys have been deleted during gc, cost:%dus", redundant, scanned, cost.Nanoseconds()/1000)
	}()

	it, err := NewDBRangeLimitIterator(tc.db.eng, minKey, maxKey,
		common.RangeROpen, 0, -1, false)
	if err != nil {
		dbLog.Infof("gc of redundant data of ttl, create db range iterator failed as:%s", err.Error())
		return
	} else if it == nil {
		return
	}
	defer it.Close()

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

		if exp, err := Int64(tc.db.eng.GetBytesNoLock(tc.db.defaultReadOpts, mk)); err == nil {
			if exp != nt {
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

func (tc *ttlRDataCollector) updateStats(scanned int64, deleted int64, cost time.Duration) {
	tc.Lock()
	tc.stats["gc-times"] += 1
	tc.stats["cost"] += cost.Nanoseconds() / 1000
	tc.stats["scanned"] += scanned
	tc.stats["deleted"] += deleted
	tc.Unlock()
}
