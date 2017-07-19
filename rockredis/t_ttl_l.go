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
	localExpCheckInterval      = 300
	localBatchedBufSize        = 1000
	localBatchedCommitInterval = 30
)

type localExpiration struct {
	*lTTLChecker
	db *RockDB
}

func newLocalExpiration(db *RockDB) *localExpiration {
	return &localExpiration{
		db:          db,
		lTTLChecker: newlTTlChecker(db),
	}
}

func (exp *localExpiration) expireAt(dataType byte, key []byte, when int64) error {
	wb := exp.db.wb
	wb.Clear()

	tk := expEncodeTimeKey(dataType, key, when)

	wb.Put(tk, PutInt64(when))

	if err := exp.db.eng.Write(exp.db.defaultWriteOpts, wb); err != nil {
		return err
	} else {
		exp.setNextCheckTime(when, false)
		return nil
	}
}

func (exp *localExpiration) rawExpireAt(dataType byte, key []byte, when int64, wb *gorocksdb.WriteBatch) error {
	tk := expEncodeTimeKey(dataType, key, when)
	wb.Put(tk, PutInt64(when))
	return nil
}

func (exp *localExpiration) ttl(byte, []byte) (int64, error) {
	return -1, nil
}

func (exp *localExpiration) delExpire(byte, []byte, *gorocksdb.WriteBatch) error {
	return nil
}

func createLocalDelFunc(dt common.DataType, db *RockDB) func(keys [][]byte) error {
	switch dt {
	case common.KV:
		return func(keys [][]byte) error {
			if err := db.BeginBatchWrite(); err != nil {
				return err
			}
			db.DelKeys(keys...)
			return db.CommitBatchWrite()
		}
	case common.HASH:
		return func(keys [][]byte) error {
			db.HMclear(keys...)
			return nil
		}
	case common.LIST:
		return func(keys [][]byte) error {
			_, err := db.LMclear(keys...)
			return err
		}
	case common.SET:
		return func(keys [][]byte) error {
			_, err := db.SMclear(keys...)
			return err
		}
	case common.ZSET:
		return func(keys [][]byte) error {
			_, err := db.ZMclear(keys...)
			return err
		}
	default:
		return nil
	}
}

// TODO, may use two buffers to store the keys and do 'copy-on-write' to reduce the
// critical section
type localBatch struct {
	sync.Mutex
	keys       [][]byte
	db         *RockDB
	dt         common.DataType
	localDelFn func([][]byte) error
}

func newLocalBatch(db *RockDB, dt common.DataType) *localBatch {
	// create a db object contains the same storage engine and options as the passed 'db' argument but isolated 'wb'
	// and 'isBatching' fields. The 'localDB' object will be used to delete the expired data independent of the passed
	// 'db' object which used by the logical layer.
	localDB := &RockDB{
		expiration:       &localExpiration{},
		cfg:              db.cfg,
		eng:              db.eng,
		dbOpts:           db.dbOpts,
		defaultWriteOpts: db.defaultWriteOpts,
		defaultReadOpts:  db.defaultReadOpts,
		wb:               gorocksdb.NewWriteBatch(),
	}

	batch := &localBatch{
		dt:   dt,
		db:   localDB,
		keys: make([][]byte, 0, localBatchedBufSize),
	}

	batch.localDelFn = createLocalDelFunc(dt, localDB)

	return batch
}

func (batch *localBatch) commit() error {
	defer batch.Unlock()
	batch.Lock()

	if len(batch.keys) == 0 {
		return nil
	}
	if err := batch.localDelFn(batch.keys); err != nil {
		return err
	} else {
		batch.keys = batch.keys[:0]
		return nil
	}
}

func (batch *localBatch) propose(key []byte) error {
	defer batch.Unlock()
	batch.Lock()

	batch.keys = append(batch.keys, key)

	if len(batch.keys) >= localBatchedBufSize {
		if err := batch.localDelFn(batch.keys); err != nil {
			return err
		}
		batch.keys = batch.keys[:0]
	}
	return nil
}

type lTTLChecker struct {
	sync.Mutex
	db *RockDB

	checking int32
	quitC    chan struct{}

	wg      sync.WaitGroup
	wb      *gorocksdb.WriteBatch
	batched [common.ALL - common.NONE]*localBatch

	//next check time
	nc int64
}

func newlTTlChecker(db *RockDB) *lTTLChecker {
	c := &lTTLChecker{
		db: db,
		nc: time.Now().Unix(),
		wb: gorocksdb.NewWriteBatch(),
	}

	for dt, _ := range c.batched {
		c.batched[dt] = newLocalBatch(db, common.DataType(dt))
	}

	return c
}

func (c *lTTLChecker) Start() {
	c.Lock()
	if !atomic.CompareAndSwapInt32(&c.checking, 0, 1) {
		c.Unlock()
		return
	}

	c.quitC = make(chan struct{})

	c.wg.Add(1)
	go func(stopCh chan struct{}) {
		dbLog.Infof("ttl checker of LocalDeletion started")
		t := time.NewTicker(time.Second * localExpCheckInterval)

		defer func() {
			t.Stop()
			c.wg.Done()
			dbLog.Infof("ttl checker of LocalDeletion exit")
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

func (c *lTTLChecker) Stop() {
	c.Lock()
	if atomic.LoadInt32(&c.checking) == 1 {
		close(c.quitC)
		c.Unlock()

		//we should unlock the Mutex before Wait to avoid
		//deadlock since the started goroutine requires the lock
		//at running
		c.wg.Wait()

		atomic.StoreInt32(&c.checking, 0)
		return
	}
	c.Unlock()
}

func (c *lTTLChecker) applyBatch(stopCh chan struct{}) {
	commitTicker := time.NewTicker(time.Second * localBatchedCommitInterval)

	defer func() {
		commitTicker.Stop()
		c.commitAllBatched()
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

func (c *lTTLChecker) commitAllBatched() {
	for _, bat := range c.batched {
		bat.commit()
	}
}

func (c *lTTLChecker) setNextCheckTime(when int64, force bool) {
	c.Lock()
	if force {
		c.nc = when
	} else if c.nc > when {
		c.nc = when
	}
	c.Unlock()
}

func (c *lTTLChecker) GetExpiredDataChan(common.DataType) chan *common.ExpiredData {
	return nil
}

func (c *lTTLChecker) check(stopChan chan struct{}) {
	defer func() {
		if e := recover(); e != nil {
			buf := make([]byte, 4096)
			n := runtime.Stack(buf, false)
			buf = buf[0:n]
			dbLog.Errorf("local deletion ttl check panic: %s:%v", buf, e)
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
	c.wb.Clear()

	it, err := NewDBRangeLimitIterator(c.db.eng, minKey, maxKey,
		common.RangeROpen, 0, -1, false)
	defer it.Close()
	if err != nil {
		c.setNextCheckTime(now+1, false)
		return
	} else if it == nil || !it.Valid() {
		c.setNextCheckTime(nc, false)
		return
	}

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
		//when := Uint64(it.Value())

		if tk == nil {
			continue
		}

		dt, k, nt, err := expDecodeTimeKey(tk)
		if err != nil {
			continue
		}

		scanned += 1
		if scanned == 1 { //log the first scanned key
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

		if err := c.batched[dataType2CommonType(dt)].propose(k); err != nil {
			dbLog.Warningf("propose to delete expired key [%s, %s] failed", TypeName[dt], string(k))
		}

		eCount += 1
		c.wb.Delete(tk)
		mk := expEncodeMetaKey(dt, k)
		c.wb.Delete(mk)

		if eCount%1024 == 0 {
			if err := c.db.eng.Write(c.db.defaultWriteOpts, c.wb); err != nil {
				dbLog.Warningf("delete expired time keys failed during ttl checking, err:%s", err.Error())
			}
			c.wb.Clear()
		}
	}

	c.setNextCheckTime(nc, false)

	if err := c.db.eng.Write(c.db.defaultWriteOpts, c.wb); err != nil {
		dbLog.Warningf("delete expired time keys failed during ttl checking, err:%s", err.Error())
	}
	c.wb.Clear()

	checkCost := time.Since(checkStart).Nanoseconds() / 1000
	dbLog.Infof("[%d/%d] keys have expired have been deleted during ttl checking, cost:%d us, the next checking will start at: %s",
		eCount, scanned, checkCost, time.Unix(nc, 0).Format(logTimeFormatStr))

	return
}
