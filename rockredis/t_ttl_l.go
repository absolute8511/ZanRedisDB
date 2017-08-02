package rockredis

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/absolute8511/gorocksdb"
)

const (
	localExpCheckInterval = 300
	localBatchedBufSize   = 16 * 1024
)

type localExpiration struct {
	*TTLChecker
	db          *RockDB
	wb          *gorocksdb.WriteBatch
	stopCh      chan struct{}
	wg          sync.WaitGroup
	localBuffer *localBatchedBuffer
}

func newLocalExpiration(db *RockDB) *localExpiration {
	exp := &localExpiration{
		db:          db,
		wb:          gorocksdb.NewWriteBatch(),
		TTLChecker:  newTTLChecker(db),
		stopCh:      make(chan struct{}),
		localBuffer: newLocalBatchedBuffer(db, localBatchedBufSize),
	}

	return exp
}

func (exp *localExpiration) expireAt(dataType byte, key []byte, when int64) error {
	wb := exp.db.wb
	wb.Clear()

	tk := expEncodeTimeKey(dataType, key, when)
	mk := expEncodeMetaKey(dataType, key)

	wb.Put(tk, mk)

	if err := exp.db.eng.Write(exp.db.defaultWriteOpts, wb); err != nil {
		return err
	} else {
		exp.setNextCheckTime(when, false)
		return nil
	}
}

func (exp *localExpiration) rawExpireAt(dataType byte, key []byte, when int64, wb *gorocksdb.WriteBatch) error {
	tk := expEncodeTimeKey(dataType, key, when)
	mk := expEncodeMetaKey(dataType, key)
	wb.Put(tk, mk)
	return nil
}

func (exp *localExpiration) ttl(byte, []byte) (int64, error) {
	return -1, nil
}

func (exp *localExpiration) delExpire(byte, []byte, *gorocksdb.WriteBatch) error {
	return nil
}

func (exp *localExpiration) check(buffer common.ExpiredDataBuffer, stop chan struct{}) error {
	return errors.New("can not check expired data through local expiration policy")
}

func (exp *localExpiration) Start() {
	exp.wg.Add(1)
	go func() {
		defer exp.wg.Done()
		exp.applyExpiration(exp.stopCh)
	}()
}

func (exp *localExpiration) applyExpiration(stop chan struct{}) {
	dbLog.Infof("start to apply-expiration using Local-Deletion policy")
	defer dbLog.Infof("apply-expiration using Local-Deletion policy exit")

	t := time.NewTicker(time.Second * localExpCheckInterval)
	defer t.Stop()

	checker := exp.TTLChecker

	for {
		select {
		case <-t.C:
			if err := checker.check(exp.localBuffer, stop); err != nil {
				dbLog.Errorf("check expired data failed at applying expiration, err:%s", err.Error())
			}

			select {
			case <-stop:
				return
			default:
				exp.localBuffer.commit()
			}
		case <-stop:
			return
		}
	}
}

type localBatchedBuffer struct {
	db      *RockDB
	wb      *gorocksdb.WriteBatch
	buff    []*expiredMeta
	batched [common.ALL - common.NONE]*localBatch
	cap     int
}

func newLocalBatchedBuffer(db *RockDB, cap int) *localBatchedBuffer {
	batchedBuff := &localBatchedBuffer{
		buff: make([]*expiredMeta, 0, cap),
		wb:   gorocksdb.NewWriteBatch(),
		db:   db,
		cap:  cap,
	}

	types := []common.DataType{common.KV, common.LIST, common.HASH,
		common.SET, common.ZSET}

	for _, t := range types {
		batchedBuff.batched[t] = newLocalBatch(db, t)
	}

	return batchedBuff
}

func (self *localBatchedBuffer) Write(meta *expiredMeta) error {
	if len(self.buff) >= self.cap {
		return fmt.Errorf("the local batched buffer is full, capacity:%d", self.cap)
	}
	self.buff = append(self.buff, meta)
	return nil
}

func (self *localBatchedBuffer) Full() bool {
	return len(self.buff) >= self.cap
}

func (self *localBatchedBuffer) commit() {
	if len(self.buff) == 0 {
		return
	}

	db := self.db
	wb := self.wb
	wb.Clear()

	for i, v := range self.buff {
		dt, key, _, err := expDecodeTimeKey(v.timeKey)
		if err != nil {
			dbLog.Errorf("decode time-key failed, bad data encounter, err:%s", err.Error())
			continue
		}

		if err := self.batched[dataType2CommonType(dt)].propose(key); err != nil {
			dbLog.Errorf("batch delete expired data of type:%s failed, err:%s", TypeName[dt], err.Error())
		}

		wb.Delete(v.timeKey)
		wb.Delete(v.metaKey)

		if i%1024 == 0 {
			if err := db.eng.Write(db.defaultWriteOpts, wb); err != nil {
				dbLog.Errorf("delete meta data about expired data failed, err:%s", err.Error())
			}
			wb.Clear()
		}
	}

	self.buff = self.buff[:0]

	for t := common.KV; t < common.ALL; t++ {
		if err := self.batched[t].commit(); err != nil {
			dbLog.Errorf("batch delete expired data of type:%s failed, err:%s", common.DataType(t).String(), err.Error())
		}
	}

	if err := db.eng.Write(db.defaultWriteOpts, wb); err != nil {
		dbLog.Errorf("delete meta data about expired data failed, %s", err.Error())
	}
	wb.Clear()
}

func (exp *localExpiration) Stop() {
	close(exp.stopCh)
	exp.wg.Wait()
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

type localBatch struct {
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
		keys: make([][]byte, 0, 1024),
	}
	batch.localDelFn = createLocalDelFunc(dt, localDB)

	return batch
}

func (batch *localBatch) commit() error {
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
	batch.keys = append(batch.keys, key)
	if len(batch.keys) >= 1024 {
		if err := batch.localDelFn(batch.keys); err != nil {
			return err
		}
		batch.keys = batch.keys[:0]
	}
	return nil
}
