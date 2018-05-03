package rockredis

import (
	"errors"
	"sync"
	"time"

	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/absolute8511/gorocksdb"
)

var localExpCheckInterval = 300

const (
	localBatchedBufSize = 16 * 1024
	// should less than max batch keys for batch write
	localBatchedMaxKeysNum = 1024
)

var (
	ErrLocalBatchFullToCommit = errors.New("batched is fully filled and should commit right now")
	ErrLocalBatchedBuffFull   = errors.New("the local batched buffer is fully filled")
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

	t := time.NewTicker(time.Second * time.Duration(localExpCheckInterval))
	defer t.Stop()

	checker := exp.TTLChecker

	for {
		select {
		case <-t.C:
			for {
				err := checker.check(exp.localBuffer, stop)
				select {
				case <-stop:
					return
				default:
					exp.localBuffer.commit()
				}
				//start the next check immediately if the last check is stopped because of the buffer is fully filled
				if err == ErrLocalBatchedBuffFull {
					continue
				} else if err != nil {
					dbLog.Errorf("check expired data failed at applying expiration, err:%s", err.Error())
				}
				break
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

func (lbb *localBatchedBuffer) Destroy() {
	if lbb.wb != nil {
		lbb.wb.Destroy()
	}
	for _, b := range lbb.batched {
		if b != nil {
			b.destroy()
		}
	}
}

func (self *localBatchedBuffer) Write(meta *expiredMeta) error {
	if len(self.buff) >= self.cap {
		return ErrLocalBatchedBuffFull
	} else {
		self.buff = append(self.buff, meta)
		return nil
	}
}

func (self *localBatchedBuffer) commit() {
	if len(self.buff) == 0 {
		return
	}

	for _, v := range self.buff {
		dt, key, _, err := expDecodeTimeKey(v.timeKey)
		if err != nil || dataType2CommonType(dt) == common.NONE {
			dbLog.Errorf("decode time-key failed, bad data encounter, err:%s", err.Error())
			continue
		}

		batched := self.batched[dataType2CommonType(dt)]

		err = batched.propose(v.timeKey, v.metaKey, key)
		if err == ErrLocalBatchFullToCommit {
			err = batched.commit()

			//propose the expired-data again as the
			//last propose has failed as the buffer is full
			batched.propose(v.timeKey, v.metaKey, key)
		}
		if err != nil {
			dbLog.Errorf("batch delete expired data of type:%s failed, err:%s", TypeName[dt], err.Error())
		}
	}

	//clean the buffer
	self.buff = self.buff[:0]
	for t := common.KV; t < common.ALL; t++ {
		if err := self.batched[t].commit(); err != nil {
			dbLog.Errorf("batch delete expired data of type:%s failed, err:%s", common.DataType(t).String(), err.Error())
		}
	}
}

func (exp *localExpiration) Stop() {
	close(exp.stopCh)
	exp.wg.Wait()
	if exp.wb != nil {
		exp.wb.Destroy()
	}
	if exp.localBuffer != nil {
		exp.localBuffer.Destroy()
	}
}

func createLocalDelFunc(dt common.DataType, db *RockDB, wb *gorocksdb.WriteBatch) func(keys [][]byte) error {
	switch dt {
	case common.KV:
		return func(keys [][]byte) error {
			defer wb.Clear()
			for _, k := range keys {
				db.KVDelWithBatch(k, wb)
			}
			err := db.eng.Write(db.defaultWriteOpts, wb)
			if err != nil {
				return err
			}
			for _, k := range keys {
				db.delPFCache(k)
			}
			return nil
		}
	case common.HASH:
		return func(keys [][]byte) error {
			defer wb.Clear()
			for _, hkey := range keys {
				if err := db.hClearWithBatch(hkey, wb); err != nil {
					return err
				}
			}
			return db.eng.Write(db.defaultWriteOpts, wb)
		}
	case common.LIST:
		return func(keys [][]byte) error {
			defer wb.Clear()
			if err := db.lMclearWithBatch(wb, keys...); err != nil {
				return err
			}
			return db.eng.Write(db.defaultWriteOpts, wb)
		}
	case common.SET:
		return func(keys [][]byte) error {
			defer wb.Clear()
			if err := db.sMclearWithBatch(wb, keys...); err != nil {
				return err
			}
			return db.eng.Write(db.defaultWriteOpts, wb)
		}
	case common.ZSET:
		return func(keys [][]byte) error {
			defer wb.Clear()
			if err := db.zMclearWithBatch(wb, keys...); err != nil {
				return err
			}
			return db.eng.Write(db.defaultWriteOpts, wb)
		}
	default:
		return nil
	}
}

type localBatch struct {
	keys       [][]byte
	dt         common.DataType
	wb         *gorocksdb.WriteBatch
	localDelFn func([][]byte) error
}

func newLocalBatch(db *RockDB, dt common.DataType) *localBatch {
	batch := &localBatch{
		dt:   dt,
		wb:   gorocksdb.NewWriteBatch(),
		keys: make([][]byte, 0, localBatchedMaxKeysNum),
	}
	batch.localDelFn = createLocalDelFunc(dt, db, batch.wb)
	return batch
}

func (batch *localBatch) destroy() {
	if batch.wb != nil {
		batch.wb.Destroy()
	}
}

func (batch *localBatch) commit() error {
	if len(batch.keys) == 0 {
		return nil
	}
	err := batch.localDelFn(batch.keys)
	batch.keys = batch.keys[:0]
	return err
}

func (batch *localBatch) propose(tk []byte, mk []byte, key []byte) error {
	if len(batch.keys) >= localBatchedMaxKeysNum {
		return ErrLocalBatchFullToCommit
	} else {
		batch.wb.Delete(tk)
		batch.wb.Delete(mk)
		batch.keys = append(batch.keys, key)
		return nil
	}
}
