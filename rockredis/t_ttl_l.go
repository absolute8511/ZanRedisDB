package rockredis

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/youzan/ZanRedisDB/common"
	"github.com/youzan/ZanRedisDB/engine"
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
	errTTLCheckTooLong        = errors.New("the local ttl scan take too long")
	errChangeTTLNotSupported  = errors.New("change ttl is not supported in current expire policy")
)

type localExpiration struct {
	*TTLChecker
	db      *RockDB
	stopCh  chan struct{}
	wg      sync.WaitGroup
	running int32
}

func newLocalExpiration(db *RockDB) *localExpiration {
	exp := &localExpiration{
		db:         db,
		TTLChecker: newTTLChecker(db),
	}

	return exp
}

func (exp *localExpiration) encodeToVersionKey(dt byte, h *headerMetaValue, key []byte) []byte {
	return key
}

func (exp *localExpiration) decodeFromVersionKey(dt byte, key []byte) ([]byte, int64, error) {
	return key, 0, nil
}

func (exp *localExpiration) encodeToRawValue(dataType byte, h *headerMetaValue) []byte {
	return h.UserData
}

func (exp *localExpiration) decodeRawValue(dataType byte, rawValue []byte) (*headerMetaValue, error) {
	var h headerMetaValue
	h.UserData = rawValue
	return &h, nil
}

func (exp *localExpiration) getRawValueForHeader(ts int64, dataType byte, key []byte) ([]byte, error) {
	return nil, nil
}

func (exp *localExpiration) isExpired(ts int64, dataType byte, key []byte, rawValue []byte, useLock bool) (bool, error) {
	return false, nil
}

func (exp *localExpiration) ExpireAt(dataType byte, key []byte, rawValue []byte, when int64) (int64, error) {
	if when == 0 {
		return 0, errChangeTTLNotSupported
	}
	wb := exp.db.wb
	defer wb.Clear()
	_, err := exp.rawExpireAt(dataType, key, rawValue, when, wb)
	if err != nil {
		return 0, err
	}
	if err := exp.db.rockEng.Write(wb); err != nil {
		return 0, err
	}
	return 1, nil
}

func (exp *localExpiration) rawExpireAt(dataType byte, key []byte, rawValue []byte, when int64, wb engine.WriteBatch) ([]byte, error) {
	tk := expEncodeTimeKey(dataType, key, when)
	mk := expEncodeMetaKey(dataType, key)
	wb.Put(tk, mk)
	exp.setNextCheckTime(when, false)
	return rawValue, nil
}

func (exp *localExpiration) ttl(int64, byte, []byte, []byte) (int64, error) {
	return -1, nil
}

func (exp *localExpiration) renewOnExpired(ts int64, dataType byte, key []byte, oldh *headerMetaValue) {
	// local expire should not renew on expired data, since it will be checked by expire handler
	// and it will clean ttl and all the sub data
	return
}

func (exp *localExpiration) delExpire(dt byte, key []byte, rawv []byte, keepV bool, wb engine.WriteBatch) ([]byte, error) {
	return rawv, nil
}

func (exp *localExpiration) check(buffer common.ExpiredDataBuffer, stop chan struct{}) error {
	return errors.New("can not check expired data through local expiration policy")
}

func (exp *localExpiration) Start() {
	if atomic.CompareAndSwapInt32(&exp.running, 0, 1) {
		exp.stopCh = make(chan struct{})
		exp.wg.Add(1)
		go func() {
			defer exp.wg.Done()
			exp.applyExpiration(exp.stopCh)
		}()
	}
}

func (exp *localExpiration) applyExpiration(stop chan struct{}) {
	dbLog.Infof("start to apply-expiration using Local-Deletion policy")
	defer dbLog.Infof("apply-expiration using Local-Deletion policy exit")

	t := time.NewTicker(time.Second * time.Duration(localExpCheckInterval))
	defer t.Stop()

	checker := exp.TTLChecker
	localBuffer := newLocalBatchedBuffer(exp.db, localBatchedBufSize)
	defer localBuffer.Destroy()
	for {
		select {
		case <-t.C:
			for {
				err := checker.check(localBuffer, stop)
				select {
				case <-stop:
					localBuffer.Clear()
					return
				default:
					localBuffer.commit()
				}
				//start the next check immediately if the last check is stopped because of the buffer is fully filled
				if err == ErrLocalBatchedBuffFull {
					// avoid 100% cpu
					time.Sleep(time.Millisecond)
					continue
				} else if err == errTTLCheckTooLong {
					// do compact for ttl
					checker.compactTTLMeta()
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
	buff    []*expiredMeta
	batched [common.ALL - common.NONE]*localBatch
	cap     int
}

func newLocalBatchedBuffer(db *RockDB, cap int) *localBatchedBuffer {
	batchedBuff := &localBatchedBuffer{
		buff: make([]*expiredMeta, 0, cap),
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

func (lbb *localBatchedBuffer) Clear() {
	lbb.buff = lbb.buff[:0]
	for _, b := range lbb.batched {
		if b != nil {
			b.clear()
		}
	}
}

func (lbb *localBatchedBuffer) Destroy() {
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
			// currently the bitmap/json type is not supported
			if err != nil {
				dbLog.Errorf("decode time-key failed, bad data encounter, err:%s, %v", err, dt)
			}
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
	if atomic.CompareAndSwapInt32(&exp.running, 1, 0) {
		close(exp.stopCh)
		exp.wg.Wait()
	}
}

func (exp *localExpiration) Destroy() {
	exp.Stop()
}

func createLocalDelFunc(dt common.DataType, db *RockDB, wb engine.WriteBatch) func(keys [][]byte) error {
	switch dt {
	case common.KV:
		return func(keys [][]byte) error {
			defer wb.Clear()
			for _, k := range keys {
				db.KVDelWithBatch(k, wb)
			}
			err := db.rockEng.Write(wb)
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
			return db.rockEng.Write(wb)
		}
	case common.LIST:
		return func(keys [][]byte) error {
			defer wb.Clear()
			if err := db.lMclearWithBatch(wb, keys...); err != nil {
				return err
			}
			return db.rockEng.Write(wb)
		}
	case common.SET:
		return func(keys [][]byte) error {
			defer wb.Clear()
			if err := db.sMclearWithBatch(wb, keys...); err != nil {
				return err
			}
			return db.rockEng.Write(wb)
		}
	case common.ZSET:
		return func(keys [][]byte) error {
			defer wb.Clear()
			if err := db.zMclearWithBatch(wb, keys...); err != nil {
				return err
			}
			return db.rockEng.Write(wb)
		}
	default:
		// TODO: currently bitmap/json is not handled
		return nil
	}
}

type localBatch struct {
	keys       [][]byte
	dt         common.DataType
	wb         engine.WriteBatch
	localDelFn func([][]byte) error
}

func newLocalBatch(db *RockDB, dt common.DataType) *localBatch {
	batch := &localBatch{
		dt:   dt,
		wb:   db.rockEng.NewWriteBatch(),
		keys: make([][]byte, 0, localBatchedMaxKeysNum),
	}
	batch.localDelFn = createLocalDelFunc(dt, db, batch.wb)
	return batch
}

func (batch *localBatch) clear() {
	if batch.wb != nil {
		batch.wb.Clear()
	}
	batch.keys = batch.keys[:0]
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
