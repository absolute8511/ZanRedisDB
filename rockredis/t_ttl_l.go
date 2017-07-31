package rockredis

import (
	"errors"
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
	db      *RockDB
	wb      *gorocksdb.WriteBatch
	stopCh  chan struct{}
	wg      sync.WaitGroup
	buffer  []*expiredMeta
	batched [common.ALL - common.NONE]*localBatch
}

func newLocalExpiration(db *RockDB) *localExpiration {
	exp := &localExpiration{
		db:         db,
		wb:         gorocksdb.NewWriteBatch(),
		TTLChecker: newTTLChecker(db, localExpCheckInterval),
		stopCh:     make(chan struct{}),
	}

	for dt, _ := range exp.batched {
		exp.batched[dt] = newLocalBatch(db, common.DataType(dt))
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

func (exp *localExpiration) WatchExpired(receiver chan *common.ExpiredData, stopCh chan struct{}) error {
	return errors.New("can not watch expired through local expiration policy")
}

func (exp *localExpiration) Start() {
	exp.wg.Add(1)
	go func() {
		defer exp.wg.Done()
		exp.applyExpiration(exp.stopCh)
	}()
}

func (exp *localExpiration) applyExpiration(stop chan struct{}) {
	exp.buffer = make([]*expiredMeta, 0, localBatchedBufSize)

	t := time.NewTicker(time.Second * localExpCheckInterval)
	defer t.Stop()

	dbLog.Infof("start to apply-expiration using Local-Deletion policy")
	defer dbLog.Infof("apply-expiration using Local-Deletion policy exit")

	checker := exp.TTLChecker

	for _ = range t.C {
		receiver := make(chan *expiredMeta, 1024)
		watchStop := make(chan struct{})

		exp.wg.Add(1)
		go func(watchStop chan struct{}) {
			defer exp.wg.Done()
			select {
			case <-stop:
				//close the watchStop channel to stop watch expired data
				//if the channel has not been closed
				select {
				case <-watchStop:
				default:
					close(watchStop)
				}
			case <-watchStop:
			}
			return
		}(watchStop)

		exp.wg.Add(1)
		go func() {
			defer exp.wg.Done()
			checker.watchExpiredOnce(receiver, watchStop)
		}()

		for meta := range receiver {
			exp.buffer = append(exp.buffer, meta)

			if len(exp.buffer) >= localBatchedBufSize {
				// close the watchStop channel to stop watch expired data when the
				// buffer is full if the channel has not been closed
				close(watchStop)

				// receive all the pending data from the 'receiver'
				for meta := range receiver {
					exp.buffer = append(exp.buffer, meta)
				}
				break
			}
		}

		select {
		case <-watchStop:
		default:
			close(watchStop)
		}

		select {
		case <-stop:
			return
		default:
			exp.commitBuffered()
		}
	}
}

func (exp *localExpiration) commitBuffered() {
	if len(exp.buffer) == 0 {
		return
	}

	exp.wb.Clear()

	for i, v := range exp.buffer {
		dt, key, _, err := expDecodeTimeKey(v.timeKey)
		if err != nil {
			dbLog.Errorf("decode time-key failed, bad data encounter, err:%s", err.Error())
			continue
		}

		if err := exp.batched[dataType2CommonType(dt)].propose(key); err != nil {
			dbLog.Errorf("batch delete expired data of type:%s failed, err:%s", TypeName[dt], err.Error())
		}

		exp.wb.Delete(v.timeKey)
		exp.wb.Delete(v.metaKey)

		if i%1024 == 0 {
			if err := exp.db.eng.Write(exp.db.defaultWriteOpts, exp.wb); err != nil {
				dbLog.Errorf("delete meta data about expired data failed, err:%s", err.Error())
			}
			exp.wb.Clear()
		}
	}

	exp.buffer = exp.buffer[:0]

	for _, batch := range exp.batched {
		if err := batch.commit(); err != nil {
			dbLog.Errorf("batch delete expired data of type:%s failed, err:%s", batch.dt.String(), err.Error())
		}
	}

	if err := exp.db.eng.Write(exp.db.defaultWriteOpts, exp.wb); err != nil {
		dbLog.Errorf("delete meta data about expired data failed, %s", err.Error())
	}
	exp.wb.Clear()
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
