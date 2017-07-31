package rockredis

import (
	"time"

	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/absolute8511/gorocksdb"
)

const (
	consistExpCheckInterval = 1
)

type consistencyExpiration struct {
	*TTLChecker
	db *RockDB
}

func newConsistencyExpiration(db *RockDB) *consistencyExpiration {
	exp := &consistencyExpiration{
		db:         db,
		TTLChecker: newTTLChecker(db, consistExpCheckInterval),
	}
	return exp
}

func (exp *consistencyExpiration) expireAt(dataType byte, key []byte, when int64) error {
	mk := expEncodeMetaKey(dataType, key)

	wb := exp.db.wb
	wb.Clear()

	if t, err := Int64(exp.db.eng.GetBytes(exp.db.defaultReadOpts, mk)); err != nil {
		return err
	} else if t != 0 {
		wb.Delete(expEncodeTimeKey(dataType, key, t))
	}

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

	if t, err := Int64(exp.db.eng.GetBytes(exp.db.defaultReadOpts, mk)); err != nil {
		return err
	} else if t != 0 {
		wb.Delete(expEncodeTimeKey(dataType, key, t))
	}

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

func (exp *consistencyExpiration) Start() {
}

func (exp *consistencyExpiration) Stop() {

}

func (exp *consistencyExpiration) delExpire(dataType byte, key []byte, wb *gorocksdb.WriteBatch) error {
	mk := expEncodeMetaKey(dataType, key)

	if t, err := Int64(exp.db.eng.GetBytes(exp.db.defaultReadOpts, mk)); err != nil {
		return err
	} else if t == 0 {
		return nil
	} else {
		tk := expEncodeTimeKey(dataType, key, t)
		wb.Delete(tk)
		wb.Delete(mk)
		return nil
	}
}

func (exp *consistencyExpiration) WatchExpired(receiver chan *common.ExpiredData, stop chan struct{}) error {
	defer close(receiver)

	metaReceiver := make(chan *expiredMeta, len(receiver))

	go func() {
		for meta := range metaReceiver {
			dt, key, _, _ := expDecodeTimeKey(meta.timeKey)
			receiver <- &common.ExpiredData{DataType: dataType2CommonType(dt), Key: key}
		}
	}()

	return exp.TTLChecker.watchExpired(metaReceiver, stop)
}
