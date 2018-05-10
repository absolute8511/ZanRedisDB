package rockredis

import (
	"time"

	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/absolute8511/gorocksdb"
)

type consistencyExpiration struct {
	*TTLChecker
	db *RockDB
}

func newConsistencyExpiration(db *RockDB) *consistencyExpiration {
	exp := &consistencyExpiration{
		db:         db,
		TTLChecker: newTTLChecker(db),
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

func (exp *consistencyExpiration) Destroy() {
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

type expiredBufferWrapper struct {
	internal common.ExpiredDataBuffer
}

func (wrapper *expiredBufferWrapper) Write(meta *expiredMeta) error {
	if dt, key, _, err := expDecodeTimeKey(meta.timeKey); err != nil {
		return err
	} else {
		return wrapper.internal.Write(dataType2CommonType(dt), key)
	}
}

func (exp *consistencyExpiration) check(buffer common.ExpiredDataBuffer, stop chan struct{}) error {
	wrapper := &expiredBufferWrapper{internal: buffer}
	return exp.TTLChecker.check(wrapper, stop)
}
