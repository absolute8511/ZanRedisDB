package rockredis

import (
	"time"

	"github.com/youzan/ZanRedisDB/common"
	"github.com/youzan/gorocksdb"
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

func (exp *consistencyExpiration) encodeToRawValue(dataType byte, h *headerMetaValue, rawValue []byte) []byte {
	return rawValue
}

func (exp *consistencyExpiration) decodeRawValue(dataType byte, rawValue []byte) ([]byte, *headerMetaValue, error) {
	var h headerMetaValue
	return rawValue, &h, nil
}

func (exp *consistencyExpiration) isExpired(dataType byte, key []byte, rawValue []byte, useLock bool) (bool, error) {
	mk := expEncodeMetaKey(dataType, key)

	var t int64
	var err error
	if useLock {
		t, err = Int64(exp.db.eng.GetBytes(exp.db.defaultReadOpts, mk))
	} else {
		t, err = Int64(exp.db.eng.GetBytesNoLock(exp.db.defaultReadOpts, mk))
	}
	dbLog.Infof("key %v ttl: %v", string(key), t)
	if err != nil || t == 0 {
		return false, err
	}
	t -= time.Now().Unix()
	return t <= 0, nil
}

func (exp *consistencyExpiration) ExpireAt(dataType byte, key []byte, rawValue []byte, when int64) error {
	wb := exp.db.wb
	wb.Clear()

	_, err := exp.rawExpireAt(dataType, key, rawValue, when, wb)
	if err != nil {
		return err
	}
	if err := exp.db.eng.Write(exp.db.defaultWriteOpts, wb); err != nil {
		return err
	}
	return nil
}

func (exp *consistencyExpiration) rawExpireAt(dataType byte, key []byte, rawValue []byte, when int64, wb *gorocksdb.WriteBatch) ([]byte, error) {
	mk := expEncodeMetaKey(dataType, key)

	if t, err := Int64(exp.db.eng.GetBytesNoLock(exp.db.defaultReadOpts, mk)); err != nil {
		return rawValue, err
	} else if t != 0 {
		wb.Delete(expEncodeTimeKey(dataType, key, t))
	}

	tk := expEncodeTimeKey(dataType, key, when)
	if when == 0 {
		wb.Delete(mk)
		wb.Delete(tk)
		return rawValue, nil
	}
	wb.Put(tk, mk)
	wb.Put(mk, PutInt64(when))

	exp.setNextCheckTime(when, false)
	return rawValue, nil
}

func (exp *consistencyExpiration) ttl(dataType byte, key []byte, rawValue []byte) (int64, error) {
	mk := expEncodeMetaKey(dataType, key)
	t, err := Int64(exp.db.eng.GetBytes(exp.db.defaultReadOpts, mk))

	if err != nil || t == 0 {
		t = -1
	} else {
		dbLog.Infof("key %v ttl: %v", string(key), t)
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

func (exp *consistencyExpiration) delExpire(dataType byte, key []byte, rawValue []byte, keepV bool, wb *gorocksdb.WriteBatch) ([]byte, error) {
	mk := expEncodeMetaKey(dataType, key)

	if t, err := Int64(exp.db.eng.GetBytesNoLock(exp.db.defaultReadOpts, mk)); err != nil {
		return rawValue, err
	} else if t == 0 {
		return rawValue, nil
	} else {
		tk := expEncodeTimeKey(dataType, key, t)
		wb.Delete(tk)
		wb.Delete(mk)
		return rawValue, nil
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
