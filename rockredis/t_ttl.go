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

type OnExpiredFunc func([]byte) error

const (
	expireCheckInterval = 1
	logTimeFormatStr    = "2006-01-02 15:04:05"
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

	if v, err := db.eng.GetBytes(db.defaultReadOpts, mk); err != nil {
		return err
	} else if v != nil {
		if preTime, err2 := Int64(v, nil); err2 == nil && preTime != when {
			preTk := expEncodeTimeKey(dataType, key, preTime)
			wb.Delete(preTk)
		}
	}

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

	if v, err := db.eng.GetBytes(db.defaultReadOpts, mk); err != nil {
		return err
	} else if v != nil {
		if preTime, err2 := Int64(v, nil); err2 == nil && preTime != when {
			preTk := expEncodeTimeKey(dataType, key, preTime)
			wb.Delete(preTk)
		}
	}

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
	if v, err := db.eng.GetBytes(db.defaultReadOpts, mk); err != nil {
		return err
	} else if v == nil {
		return nil
	} else if when, err2 := Int64(v, nil); err2 != nil {
		return err2
	} else {
		tk := expEncodeTimeKey(dataType, key, when)

		wb.Delete(mk)
		wb.Delete(tk)
		return nil
	}
}

type TTLChecker struct {
	sync.Mutex
	db *RockDB

	cbs      map[byte]OnExpiredFunc
	quitC    chan struct{}
	checking int32

	//next check time
	nc int64
}

func NewTTLChecker(db *RockDB) *TTLChecker {
	c := &TTLChecker{
		db:  db,
		cbs: make(map[byte]OnExpiredFunc),
		nc:  time.Now().Unix(),
	}

	return c
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

func (c *TTLChecker) RegisterKVExpired(f OnExpiredFunc) {
	c.register(KVType, f)
}

func (c *TTLChecker) RegisterListExpired(f OnExpiredFunc) {
	c.register(ListType, f)
}

func (c *TTLChecker) RegisterSetExpired(f OnExpiredFunc) {
	c.register(SetType, f)
}

func (c *TTLChecker) RegisterZSetExpired(f OnExpiredFunc) {
	c.register(ZSetType, f)
}

func (c *TTLChecker) RegisterHashExpired(f OnExpiredFunc) {
	c.register(HashType, f)
}

func (c *TTLChecker) register(dataType byte, f OnExpiredFunc) {
	c.cbs[dataType] = f
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

		cb := c.cbs[dt]
		if tk == nil || cb == nil {
			continue
		}

		if exp, err := Int64(c.db.eng.GetBytes(c.db.defaultReadOpts, mk)); err == nil {
			// check expire again
			if exp <= now {
				cb(k)
				eCount += 1
			}
		}
	}

	c.setNextCheckTime(nc, true)

	checkCost := time.Since(checkStart).Nanoseconds() / 1000
	dbLog.Infof("[%d/%d] keys have expired during ttl checking, cost:%d us, the next checking will start at: %s",
		eCount, scanned, checkCost, time.Unix(nc, 0).Format(logTimeFormatStr))

	return
}
