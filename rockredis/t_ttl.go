package rockredis

import (
	"encoding/binary"
	"errors"
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

const (
	expireCheckInterval = 1
	expireLimitRange    = 8 * 1024
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

func (db *RockDB) KVExpire(key []byte, duration int64, c common.TTLChecker) error {
	return db.expire(KVType, key, duration, c)
}

func (db *RockDB) HashExpire(key []byte, duration int64, c common.TTLChecker) error {
	return db.expire(HashType, key, duration, c)
}

func (db *RockDB) ListExpire(key []byte, duration int64, c common.TTLChecker) error {
	return db.expire(ListType, key, duration, c)
}

func (db *RockDB) SetExpire(key []byte, duration int64, c common.TTLChecker) error {
	return db.expire(SetType, key, duration, c)
}

func (db *RockDB) ZSetExpire(key []byte, duration int64, c common.TTLChecker) error {
	return db.expire(ZSetType, key, duration, c)
}

func (db *RockDB) expire(dataType byte, key []byte, duration int64, c common.TTLChecker) error {
	return db.expireAt(dataType, key, time.Now().Unix()+duration, c)
}

func (db *RockDB) expireAt(dataType byte, key []byte, when int64, c common.TTLChecker) error {
	mk := expEncodeMetaKey(dataType, key)
	tk := expEncodeTimeKey(dataType, key, when)

	wb := db.wb
	wb.Clear()

	wb.Put(tk, mk)
	wb.Put(mk, PutInt64(when))

	if err := db.eng.Write(db.defaultWriteOpts, wb); err != nil {
		return err
	} else {
		//the underline type of c should be rockredis.*TTLChecker as using the RockDB to store the
		//expire data
		c.(*TTLChecker).setNextCheckTime(when, false)
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

func (db *RockDB) KVPersist(key []byte) error {
	db.wb.Clear()
	if err := db.delExpire(KVType, key, db.wb); err != nil {
		return err
	} else {
		return db.eng.Write(db.defaultWriteOpts, db.wb)
	}
}

func (db *RockDB) HashPersist(key []byte) error {
	db.wb.Clear()
	if err := db.delExpire(HashType, key, db.wb); err != nil {
		return err
	} else {
		return db.eng.Write(db.defaultWriteOpts, db.wb)
	}
}

func (db *RockDB) ListPersist(key []byte) error {
	db.wb.Clear()
	if err := db.delExpire(ListType, key, db.wb); err != nil {
		return err
	} else {
		return db.eng.Write(db.defaultWriteOpts, db.wb)
	}
}

func (db *RockDB) SetPersist(key []byte) error {
	db.wb.Clear()
	if err := db.delExpire(SetType, key, db.wb); err != nil {
		return err
	} else {
		return db.eng.Write(db.defaultWriteOpts, db.wb)
	}
}

func (db *RockDB) ZSetPersist(key []byte) error {
	db.wb.Clear()
	if err := db.delExpire(ZSetType, key, db.wb); err != nil {
		return err
	} else {
		return db.eng.Write(db.defaultWriteOpts, db.wb)
	}
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

	cbs      map[byte]common.OnExpiredFunc
	quitC    chan struct{}
	checking int32

	//next check time
	nc int64
}

func NewTTLChecker(db *RockDB) *TTLChecker {
	c := &TTLChecker{
		db:  db,
		cbs: make(map[byte]common.OnExpiredFunc),
		nc:  time.Now().Unix(),
	}

	return c
}

func (c *TTLChecker) ResetDB(rockdb *RockDB) {
	c.Lock()
	c.db = rockdb
	c.Unlock()
}

func (c *TTLChecker) Start() {
	if atomic.CompareAndSwapInt32(&c.checking, 0, 1) {
		c.quitC = make(chan struct{})

		cTicker := time.NewTicker(time.Second * expireCheckInterval)
		defer cTicker.Stop()

		for {
			select {
			case <-cTicker.C:
				c.check()
			case <-c.quitC:
				return
			}
		}
	}
}

func (c *TTLChecker) Stop() {
	if atomic.CompareAndSwapInt32(&c.checking, 1, 0) {
		close(c.quitC)
	}
}

func (c *TTLChecker) RegisterKVExpired(f common.OnExpiredFunc) {
	c.register(KVType, f)
}

func (c *TTLChecker) RegisterListExpired(f common.OnExpiredFunc) {
	c.register(ListType, f)
}

func (c *TTLChecker) RegisterSetExpired(f common.OnExpiredFunc) {
	c.register(SetType, f)
}

func (c *TTLChecker) RegisterZSetExpired(f common.OnExpiredFunc) {
	c.register(ZSetType, f)
}

func (c *TTLChecker) RegisterHashExpired(f common.OnExpiredFunc) {
	c.register(HashType, f)
}

func (c *TTLChecker) register(dataType byte, f common.OnExpiredFunc) {
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

func (c *TTLChecker) check() {
	now := time.Now().Unix()

	c.Lock()
	nc := c.nc
	db := c.db
	c.Unlock()

	if now < nc {
		return
	}

	nc = now + 3600

	minKey := expEncodeTimeKey(NoneType, nil, 0)
	maxKey := expEncodeTimeKey(maxDataType, nil, nc)

	stopCheck := false
	offset := 0

	for ; !stopCheck; offset += expireLimitRange {
		it, err := NewDBRangeLimitIterator(c.db.eng, minKey, maxKey,
			common.RangeClose, offset, expireLimitRange, false)

		if err != nil {
			nc = now + 1
			it.Close()
			break
		} else if it == nil || !it.Valid() {
			it.Close()
			break
		}

		now = time.Now().Unix()

		for ; it.Valid(); it.Next() {
			tk := it.Key()
			mk := it.Value()

			dt, k, nt, err := expDecodeTimeKey(tk)

			if err != nil {
				continue
			}

			if nt > now {
				//the next ttl check time is nt!
				nc = nt
				stopCheck = true
				break
			}

			cb := c.cbs[dt]
			if tk == nil || cb == nil {
				continue
			}

			if exp, err := Int64(db.eng.GetBytes(db.defaultReadOpts, mk)); err == nil {
				// check expire again
				if exp <= now {
					cb(k)

					//db.wb.Clear()
					//db.wb.Delete(tk)
					//db.wb.Delete(mk)
					//db.eng.Write(db.defaultWriteOpts, db.wb)
				}
			}
		}
		it.Close()
	}

	c.setNextCheckTime(nc, true)

	return
}
