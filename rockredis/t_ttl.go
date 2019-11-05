package rockredis

import (
	"encoding/binary"
	"errors"
	"runtime"
	"sync"
	"time"

	"github.com/youzan/ZanRedisDB/common"
	"github.com/youzan/ZanRedisDB/engine"
	"github.com/youzan/gorocksdb"
)

var (
	errExpMetaKey = errors.New("invalid expire meta key")
	errExpTimeKey = errors.New("invalid expire time key")
)

const (
	logTimeFormatStr = "2006-01-02 15:04:05"
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

type expiredMeta struct {
	timeKey []byte
	metaKey []byte
	UTC     int64
}

type expiredMetaBuffer interface {
	Write(*expiredMeta) error
}

type expiration interface {
	getRawValueForHeader(ts int64, dt byte, key []byte) ([]byte, error)
	ExpireAt(dt byte, key []byte, rawValue []byte, when int64) (int64, error)
	rawExpireAt(dt byte, key []byte, rawValue []byte, when int64, wb *gorocksdb.WriteBatch) ([]byte, error)
	// should be called only in read operation
	ttl(ts int64, dt byte, key []byte, rawValue []byte) (int64, error)
	// if in raft write loop should avoid lock, otherwise lock should be used
	isExpired(ts int64, dt byte, key []byte, rawValue []byte, useLock bool) (bool, error)
	encodeToVersionKey(dt byte, h *headerMetaValue, key []byte) []byte
	decodeFromVersionKey(dt byte, vk []byte) ([]byte, int64, error)
	decodeRawValue(dt byte, rawValue []byte) ([]byte, *headerMetaValue, error)
	encodeToRawValue(dt byte, h *headerMetaValue, realValue []byte) []byte
	delExpire(dt byte, key []byte, rawValue []byte, keepValue bool, wb *gorocksdb.WriteBatch) ([]byte, error)
	check(common.ExpiredDataBuffer, chan struct{}) error
	Start()
	Stop()
	Destroy()
}

func (db *RockDB) expire(ts int64, dataType byte, key []byte, rawValue []byte, duration int64) (int64, error) {
	var err error
	if rawValue == nil {
		rawValue, err = db.expiration.getRawValueForHeader(ts, dataType, key)
		if err != nil {
			return 0, err
		}
	}
	return db.expiration.ExpireAt(dataType, key, rawValue, ts/int64(time.Second)+duration)
}

func (db *RockDB) KVTtl(key []byte) (t int64, err error) {
	tn := time.Now().UnixNano()
	v, err := db.expiration.getRawValueForHeader(tn, KVType, key)
	if err != nil {
		return -1, err
	}
	return db.ttl(tn, KVType, key, v)
}

func (db *RockDB) HashTtl(key []byte) (t int64, err error) {
	tn := time.Now().UnixNano()
	v, err := db.expiration.getRawValueForHeader(tn, HashType, key)
	if err != nil {
		return -1, err
	}
	return db.ttl(tn, HashType, key, v)
}

func (db *RockDB) ListTtl(key []byte) (t int64, err error) {
	tn := time.Now().UnixNano()
	v, err := db.expiration.getRawValueForHeader(tn, ListType, key)
	if err != nil {
		return -1, err
	}
	return db.ttl(tn, ListType, key, v)
}

func (db *RockDB) SetTtl(key []byte) (t int64, err error) {
	tn := time.Now().UnixNano()
	v, err := db.expiration.getRawValueForHeader(tn, SetType, key)
	if err != nil {
		return -1, err
	}
	return db.ttl(tn, SetType, key, v)
}

func (db *RockDB) ZSetTtl(key []byte) (t int64, err error) {
	tn := time.Now().UnixNano()
	v, err := db.expiration.getRawValueForHeader(tn, ZSetType, key)
	if err != nil {
		return -1, err
	}
	return db.ttl(tn, ZSetType, key, v)
}

type TTLChecker struct {
	sync.Mutex

	db       *RockDB
	watching int32
	wg       sync.WaitGroup

	//next check time
	nc int64
}

func newTTLChecker(db *RockDB) *TTLChecker {
	c := &TTLChecker{
		db: db,
		nc: time.Now().Unix(),
	}
	return c
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

func (c *TTLChecker) check(expiredBuf expiredMetaBuffer, stop chan struct{}) (err error) {
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
		return nil
	}

	nc = now + 3600

	minKey := expEncodeTimeKey(NoneType, nil, 0)
	maxKey := expEncodeTimeKey(maxDataType, nil, nc)

	var eCount int64
	var scanned int64
	checkStart := time.Now()

	it, err := engine.NewDBRangeLimitIterator(c.db.eng, minKey, maxKey,
		common.RangeROpen, 0, -1, false)
	if err != nil {
		c.setNextCheckTime(now, false)
		return err
	} else if it == nil {
		c.setNextCheckTime(nc, true)
		return nil
	}
	defer it.Close()

	for ; it.Valid(); it.Next() {
		if scanned%100 == 0 {
			select {
			case <-stop:
				nc = now
				break
			default:
			}
		}
		tk := it.Key()
		mk := it.Value()

		if tk == nil {
			continue
		}

		dt, k, nt, dErr := expDecodeTimeKey(tk)
		if dErr != nil {
			continue
		}

		scanned += 1
		if scanned == 1 {
			//log the first scanned key
			dbLog.Debugf("ttl check start at key:[%s] of type:%s whose expire time is: %s", string(k),
				TypeName[dt], time.Unix(nt, 0).Format(logTimeFormatStr))
		}

		if nt > now {
			//the next ttl check time is nt!
			nc = nt
			dbLog.Debugf("ttl check end at key:[%s] of type:%s whose expire time is: %s", string(k),
				TypeName[dt], time.Unix(nt, 0).Format(logTimeFormatStr))
			break
		}

		eCount += 1

		err = expiredBuf.Write(&expiredMeta{timeKey: tk, metaKey: mk, UTC: nt})
		if err != nil {
			nc = now
			break
		}
	}

	c.setNextCheckTime(nc, true)

	checkCost := time.Since(checkStart).Nanoseconds() / 1000
	if dbLog.Level() >= common.LOG_DEBUG || eCount > 10000 || checkCost >= time.Second.Nanoseconds() {
		dbLog.Infof("[%d/%d] keys have expired during ttl checking, cost:%d us", eCount, scanned, checkCost)
	}

	return err
}
