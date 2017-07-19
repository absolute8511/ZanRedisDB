package rockredis

import (
	"encoding/binary"
	"errors"
	"time"

	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/absolute8511/gorocksdb"
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

type expiration interface {
	rawExpireAt(byte, []byte, int64, *gorocksdb.WriteBatch) error
	expireAt(byte, []byte, int64) error
	ttl(byte, []byte) (int64, error)
	delExpire(byte, []byte, *gorocksdb.WriteBatch) error
	GetExpiredDataChan(common.DataType) chan *common.ExpiredData
	Start()
	Stop()
}

func (db *RockDB) expire(dataType byte, key []byte, duration int64) error {
	return db.expiration.expireAt(dataType, key, time.Now().Unix()+duration)
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
