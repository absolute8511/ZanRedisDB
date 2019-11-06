package rockredis

import (
	"encoding/binary"
	"errors"
	math "math"
	"time"

	"github.com/youzan/ZanRedisDB/common"
	"github.com/youzan/gorocksdb"
)

var errHeaderMetaValue = errors.New("invalid header meta value")
var errHeaderVersion = errors.New("invalid header version")
var errExpOverflow = errors.New("expiration time overflow")

const headerV1Len = 1 + 4 + 8

type headerMetaValue struct {
	Ver          byte
	ExpireAt     uint32
	ValueVersion int64
	UserData     []byte
}

func newHeaderMetaV1() *headerMetaValue {
	return &headerMetaValue{
		Ver: byte(common.ValueHeaderV1),
	}
}

func (h *headerMetaValue) hdlen() int {
	if h.Ver == byte(common.ValueHeaderV1) {
		return headerV1Len
	}
	return 0
}

// only useful for compact ttl policy
func (h *headerMetaValue) ttl(ts int64) int64 {
	if h.ExpireAt == 0 {
		return -1
	}
	// should not use time now to check ttl, since it may be different on different nodes
	ttl := int64(h.ExpireAt) - ts/int64(time.Second)
	if ttl <= 0 {
		ttl = -1
	}
	return ttl
}

// only useful for compact ttl policy
func (h *headerMetaValue) isExpired(ts int64) bool {
	if h.Ver != byte(common.ValueHeaderV1) {
		return false
	}
	if h.ExpireAt == 0 || ts == 0 {
		return false
	}
	// should not use time now to check ttl, since it may be different on different nodes
	ttl := int64(h.ExpireAt) - ts/int64(time.Second)
	return ttl <= 0
}

func (h *headerMetaValue) encodeTo(old []byte) (int, []byte) {
	switch h.Ver {
	case byte(0):
		return 0, old
	case byte(common.ValueHeaderV1):
		b := old
		if len(old) < headerV1Len {
			b = make([]byte, headerV1Len)
		}
		b[0] = h.Ver
		binary.BigEndian.PutUint32(b[1:], uint32(h.ExpireAt))
		binary.BigEndian.PutUint64(b[1+4:], uint64(h.ValueVersion))
		return headerV1Len, b
	default:
		panic("unknown value header")
	}
}

func (h *headerMetaValue) encodeWithDataTo(old []byte) []byte {
	var n int
	n, old = h.encodeTo(old)
	if h.UserData == nil {
		return old
	}
	copy(old[n:], h.UserData)
	return old
}

func (h *headerMetaValue) encodeWithData() []byte {
	b := make([]byte, h.hdlen()+len(h.UserData))
	return h.encodeWithDataTo(b)
}

func (h *headerMetaValue) encode() []byte {
	b := make([]byte, h.hdlen())
	_, b = h.encodeTo(b)
	return b
}

func (h *headerMetaValue) decode(b []byte) (int, error) {
	if len(b) < headerV1Len {
		return 0, errHeaderMetaValue
	}
	h.Ver = b[0]
	if h.Ver != byte(common.ValueHeaderV1) {
		return 0, errHeaderVersion
	}
	h.ExpireAt = binary.BigEndian.Uint32(b[1:])
	h.ValueVersion = int64(binary.BigEndian.Uint64(b[1+4:]))
	h.UserData = b[headerV1Len:]
	return headerV1Len, nil
}

type compactExpiration struct {
	db       *RockDB
	localExp *localExpiration
}

func newCompactExpiration(db *RockDB) *compactExpiration {
	exp := &compactExpiration{
		db:       db,
		localExp: newLocalExpiration(db),
	}
	return exp
}

func encodeVerKey(h *headerMetaValue, key []byte) []byte {
	var b []byte
	b, _ = EncodeMemCmpKey(b, key, defaultSep, h.ValueVersion, defaultSep)
	return b
}

func (exp *compactExpiration) encodeToVersionKey(dt byte, h *headerMetaValue, key []byte) []byte {
	switch dt {
	case HashType:
		return encodeVerKey(h, key)
	default:
		return exp.localExp.encodeToVersionKey(dt, h, key)
	}
}

func (exp *compactExpiration) decodeFromVersionKey(dt byte, key []byte) ([]byte, int64, error) {
	switch dt {
	case HashType:
		vals, err := Decode(key, len(key))
		if err != nil {
			return nil, 0, err
		}
		if len(vals) < 4 {
			return nil, 0, errors.New("error version key")
		}
		// key, sep, value version, sep
		key := vals[0].([]byte)
		ver := vals[2].(int64)
		return key, ver, nil
	default:
		return exp.localExp.decodeFromVersionKey(dt, key)
	}
}

func (exp *compactExpiration) encodeToRawValue(dataType byte, h *headerMetaValue, realValue []byte) []byte {
	switch dataType {
	case HashType, KVType:
		if h == nil {
			h = newHeaderMetaV1()
		}
		h.UserData = realValue
		v := h.encodeWithData()
		return v
	default:
		return exp.localExp.encodeToRawValue(dataType, h, realValue)
	}
}

func (exp *compactExpiration) decodeRawValue(dataType byte, rawValue []byte) ([]byte, *headerMetaValue, error) {
	h := newHeaderMetaV1()
	switch dataType {
	case HashType, KVType:
		if rawValue == nil {
			return nil, h, nil
		}
		_, err := h.decode(rawValue)
		if err != nil {
			return rawValue, h, err
		}
		return h.UserData, h, nil
	default:
		return exp.localExp.decodeRawValue(dataType, rawValue)
	}
}

func (exp *compactExpiration) getRawValueForHeader(ts int64, dataType byte, key []byte) ([]byte, error) {
	switch dataType {
	case KVType:
		_, _, v, _, err := exp.db.getRawDBKVValue(ts, key, true)
		return v, err
	case HashType:
		sizeKey := hEncodeSizeKey(key)
		v, err := exp.db.eng.GetBytes(exp.db.defaultReadOpts, sizeKey)
		return v, err
	default:
		return exp.localExp.getRawValueForHeader(ts, dataType, key)
	}
}

func (exp *compactExpiration) isExpired(ts int64, dataType byte, key []byte, rawValue []byte, useLock bool) (bool, error) {
	switch dataType {
	case HashType, KVType:
		if rawValue == nil {
			return false, nil
		}
		var h headerMetaValue
		_, err := h.decode(rawValue)
		if err != nil {
			return false, err
		}
		return h.isExpired(ts), nil
	default:
		return exp.localExp.isExpired(ts, dataType, key, rawValue, useLock)
	}
}

func (exp *compactExpiration) ExpireAt(dataType byte, key []byte, rawValue []byte, when int64) (int64, error) {
	switch dataType {
	case HashType, KVType:
		wb := exp.db.wb
		wb.Clear()
		if rawValue == nil {
			// key not exist
			return 0, nil
		}
		newValue, err := exp.rawExpireAt(dataType, key, rawValue, when, wb)
		if err != nil {
			return 0, err
		}
		if dataType == KVType {
			key = encodeKVKey(key)
		} else if dataType == HashType {
			key = hEncodeSizeKey(key)
		}
		wb.Put(key, newValue)
		if err := exp.db.eng.Write(exp.db.defaultWriteOpts, wb); err != nil {
			return 0, err
		}
		return 1, nil
	default:
		return exp.localExp.ExpireAt(dataType, key, rawValue, when)
	}
}

func (exp *compactExpiration) rawExpireAt(dataType byte, key []byte, rawValue []byte, when int64, wb *gorocksdb.WriteBatch) ([]byte, error) {
	switch dataType {
	case HashType, KVType:
		h := newHeaderMetaV1()
		if when >= int64(math.MaxUint32-1) {
			return nil, errExpOverflow
		}
		_, err := h.decode(rawValue)
		if err != nil {
			return nil, err
		}
		h.ExpireAt = uint32(when)
		_, v := h.encodeTo(rawValue)
		return v, nil
	default:
		return exp.localExp.rawExpireAt(dataType, key, rawValue, when, wb)
	}
}

func (exp *compactExpiration) ttl(ts int64, dataType byte, key []byte, rawValue []byte) (int64, error) {
	switch dataType {
	case KVType, HashType:
		if rawValue == nil {
			return -1, nil
		}
		var h headerMetaValue
		_, err := h.decode(rawValue)
		if err != nil {
			return 0, err
		}
		return h.ttl(ts), nil
	default:
		return exp.localExp.ttl(ts, dataType, key, rawValue)
	}
}

func (exp *compactExpiration) renewOnExpired(ts int64, dataType byte, key []byte, oldh *headerMetaValue) {
	if oldh == nil {
		return
	}
	switch dataType {
	case KVType, HashType:
		oldh.ExpireAt = 0
		oldh.UserData = nil
		oldh.ValueVersion = ts
	default:
		exp.localExp.renewOnExpired(ts, dataType, key, oldh)
	}
}

func (exp *compactExpiration) Start() {
	exp.localExp.Start()
}

func (exp *compactExpiration) Destroy() {
	exp.localExp.Destroy()
}

func (exp *compactExpiration) Stop() {
	exp.localExp.Stop()
}

func (exp *compactExpiration) delExpire(dataType byte, key []byte, rawValue []byte, keepValue bool, wb *gorocksdb.WriteBatch) ([]byte, error) {
	switch dataType {
	case HashType, KVType:
		if !keepValue {
			return rawValue, nil
		}
		newValue, err := exp.rawExpireAt(dataType, key, rawValue, 0, wb)
		if err != nil {
			return newValue, err
		}
		return newValue, nil
	default:
		return exp.localExp.delExpire(dataType, key, rawValue, keepValue, wb)
	}
}

func (exp *compactExpiration) check(buffer common.ExpiredDataBuffer, stop chan struct{}) error {
	return nil
}
