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

func (h *headerMetaValue) encodeTo(old []byte) []byte {
	b := old
	if len(old) < headerV1Len {
		b = make([]byte, headerV1Len)
	}
	b[0] = h.Ver
	binary.BigEndian.PutUint32(b[1:], uint32(h.ExpireAt))
	binary.BigEndian.PutUint64(b[1+4:], uint64(h.ValueVersion))
	return b
}

func (h *headerMetaValue) encode() []byte {
	b := make([]byte, headerV1Len)
	return h.encodeTo(b)
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

func (exp *compactExpiration) encodeToRawValue(dataType byte, h *headerMetaValue, realValue []byte) []byte {
	if dataType != KVType {
		return exp.localExp.encodeToRawValue(dataType, h, realValue)
	}
	if h == nil {
		h = newHeaderMetaV1()
	}
	v := h.encode()
	if realValue == nil {
		return v
	}
	v = append(v, realValue...)
	return v
}

func (exp *compactExpiration) decodeRawValue(dataType byte, rawValue []byte) ([]byte, *headerMetaValue, error) {
	h := newHeaderMetaV1()
	if dataType != KVType {
		return exp.localExp.decodeRawValue(dataType, rawValue)
	}
	if rawValue == nil {
		return nil, h, nil
	}
	n, err := h.decode(rawValue)
	if err != nil {
		return rawValue, h, err
	}
	return rawValue[n:], h, nil
}

func (exp *compactExpiration) isExpired(dataType byte, key []byte, rawValue []byte) (bool, error) {
	if dataType != KVType {
		return exp.localExp.isExpired(dataType, key, rawValue)
	}
	if rawValue == nil {
		return false, nil
	}
	var h headerMetaValue
	_, err := h.decode(rawValue)
	if err != nil {
		return false, err
	}
	if h.ExpireAt == 0 {
		return false, nil
	}
	ttl := int64(h.ExpireAt) - time.Now().Unix()
	return ttl <= 0, nil
}

func (exp *compactExpiration) ExpireAt(dataType byte, key []byte, rawValue []byte, when int64) error {
	if dataType != KVType {
		return exp.localExp.ExpireAt(dataType, key, rawValue, when)
	}
	wb := exp.db.wb
	wb.Clear()
	newValue, err := exp.rawExpireAt(dataType, key, rawValue, when, wb)
	if err != nil {
		return err
	}
	key = encodeKVKey(key)
	wb.Put(key, newValue)
	if err := exp.db.eng.Write(exp.db.defaultWriteOpts, wb); err != nil {
		return err
	}
	return nil
}

func (exp *compactExpiration) rawExpireAt(dataType byte, key []byte, rawValue []byte, when int64, wb *gorocksdb.WriteBatch) ([]byte, error) {
	if dataType != KVType {
		return exp.localExp.rawExpireAt(dataType, key, rawValue, when, wb)
	}
	h := newHeaderMetaV1()
	if when >= int64(math.MaxUint32-1) {
		return nil, errExpOverflow
	}
	h.ExpireAt = uint32(when)
	h.ValueVersion = 0

	v := h.encodeTo(rawValue)
	return v, nil
}

func (exp *compactExpiration) ttl(dataType byte, key []byte, rawValue []byte) (int64, error) {
	if dataType != KVType {
		return exp.localExp.ttl(dataType, key, rawValue)
	}
	if rawValue == nil {
		return -1, nil
	}
	var h headerMetaValue
	_, err := h.decode(rawValue)
	if err != nil {
		return 0, err
	}

	if h.ExpireAt == 0 {
		return -1, nil
	}
	ttl := int64(h.ExpireAt) - time.Now().Unix()
	if ttl <= 0 {
		ttl = -1
	}
	return ttl, nil
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
	if dataType != KVType {
		return exp.localExp.delExpire(dataType, key, rawValue, keepValue, wb)
	}

	if !keepValue {
		return rawValue, nil
	}
	newValue, err := exp.rawExpireAt(dataType, key, rawValue, 0, wb)
	if err != nil {
		return newValue, err
	}
	return newValue, nil
}

func (exp *compactExpiration) check(buffer common.ExpiredDataBuffer, stop chan struct{}) error {
	return nil
}
