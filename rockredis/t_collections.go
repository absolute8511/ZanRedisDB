package rockredis

import (
	"errors"
	"time"
)

const (
	collStartSep byte = ':'
	collStopSep  byte = collStartSep + 1
)

type collVerKeyInfo struct {
	OldHeader  *headerMetaValue
	Expired    bool
	Table      []byte
	VerKey     []byte
	RangeStart []byte
	RangeEnd   []byte
}

// decoded meta for compatible with old format
func (info collVerKeyInfo) MetaData() []byte {
	return info.OldHeader.UserData
}

func (info collVerKeyInfo) IsNotExistOrExpired() bool {
	return info.Expired || info.MetaData() == nil
}

func checkCollKFSize(key []byte, field []byte) error {
	if len(key) > MaxKeySize || len(key) == 0 {
		return errKeySize
	} else if len(field) > MaxSubKeyLen {
		return errSubKeySize
	}
	return nil
}

func encodeMetaKey(dt byte, key []byte) []byte {
	switch dt {
	case KVType:
		key = encodeKVKey(key)
	case HashType, HSizeType:
		key = hEncodeSizeKey(key)
	case SetType, SSizeType:
		key = sEncodeSizeKey(key)
	case BitmapType, BitmapMetaType:
		key = bitEncodeMetaKey(key)
	case ListType, LMetaType:
		key = lEncodeMetaKey(key)
	case ZSetType, ZSizeType:
		key = zEncodeSizeKey(key)
	default:
	}
	return key
}

// in raft write loop should avoid lock.
func (db *RockDB) getCollVerKey(ts int64, dt byte, key []byte, useLock bool) (collVerKeyInfo, error) {
	info, err := db.GetCollVersionKey(ts, dt, key, useLock)
	if err != nil {
		return info, err
	}
	return info, nil
}

func (db *RockDB) getCollVerKeyForRange(ts int64, dt byte, key []byte, useLock bool) (collVerKeyInfo, error) {
	info, err := db.GetCollVersionKey(ts, dt, key, useLock)
	if err != nil {
		return info, err
	}
	switch dt {
	case SetType:
		info.RangeStart = sEncodeStartKey(info.Table, info.VerKey)
		info.RangeEnd = sEncodeStopKey(info.Table, info.VerKey)
	case HashType:
		info.RangeStart = hEncodeStartKey(info.Table, info.VerKey)
		info.RangeEnd = hEncodeStopKey(info.Table, info.VerKey)
	case ZSetType:
		info.RangeStart = zEncodeStartKey(info.Table, info.VerKey)
		info.RangeEnd = zEncodeStopKey(info.Table, info.VerKey)
	default:
	}
	return info, nil
}

func (db *RockDB) GetCollVersionKey(ts int64, dt byte, key []byte, useLock bool) (collVerKeyInfo, error) {
	var keyInfo collVerKeyInfo
	table, rk, err := extractTableFromRedisKey(key)
	if err != nil {
		return keyInfo, err
	}
	if len(table) == 0 {
		return keyInfo, errTableName
	}
	keyInfo.Table = table
	if err := checkKeySize(rk); err != nil {
		return keyInfo, err
	}
	keyInfo.OldHeader, keyInfo.Expired, err = db.collHeaderMeta(ts, dt, key, useLock)
	if err != nil {
		return keyInfo, err
	}
	keyInfo.VerKey = db.expiration.encodeToVersionKey(dt, keyInfo.OldHeader, rk)
	return keyInfo, nil
}

func (db *RockDB) prepareCollKeyForWrite(ts int64, dt byte, key []byte, field []byte) (collVerKeyInfo, error) {
	var keyInfo collVerKeyInfo
	table, rk, err := extractTableFromRedisKey(key)
	if err != nil {
		return keyInfo, err
	}
	keyInfo.Table = table
	if err := checkCollKFSize(rk, field); err != nil {
		return keyInfo, err
	}

	keyInfo.OldHeader, keyInfo.Expired, err = db.collHeaderMeta(ts, dt, key, false)
	if err != nil {
		return keyInfo, err
	}

	if keyInfo.IsNotExistOrExpired() {
		// since renew on expired may change the header meta in old header in some expire policy,
		// then the renewed meta data should also be return for different expire policy
		db.expiration.renewOnExpired(ts, dt, key, keyInfo.OldHeader)
	}
	keyInfo.VerKey = db.expiration.encodeToVersionKey(dt, keyInfo.OldHeader, rk)
	return keyInfo, nil
}

func (db *RockDB) collHeaderMeta(ts int64, dt byte, key []byte, useLock bool) (*headerMetaValue, bool, error) {
	var sizeKey []byte
	switch dt {
	case HashType:
		sizeKey = hEncodeSizeKey(key)
	case SetType:
		sizeKey = sEncodeSizeKey(key)
	case BitmapType:
		sizeKey = bitEncodeMetaKey(key)
	case ListType:
		sizeKey = lEncodeMetaKey(key)
	case ZSetType:
		sizeKey = zEncodeSizeKey(key)
	default:
		return nil, false, errors.New("unsupported collection type")
	}
	var v []byte
	var err error
	if useLock {
		v, err = db.eng.GetBytes(db.defaultReadOpts, sizeKey)
	} else {
		v, err = db.eng.GetBytesNoLock(db.defaultReadOpts, sizeKey)
	}
	if err != nil {
		return nil, false, err
	}
	h, err := db.expiration.decodeRawValue(dt, v)
	if err != nil {
		return h, false, err
	}
	isExpired, err := db.expiration.isExpired(ts, dt, key, v, useLock)
	return h, isExpired, err
}

func (db *RockDB) collKeyExists(dt byte, key []byte) (int64, error) {
	h, expired, err := db.collHeaderMeta(time.Now().UnixNano(), dt, key, true)
	if err != nil {
		return 0, err
	}
	if expired || h.UserData == nil {
		return 0, nil
	}
	return 1, nil
}

func (db *RockDB) collExpire(ts int64, dt byte, key []byte, duration int64) (int64, error) {
	oldh, expired, err := db.collHeaderMeta(ts, dt, key, false)
	if err != nil || expired || oldh.UserData == nil {
		return 0, err
	}

	rawV := db.expiration.encodeToRawValue(dt, oldh)
	return db.ExpireAt(dt, key, rawV, duration+ts/int64(time.Second))
}

func (db *RockDB) collPersist(ts int64, dt byte, key []byte) (int64, error) {
	oldh, expired, err := db.collHeaderMeta(ts, dt, key, false)
	if err != nil || expired || oldh.UserData == nil {
		return 0, err
	}

	rawV := db.expiration.encodeToRawValue(dt, oldh)
	return db.ExpireAt(dt, key, rawV, 0)
}
