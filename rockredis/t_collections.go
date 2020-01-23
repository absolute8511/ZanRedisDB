package rockredis

import "errors"

const (
	collStartSep byte = ':'
	collStopSep  byte = collStartSep + 1
)

func checkCollKFSize(key []byte, field []byte) error {
	if len(key) > MaxKeySize || len(key) == 0 {
		return errKeySize
	} else if len(field) > MaxSubKeyLen {
		return errSubKeySize
	}
	return nil
}

func (db *RockDB) GetCollVersionKey(ts int64, dt byte, key []byte) (bool, []byte, []byte, []byte, error) {
	table, rk, err := extractTableFromRedisKey(key)
	if err != nil {
		return false, nil, nil, nil, err
	}

	oldh, metaV, expired, err := db.collHeaderMeta(ts, dt, key, true)
	if err != nil {
		return false, nil, nil, nil, err
	}
	if expired || metaV == nil {
		return true, nil, nil, nil, nil
	}
	verKey := db.expiration.encodeToVersionKey(dt, oldh, rk)
	return false, metaV, table, verKey, nil
}

func (db *RockDB) prepareCollKeyForWrite(ts int64, dt byte, key []byte, field []byte) (*headerMetaValue, []byte, []byte, error) {
	table, rk, err := extractTableFromRedisKey(key)
	if err != nil {
		return nil, nil, nil, err
	}
	if err := checkCollKFSize(rk, field); err != nil {
		return nil, nil, nil, err
	}

	oldh, metaV, expired, err := db.collHeaderMeta(ts, dt, key, false)
	if err != nil {
		return nil, nil, nil, err
	}

	if expired || metaV == nil {
		db.expiration.renewOnExpired(ts, dt, key, oldh)
	}
	verKey := db.expiration.encodeToVersionKey(dt, oldh, rk)
	return oldh, table, verKey, nil
}

func (db *RockDB) collHeaderMeta(ts int64, dt byte, key []byte, useLock bool) (*headerMetaValue, []byte, bool, error) {
	var sizeKey []byte
	switch dt {
	case HashType:
		sizeKey = hEncodeSizeKey(key)
	case SetType:
		sizeKey = sEncodeSizeKey(key)
	case BitmapType:
		sizeKey = bitEncodeMetaKey(key)
	case ListType:
	case ZSetType:
	default:
		return nil, nil, false, errors.New("unsupported collection type")
	}
	var v []byte
	var err error
	if useLock {
		v, err = db.eng.GetBytes(db.defaultReadOpts, sizeKey)
	} else {
		v, err = db.eng.GetBytesNoLock(db.defaultReadOpts, sizeKey)
	}
	if err != nil {
		return nil, nil, false, err
	}
	rv, h, err := db.expiration.decodeRawValue(dt, v)
	if err != nil {
		return h, rv, false, err
	}
	isExpired, err := db.expiration.isExpired(ts, dt, key, v, useLock)
	return h, rv, isExpired, err
}
