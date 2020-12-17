package rockredis

import (
	"encoding/binary"
	"errors"
	"time"
)

const (
	collStartSep              byte  = ':'
	collStopSep               byte  = collStartSep + 1
	collectionLengthForMetric int64 = 128
)

var (
	errCollKey          = errors.New("invalid collection key")
	errCollTypeMismatch = errors.New("decoded collection type mismatch")
)

// note for list/bitmap/zscore subkey is different
func encodeCollSubKey(dt byte, table []byte, key []byte, subkey []byte) []byte {
	if dt != HashType && dt != SetType && dt != ZSetType {
		panic(errDataType)
	}
	buf := make([]byte, getDataTablePrefixBufLen(dt, table)+len(key)+len(subkey)+1+2)

	pos := encodeDataTablePrefixToBuf(buf, dt, table)

	binary.BigEndian.PutUint16(buf[pos:], uint16(len(key)))
	pos += 2

	copy(buf[pos:], key)
	pos += len(key)

	buf[pos] = collStartSep
	pos++
	copy(buf[pos:], subkey)

	return buf
}

// note for list/bitmap/zscore subkey is different
func decodeCollSubKey(dbk []byte) (byte, []byte, []byte, []byte, error) {
	if len(dbk) < 0 {
		return 0, nil, nil, nil, errDataType
	}
	dt := dbk[0]
	if dt != HashType && dt != SetType && dt != ZSetType {
		return dt, nil, nil, nil, errDataType
	}
	table, pos, err := decodeDataTablePrefixFromBuf(dbk, dt)
	if err != nil {
		return dt, nil, nil, nil, err
	}

	if pos+2 > len(dbk) {
		return dt, nil, nil, nil, errCollKey
	}

	keyLen := int(binary.BigEndian.Uint16(dbk[pos:]))
	pos += 2

	if keyLen+pos > len(dbk) {
		return dt, table, nil, nil, errCollKey
	}

	key := dbk[pos : pos+keyLen]
	pos += keyLen

	if dbk[pos] != collStartSep {
		return dt, table, nil, nil, errCollKey
	}
	pos++
	subkey := dbk[pos:]
	return dt, table, key, subkey, nil
}

// decode the versioned collection key (in wait compact policy) and convert to raw redis key
func convertCollDBKeyToRawKey(dbk []byte) (byte, []byte, int64, error) {
	if len(dbk) < 0 {
		return 0, nil, 0, errDataType
	}
	dt := dbk[0]
	var table []byte
	var verk []byte
	var err error
	switch dt {
	case HashType, SetType, ZSetType:
		_, table, verk, _, err = decodeCollSubKey(dbk)
	case ListType:
		table, verk, _, err = lDecodeListKey(dbk)
	case ZScoreType:
		table, verk, _, _, err = zDecodeScoreKey(dbk)
	case BitmapType:
		table, verk, _, err = decodeBitmapKey(dbk)
	default:
		return 0, nil, 0, errDataType
	}
	if err != nil {
		return dt, nil, 0, err
	}
	rk, ver, err := decodeVerKey(verk)
	if err != nil {
		return dt, nil, 0, err
	}
	rawKey := packRedisKey(table, rk)
	return dt, rawKey, ver, nil
}

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

func encodeMetaKey(dt byte, key []byte) ([]byte, error) {
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
	case ZSetType, ZSizeType, ZScoreType:
		key = zEncodeSizeKey(key)
	default:
		return nil, errDataType
	}
	return key, nil
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

// if run in raft write loop should avoid lock.
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

// note this may use write batch in db
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

// TODO: maybe we do not need read the meta if only expired is needed and we use the local_deletion policy,
// since the ttl is not available in local_deletion policy (can reduce 1 get to db)
func (db *RockDB) collHeaderMeta(ts int64, dt byte, key []byte, useLock bool) (*headerMetaValue, bool, error) {
	var sizeKey []byte
	sizeKey, err := encodeMetaKey(dt, key)
	if err != nil {
		return nil, false, err
	}
	var v []byte
	if useLock {
		v, err = db.GetBytes(sizeKey)
	} else {
		v, err = db.GetBytesNoLock(sizeKey)
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
