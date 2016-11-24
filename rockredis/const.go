package rockredis

import (
	"errors"
)

type ErrType int64

const (
	NotFound ErrType = 1
)

type DataType byte

// for out use
const (
	NONE DataType = iota
	KV
	LIST
	HASH
	SET
	ZSET
	ALL
)

func (d DataType) String() string {
	switch d {
	case KV:
		return KVName
	case LIST:
		return ListName
	case HASH:
		return HashName
	case SET:
		return SetName
	case ZSET:
		return ZSetName
	default:
		return "unknown"
	}
}

const (
	KVName   = "KV"
	ListName = "LIST"
	HashName = "HASH"
	SetName  = "SET"
	ZSetName = "ZSET"
)

// for backend store
const (
	NoneType   byte = 0
	KVType     byte = 1
	HashType   byte = 2
	HSizeType  byte = 3
	ListType   byte = 4
	LMetaType  byte = 5
	ZSetType   byte = 6
	ZSizeType  byte = 7
	ZScoreType byte = 8
	SetType    byte = 11
	SSizeType  byte = 12

	// this type has a custom partition key length
	// to allow all the data store in the same partition
	FixPartType byte = 20
	// this type allow the transaction in the same tx group,
	// which will be stored in the same partition
	TxGroupType byte = 21
	maxDataType byte = 100

	// use the exp table to store all the expire time for the key
	// and scan periodically to delete the expired keys
	ExpTimeType byte = 101
	ExpMetaType byte = 102
)

var (
	TypeName = map[byte]string{
		KVType:     "kv",
		HashType:   "hash",
		HSizeType:  "hsize",
		ListType:   "list",
		LMetaType:  "lmeta",
		ZSetType:   "zset",
		ZSizeType:  "zsize",
		ZScoreType: "zscore",
		SetType:    "set",
		SSizeType:  "ssize",
	}
)

const (
	defaultScanCount int = 10
	MAX_BATCH_NUM        = 5000
	RANGE_DELETE_NUM     = 100000
)

var (
	errKeySize          = errors.New("invalid key size")
	errValueSize        = errors.New("invalid value size")
	errZSetMemberSize   = errors.New("invalid zset member size")
	errListIndex        = errors.New("invalid list index")
	errTooMuchBatchSize = errors.New("the batch size exceed the limit")
)

const (
	MaxDatabases int = 10240

	MaxTableNameLen int = 255
	//max key size
	MaxKeySize int = 1024

	//max hash field size
	MaxHashFieldSize int = 1024

	//max zset member size
	MaxZSetMemberSize int = 1024

	//max set member size
	MaxSetMemberSize int = 1024

	//max value size
	MaxValueSize int = 1024 * 1024 * 32
)

var (
	ErrZScoreMiss   = errors.New("zset score miss")
	ErrWriteInROnly = errors.New("write not support in readonly mode")
)

const (
	RangeClose uint8 = 0x00
	RangeLOpen uint8 = 0x01
	RangeROpen uint8 = 0x10
	RangeOpen  uint8 = 0x11
)
