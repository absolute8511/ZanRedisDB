package rockredis

import (
	"errors"
)

const (
	EngType = "rockredis"
)

type ErrType int64

const (
	NotFound ErrType = 1
)

// for backend store
const (
	NoneType byte = 0
	// 0~10 reserved for system usage

	// table count, stats, index, schema, and etc.
	TableMetaType      byte = 10
	TableIndexMetaType byte = 11

	// for data
	KVType    byte = 21
	HashType  byte = 22
	HSizeType byte = 23
	// current using array list
	ListType   byte = 24
	LMetaType  byte = 25
	ZSetType   byte = 26
	ZSizeType  byte = 27
	ZScoreType byte = 28
	SetType    byte = 29
	SSizeType  byte = 30

	JSONType byte = 31

	ColumnType byte = 38 // used for column store for OLAP

	// for secondary index data
	IndexDataType byte = 40

	FullTextIndexDataType byte = 50
	// this type has a custom partition key length
	// to allow all the data store in the same partition
	// this type allow the transaction in the same tx group,
	// which will be stored in the same partition
	FixPartType byte = 80
	// in case there are more than 100 kinds of data types,
	// we use the extanded data for more types
	ExtandType  byte = 90
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
		JSONType:   "json",
	}
)

const (
	defaultScanCount int = 100
	MAX_BATCH_NUM        = 5000
	RangeDeleteNum       = 500
)

var (
	errKeySize          = errors.New("invalid key size")
	errValueSize        = errors.New("invalid value size")
	errZSetMemberSize   = errors.New("invalid zset member size")
	errTooMuchBatchSize = errors.New("the batch size exceed the limit")
	errDBClosed         = errors.New("the db is closed")
	errNotMatch         = errors.New("not match")
	errUnsuportType     = errors.New("unsupport type")
)

const (
	MaxDatabases int = 10240

	MaxTableNameLen int = 255
	MaxColumnLen    int = 255
	//max key size
	MaxKeySize int = 10240

	//max hash field size
	MaxHashFieldSize int = 10240

	//max zset member size
	MaxZSetMemberSize int = 10240

	//max set member size
	MaxSetMemberSize int = 10240

	//max value size
	MaxValueSize int = 1024 * 1024 * 8
)

var (
	ErrZScoreMiss   = errors.New("zset score miss")
	ErrWriteInROnly = errors.New("write not support in readonly mode")
)
