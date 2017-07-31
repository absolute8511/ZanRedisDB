package common

import (
	"errors"
	"strings"

	"github.com/absolute8511/redcon"
)

const (
	DIR_PERM  = 0755
	FILE_PERM = 0644
)

var (
	SCAN_CURSOR_SEP = []byte(";")
	SCAN_NODE_SEP   = []byte(":")
)

var (
	ErrInvalidCommand    = errors.New("invalid command")
	ErrStopped           = errors.New("the node stopped")
	ErrTimeout           = errors.New("queue request timeout")
	ErrInvalidArgs       = errors.New("invalid arguments")
	ErrInvalidRedisKey   = errors.New("invalid redis key")
	ErrInvalidScanType   = errors.New("invalid scan type")
	ErrEpochMismatch     = errors.New("epoch mismatch")
	ErrInvalidTableName  = errors.New("table name is invalid")
	ErrInvalidScanCursor = errors.New("invalid scan cursor")
	ErrScanCursorNoTable = errors.New("scan cursor must has table")
	ErrUnexpectError     = errors.New("unexpected error")
	ErrInvalidPrefix     = errors.New("invalid prefix")
)

// for out use
type DataType byte

const (
	NONE DataType = iota
	KV
	LIST
	HASH
	SET
	ZSET
	ALL
)

const (
	KVName   = "KV"
	ListName = "LIST"
	HashName = "HASH"
	SetName  = "SET"
	ZSetName = "ZSET"
)

const (
	KEYSEP = byte(':')
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

type WriteCmd struct {
	Operation string
	Args      [][]byte
}

type KVRecord struct {
	Key   []byte
	Value []byte
}

type KVRecordRet struct {
	Rec KVRecord
	Err error
}

const (
	MAX_BATCH_NUM       = 5000
	MinScore      int64 = -1<<63 + 1
	MaxScore      int64 = 1<<63 - 1
	InvalidScore  int64 = -1 << 63
	MAX_SCAN_JOB        = 10
)

const (
	RangeClose uint8 = 0x00
	RangeLOpen uint8 = 0x01
	RangeROpen uint8 = 0x10
	RangeOpen  uint8 = 0x11
)

func ExtractNamesapce(rawKey []byte) (string, []byte, error) {
	index := bytes.IndexByte(rawKey, ':')
	if index <= 0 {
		return "", nil, ErrInvalidRedisKey
	}
	namespace := string(rawKey[:index])
	realKey := rawKey[index+1:]
	return namespace, realKey, nil
}

func ExtractTable(rawKey []byte) ([]byte, []byte, error) {
	pos := bytes.IndexByte(rawKey, KEYSEP)
	if pos == -1 {
		return nil, nil, ErrInvalidPrefix
	}

	table := rawKey[:pos]
	other := rawKey[pos+1:]
	return table, other, nil
}

type ScorePair struct {
	Score  int64
	Member []byte
}

type CommandFunc func(redcon.Conn, redcon.Command)
type CommandRspFunc func(redcon.Conn, redcon.Command, interface{})
type InternalCommandFunc func(redcon.Command, int64) (interface{}, error)
type MergeCommandFunc func(redcon.Command) (interface{}, error)

type CmdRouter struct {
	wcmds        map[string]CommandFunc
	rcmds        map[string]CommandFunc
	internalCmds map[string]InternalCommandFunc
	mergeCmds    map[string]MergeCommandFunc
}

func NewCmdRouter() *CmdRouter {
	return &CmdRouter{
		wcmds:        make(map[string]CommandFunc),
		rcmds:        make(map[string]CommandFunc),
		internalCmds: make(map[string]InternalCommandFunc),
		mergeCmds:    make(map[string]MergeCommandFunc),
	}
}

func (r *CmdRouter) Register(isWrite bool, name string, f CommandFunc) bool {
	cmds := r.wcmds
	if !isWrite {
		cmds = r.rcmds
	}
	if _, ok := cmds[strings.ToLower(name)]; ok {
		return false
	}
	cmds[name] = f
	return true
}

func (r *CmdRouter) GetCmdHandler(name string) (CommandFunc, bool, bool) {
	v, ok := r.rcmds[strings.ToLower(name)]
	if ok {
		return v, false, ok
	}
	v, ok = r.wcmds[strings.ToLower(name)]
	return v, true, ok
}

func (r *CmdRouter) RegisterInternal(name string, f InternalCommandFunc) bool {
	if _, ok := r.internalCmds[strings.ToLower(name)]; ok {
		return false
	}
	r.internalCmds[name] = f
	return true
}

func (r *CmdRouter) GetInternalCmdHandler(name string) (InternalCommandFunc, bool) {
	v, ok := r.internalCmds[strings.ToLower(name)]
	return v, ok
}

func (r *CmdRouter) RegisterMerge(name string, f MergeCommandFunc) bool {
	if _, ok := r.mergeCmds[strings.ToLower(name)]; ok {
		return false
	}
	r.mergeCmds[name] = f
	return true
}

func (r *CmdRouter) GetMergeCmdHandler(name string) (MergeCommandFunc, bool) {
	v, ok := r.mergeCmds[strings.ToLower(name)]
	return v, ok
}

type StringArray []string

func (a *StringArray) Set(s string) error {
	*a = append(*a, s)
	return nil
}

func (a *StringArray) String() string {
	return strings.Join(*a, ",")
}

const (
	MAX_PARTITION_NUM = 1024
	MAX_REPLICATOR    = 5
)

type MemberInfo struct {
	// the replica id
	ID uint64 `json:"id"`
	// the node id replica belong
	NodeID    uint64 `json:"node_id"`
	GroupName string `json:"group_name"`
	// group id the replica belong (different from namespace)
	GroupID  uint64   `json:"group_id"`
	RaftURLs []string `json:"peer_urls"`
}

func (self *MemberInfo) IsEqual(other *MemberInfo) bool {
	if self == nil || other == nil {
		return false
	}
	if self.ID != other.ID || self.NodeID != other.NodeID ||
		self.GroupName != other.GroupName || self.GroupID != other.GroupID {
		return false
	}
	if len(self.RaftURLs) != len(other.RaftURLs) {
		return false
	}
	for i, v := range self.RaftURLs {
		if v != other.RaftURLs[i] {
			return false
		}
	}
	return true
}

type SnapshotSyncInfo struct {
	ReplicaID   uint64
	NodeID      uint64
	RemoteAddr  string
	HttpAPIPort string
	DataRoot    string
	RsyncModule string
}

type IClusterInfo interface {
	GetSnapshotSyncInfo(string) ([]SnapshotSyncInfo, error)
	// return leader node id, leader epoch, for this namespace
	GetNamespaceLeader(fullNS string) (uint64, int64, error)
	UpdateMeForNamespaceLeader(fullNS string, oldEpoch int64) (int64, error)
}

type ScanResult struct {
	Keys       [][]byte
	NextCursor []byte
	PartionId  string
	Error      error
}

type IndexState int32

const (
	InitIndex      IndexState = 0
	BuildingIndex  IndexState = 1
	BuildDoneIndex IndexState = 2
	ReadyIndex     IndexState = 3
	DeletedIndex   IndexState = 4
)

type IndexPropertyDType int32

const (
	Int64V  IndexPropertyDType = 0
	Int32V  IndexPropertyDType = 1
	StringV IndexPropertyDType = 2
)

type HsetIndexSchema struct {
	Name       string             `json:"name"`
	IndexField string             `json:"index_field"`
	PrefixLen  int32              `json:"prefix_len"`
	Unique     int32              `json:"unique"`
	ValueType  IndexPropertyDType `json:"value_type"`
	State      IndexState         `json:"state"`
}

type JsonIndexSchema struct {
	State IndexState `json:"state"`
}

type IndexSchema struct {
	HsetIndexes []*HsetIndexSchema `json:"hset_indexes"`
	JsonIndexes []*JsonIndexSchema `json:"json_indexes"`
}

type FullScanResult struct {
	Results    []interface{}
	Type       DataType
	NextCursor []byte
	PartionId  string
	Error      error
}

type FieldPair struct {
	Field []byte
	Value []byte
}
