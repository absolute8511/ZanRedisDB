package common

import (
	"bytes"
	"errors"
	"strings"

	"github.com/tidwall/redcon"
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

func ExtractRawKey(rawKey []byte) (ns, table string, realKey []byte, err error) {
	ns, newKey, err := ExtractNamesapce(rawKey)
	if err != nil {
		return "", "", nil, err
	}

	index := bytes.IndexByte(newKey, ':')
	if index <= 0 {
		return ns, "", newKey, nil
	}

	table = string(newKey[:index])

	realKey = newKey[index+1:]
	return
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
	cmds         map[string]CommandFunc
	internalCmds map[string]InternalCommandFunc
	scanCmds     map[string]MergeCommandFunc
}

func NewCmdRouter() *CmdRouter {
	return &CmdRouter{
		cmds:         make(map[string]CommandFunc),
		internalCmds: make(map[string]InternalCommandFunc),
		scanCmds:     make(map[string]MergeCommandFunc),
	}
}

func (r *CmdRouter) Register(name string, f CommandFunc) bool {
	if _, ok := r.cmds[strings.ToLower(name)]; ok {
		return false
	}
	r.cmds[name] = f
	return true
}

func (r *CmdRouter) GetCmdHandler(name string) (CommandFunc, bool) {
	v, ok := r.cmds[strings.ToLower(name)]
	return v, ok
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
	if _, ok := r.scanCmds[strings.ToLower(name)]; ok {
		return false
	}
	r.scanCmds[name] = f
	return true
}

func (r *CmdRouter) GetMergeCmdHandler(name string) (MergeCommandFunc, bool) {
	v, ok := r.scanCmds[strings.ToLower(name)]
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
	Result     interface{}
	NextCursor []byte
	NodeInfo   string
	Error      error
}
