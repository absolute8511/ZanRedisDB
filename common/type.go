package common

import (
	"container/heap"
	"errors"
	"math"
	"strings"

	"bytes"

	"github.com/absolute8511/redcon"
)

const (
	DIR_PERM  = 0755
	FILE_PERM = 0644
)

const (
	LearnerRoleLogSyncer = "role_log_syncer"
	LearnerRoleSearcher  = "role_searcher"
)

var (
	SCAN_CURSOR_SEP = []byte(";")
	SCAN_NODE_SEP   = []byte(":")
)

var (
	ErrInvalidCommand    = errors.New("invalid command")
	ErrStopped           = errors.New("the node stopped")
	ErrQueueTimeout      = errors.New("queue request timeout")
	ErrInvalidArgs       = errors.New("invalid arguments")
	ErrInvalidRedisKey   = errors.New("invalid redis key")
	ErrInvalidScanType   = errors.New("invalid scan type")
	ErrEpochMismatch     = errors.New("epoch mismatch")
	ErrInvalidTableName  = errors.New("table name is invalid")
	ErrInvalidScanCursor = errors.New("invalid scan cursor")
	ErrScanCursorNoTable = errors.New("scan cursor must has table")
	ErrUnexpectError     = errors.New("unexpected error")
	ErrInvalidPrefix     = errors.New("invalid prefix")
	ErrNotSupport        = errors.New("not supported")
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

type ExpirationPolicy byte

const (
	// LocalDeletion indicates the expired data would be deleted by the underlying storage system automatically and the logical layer
	// do not need to care about the data expiration. Every node in the cluster should start the 'TTLChecker' of the storage system
	// with this policy.
	LocalDeletion ExpirationPolicy = iota

	// ConsistencyDeletion indicates all the expired data should be deleted through Raft, the underlying storage system should
	// not delete any data and all the expired keys should be sent to the expired channel. Only the leader should starts
	// the 'TTLChecker' with this policy.
	ConsistencyDeletion

	//
	PeriodicalRotation

	UnknownPolicy
)

const (
	DefaultExpirationPolicy = "local_deletion"
)

func StringToExpirationPolicy(s string) (ExpirationPolicy, error) {
	switch s {
	case "local_deletion":
		return LocalDeletion, nil
	case "consistency_deletion":
		return ConsistencyDeletion, nil
	default:
		return UnknownPolicy, errors.New("unknown policy")
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

type KVals struct {
	PK   []byte
	Vals [][]byte
}

type KFVals struct {
	PK   []byte
	Vals []KVRecordRet
}

var (
	MAX_BATCH_NUM         = 5000
	MinScore      float64 = math.Inf(-1)
	MaxScore      float64 = math.Inf(1)
	InvalidScore  int64   = -1 << 63
	MAX_SCAN_JOB          = 10
)

const (
	RangeClose uint8 = 0x00
	RangeLOpen uint8 = 0x01
	RangeROpen uint8 = 0x10
	RangeOpen  uint8 = 0x11
)

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
	Score  float64
	Member []byte
}

type CommandFunc func(redcon.Conn, redcon.Command)
type CommandRspFunc func(redcon.Conn, redcon.Command, interface{})
type InternalCommandFunc func(redcon.Command, int64) (interface{}, error)
type MergeCommandFunc func(redcon.Command) (interface{}, error)
type MergeWriteCommandFunc func(redcon.Command, interface{}) (interface{}, error)

type CmdRouter struct {
	wcmds          map[string]CommandFunc
	rcmds          map[string]CommandFunc
	mergeCmds      map[string]MergeCommandFunc
	mergeWriteCmds map[string]MergeCommandFunc
}

func NewCmdRouter() *CmdRouter {
	return &CmdRouter{
		wcmds:          make(map[string]CommandFunc),
		rcmds:          make(map[string]CommandFunc),
		mergeCmds:      make(map[string]MergeCommandFunc),
		mergeWriteCmds: make(map[string]MergeCommandFunc),
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

func (r *CmdRouter) RegisterMerge(name string, f MergeCommandFunc) bool {
	if _, ok := r.mergeCmds[strings.ToLower(name)]; ok {
		return false
	}
	r.mergeCmds[name] = f
	return true
}

func (r *CmdRouter) RegisterWriteMerge(name string, f MergeCommandFunc) bool {
	if _, ok := r.mergeWriteCmds[strings.ToLower(name)]; ok {
		return false
	}
	r.mergeWriteCmds[name] = f
	return true
}

// return handler, iswrite, isexist
func (r *CmdRouter) GetMergeCmdHandler(name string) (MergeCommandFunc, bool, bool) {
	v, ok := r.mergeCmds[strings.ToLower(name)]
	if ok {
		return v, false, ok
	}
	v, ok = r.mergeWriteCmds[strings.ToLower(name)]
	return v, true, ok
}

type SMCmdRouter struct {
	smCmds map[string]InternalCommandFunc
}

func NewSMCmdRouter() *SMCmdRouter {
	return &SMCmdRouter{
		smCmds: make(map[string]InternalCommandFunc),
	}
}

func (r *SMCmdRouter) RegisterInternal(name string, f InternalCommandFunc) bool {
	if _, ok := r.smCmds[strings.ToLower(name)]; ok {
		return false
	}
	r.smCmds[name] = f
	return true
}

func (r *SMCmdRouter) GetInternalCmdHandler(name string) (InternalCommandFunc, bool) {
	v, ok := r.smCmds[strings.ToLower(name)]
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
	GetClusterName() string
	GetSnapshotSyncInfo(fullNS string) ([]SnapshotSyncInfo, error)
	UpdateMeForNamespaceLeader(fullNS string) (bool, error)
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
	MaxIndexState  IndexState = 5
)

type IndexPropertyDType int32

const (
	Int64V  IndexPropertyDType = 0
	Int32V  IndexPropertyDType = 1
	StringV IndexPropertyDType = 2
	MaxVT   IndexPropertyDType = 3
)

type HsetIndexSchema struct {
	Name       string             `json:"name"`
	IndexField string             `json:"index_field"`
	PrefixLen  int32              `json:"prefix_len"`
	Unique     int32              `json:"unique"`
	ValueType  IndexPropertyDType `json:"value_type"`
	State      IndexState         `json:"state"`
}

func (s *HsetIndexSchema) IsValidNewSchema() bool {
	return s.Name != "" && s.IndexField != "" && s.ValueType < MaxVT && s.State < MaxIndexState
}

type HIndexRespWithValues struct {
	PKey       []byte
	IndexV     interface{}
	HsetValues [][]byte
	index      int
}

type SearchResultHeap []*HIndexRespWithValues

func (sh SearchResultHeap) Len() int { return len(sh) }
func (sh SearchResultHeap) Less(i, j int) bool {
	var rightV int64
	switch realV := sh[i].IndexV.(type) {
	case []byte:
		comp := bytes.Compare(realV, sh[j].IndexV.([]byte))
		if comp == 0 {
			return bytes.Compare(sh[i].PKey, sh[j].PKey) == -1
		}
		return comp == -1
	case int:
		rightV = int64(sh[j].IndexV.(int))
		leftV := int64(realV)
		if rightV == leftV {
			return bytes.Compare(sh[i].PKey, sh[j].PKey) == -1
		}
		return leftV < rightV
	case int32:
		rightV = int64(sh[j].IndexV.(int32))
		leftV := int64(realV)
		if rightV == leftV {
			return bytes.Compare(sh[i].PKey, sh[j].PKey) == -1
		}
		return leftV < rightV
	case int64:
		rightV = int64(sh[j].IndexV.(int64))
		leftV := int64(realV)
		if rightV == leftV {
			return bytes.Compare(sh[i].PKey, sh[j].PKey) == -1
		}
		return leftV < rightV
	}
	return false
}

func (sh SearchResultHeap) Swap(i, j int) {
	sh[i], sh[j] = sh[j], sh[i]
	sh[i].index = i
	sh[j].index = j
}

func (sh *SearchResultHeap) Push(x interface{}) {
	n := len(*sh)
	v := x.(*HIndexRespWithValues)
	v.index = n
	*sh = append(*sh, v)
}

func (sh *SearchResultHeap) Pop() interface{} {
	old := *sh
	n := len(old)
	v := old[n-1]
	v.index = -1
	*sh = old[0 : n-1]
	return v
}

func (sh *SearchResultHeap) update(item *HIndexRespWithValues) {
	heap.Fix(sh, item.index)
}

type JSONIndexSchema struct {
	State IndexState `json:"state"`
}

type IndexSchema struct {
	HsetIndexes []*HsetIndexSchema `json:"hset_indexes"`
	JSONIndexes []*JSONIndexSchema `json:"json_indexes"`
}

type ExpiredDataBuffer interface {
	Write(DataType, []byte) error
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
