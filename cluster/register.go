package cluster

import (
	"errors"
	"github.com/absolute8511/ZanRedisDB/common"
)

var (
	ErrKeyAlreadyExist = errors.New("Key already exist")
	ErrKeyNotFound     = errors.New("Key not found")
)

type EpochType int64

type NodeInfo struct {
	RegID             uint64
	ID                string
	NodeIP            string
	Hostname          string
	RedisPort         string
	HttpPort          string
	RpcPort           string
	RaftTransportAddr string
	Version           string
	Tags              map[string]bool
	DataRoot          string
	RsyncModule       string
	epoch             EpochType
}

func (self *NodeInfo) GetID() string {
	return self.ID
}

func (self *NodeInfo) Epoch() EpochType {
	return self.epoch
}

func (self *NodeInfo) GetRegisterID() uint64 {
	return self.RegID
}

type ClusterMetaInfo struct {
	MaxGID   int64
	MaxRegID uint64
}

type NamespaceMetaInfo struct {
	PartitionNum int
	Replica      int
	// to verify the data of the create -> delete -> create with same namespace
	MagicCode int64
	MinGID    int64
	metaEpoch EpochType
	EngType   string
	Tags      map[string]bool
}

func (self *NamespaceMetaInfo) MetaEpoch() EpochType {
	return self.metaEpoch
}

type RemovingInfo struct {
	RemoveTime      int64
	RemoveReplicaID uint64
}

type PartitionReplicaInfo struct {
	RaftNodes []string
	RaftIDs   map[string]uint64
	Removings map[string]RemovingInfo
	MaxRaftID int64
	epoch     EpochType
}

func (self *PartitionReplicaInfo) GetISR() []string {
	if len(self.Removings) == 0 {
		return self.RaftNodes
	}
	isr := make([]string, 0, len(self.RaftNodes))
	for _, v := range self.RaftNodes {
		if _, ok := self.Removings[v]; ok {
			continue
		}
		isr = append(isr, v)
	}
	return isr
}

func (self *PartitionReplicaInfo) Epoch() EpochType {
	return self.epoch
}

type RealLeader struct {
	Leader string
	epoch  EpochType
}

type PartitionMetaInfo struct {
	Name          string
	Partition     int
	currentLeader RealLeader
	NamespaceMetaInfo
	PartitionReplicaInfo
}

func (self *PartitionMetaInfo) IsISRQuorum() bool {
	return len(self.GetISR()) > self.Replica/2
}

func (self *PartitionMetaInfo) GetRealLeader() string {
	return self.currentLeader.Leader
}

func (self *PartitionMetaInfo) GetCopy() *PartitionMetaInfo {
	newp := *self
	newp.RaftNodes = make([]string, len(self.RaftNodes))
	copy(newp.RaftNodes, self.RaftNodes)
	newp.RaftIDs = make(map[string]uint64, len(self.RaftIDs))
	for k, v := range self.RaftIDs {
		newp.RaftIDs[k] = v
	}
	newp.Removings = make(map[string]RemovingInfo, len(self.Removings))
	for k, v := range self.Removings {
		newp.Removings[k] = v
	}
	return &newp
}

func (self *PartitionMetaInfo) GetDesp() string {
	return common.GetNsDesp(self.Name, self.Partition)
}

type ConsistentStore interface {
	WriteKey(key, value string) error
	ReadKey(key string) (string, error)
	ListKey(key string) ([]string, error)
}

type Register interface {
	InitClusterID(id string)
	// all registered pd nodes.
	GetAllPDNodes() ([]NodeInfo, error)
	// should return both the meta info for namespace and the replica info for partition
	// epoch should be updated while return
	GetNamespacePartInfo(ns string, partition int) (*PartitionMetaInfo, error)
	// get  meta info only
	GetNamespaceMetaInfo(ns string) (NamespaceMetaInfo, error)
	GetNamespaceInfo(ns string) ([]PartitionMetaInfo, error)
	GetAllNamespaces() (map[string]map[int]PartitionMetaInfo, EpochType, error)
	GetNamespacesNotifyChan() chan struct{}
	Stop()
}

// We need check leader before do any modify to etcd.
// Make sure all returned value should be copied to avoid modify by outside.
type PDRegister interface {
	Register
	Register(nodeData *NodeInfo) error // update
	Unregister(nodeData *NodeInfo) error
	// the cluster root modify index
	GetClusterEpoch() (EpochType, error)
	AcquireAndWatchLeader(leader chan *NodeInfo, stop chan struct{})

	GetDataNodes() ([]NodeInfo, error)
	// watching the cluster data node, should return the newest for the first time.
	WatchDataNodes(nodeC chan []NodeInfo, stopC chan struct{})
	// create and write the meta info to meta node
	CreateNamespace(ns string, meta *NamespaceMetaInfo) error
	UpdateNamespaceMetaInfo(ns string, meta *NamespaceMetaInfo, oldGen EpochType) error
	// create partition path
	CreateNamespacePartition(ns string, partition int) error
	IsExistNamespace(ns string) (bool, error)
	IsExistNamespacePartition(ns string, partition int) (bool, error)
	DeleteNamespacePart(ns string, partition int) error
	DeleteWholeNamespace(ns string) error
	//
	// update the replica info about replica node list, epoch for partition
	// Note: update should do check-and-set to avoid unexpected override.
	// the epoch in replicaInfo should be updated to the new epoch
	// if no partition, replica info node should create only once.
	UpdateNamespacePartReplicaInfo(ns string, partition int, replicaInfo *PartitionReplicaInfo, oldGen EpochType) error
	PrepareNamespaceMinGID() (int64, error)
}

type DataNodeRegister interface {
	Register
	Register(nodeData *NodeInfo) error // update
	Unregister(nodeData *NodeInfo) error
	// get the newest pd leader and watch the change of it.
	WatchPDLeader(leader chan *NodeInfo, stop chan struct{}) error
	GetNodeInfo(nid string) (NodeInfo, error)
	// while losing leader, update to empty nid
	// while became the new leader, update to my node
	UpdateNamespaceLeader(ns string, partition int, rl RealLeader, oldGen EpochType) (EpochType, error)
	GetNamespaceLeader(ns string, partition int) (string, EpochType, error)
	NewRegisterNodeID() (uint64, error)
}
