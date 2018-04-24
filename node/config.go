package node

import (
	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/absolute8511/ZanRedisDB/rockredis"
)

type NamespaceConfig struct {
	// namespace full name with partition
	Name string `json:"name"`
	// namespace name without partition
	BaseName         string          `json:"base_name"`
	EngType          string          `json:"eng_type"`
	PartitionNum     int             `json:"partition_num"`
	SnapCount        int             `json:"snap_count"`
	SnapCatchup      int             `json:"snap_catchup"`
	Replicator       int             `json:"replicator"`
	OptimizedFsync   bool            `json:"optimized_fsync"`
	RaftGroupConf    RaftGroupConfig `json:"raft_group_conf"`
	ExpirationPolicy string          `json:"expiration_policy"`
}

func NewNSConfig() *NamespaceConfig {
	return &NamespaceConfig{
		SnapCount:        400000,
		SnapCatchup:      100000,
		ExpirationPolicy: common.DefaultExpirationPolicy,
	}
}

type NamespaceDynamicConf struct {
}

type RaftGroupConfig struct {
	GroupID   uint64        `json:"group_id"`
	SeedNodes []ReplicaInfo `json:"seed_nodes"`
}

type MachineConfig struct {
	// server node id
	NodeID              uint64                `json:"node_id"`
	BroadcastAddr       string                `json:"broadcast_addr"`
	HttpAPIPort         int                   `json:"http_api_port"`
	LocalRaftAddr       string                `json:"local_raft_addr"`
	DataRootDir         string                `json:"data_root_dir"`
	ElectionTick        int                   `json:"election_tick"`
	TickMs              int                   `json:"tick_ms"`
	KeepWAL             int                   `json:"keep_wal"`
	LearnerRole         string                `json:"learner_role"`
	RemoteSyncCluster   string                `json:"remote_sync_cluster"`
	StateMachineType    string                `json:"state_machine_type"`
	RocksDBOpts         rockredis.RockOptions `json:"rocksdb_opts"`
	RocksDBSharedConfig *rockredis.SharedRockConfig
}

type ReplicaInfo struct {
	NodeID    uint64 `json:"node_id"`
	ReplicaID uint64 `json:"replica_id"`
	RaftAddr  string `json:"raft_addr"`
}

type RaftConfig struct {
	GroupID uint64 `json:"group_id"`
	// group name is combined namespace-partition string
	GroupName string `json:"group_name"`
	// this is replica id
	ID uint64 `json:"id"`
	// local server transport address, it
	// can be used by several raft group
	RaftAddr       string                 `json:"raft_addr"`
	DataDir        string                 `json:"data_dir"`
	WALDir         string                 `json:"wal_dir"`
	KeepWAL        int                    `json:"keep_wal"`
	SnapDir        string                 `json:"snap_dir"`
	RaftPeers      map[uint64]ReplicaInfo `json:"raft_peers"`
	SnapCount      int                    `json:"snap_count"`
	SnapCatchup    int                    `json:"snap_catchup"`
	Replicator     int                    `json:"replicator"`
	OptimizedFsync bool                   `json:"optimized_fsync"`
	nodeConfig     *MachineConfig
}
