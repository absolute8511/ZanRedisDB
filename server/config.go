package server

import (
	"github.com/absolute8511/ZanRedisDB/node"
)

type ServerConfig struct {
	NodeID uint64 `json:"node_id"`
	// this cluster id is used for server transport to tell
	// different global cluster
	ClusterID          uint64                `json:"cluster_id"`
	BroadcastInterface string                `json:"broadcast_interface"`
	BroadcastAddr      string                `json:"broadcast_addr"`
	RedisAPIPort       int                   `json:"redis_api_port"`
	HttpAPIPort        int                   `json:"http_api_port"`
	DataDir            string                `json:"data_dir"`
	LocalRaftAddr      string                `json:"local_raft_addr"`
	Namespaces         []NamespaceNodeConfig `json:"namespaces"`
}

type NamespaceConfig struct {
	Name          string          `json:"name"`
	EngType       string          `json:"eng_type"`
	SnapCount     int             `json:"snap_count"`
	SnapCatchup   int             `json:"snap_catchup"`
	RaftGroupConf RaftGroupConfig `json:"raft_group_conf"`
}

type NamespaceNodeConfig struct {
	Name           string `json:"name"`
	LocalReplicaID uint64 `json:"local_replica_id"`
	Join           bool   `json:"join"`
}

type RaftGroupConfig struct {
	GroupID   uint64             `json:"group_id"`
	SeedNodes []node.ReplicaInfo `json:"seed_nodes"`
}

type ConfigFile struct {
	ServerConf ServerConfig `json:"server_conf"`
}
