package server

type ServerConfig struct {
	BroadcastInterface string                `json:"broadcast_interface"`
	BroadcastAddr      string                `json:"broadcast_addr"`
	RedisAPIPort       int                   `json:"redis_api_port"`
	HttpAPIPort        int                   `json:"http_api_port"`
	DataDir            string                `json:"data_dir"`
	Namespaces         []NamespaceNodeConfig `json:"namespaces"`
}

type NamespaceConfig struct {
	Name        string        `json:"name"`
	EngType     string        `json:"eng_type"`
	SnapCount   int           `json:"snap_count"`
	SnapCatchup int           `json:"snap_catchup"`
	ClusterConf ClusterConfig `json:"cluster_conf"`
}

type NamespaceNodeConfig struct {
	Name          string `json:"name"`
	LocalNodeID   int    `json:"local_node_id"`
	LocalRaftAddr string `json:"local_raft_addr"`
	Join          bool   `json:"join"`
}

type ClusterMemberInfo struct {
	ID   int    `json:"id"`
	Addr string `json:"addr"`
}

type ClusterConfig struct {
	ClusterID uint64              `json:"cluster_id"`
	SeedNodes []ClusterMemberInfo `json:"seed_nodes"`
}

type ConfigFile struct {
	ServerConf ServerConfig `json:"server_conf"`
}
