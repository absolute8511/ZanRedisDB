package server

type ServerConfig struct {
	BroadcastInterface string                `json:"broadcast_interface"`
	RedisAPIPort       int                   `json:"redis_api_port"`
	HTTPAPIPort        int                   `json:"httpapi_port"`
	DataDir            string                `json:"data_dir"`
	Namespaces         []NamespaceNodeConfig `json:"namespaces"`
}

type NamespaceConfig struct {
	Name        string        `json:"name"`
	EngType     string        `json:"eng_type"`
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
