package server

type ServerConfig struct {
	BroadcastInterface string   `json:"broadcast_interface"`
	RedisAPIPort       int      `json:"redis_api_port"`
	HTTPAPIPort        int      `json:"httpapi_port"`
	DataDir            string   `json:"data_dir"`
	Namespaces         []string `json:"namespaces"`
}

type NamespaceConfig struct {
	Name          string `json:"name"`
	EngType       string `json:"eng_type"`
	LocalRaftAddr string `json:"local_raft_addr"`
}

type ClusterMemberInfo struct {
	ID   int    `json:"id"`
	Addr string `json:"addr"`
}

type ClusterConfig struct {
	ClusterID   uint64              `json:"cluster_id"`
	SeedNodes   []ClusterMemberInfo `json:"seed_nodes"`
	LocalNodeID int                 `json:"local_node_id"`
}

type ConfigFile struct {
	ServerConf  ServerConfig  `json:"server_conf"`
	ClusterConf ClusterConfig `json:"cluster_conf"`
}
