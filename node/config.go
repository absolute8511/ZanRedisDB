package node

type NodeConfig struct {
	BroadcastAddr string `json:"broadcast_addr"`
	HttpAPIPort   int    `json:"http_api_port"`
}

type RaftConfig struct {
	ClusterID   uint64             `json:"cluster_id"`
	Namespace   string             `json:"namespace"`
	ID          int                `json:"id"`
	RaftAddr    string             `json:"raft_addr"`
	DataDir     string             `json:"data_dir"`
	WALDir      string             `json:"wal_dir"`
	SnapDir     string             `json:"snap_dir"`
	RaftPeers   map[int]string     `json:"raft_peers"`
	PeersConfig map[int]NodeConfig `json:"peers_config"`
	SnapCount   int                `json:"snap_count"`
	SnapCatchup int                `json:"snap_catchup"`
	nodeConfig  *NodeConfig
}
