# zankv 分析

## 目录介绍

	├── Godeps   								godep文件
	├── Makefile								Makefile文件
	├── README.md								ReadMe文件
	├── apps									应用目录
	│   ├── placedriver						
	│   │   └── main.go
	│   └── zankv								zankv主目录
	│       ├── main.go
	├── build-pb.sh							pb编译脚本
	├── cluster								集群相关
	│   ├── common.go
	│   ├── coordinator_stats.go
	│   ├── datanode_coord					数据协调
	│   │   └── data_node_coordinator.go
	│   ├── pdnode_coord					
	│   │   ├── pd_api.go
	│   │   ├── pd_coordinator.go
	│   │   └── place_driver.go
	│   ├── register.go
	│   ├── register_etcd.go
	│   └── util.go
	├── common								公共目录
	│   ├── api_request.go
	│   ├── api_response.go
	│   ├── binary.go
	│   ├── file_sync.go
	│   ├── file_sync_test.go
	│   ├── listener.go
	│   ├── logger.go
	│   ├── stats.go
	│   ├── type.go
	│   └── util.go
	├── default.conf							默认配置
	├── default2.conf						默认配置
	├── dist.sh
	├── node									node相关
	│   ├── config.go
	│   ├── hash.go
	│   ├── keys.go
	│   ├── kvstore.go
	│   ├── list.go
	│   ├── multi.go
	│   ├── namespace.go
	│   ├── node.go
	│   ├── raft.go
	│   ├── raft_internal.pb.go
	│   ├── raft_internal.proto
	│   ├── raft_storage.go
	│   ├── raft_test.go
	│   ├── scan.go
	│   ├── set.go
	│   ├── util.go
	│   └── zset.go
	├── pdserver
	│   ├── config.go
	│   ├── http.go
	│   ├── pdconf.example
	│   └── server.go
	├── pre-dist.sh
	├── put_test.sh
	├── raft									raft相关
	│   ├── README.md
	│   ├── design.md
	│   ├── diff_test.go
	│   ├── doc.go
	│   ├── example_test.go
	│   ├── log.go
	│   ├── log_test.go
	│   ├── log_unstable.go
	│   ├── log_unstable_test.go
	│   ├── logger.go
	│   ├── node.go
	│   ├── node_bench_test.go
	│   ├── node_test.go
	│   ├── progress.go
	│   ├── progress_test.go
	│   ├── raft.go
	│   ├── raft_flow_control_test.go
	│   ├── raft_paper_test.go
	│   ├── raft_snap_test.go
	│   ├── raft_test.go
	│   ├── raftpb
	│   │   ├── raft.pb.go
	│   │   └── raft.proto
	│   ├── rawnode.go
	│   ├── rawnode_test.go
	│   ├── read_only.go
	│   ├── status.go
	│   ├── storage.go
	│   ├── storage_test.go
	│   ├── util.go
	│   └── util_test.go
	├── rockredis								rocksdb实现的redis
	│   ├── const.go
	│   ├── doc.go
	│   ├── encode.go
	│   ├── encode_darwin.go
	│   ├── iterator.go
	│   ├── rockredis.go
	│   ├── rockredis_test.go
	│   ├── scan.go
	│   ├── t_hash.go
	│   ├── t_hash_test.go
	│   ├── t_kv.go
	│   ├── t_kv_test.go
	│   ├── t_list.go
	│   ├── t_list_test.go
	│   ├── t_set.go
	│   ├── t_set_test.go
	│   ├── t_table.go
	│   ├── t_zset.go
	│   ├── t_zset_test.go
	│   └── util.go
	├── rsyncd.conf
	├── server								server
	│   ├── config.go
	│   ├── httpapi.go
	│   ├── merge.go
	│   ├── redis_api.go
	│   ├── redis_api_test.go
	│   ├── server.go
	│   ├── server_test.go
	│   └── util.go
	├── snap									snap
	│   ├── db.go
	│   ├── message.go
	│   ├── metrics.go
	│   ├── snappb
	│   │   ├── snap.pb.go
	│   │   └── snap.proto
	│   ├── snapshotter.go
	│   └── snapshotter_test.go
	├── start_cluster.sh
	├── stats									状态
	│   ├── leader.go
	│   ├── queue.go
	│   ├── server.go
	│   └── stats.go
	├── tools									工具
	│   └── bench
	│       └── main.go
	├── transport								raft协议
	│   └── rafthttp
	│       ├── coder.go
	│       ├── doc.go
	│       ├── fake_roundtripper_test.go
	│       ├── functional_test.go
	│       ├── http.go
	│       ├── http_test.go
	│       ├── metrics.go
	│       ├── msg_codec.go
	│       ├── msg_codec_test.go
	│       ├── msgappv2_codec.go
	│       ├── msgappv2_codec_test.go
	│       ├── peer.go
	│       ├── peer_status.go
	│       ├── peer_test.go
	│       ├── pipeline.go
	│       ├── pipeline_test.go
	│       ├── probing_status.go
	│       ├── remote.go
	│       ├── snapshot_sender.go
	│       ├── snapshot_test.go
	│       ├── stream.go
	│       ├── stream_test.go
	│       ├── transport.go
	│       ├── transport_bench_test.go
	│       ├── transport_test.go
	│       ├── urlpick.go
	│       ├── urlpick_test.go
	│       ├── util.go
	│       └── util_test.go
	├── wal									wal
	│   ├── decoder.go
	│   ├── doc.go
	│   ├── encoder.go
	│   ├── file_pipeline.go
	│   ├── metrics.go
	│   ├── record_test.go
	│   ├── repair.go
	│   ├── repair_test.go
	│   ├── util.go
	│   ├── wal.go
	│   ├── wal_bench_test.go
	│   ├── wal_test.go
	│   ├── wal_unix.go
	│   ├── wal_windows.go
	│   └── walpb
	│       ├── record.go
	│       ├── record.pb.go
	│       └── record.proto
	└── yz_cp_simple

## 启动流程分析

zankv启动入口在 apps/zankv/main.go里.其中:program 实现了Service接口

Service接口定义如下:
	
	type Service interface {
		// Init is called before the program/service is started and after it's
		// determined if the program is running as a Windows Service.
		Init(Environment) error

		// Start is called after Init. This method must be non-blocking.
		Start() error

		// Stop is called in response to syscall.SIGINT, syscall.SIGTERM, or when a
		// Windows Service is stopped.
		Stop() error
	}

main函数中调用了svc.Run函数，Run函数中会分别调用Service接口的Init, Start, Stop函数。

对于zankv来说，主要启动逻辑在main.go中的Start()函数。

### main.go#program::Start

Start函数首先读取配置文件,配置结构体如下:

	type ServerConfig struct {
		// this cluster id is used for server transport to tell
		// different global cluster
		ClusterID            string   `json:"cluster_id"`
		EtcdClusterAddresses string   `json:"etcd_cluster_addresses"`
		BroadcastInterface   string   `json:"broadcast_interface"`
		BroadcastAddr        string   `json:"broadcast_addr"`
		RedisAPIPort         int      `json:"redis_api_port"`
		HttpAPIPort          int      `json:"http_api_port"`
		ProfilePort          int      `json:"profile_port"`
		DataDir              string   `json:"data_dir"`
		DataRsyncModule      string   `json:"data_rsync_module"`
		LocalRaftAddr        string   `json:"local_raft_addr"`
		Tags                 []string `json:"tags"`

		ElectionTick int `json:"election_tick"`
		TickMs       int `json:"tick_ms"`
		// default rocksdb options, can be override by namespace config
		RocksDBOpts rockredis.RockOptions `json:"rocksdb_opts"`
		Namespaces  []NamespaceNodeConfig `json:"namespaces"`
		MaxScanJob  int                   `json:"max_scan_job"`
	}

	type NamespaceNodeConfig struct {
		Name           string `json:"name"`
		LocalReplicaID uint64 `json:"local_replica_id"`
	}

	type ConfigFile struct {
		ServerConf ServerConfig `json:"server_conf"`
	}


根据配置，创建新的Server:
	
	app := server.NewServer(serverConf)
	
如果配置文件中配置了namespace，那么根据配置来读取namespace，并初始化。

根据配置文件中namespace中的Name来读取namespace内容，读取namespace的内容后，调用
	
	app.InitKVNamespace

启动server:
	
	app.Start()
	

### server/server.go#Server::Start

该函数，首先调用了rafthttp.Transport::Start来启动Raft，然后调用serveRaft.
根据配置（EtcdClusterAddresses是否为""，来创建数据协调器）来判断是启动数据协调器还是启动node的namespaceManager

	if self.dataCoord != nil {
		err := self.dataCoord.Start()
		if err != nil {
			sLog.Fatalf("data coordinator start failed: %v", err)
		}
	} else {
		self.nsMgr.Start()
	}
	
接着启动Redis协议服务器，以及http协议服务器

	go func() {
		defer self.wg.Done()
		self.serveRedisAPI(self.conf.RedisAPIPort, self.stopC)
	}()
	go func() {
		defer self.wg.Done()
		self.serveHttpAPI(self.conf.HttpAPIPort, self.stopC)
	}()