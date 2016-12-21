package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/absolute8511/ZanRedisDB/node"
	"github.com/absolute8511/ZanRedisDB/server"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/judwhite/go-svc/svc"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

var (
	flagSet     = flag.NewFlagSet("zanredisdb", flag.ExitOnError)
	cluster     = flagSet.String("cluster", "http://127.0.0.1:9021", "comma separated cluster peers")
	id          = flagSet.Int("id", 1, "node ID")
	raftAddr    = flagSet.String("raftaddr", "", "the raft address of local")
	clusterID   = flagSet.Uint64("cluster_id", 1000, "cluster id of raft")
	kvport      = flagSet.Int("port", 9121, "key-value http server port")
	redisport   = flagSet.Int("redis_port", 9131, "key-value redis server port")
	join        = flagSet.Bool("join", false, "join an existing cluster")
	dataDir     = flagSet.String("data", "", "the data path")
	showVersion = flagSet.Bool("version", false, "print version string and exit")
)

type ClusterMemberInfo struct {
	ID   int    `json:"id"`
	Addr string `json:"addr"`
}

type program struct {
	server *server.Server
}

func main() {
	defer log.Printf("main exit")
	prg := &program{}
	if err := svc.Run(prg, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGINT); err != nil {
		log.Fatal(err)
	}
}

func (p *program) Init(env svc.Environment) error {
	if env.IsWindowsService() {
		dir := filepath.Dir(os.Args[0])
		return os.Chdir(dir)
	}
	return nil
}

func (p *program) Start() error {
	flagSet.Parse(os.Args[1:])

	if *showVersion {
		fmt.Println(common.String("ZanRedisDB"))
		os.Exit(0)
	}
	if *dataDir == "" {
		tmpDir, err := ioutil.TempDir("", fmt.Sprintf("rocksdb-test-%d", time.Now().UnixNano()))
		if err != nil {
			panic(err)
		}
		*dataDir = tmpDir
	}
	fmt.Printf("open dir:%v\n", *dataDir)

	// raft provides a commit stream for the proposals from the http api
	clusterInfo := make([]ClusterMemberInfo, 0)
	err := json.Unmarshal([]byte(*cluster), &clusterInfo)
	if err != nil {
		panic(err)
	}
	clusterNodes := make(map[int]string)
	for _, v := range clusterInfo {
		clusterNodes[v.ID] = v.Addr
	}
	if *raftAddr == "" {
		*raftAddr, _ = clusterNodes[*id]
	}
	fmt.Printf("local %v start with cluster: %v\n", *raftAddr, clusterNodes)

	kvOpts := server.ServerConfig{
		DataDir: *dataDir,
		EngType: "rocksdb",
	}

	server := server.NewServer(kvOpts)
	server.InitKVNamespace(*clusterID, *id, *raftAddr, clusterNodes, *join)
	_, ok := clusterNodes[*id]
	if ok {
		var m node.MemberInfo
		m.ID = uint64(*id)
		m.ClusterID = *clusterID
		m.DataDir = *dataDir
		m.RaftURLs = append(m.RaftURLs, *raftAddr)
		m.Broadcast = "127.0.0.1"
		data, _ := json.Marshal(m)
		go func() {
			cc := raftpb.ConfChange{
				Type:    raftpb.ConfChangeUpdateNode,
				NodeID:  uint64(*id),
				Context: data,
			}
			time.Sleep(time.Second)
			server.ProposeConfChange(cc)
		}()
	}
	server.ServeAPI()
	p.server = server
	return nil
}

func (p *program) Stop() error {
	if p.server != nil {
		p.server.Stop()
	}
	return nil
}
