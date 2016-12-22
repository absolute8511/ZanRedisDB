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
	flagSet        = flag.NewFlagSet("zanredisdb", flag.ExitOnError)
	configFilePath = flagSet.String("config", "", "the config file path to read")
	raftAddr       = flagSet.String("raftaddr", "", "the raft address of local")
	join           = flagSet.Bool("join", false, "join an existing cluster")
	showVersion    = flagSet.Bool("version", false, "print version string and exit")
)

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
	var configFile server.ConfigFile
	if *configFilePath != "" {
		d, err := ioutil.ReadFile(*configFilePath)
		if err != nil {
			panic(err)
		}
		err = json.Unmarshal(d, &configFile)
		if err != nil {
			panic(err)
		}
	}
	if configFile.ServerConf.DataDir == "" {
		tmpDir, err := ioutil.TempDir("", fmt.Sprintf("rocksdb-test-%d", time.Now().UnixNano()))
		if err != nil {
			panic(err)
		}
		configFile.ServerConf.DataDir = tmpDir
	}

	// raft provides a commit stream for the proposals from the http api
	clusterNodes := make(map[int]string)
	for _, v := range configFile.ClusterConf.SeedNodes {
		clusterNodes[v.ID] = v.Addr
	}
	if *raftAddr == "" {
		*raftAddr, _ = clusterNodes[configFile.ClusterConf.LocalNodeID]
	}
	fmt.Printf("local %v start with cluster: %v\n", *raftAddr, clusterNodes)

	kvOpts := configFile.ServerConf
	nsConf := &server.NamespaceConfig{
		Name:          "default",
		EngType:       "rocksdb",
		LocalRaftAddr: *raftAddr,
	}

	id := configFile.ClusterConf.LocalNodeID
	clusterID := configFile.ClusterConf.ClusterID
	loadConf, _ := json.MarshalIndent(configFile, "", " ")
	fmt.Printf("loading with conf:%v\n", string(loadConf))
	server := server.NewServer(kvOpts)
	server.InitKVNamespace(clusterID, id, clusterNodes, *join, nsConf)
	_, ok := clusterNodes[id]
	if ok {
		var m node.MemberInfo
		m.ID = uint64(id)
		m.ClusterID = clusterID
		m.DataDir = kvOpts.DataDir
		m.RaftURLs = append(m.RaftURLs, *raftAddr)
		m.Broadcast = "127.0.0.1"
		data, _ := json.Marshal(m)
		go func() {
			cc := raftpb.ConfChange{
				Type:    raftpb.ConfChangeUpdateNode,
				NodeID:  uint64(id),
				Context: data,
			}
			time.Sleep(time.Second)
			server.ProposeConfChange(nsConf.Name, cc)
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
