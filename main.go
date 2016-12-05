package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/absolute8511/ZanRedisDB/node"
	"github.com/absolute8511/ZanRedisDB/redisapi"
	"github.com/absolute8511/ZanRedisDB/store"
	"github.com/coreos/etcd/raft/raftpb"
	"io/ioutil"
	"os"
	"time"
)

var (
	flagSet   = flag.NewFlagSet("zanredisdb", flag.ExitOnError)
	cluster   = flagSet.String("cluster", "http://127.0.0.1:9021", "comma separated cluster peers")
	id        = flagSet.Int("id", 1, "node ID")
	raftAddr  = flagSet.String("raftaddr", "", "the raft address of local")
	clusterID = flagSet.Uint64("cluster_id", 1000, "cluster id of raft")
	kvport    = flagSet.Int("port", 9121, "key-value http server port")
	redisport = flagSet.Int("redis_port", 9131, "key-value redis server port")
	join      = flagSet.Bool("join", false, "join an existing cluster")
	dataDir   = flagSet.String("data", "", "the data path")
)

type ClusterMemberInfo struct {
	ID   int    `json:"id"`
	Addr string `json:"addr"`
}

func main() {
	flagSet.Parse(os.Args[1:])

	confChangeC := make(chan raftpb.ConfChange)
	defer close(confChangeC)
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
	kvOpts := &store.KVOptions{
		DataDir: *dataDir,
		EngType: "rocksdb",
	}

	kvs, nodeStopC := node.NewKVNode(kvOpts, *clusterID, *id, *raftAddr,
		clusterNodes, *join, confChangeC)
	defer kvs.Stop()

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
			confChangeC <- cc
		}()
	}
	// TODO: we should start api server until replay log finished and
	// also while we recovery we need to disable api.
	go redisapi.ServeRedisAPI(*redisport, nodeStopC)
	// the key-value http handler will propose updates to raft
	serveHttpKVAPI(kvs, *kvport, confChangeC, nodeStopC)
}
