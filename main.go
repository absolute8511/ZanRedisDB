package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/absolute8511/gorocksdb"
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
	kvport    = flagSet.Int("port", 9121, "key-value server port")
	join      = flagSet.Bool("join", false, "join an existing cluster")
	dataDir   = flagSet.String("data", "", "the data path")
)

type ClusterMemberInfo struct {
	ID   int    `json:"id"`
	Addr string `json:"addr"`
}

func main() {
	flagSet.Parse(os.Args[1:])

	proposeC := make(chan []byte)
	defer close(proposeC)
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
	var kvs *kvstore
	getSnapshot := func() ([]byte, error) { return kvs.getSnapshot() }
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
	commitC, errorC, raftNode := newRaftNode(*clusterID, *id, *raftAddr, *dataDir,
		clusterNodes, *join, getSnapshot, proposeC, confChangeC)

	kvOpts := &KVOptions{
		DataDir:          *dataDir,
		DefaultReadOpts:  gorocksdb.NewDefaultReadOptions(),
		DefaultWriteOpts: gorocksdb.NewDefaultWriteOptions(),
	}

	kvs = newKVStore(kvOpts, raftNode, proposeC, commitC, errorC)
	defer kvs.Close()
	go raftNode.startRaft(kvs)

	_, ok := clusterNodes[*id]
	if ok {
		var m MemberInfo
		m.ID = uint64(*id)
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
	// the key-value http handler will propose updates to raft
	serveHttpKVAPI(kvs, *kvport, confChangeC, errorC)
}
