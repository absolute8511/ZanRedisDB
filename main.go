package main

import (
	"flag"
	"fmt"
	"github.com/absolute8511/gorocksdb"
	"github.com/coreos/etcd/raft/raftpb"
	"io/ioutil"
	"os"
	"strings"
	"time"
)

var (
	flagSet   = flag.NewFlagSet("zanredisdb", flag.ExitOnError)
	cluster   = flagSet.String("cluster", "http://127.0.0.1:9021", "comma separated cluster peers")
	id        = flagSet.Int("id", 1, "node ID")
	clusterID = flagSet.Uint64("cluster_id", 1000, "cluster id of raft")
	kvport    = flagSet.Int("port", 9121, "key-value server port")
	join      = flagSet.Bool("join", false, "join an existing cluster")
	dataDir   = flagSet.String("data", "", "the data path")
)

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
	commitC, errorC, raftNode := newRaftNode(*clusterID, *id, *dataDir,
		strings.Split(*cluster, ","), *join, getSnapshot, proposeC, confChangeC)

	kvOpts := &KVOptions{
		DataDir:          *dataDir,
		DefaultReadOpts:  gorocksdb.NewDefaultReadOptions(),
		DefaultWriteOpts: gorocksdb.NewDefaultWriteOptions(),
	}

	kvs = newKVStore(kvOpts, proposeC, commitC, errorC)
	defer kvs.Close()
	go raftNode.startRaft(kvs)

	// the key-value http handler will propose updates to raft
	serveHttpKVAPI(kvs, *kvport, confChangeC, errorC)
}
