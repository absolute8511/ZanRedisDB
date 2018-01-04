package server

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/absolute8511/ZanRedisDB/node"
	"github.com/absolute8511/ZanRedisDB/rockredis"
	"github.com/siddontang/goredis"
	"github.com/stretchr/testify/assert"
)

var testClusterOnce sync.Once
var kvsCluster []*Server
var learnerServers []*Server
var redisClusterPort int
var gtmpClusterDir string

func TestMain(m *testing.M) {
	//SetLogger(2, newTestLogger(t))
	if testing.Verbose() {
		rockredis.SetLogLevel(4)
		node.SetLogLevel(4)
	}
	ret := m.Run()
	if kvs != nil {
		kvs.Stop()
	}
	if kvsMerge != nil {
		kvsMerge.Stop()
	}
	if kvsFullScan != nil {
		kvsFullScan.Stop()
	}
	for _, v := range kvsCluster {
		v.Stop()
	}
	if ret == 0 {
		if strings.Contains(gtmpClusterDir, "rocksdb-test") {
			fmt.Println("removing: ", gtmpClusterDir)
			os.RemoveAll(gtmpClusterDir)
		}
		if strings.Contains(gtmpMergeDir, "rocksdb-test") {
			fmt.Println("removing: ", gtmpMergeDir)
			os.RemoveAll(gtmpMergeDir)
		}
		if strings.Contains(gtmpScanDir, "rocksdb-test") {
			fmt.Println("removing: ", gtmpScanDir)
			os.RemoveAll(gtmpScanDir)
		}
		if strings.Contains(gtmpDir, "rocksdb-test") {
			fmt.Println("removing: ", gtmpDir)
			os.RemoveAll(gtmpDir)
		}
	}
	os.Exit(ret)
}

func startTestCluster(t *testing.T, replicaNum int, syncLearnerNum int) ([]*Server, []*Server, int, string) {
	ctmpDir, err := ioutil.TempDir("", fmt.Sprintf("rocksdb-test-%d", time.Now().UnixNano()))
	assert.Nil(t, err)
	//SetLogger(2, newTestLogger(t))
	node.SetLogger(2, newTestLogger(t))
	rockredis.SetLogger(2, newTestLogger(t))
	t.Logf("dir:%v\n", ctmpDir)
	kvsClusterTmp := make([]*Server, 0, replicaNum)
	learnerServersTmp := make([]*Server, 0, syncLearnerNum)
	rport := 52845
	raftPort := 52745
	seedNodes := make([]node.ReplicaInfo, 0, replicaNum)
	for index := 0; index < replicaNum; index++ {
		raftAddr := "http://127.0.0.1:" + strconv.Itoa(raftPort+index)
		var replica node.ReplicaInfo
		replica.NodeID = uint64(1 + index)
		replica.ReplicaID = uint64(1 + index)
		replica.RaftAddr = raftAddr
		seedNodes = append(seedNodes, replica)
	}
	for index := 0; index < replicaNum+syncLearnerNum; index++ {
		tmpDir := path.Join(ctmpDir, strconv.Itoa(index))
		os.MkdirAll(tmpDir, 0700)
		ioutil.WriteFile(
			path.Join(tmpDir, "myid"),
			[]byte(strconv.FormatInt(int64(1+index), 10)),
			common.FILE_PERM)
		raftAddr := "http://127.0.0.1:" + strconv.Itoa(raftPort+index)
		redisport := rport + index
		var replica node.ReplicaInfo
		replica.NodeID = uint64(1 + index)
		replica.ReplicaID = uint64(1 + index)
		replica.RaftAddr = raftAddr
		kvOpts := ServerConfig{
			ClusterID:     "unit-test-cluster",
			DataDir:       tmpDir,
			RedisAPIPort:  redisport,
			LocalRaftAddr: raftAddr,
			BroadcastAddr: "127.0.0.1",
			TickMs:        100,
			ElectionTick:  5,
		}
		if index >= replicaNum {
			kvOpts.LearnerRole = common.LearnerRoleLogSyncer
		}
		if testing.Verbose() {
			rockredis.SetLogLevel(4)
			node.SetLogLevel(4)
		}
		nsConf := node.NewNSConfig()
		nsConf.Name = "default-0"
		nsConf.BaseName = "default"
		nsConf.EngType = rockredis.EngType
		nsConf.PartitionNum = 1
		nsConf.Replicator = replicaNum
		nsConf.RaftGroupConf.GroupID = 1000
		nsConf.RaftGroupConf.SeedNodes = seedNodes
		nsConf.ExpirationPolicy = "consistency_deletion"
		kv := NewServer(kvOpts)
		if _, err := kv.InitKVNamespace(replica.ReplicaID, nsConf, false); err != nil {
			t.Fatalf("failed to init namespace: %v", err)
		}
		kv.Start()
		if index >= replicaNum {
			learnerServersTmp = append(learnerServersTmp, kv)
		} else {
			kvsClusterTmp = append(kvsClusterTmp, kv)
		}
	}

	time.Sleep(time.Second * 3)
	return kvsClusterTmp, learnerServersTmp, rport, ctmpDir
}

func getTestClusterConn(t *testing.T) *goredis.PoolConn {
	testOnce.Do(func() {
		kvsCluster, learnerServers, redisClusterPort, gtmpClusterDir = startTestCluster(t, 3, 1)
	},
	)
	c := goredis.NewClient("127.0.0.1:"+strconv.Itoa(redisClusterPort), "")
	c.SetMaxIdleConns(4)
	conn, err := c.Get()
	if err != nil {
		t.Fatal(err)
	}
	return conn
}

func TestStartCluster(t *testing.T) {
	c := getTestClusterConn(t)
	defer c.Close()

	assert.Equal(t, 3, len(kvsCluster))
	var leaderNode *node.NamespaceNode
	for _, n := range kvsCluster {
		replicaNode := n.GetNamespaceFromFullName("default-0")
		assert.NotNil(t, replicaNode)
		if replicaNode.Node.IsLead() {
			leaderNode = replicaNode
			break
		}
	}

	assert.Equal(t, 1, len(learnerServers))
	learnerNode := learnerServers[0].GetNamespaceFromFullName("default-0")
	assert.NotNil(t, learnerNode)
	m := learnerNode.Node.GetLocalMemberInfo()
	nsStats := learnerNode.Node.GetStats()
	assert.Equal(t, common.LearnerRoleLogSyncer, nsStats.InternalStats["role"])

	raftStats := leaderNode.Node.GetRaftStatus()
	_, ok := raftStats.Progress[m.ID]
	assert.Equal(t, false, ok)

	err := leaderNode.Node.ProposeAddLearner(*m)
	assert.Nil(t, err)
	time.Sleep(time.Second * 5)

	key := "default:test-cluster:a"
	rsp, err := goredis.String(c.Do("set", key, "1234"))
	assert.Nil(t, err)
	assert.Equal(t, OK, rsp)

	nsStats = learnerNode.Node.GetStats()
	assert.Equal(t, int64(1), nsStats.InternalStats["synced"])
	sindex := nsStats.InternalStats["synced_index"].(uint64)
	assert.Equal(t, true, sindex > uint64(3))

	v, err := goredis.String(c.Do("get", key))
	assert.Nil(t, err)
	assert.Equal(t, "1234", v)
	_, err = goredis.Int(c.Do("del", key))
	assert.Nil(t, err)

	nsStats = learnerNode.Node.GetStats()
	assert.Equal(t, int64(2), nsStats.InternalStats["synced"])
	assert.Equal(t, sindex+1, nsStats.InternalStats["synced_index"])

	n, err := goredis.Int(c.Do("exists", key))
	assert.Nil(t, err)
	assert.Equal(t, 0, n)

	raftStats = leaderNode.Node.GetRaftStatus()
	pr := raftStats.Progress[m.ID]
	assert.Equal(t, true, pr.IsLearner)
}

func TestRestartFollower(t *testing.T) {
	assert.Nil(t, nil)
}

func TestRestartLeader(t *testing.T) {
	// TODO:
}

func TestMarkLeaderAsRemoving(t *testing.T) {
	// TODO:
}

func TestMarkFollowerAsRemoving(t *testing.T) {
	// TODO:
}

func TestTransferLeaderWhileReplicaNotReady(t *testing.T) {
	// TODO: test transfer leader while replica is restarting and not catchuped fully.
	// should only transfer leader when replica has almost the newest raft logs
}
func TestMarkAsRemovingWhileNotEnoughAlives(t *testing.T) {
	// TODO:
	// should not mark as remove while there is not enough for replica
}

func TestRestartWithCleanData(t *testing.T) {
	// TODO:
}

func TestRestartWithMigrate(t *testing.T) {
	// TODO:
	// stop for a while and wait the data migrate to others
	// and then start this node to join the cluster and wait
	// data migrate back to this node
}

func TestRestartCluster(t *testing.T) {
	// TODO:
	// stop all nodes in cluster and start one by one
}

func TestRestartWithForceAlone(t *testing.T) {
	// TODO: test force restart with alone
}
