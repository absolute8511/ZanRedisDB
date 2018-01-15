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

type testClusterInfo struct {
	server    *Server
	nsConf    *node.NamespaceConfig
	redisPort int
	replicaID uint64
}

var testClusterOnce sync.Once
var kvsCluster []testClusterInfo
var learnerServers []*Server
var gtmpClusterDir string
var seedNodes []node.ReplicaInfo

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
		v.server.Stop()
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

func startTestCluster(t *testing.T, replicaNum int, syncLearnerNum int) ([]testClusterInfo, []node.ReplicaInfo, []*Server, string) {
	rport := 52845
	return startTestClusterWithBasePort(t, rport, replicaNum, syncLearnerNum)
}

func startTestClusterWithBasePort(t *testing.T, portBase int, replicaNum int, syncLearnerNum int) ([]testClusterInfo, []node.ReplicaInfo, []*Server, string) {
	ctmpDir, err := ioutil.TempDir("", fmt.Sprintf("rocksdb-test-%d", time.Now().UnixNano()))
	assert.Nil(t, err)
	SetLogger(2, newTestLogger(t))
	node.SetLogger(2, newTestLogger(t))
	rockredis.SetLogger(2, newTestLogger(t))
	t.Logf("dir:%v\n", ctmpDir)
	kvsClusterTmp := make([]testClusterInfo, 0, replicaNum)
	learnerServersTmp := make([]*Server, 0, syncLearnerNum)
	rport := portBase
	raftPort := portBase - 100
	tmpSeeds := make([]node.ReplicaInfo, 0, replicaNum)
	for index := 0; index < replicaNum; index++ {
		raftAddr := "http://127.0.0.1:" + strconv.Itoa(raftPort+index)
		var replica node.ReplicaInfo
		replica.NodeID = uint64(1 + index)
		replica.ReplicaID = uint64(1 + index)
		replica.RaftAddr = raftAddr
		tmpSeeds = append(tmpSeeds, replica)
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
		grpcPort := rport - 110 + index
		var replica node.ReplicaInfo
		replica.NodeID = uint64(1 + index)
		replica.ReplicaID = uint64(1 + index)
		replica.RaftAddr = raftAddr
		kvOpts := ServerConfig{
			ClusterID:     "unit-test-cluster",
			DataDir:       tmpDir,
			RedisAPIPort:  redisport,
			GrpcAPIPort:   grpcPort,
			LocalRaftAddr: raftAddr,
			BroadcastAddr: "127.0.0.1",
			TickMs:        20,
			ElectionTick:  20,
		}
		if index >= replicaNum {
			kvOpts.LearnerRole = common.LearnerRoleLogSyncer
			// use test:// will ignore the remote cluster fail
			kvOpts.RemoteSyncCluster = "test://127.0.0.1:" + strconv.Itoa(51845-110)
		}
		if testing.Verbose() {
			rockredis.SetLogLevel(4)
			node.SetLogLevel(4)
			sLog.SetLevel(4)
		}
		nsConf := node.NewNSConfig()
		nsConf.Name = "default-0"
		nsConf.BaseName = "default"
		nsConf.EngType = rockredis.EngType
		nsConf.PartitionNum = 1
		nsConf.Replicator = replicaNum
		nsConf.RaftGroupConf.GroupID = 1000
		nsConf.RaftGroupConf.SeedNodes = tmpSeeds
		nsConf.ExpirationPolicy = "consistency_deletion"
		kv := NewServer(kvOpts)
		if _, err := kv.InitKVNamespace(replica.ReplicaID, nsConf, false); err != nil {
			t.Fatalf("failed to init namespace: %v", err)
		}
		kv.Start()
		if index >= replicaNum {
			learnerServersTmp = append(learnerServersTmp, kv)
		} else {
			kvsClusterTmp = append(kvsClusterTmp, testClusterInfo{server: kv,
				nsConf: nsConf, redisPort: redisport, replicaID: replica.ReplicaID})
		}
	}

	time.Sleep(time.Second * 3)
	return kvsClusterTmp, tmpSeeds, learnerServersTmp, ctmpDir
}

func getTestClusterConn(t *testing.T, needLeader bool) *goredis.PoolConn {
	testClusterOnce.Do(func() {
		kvsCluster, seedNodes, learnerServers, gtmpClusterDir = startTestCluster(t, 3, 1)
	},
	)
	if needLeader {
		leaderNode := waitForLeader(t, time.Minute)
		assert.NotNil(t, leaderNode)
	}
	rport := 0
	for _, n := range kvsCluster {
		replicaNode := n.server.GetNamespaceFromFullName("default-0")
		assert.NotNil(t, replicaNode)
		if needLeader {
			if replicaNode.Node.IsLead() {
				rport = n.redisPort
				break
			}
		} else {
			rport = n.redisPort
			break
		}
	}
	c := goredis.NewClient("127.0.0.1:"+strconv.Itoa(rport), "")
	c.SetMaxIdleConns(4)
	conn, err := c.Get()
	if err != nil {
		t.Fatal(err)
	}
	return conn
}

func waitForLeader(t *testing.T, w time.Duration) *node.NamespaceNode {
	start := time.Now()
	for {
		for _, n := range kvsCluster {
			replicaNode := n.server.GetNamespaceFromFullName("default-0")
			assert.NotNil(t, replicaNode)
			if replicaNode.Node.IsLead() {
				leaderNode := replicaNode
				return leaderNode
			}
		}
		if time.Since(start) > w {
			t.Fatalf("\033[31m timed out %v for wait leader \033[39m\n", time.Since(start))
			break
		}
		time.Sleep(time.Second)
	}
	return nil
}

func waitSyncedWithCommit(t *testing.T, w time.Duration, leaderci uint64, node *node.NamespaceNode) {
	start := time.Now()
	for {
		nsStats := node.Node.GetStats()
		ci := node.Node.GetCommittedIndex()
		if ci >= leaderci {
			assert.Equal(t, leaderci, ci)
			sindex := nsStats.InternalStats["synced_index"].(uint64)
			if sindex == ci {
				break
			} else {
				t.Logf("waiting matched synced commit: %v vs %v", sindex, leaderci)
			}
		} else {
			t.Logf("waiting matched commit: %v vs %v", ci, leaderci)
		}
		time.Sleep(time.Second)
		if time.Since(start) > w {
			t.Errorf("\033[31m timed out %v for wait raft stats \033[39m\n", time.Since(start))
			break
		}
	}
}
func TestStartCluster(t *testing.T) {
	remoteServers, _, _, remoteDir := startTestClusterWithBasePort(t, 51845, 1, 0)
	defer func() {
		for _, remote := range remoteServers {
			remote.server.Stop()
		}
		os.RemoveAll(remoteDir)
	}()

	c := getTestClusterConn(t, true)
	defer c.Close()

	assert.Equal(t, 3, len(kvsCluster))
	leaderNode := waitForLeader(t, time.Minute)
	assert.NotNil(t, leaderNode)

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
	time.Sleep(time.Second * 3)
	assert.Equal(t, true, learnerNode.IsReady())

	key := "default:test-cluster:a"
	rsp, err := goredis.String(c.Do("set", key, "1234"))
	assert.Nil(t, err)
	assert.Equal(t, OK, rsp)

	// wait raft log synced
	time.Sleep(time.Second)
	leaderci := leaderNode.Node.GetCommittedIndex()
	waitSyncedWithCommit(t, time.Minute, leaderci, learnerNode)

	v, err := goredis.String(c.Do("get", key))
	assert.Nil(t, err)
	assert.Equal(t, "1234", v)
	_, err = goredis.Int(c.Do("del", key))
	assert.Nil(t, err)

	time.Sleep(time.Second * 3)
	newci := leaderNode.Node.GetCommittedIndex()
	assert.Equal(t, leaderci+1, newci)
	leaderci = newci
	waitSyncedWithCommit(t, time.Minute, leaderci, learnerNode)

	n, err := goredis.Int(c.Do("exists", key))
	assert.Nil(t, err)
	assert.Equal(t, 0, n)

	raftStats = leaderNode.Node.GetRaftStatus()
	pr := raftStats.Progress[m.ID]
	assert.Equal(t, true, pr.IsLearner)

	learnerNode.Close()
	time.Sleep(time.Second)
	learnerNode = learnerServers[0].GetNamespaceFromFullName("default-0")
	assert.Nil(t, learnerNode)

	_, err = goredis.Int(c.Do("del", key))
	assert.Nil(t, err)

	// restart will replay all logs
	nsConf := node.NewNSConfig()
	nsConf.Name = "default-0"
	nsConf.BaseName = "default"
	nsConf.EngType = rockredis.EngType
	nsConf.PartitionNum = 1
	nsConf.Replicator = 3
	nsConf.RaftGroupConf.GroupID = 1000
	nsConf.ExpirationPolicy = "consistency_deletion"
	learnerNode, err = learnerServers[0].InitKVNamespace(m.ID, nsConf, true)
	assert.Nil(t, err)
	err = learnerNode.Start(false)
	assert.Nil(t, err)

	leaderci = leaderNode.Node.GetCommittedIndex()

	waitSyncedWithCommit(t, time.Minute, leaderci, learnerNode)
}

func TestRestartFollower(t *testing.T) {
	c := getTestClusterConn(t, true)
	defer c.Close()

	assert.Equal(t, 3, len(kvsCluster))
	leaderNode := waitForLeader(t, time.Minute)
	assert.NotNil(t, leaderNode)
	var followerS testClusterInfo
	var follower *node.NamespaceNode
	for _, n := range kvsCluster {
		replicaNode := n.server.GetNamespaceFromFullName("default-0")
		assert.NotNil(t, replicaNode)
		if !replicaNode.Node.IsLead() {
			followerS = n
			follower = replicaNode
			break
		}
	}

	m := follower.Node.GetLocalMemberInfo()
	follower.Close()

	key := "default:test-cluster:a"
	rsp, err := goredis.String(c.Do("set", key, "1234"))
	assert.Nil(t, err)
	assert.Equal(t, OK, rsp)

	follower, err = followerS.server.InitKVNamespace(m.ID, followerS.nsConf, true)
	assert.Nil(t, err)
	follower.Start(false)
	leaderci := leaderNode.Node.GetCommittedIndex()
	waitSyncedWithCommit(t, time.Minute, leaderci, follower)
}

func TestRestartCluster(t *testing.T) {
	// stop all nodes in cluster and start one by one
	c := getTestClusterConn(t, true)
	defer c.Close()

	assert.Equal(t, 3, len(kvsCluster))

	leaderNode := waitForLeader(t, time.Minute)
	assert.NotNil(t, leaderNode)

	ci := leaderNode.Node.GetCommittedIndex()

	key := "default:test-cluster:a"
	rsp, err := goredis.String(c.Do("set", key, "1234"))
	assert.Nil(t, err)
	assert.Equal(t, OK, rsp)

	for _, s := range kvsCluster {
		node := s.server.GetNamespaceFromFullName("default-0")
		node.Close()
	}

	for _, s := range kvsCluster {
		node, err := s.server.InitKVNamespace(s.replicaID, s.nsConf, true)
		assert.Nil(t, err)
		assert.NotNil(t, node)
		err = node.Start(false)
		assert.Nil(t, err)
	}
	time.Sleep(time.Second * 2)

	hasLeader := false
	for _, s := range kvsCluster {
		replicaNode := s.server.GetNamespaceFromFullName("default-0")
		assert.NotNil(t, replicaNode)
		newci := replicaNode.Node.GetCommittedIndex()
		assert.Equal(t, ci+1+1, newci)
		if replicaNode.Node.IsLead() {
			hasLeader = true
		}
	}
	assert.Equal(t, true, hasLeader)
}
