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

	"github.com/siddontang/goredis"
	"github.com/stretchr/testify/assert"
	"github.com/youzan/ZanRedisDB/common"
	"github.com/youzan/ZanRedisDB/node"
	"github.com/youzan/ZanRedisDB/rockredis"
)

var clusterName = "cluster-unittest-server"

type fakeClusterInfo struct {
	clusterName string
	snapSyncs   []common.SnapshotSyncInfo
}

func (ci *fakeClusterInfo) GetClusterName() string {
	return ci.clusterName
}

func (ci *fakeClusterInfo) GetSnapshotSyncInfo(fullNS string) ([]common.SnapshotSyncInfo, error) {
	return ci.snapSyncs, nil
}

func (ci *fakeClusterInfo) UpdateMeForNamespaceLeader(fullNS string) (bool, error) {
	return false, nil
}

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
var remoteClusterBasePort = 51845

//
var testSnap = 10
var testSnapCatchup = 3

func TestMain(m *testing.M) {
	//SetLogger(int32(common.LOG_INFO), newTestLogger(t))
	//rockredis.SetLogger(int32(common.LOG_INFO), newTestLogger(t))
	//node.SetLogger(int32(common.LOG_INFO), newTestLogger(t))
	node.EnableForTest()

	if testing.Verbose() {
		rockredis.SetLogLevel(int32(common.LOG_DETAIL))
		node.SetLogLevel(int(common.LOG_DETAIL))
		sLog.SetLevel(int32(common.LOG_DETAIL))
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
	t.Logf("dir:%v\n", ctmpDir)
	kvsClusterTmp := make([]testClusterInfo, 0, replicaNum)
	learnerServersTmp := make([]*Server, 0, syncLearnerNum)
	rport := portBase
	raftPort := portBase - 100
	tmpSeeds := make([]node.ReplicaInfo, 0, replicaNum)
	fakeCI := &fakeClusterInfo{}
	fakeCI.clusterName = clusterName
	for index := 0; index < replicaNum; index++ {
		raftAddr := "http://127.0.0.1:" + strconv.Itoa(raftPort+index)
		var replica node.ReplicaInfo
		replica.NodeID = uint64(1 + index)
		replica.ReplicaID = uint64(1 + index)
		replica.RaftAddr = raftAddr
		tmpSeeds = append(tmpSeeds, replica)
		var ssi common.SnapshotSyncInfo
		ssi.NodeID = replica.NodeID
		ssi.ReplicaID = replica.ReplicaID
		ssi.RemoteAddr = "127.0.0.1"
		ssi.HttpAPIPort = strconv.Itoa(rport - 210 + index)
		ssi.DataRoot = path.Join(ctmpDir, strconv.Itoa(index))
		fakeCI.snapSyncs = append(fakeCI.snapSyncs, ssi)
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
		httpPort := rport - 210 + index
		var replica node.ReplicaInfo
		replica.NodeID = uint64(1 + index)
		replica.ReplicaID = uint64(1 + index)
		replica.RaftAddr = raftAddr
		kvOpts := ServerConfig{
			ClusterID:     "unit-test-cluster",
			DataDir:       tmpDir,
			RedisAPIPort:  redisport,
			GrpcAPIPort:   grpcPort,
			HttpAPIPort:   httpPort,
			LocalRaftAddr: raftAddr,
			BroadcastAddr: "127.0.0.1",
			TickMs:        20,
			ElectionTick:  20,
		}
		if index >= replicaNum {
			kvOpts.LearnerRole = common.LearnerRoleLogSyncer
			// use test:// will ignore the remote cluster fail
			kvOpts.RemoteSyncCluster = "test://127.0.0.1:" + strconv.Itoa(remoteClusterBasePort-110)
		}

		nsConf := node.NewNSConfig()
		nsConf.Name = "default-0"
		nsConf.BaseName = "default"
		nsConf.EngType = rockredis.EngType
		nsConf.PartitionNum = 1
		nsConf.SnapCount = testSnap
		nsConf.SnapCatchup = testSnapCatchup
		nsConf.Replicator = replicaNum
		nsConf.RaftGroupConf.GroupID = 1000
		nsConf.RaftGroupConf.SeedNodes = tmpSeeds
		nsConf.ExpirationPolicy = "consistency_deletion"
		kv := NewServer(kvOpts)
		kv.nsMgr.SetIClusterInfo(fakeCI)
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

func getTestConnForPort(t *testing.T, port int) *goredis.PoolConn {
	c := goredis.NewClient("127.0.0.1:"+strconv.Itoa(port), "")
	c.SetMaxIdleConns(4)
	conn, err := c.Get()
	if err != nil {
		t.Fatal(err)
	}
	return conn
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

func waitSyncedWithCommit(t *testing.T, w time.Duration, leaderci uint64, node *node.NamespaceNode, logSyncer bool) {
	start := time.Now()
	for {
		nsStats := node.Node.GetStats("")
		ci := node.Node.GetCommittedIndex()
		if ci >= leaderci {
			assert.Equal(t, leaderci, ci)

			v := nsStats.InternalStats["synced_index"]
			if v != nil {
				sindex := v.(uint64)
				if sindex == ci {
					break
				} else {
					t.Logf("waiting matched synced commit: %v vs %v", sindex, leaderci)
				}
			} else if !logSyncer {
				break
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
func TestStartClusterWithLogSyncer(t *testing.T) {
	//SetLogger(int32(common.LOG_INFO), newTestLogger(t))
	//rockredis.SetLogger(int32(common.LOG_INFO), newTestLogger(t))
	//node.SetLogger(int32(common.LOG_INFO), newTestLogger(t))
	remoteServers, _, _, remoteDir := startTestClusterWithBasePort(t, remoteClusterBasePort, 1, 0)
	defer func() {
		for _, remote := range remoteServers {
			remote.server.Stop()
		}
		os.RemoveAll(remoteDir)
	}()
	assert.Equal(t, 1, len(remoteServers))
	remoteConn := getTestConnForPort(t, remoteServers[0].redisPort)
	defer remoteConn.Close()
	remoteNode := remoteServers[0].server.GetNamespaceFromFullName("default-0")
	assert.NotNil(t, remoteNode)

	c := getTestClusterConn(t, true)
	defer c.Close()

	assert.Equal(t, 3, len(kvsCluster))
	leaderNode := waitForLeader(t, time.Minute)
	assert.NotNil(t, leaderNode)

	assert.Equal(t, 1, len(learnerServers))
	learnerNode := learnerServers[0].GetNamespaceFromFullName("default-0")
	assert.NotNil(t, learnerNode)
	m := learnerNode.Node.GetLocalMemberInfo()
	nsStats := learnerNode.Node.GetStats("")
	assert.Equal(t, common.LearnerRoleLogSyncer, nsStats.InternalStats["role"])

	raftStats := leaderNode.Node.GetRaftStatus()
	_, ok := raftStats.Progress[m.ID]
	assert.Equal(t, false, ok)

	node.SetSyncerOnly(true)
	err := leaderNode.Node.ProposeAddLearner(*m)
	assert.Nil(t, err)
	time.Sleep(time.Second * 3)
	assert.Equal(t, true, learnerNode.IsReady())

	node.SetSyncerOnly(false)
	key := "default:test-cluster:a"
	key2 := "default:test-cluster:a2"
	keySnapTest := "default:test-cluster:a-snaptest"
	rsp, err := goredis.String(c.Do("set", key, "1234"))
	assert.Nil(t, err)
	assert.Equal(t, OK, rsp)
	rsp, err = goredis.String(c.Do("set", key2, "a2-1234"))
	assert.Nil(t, err)
	assert.Equal(t, OK, rsp)
	writeTs := time.Now().UnixNano()

	node.SetSyncerOnly(true)
	// wait raft log synced
	time.Sleep(time.Second)
	leaderci := leaderNode.Node.GetCommittedIndex()
	waitSyncedWithCommit(t, time.Minute, leaderci, learnerNode, true)
	_, remoteIndex, ts := remoteNode.Node.GetRemoteClusterSyncedRaft(clusterName)
	assert.Equal(t, leaderci, remoteIndex)
	assert.True(t, ts <= writeTs)

	v, err := goredis.String(c.Do("get", key))
	assert.Nil(t, err)
	assert.Equal(t, "1234", v)

	v, err = goredis.String(remoteConn.Do("get", key))
	assert.Nil(t, err)
	assert.Equal(t, "1234", v)
	v, err = goredis.String(remoteConn.Do("get", key2))
	assert.Nil(t, err)
	assert.Equal(t, "a2-1234", v)

	node.SetSyncerOnly(false)
	_, err = goredis.Int(c.Do("del", key))
	assert.Nil(t, err)
	writeTs2 := time.Now().UnixNano()

	time.Sleep(time.Second * 3)
	newci := leaderNode.Node.GetCommittedIndex()
	assert.Equal(t, leaderci+1, newci)
	leaderci = newci
	t.Logf("current leader commit: %v", leaderci)
	waitSyncedWithCommit(t, time.Minute, leaderci, learnerNode, true)
	_, remoteIndex, ts = remoteNode.Node.GetRemoteClusterSyncedRaft(clusterName)
	assert.Equal(t, leaderci, remoteIndex)
	assert.True(t, ts <= writeTs2)
	assert.True(t, ts > writeTs)

	n, err := goredis.Int(c.Do("exists", key))
	assert.Nil(t, err)
	assert.Equal(t, 0, n)

	n, err = goredis.Int(remoteConn.Do("exists", key))
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
	if testSnap <= 10 {
		// test restart from snap and transfer snap to remote
		for i := 0; i < testSnap; i++ {
			rsp, err := goredis.String(c.Do("set", key, "12345"))
			assert.Nil(t, err)
			assert.Equal(t, OK, rsp)
			rsp, err = goredis.String(c.Do("set", keySnapTest, "12346"))
			assert.Nil(t, err)
			assert.Equal(t, OK, rsp)
		}
	}
	writeTs3 := time.Now().UnixNano()

	time.Sleep(time.Second)
	// restart will replay all logs
	nsConf := node.NewNSConfig()
	nsConf.Name = "default-0"
	nsConf.BaseName = "default"
	nsConf.EngType = rockredis.EngType
	nsConf.PartitionNum = 1
	nsConf.Replicator = 3
	nsConf.SnapCount = testSnap
	nsConf.SnapCatchup = testSnapCatchup
	nsConf.RaftGroupConf.GroupID = 1000
	nsConf.ExpirationPolicy = "consistency_deletion"
	learnerNode, err = learnerServers[0].InitKVNamespace(m.ID, nsConf, true)
	assert.Nil(t, err)
	node.SetSyncerOnly(true)
	err = learnerNode.Start(false)
	assert.Nil(t, err)

	leaderci = leaderNode.Node.GetCommittedIndex()
	t.Logf("current leader commit: %v", leaderci)

	waitSyncedWithCommit(t, time.Minute, leaderci, learnerNode, true)
	_, remoteIndex, ts = remoteNode.Node.GetRemoteClusterSyncedRaft(clusterName)
	assert.Equal(t, leaderci, remoteIndex)
	assert.True(t, ts <= writeTs3)
	assert.True(t, ts > writeTs2)
	node.SetSyncerOnly(false)

	v, err = goredis.String(c.Do("get", key))
	assert.Nil(t, err)
	assert.Equal(t, "12345", v)

	v, err = goredis.String(remoteConn.Do("get", key))
	assert.Nil(t, err)
	assert.Equal(t, "12345", v)

	v, err = goredis.String(c.Do("get", keySnapTest))
	assert.Nil(t, err)
	assert.Equal(t, "12346", v)

	v, err = goredis.String(remoteConn.Do("get", keySnapTest))
	assert.Nil(t, err)
	assert.Equal(t, "12346", v)
}

func TestRecoveryNewerSnapReplaying(t *testing.T) {
	// TODO: test recovery from newer snapshot, and no need
	// replay old logs, check replaying status
}

func TestRestartFollower(t *testing.T) {
	if testing.Verbose() {
		SetLogger(int32(common.LOG_DETAIL), newTestLogger(t))
		rockredis.SetLogger(int32(common.LOG_DETAIL), newTestLogger(t))
		node.SetLogger(int32(common.LOG_DEBUG), newTestLogger(t))
	}
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
	waitSyncedWithCommit(t, time.Second*30, leaderci, follower, false)
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
