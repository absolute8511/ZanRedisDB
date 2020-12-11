package server

import (
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/absolute8511/glog"
	"github.com/siddontang/goredis"
	"github.com/stretchr/testify/assert"
	"github.com/youzan/ZanRedisDB/common"
	"github.com/youzan/ZanRedisDB/engine"
	"github.com/youzan/ZanRedisDB/node"
	"github.com/youzan/ZanRedisDB/pkg/fileutil"
	"github.com/youzan/ZanRedisDB/raft"
	"github.com/youzan/ZanRedisDB/rockredis"
	"github.com/youzan/ZanRedisDB/slow"
	"github.com/youzan/ZanRedisDB/transport/rafthttp"
	"github.com/youzan/ZanRedisDB/wal"
)

var clusterName = "cluster-unittest-server"

const (
	testEngineType = "rocksdb"
)

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
var remoteClusterBasePort = 22345
var fullscanTestPortBase = 52345
var mergeTestPortBase = 42345
var redisAPITestPortBase = 12345
var localClusterTestPortBase = 24345
var tmpUsedClusterPortBase = 23345

//
var testSnap = 10
var testSnapCatchup = 3

func TestMain(m *testing.M) {
	node.EnableForTest()
	node.EnableSlowLimiterTest(true)

	flag.Parse()

	if testing.Verbose() {
		SetLogger(int32(common.LOG_DETAIL), common.NewLogger())
		rockredis.SetLogger(int32(common.LOG_DEBUG), common.NewLogger())
		slow.SetLogger(int32(common.LOG_DEBUG), common.NewLogger())
		node.SetLogger(int32(common.LOG_DEBUG), common.NewLogger())
		engine.SetLogger(int32(common.LOG_DEBUG), common.NewLogger())
	}

	ret := m.Run()
	if gkvs != nil {
		gkvs.Stop()
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
	glog.Flush()
	os.Exit(ret)
}

func startTestCluster(t *testing.T, replicaNum int, syncLearnerNum int) ([]testClusterInfo, []node.ReplicaInfo, []*Server, string) {
	rport := localClusterTestPortBase
	kvs, ns, servers, path, err := startTestClusterWithBasePort("unit-test-localcluster", rport, replicaNum, syncLearnerNum, false)
	if err != nil {
		t.Fatalf("init cluster failed: %v", err)
	}
	return kvs, ns, servers, path
}

func startTestClusterWithEmptySM(replicaNum int) ([]testClusterInfo, []node.ReplicaInfo, []*Server, string, error) {
	rport := tmpUsedClusterPortBase
	return startTestClusterWithBasePort("unit-test-emptysm", rport, replicaNum, 0, true)
}

func startTestClusterWithBasePort(cid string, portBase int, replicaNum int,
	syncLearnerNum int, useEmptySM bool) ([]testClusterInfo, []node.ReplicaInfo, []*Server, string, error) {
	ctmpDir, err := ioutil.TempDir("", fmt.Sprintf("rocksdb-test-%d", time.Now().UnixNano()))
	if err != nil {
		return nil, nil, nil, "", err
	}
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
		grpcPort := rport - 310 + index
		httpPort := rport - 210 + index
		var replica node.ReplicaInfo
		replica.NodeID = uint64(1 + index)
		replica.ReplicaID = uint64(1 + index)
		replica.RaftAddr = raftAddr
		kvOpts := ServerConfig{
			ClusterID:      cid,
			DataDir:        tmpDir,
			RedisAPIPort:   redisport,
			GrpcAPIPort:    grpcPort,
			HttpAPIPort:    httpPort,
			LocalRaftAddr:  raftAddr,
			BroadcastAddr:  "127.0.0.1",
			TickMs:         20,
			ElectionTick:   20,
			UseRocksWAL:    false,
			SharedRocksWAL: true,
		}
		kvOpts.RocksDBOpts.EnablePartitionedIndexFilter = true
		kvOpts.RocksDBOpts.EngineType = testEngineType

		if index >= replicaNum {
			kvOpts.LearnerRole = common.LearnerRoleLogSyncer
			// use test:// will ignore the remote cluster fail
			kvOpts.RemoteSyncCluster = "test://127.0.0.1:" + strconv.Itoa(remoteClusterBasePort-310)
		}
		if useEmptySM {
			kvOpts.StateMachineType = "empty_sm"
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
		nsConf.ExpirationPolicy = common.WaitCompactExpirationPolicy
		nsConf.DataVersion = common.ValueHeaderV1Str
		kv, _ := NewServer(kvOpts)
		kv.nsMgr.SetIClusterInfo(fakeCI)
		if _, err := kv.InitKVNamespace(replica.ReplicaID, nsConf, false); err != nil {
			return kvsClusterTmp, tmpSeeds, learnerServersTmp, ctmpDir, err
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
	return kvsClusterTmp, tmpSeeds, learnerServersTmp, ctmpDir, nil
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

func waitForLeaderForClusters(w time.Duration, kvs []testClusterInfo) (*node.NamespaceNode, error) {
	start := time.Now()
	for {
		for _, n := range kvs {
			replicaNode := n.server.GetNamespaceFromFullName("default-0")
			if replicaNode.Node.IsLead() {
				leaderNode := replicaNode
				return leaderNode, nil
			}
		}
		if time.Since(start) > w {
			return nil, fmt.Errorf("\033[31m timed out %v for wait leader \033[39m\n", time.Since(start))
		}
		time.Sleep(time.Second)
	}
	return nil, errors.New("no leader")
}

func waitSyncedWithCommit(t *testing.T, w time.Duration, leaderci uint64, node *node.NamespaceNode, logSyncer bool) {
	start := time.Now()
	for {
		nsStats := node.Node.GetStats("", true)
		ci := node.Node.GetAppliedIndex()
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
	remoteServers, _, _, remoteDir, err := startTestClusterWithBasePort("unit-test-remotecluster", remoteClusterBasePort, 1, 0, false)
	defer func() {
		for _, remote := range remoteServers {
			remote.server.Stop()
		}
		os.RemoveAll(remoteDir)
	}()
	assert.Nil(t, err)
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
	nsStats := learnerNode.Node.GetStats("", true)
	assert.Equal(t, common.LearnerRoleLogSyncer, nsStats.InternalStats["role"])

	raftStats := leaderNode.Node.GetRaftStatus()
	_, ok := raftStats.Progress[m.ID]
	assert.Equal(t, false, ok)

	node.SetSyncerOnly(true)
	err = leaderNode.Node.ProposeAddLearner(*m)
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
	leaderci := leaderNode.Node.GetAppliedIndex()
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
	newci := leaderNode.Node.GetAppliedIndex()
	assert.Equal(t, leaderci+1, newci)
	leaderci = newci
	t.Logf("current leader commit: %v", leaderci)
	waitSyncedWithCommit(t, time.Minute, leaderci, learnerNode, true)
	_, remoteIndex, ts = remoteNode.Node.GetRemoteClusterSyncedRaft(clusterName)
	t.Logf("remote synced: %v, %v (%v-%v)", remoteIndex, ts, writeTs, writeTs2)
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
	nsConf.ExpirationPolicy = common.WaitCompactExpirationPolicy
	nsConf.DataVersion = common.ValueHeaderV1Str
	learnerNode, err = learnerServers[0].InitKVNamespace(m.ID, nsConf, true)
	assert.Nil(t, err)
	node.SetSyncerOnly(true)
	err = learnerNode.Start(false)
	assert.Nil(t, err)

	leaderci = leaderNode.Node.GetAppliedIndex()
	t.Logf("current leader commit: %v", leaderci)

	waitSyncedWithCommit(t, time.Minute, leaderci, learnerNode, true)
	_, remoteIndex, ts = remoteNode.Node.GetRemoteClusterSyncedRaft(clusterName)
	t.Logf("remote synced: %v, %v (%v-%v)", remoteIndex, ts, writeTs2, writeTs3)
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
	//if testing.Verbose() {
	//	SetLogger(int32(common.LOG_DETAIL), newTestLogger(t))
	//	rockredis.SetLogger(int32(common.LOG_DETAIL), newTestLogger(t))
	//	node.SetLogger(int32(common.LOG_DEBUG), newTestLogger(t))
	//}
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

	time.Sleep(time.Second)
	follower, err = followerS.server.InitKVNamespace(m.ID, followerS.nsConf, true)
	assert.Nil(t, err)
	follower.Start(false)
	leaderci := leaderNode.Node.GetAppliedIndex()
	waitSyncedWithCommit(t, time.Second*30, leaderci, follower, false)
}

func TestRestartCluster(t *testing.T) {
	// stop all nodes in cluster and start one by one
	c := getTestClusterConn(t, true)
	defer c.Close()

	assert.Equal(t, 3, len(kvsCluster))

	leaderNode := waitForLeader(t, time.Minute)
	assert.NotNil(t, leaderNode)

	ci := leaderNode.Node.GetAppliedIndex()
	key := "default:test-cluster:a"
	rsp, err := goredis.String(c.Do("set", key, "1234"))
	assert.Nil(t, err)
	assert.Equal(t, OK, rsp)

	for _, s := range kvsCluster {
		node := s.server.GetNamespaceFromFullName("default-0")
		node.Close()
	}
	time.Sleep(time.Second)

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
		newci := replicaNode.Node.GetAppliedIndex()
		assert.Equal(t, ci+1+1, newci)
		if replicaNode.Node.IsLead() {
			hasLeader = true
		}
	}
	assert.Equal(t, true, hasLeader)
}

func TestReadWriteAfterStopped(t *testing.T) {
	// TODO: stop all nodes in cluster and send read write should error
	c := getTestClusterConn(t, true)
	defer c.Close()

	assert.Equal(t, 3, len(kvsCluster))

	leaderNode := waitForLeader(t, time.Minute)
	assert.NotNil(t, leaderNode)
}

func TestCompactCancelAfterStopped(t *testing.T) {
	// TODO: stop all nodes in cluster should cancel the running compaction
	// also check if compact error after closed
	c := getTestClusterConn(t, true)
	defer c.Close()

	assert.Equal(t, 3, len(kvsCluster))

	leaderNode := waitForLeader(t, time.Minute)
	assert.NotNil(t, leaderNode)
}

func TestOptimizeExpireMeta(t *testing.T) {
	c := getTestClusterConn(t, true)
	defer c.Close()

	assert.Equal(t, 3, len(kvsCluster))

	leaderNode := waitForLeader(t, time.Minute)
	assert.NotNil(t, leaderNode)
	key1 := "default:test:exp_compact"
	ttl := 2

	for i := 0; i < 1000; i++ {
		_, err := goredis.String(c.Do("setex", key1+strconv.Itoa(i), ttl, "hello"))
		assert.Nil(t, err)
	}
	for _, s := range kvsCluster {
		s.server.nsMgr.DisableOptimizeDB(true)
		s.server.nsMgr.OptimizeDBExpire("default")
	}
	for _, s := range kvsCluster {
		s.server.nsMgr.DisableOptimizeDB(false)
		s.server.nsMgr.OptimizeDBExpire("default")
	}
}

func readWALNames(dirpath string) []string {
	names, err := fileutil.ReadDir(dirpath)
	if err != nil {
		return nil
	}
	wnames := checkWalNames(names)
	if len(wnames) == 0 {
		return nil
	}
	return wnames
}
func checkWalNames(names []string) []string {
	wnames := make([]string, 0)
	for _, name := range names {
		if _, _, err := parseWALName(name); err != nil {
			// don't complain about left over tmp files
			if !strings.HasSuffix(name, ".tmp") {
			}
			continue
		}
		wnames = append(wnames, name)
	}
	return wnames
}

func parseWALName(str string) (seq, index uint64, err error) {
	if !strings.HasSuffix(str, ".wal") {
		return 0, 0, errors.New("bad wal file")
	}
	_, err = fmt.Sscanf(str, "%016x-%016x.wal", &seq, &index)
	return seq, index, err
}

func TestLeaderRestartWithTailWALLogLost(t *testing.T) {
	// stop all nodes in cluster and start one by one
	kvs, _, _, path, err := startTestClusterWithBasePort("unit-test-leaderwallost", tmpUsedClusterPortBase, 1, 0, false)
	if err != nil {
		t.Fatalf("init cluster failed: %v", err)
	}
	if path != "" {
		defer os.RemoveAll(path)
	}
	time.Sleep(time.Second)
	defer func() {
		for _, n := range kvs {
			n.server.Stop()
		}
	}()
	leaderNode, err := waitForLeaderForClusters(time.Minute, kvs)
	assert.Nil(t, err)
	assert.NotNil(t, leaderNode)
	rport := kvs[0].redisPort

	client := goredis.NewClient("127.0.0.1:"+strconv.Itoa(rport), "")
	client.SetMaxIdleConns(4)
	defer client.Close()
	c, err := client.Get()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	assert.Equal(t, 1, len(kvs))

	key := "default:test-cluster:a"
	rsp, err := goredis.String(c.Do("set", key, "1234"))
	assert.Nil(t, err)
	assert.Equal(t, OK, rsp)
	for i := 0; i < testSnap*100; i++ {
		rsp, err := goredis.String(c.Do("set", key, strconv.Itoa(i)+"aaaaa"))
		assert.Nil(t, err)
		assert.Equal(t, OK, rsp)
	}

	ci := leaderNode.Node.GetAppliedIndex()
	for _, s := range kvs {
		node := s.server.GetNamespaceFromFullName("default-0")
		node.Close()
	}
	// truncate the leader logs
	// note we only truncate the logs which will keep recent valid snapshot
	raftConf := leaderNode.Node.GetRaftConfig()
	names := readWALNames(raftConf.WALDir)
	lastWAL := filepath.Join(raftConf.WALDir, names[len(names)-1])
	snaps, err := wal.ValidSnapshotEntries(raftConf.WALDir)
	t.Logf("truncating the wal: %s", lastWAL)
	t.Logf("snaps in the wal: %v", snaps)
	for i := 140; i > 0; i-- {
		os.Truncate(lastWAL, int64(i*1000))
		snaps2, err := wal.ValidSnapshotEntries(raftConf.WALDir)
		assert.Nil(t, err)
		if len(snaps2) < len(snaps)-1 {
			t.Logf("snaps in the wal after truncate: %v", snaps2)
			break
		}
	}
	// wait namespace deleted by callback
	time.Sleep(time.Second)

	for _, s := range kvs {
		node, err := s.server.InitKVNamespace(s.replicaID, s.nsConf, true)
		assert.Nil(t, err)
		assert.NotNil(t, node)
		err = node.Start(false)
		assert.Nil(t, err)
		if err != nil {
			return
		}
	}
	time.Sleep(time.Second * 2)

	hasLeader := false
	newci := uint64(0)
	for _, s := range kvs {
		replicaNode := s.server.GetNamespaceFromFullName("default-0")
		assert.NotNil(t, replicaNode)
		newci = replicaNode.Node.GetAppliedIndex()
		t.Logf("restarted ci: %v, old %v", newci, ci)
		assert.True(t, newci < ci)
		if replicaNode.Node.IsLead() {
			hasLeader = true
		}
	}
	assert.Equal(t, true, hasLeader)
	rsp, err = goredis.String(c.Do("get", key))
	assert.Nil(t, err)
	assert.Equal(t, strconv.Itoa(testSnap*100-int(ci+2-newci))+"aaaaa", rsp)
}

func TestFollowRestartWithTailWALLogLost(t *testing.T) {
	// stop all nodes in cluster and start one by one
	kvs, _, _, path, err := startTestClusterWithBasePort("unit-test-follower-wallost", tmpUsedClusterPortBase, 2, 0, false)
	if err != nil {
		t.Fatalf("init cluster failed: %v", err)
	}
	if path != "" {
		defer os.RemoveAll(path)
	}
	time.Sleep(time.Second)
	defer func() {
		for _, n := range kvs {
			n.server.Stop()
		}
	}()
	leaderNode, err := waitForLeaderForClusters(time.Minute, kvs)
	assert.Nil(t, err)
	assert.NotNil(t, leaderNode)
	var follower *node.NamespaceNode
	rport := 0
	for _, n := range kvs {
		replicaNode := n.server.GetNamespaceFromFullName("default-0")
		if !replicaNode.Node.IsLead() {
			follower = replicaNode
		} else {
			rport = n.redisPort
		}
	}

	client := goredis.NewClient("127.0.0.1:"+strconv.Itoa(rport), "")
	client.SetMaxIdleConns(4)
	defer client.Close()
	c, err := client.Get()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	assert.Equal(t, 2, len(kvs))

	key := "default:test-cluster:a"
	rsp, err := goredis.String(c.Do("set", key, "1234"))
	assert.Nil(t, err)
	assert.Equal(t, OK, rsp)
	for i := 0; i < testSnap*100; i++ {
		rsp, err := goredis.String(c.Do("set", key, strconv.Itoa(i)+"aaaaa"))
		assert.Nil(t, err)
		assert.Equal(t, OK, rsp)
	}

	time.Sleep(time.Second)
	ci := leaderNode.Node.GetAppliedIndex()
	raftConf := follower.Node.GetRaftConfig()
	for _, s := range kvs {
		node := s.server.GetNamespaceFromFullName("default-0")
		node.Close()
	}
	// truncate the logs
	// note we only truncate the logs which will keep recent valid snapshot
	names := readWALNames(raftConf.WALDir)
	lastWAL := filepath.Join(raftConf.WALDir, names[len(names)-1])
	t.Logf("truncating the wal: %s", lastWAL)
	snaps, err := wal.ValidSnapshotEntries(raftConf.WALDir)
	for i := 140; i > 0; i-- {
		os.Truncate(lastWAL, int64(i*1000))
		snaps2, err := wal.ValidSnapshotEntries(raftConf.WALDir)
		assert.Nil(t, err)
		if len(snaps2) < len(snaps)-1 {
			t.Logf("snaps in the wal after truncate: %v", snaps2)
			break
		}
	}
	// wait namespace deleted by callback
	time.Sleep(time.Second)

	for _, s := range kvs {
		node, err := s.server.InitKVNamespace(s.replicaID, s.nsConf, true)
		assert.Nil(t, err)
		assert.NotNil(t, node)
		err = node.Start(false)
		assert.Nil(t, err)
		if err != nil {
			return
		}
	}
	time.Sleep(time.Second * 2)

	hasLeader := false
	leaderNode, err = waitForLeaderForClusters(time.Second*10, kvs)
	assert.Nil(t, err)
	assert.NotNil(t, leaderNode)
	newci := uint64(0)
	for _, s := range kvs {
		replicaNode := s.server.GetNamespaceFromFullName("default-0")
		assert.NotNil(t, replicaNode)
		newci = replicaNode.Node.GetAppliedIndex()
		assert.Equal(t, ci+1, newci)
		if replicaNode.Node.IsLead() {
			hasLeader = true
		}
	}
	assert.Equal(t, true, hasLeader)
	rsp, err = goredis.String(c.Do("stale.get", key))
	assert.Nil(t, err)
	assert.Equal(t, strconv.Itoa(testSnap*100-1)+"aaaaa", rsp)
}

func BenchmarkWriteToClusterWithEmptySM(b *testing.B) {
	costStatsLevel = 3
	sLog.SetLevel(int32(common.LOG_WARN))
	rockredis.SetLogger(int32(common.LOG_ERR), nil)
	node.SetLogLevel(int(common.LOG_WARN))
	raft.SetLogger(nil)
	rafthttp.SetLogLevel(0)

	kvs, _, _, dir, err := startTestClusterWithEmptySM(3)
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	defer func() {
		for _, v := range kvs {
			v.server.Stop()
		}
	}()
	_, err = waitForLeaderForClusters(time.Minute, kvs)
	if err != nil {
		panic(err)
	}
	rport := 0
	for _, n := range kvs {
		replicaNode := n.server.GetNamespaceFromFullName("default-0")
		if replicaNode.Node.IsLead() {
			rport = n.redisPort
			break
		}
	}
	c := goredis.NewClient("127.0.0.1:"+strconv.Itoa(rport), "")
	c.SetMaxIdleConns(10)

	b.ResetTimer()
	b.Run("bench-set", func(b *testing.B) {
		b.ReportAllocs()
		start := time.Now()
		var wg sync.WaitGroup
		for i := 0; i < 30; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				conn, err := c.Get()
				if err != nil {
					panic(err)
				}
				defer conn.Close()
				for i := 0; i < b.N; i++ {
					key := "default:test-cluster:a"
					_, err := goredis.String(conn.Do("set", key, "1234"))
					if err != nil {
						panic(err)
					}
				}
			}()
		}
		wg.Wait()
		cost := time.Since(start)
		b.Logf("cost time: %v for %v", cost, b.N)
	})

	b.StopTimer()
}

func BenchmarkGetOpWithLockAndNoLock(b *testing.B) {
	costStatsLevel = 3
	sLog.SetLevel(int32(common.LOG_WARN))
	rockredis.SetLogger(int32(common.LOG_ERR), nil)
	node.SetLogLevel(int(common.LOG_WARN))
	raft.SetLogger(nil)
	rafthttp.SetLogLevel(0)

	kvs, _, _, dir, err := startTestClusterWithBasePort("bench-test-get", tmpUsedClusterPortBase, 1, 0, false)
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	defer func() {
		for _, v := range kvs {
			v.server.Stop()
		}
	}()
	_, err = waitForLeaderForClusters(time.Minute, kvs)
	if err != nil {
		panic(err)
	}
	rport := 0
	for _, n := range kvs {
		replicaNode := n.server.GetNamespaceFromFullName("default-0")
		if replicaNode.Node.IsLead() {
			rport = n.redisPort
			break
		}
	}
	c := goredis.NewClient("127.0.0.1:"+strconv.Itoa(rport), "")
	c.SetMaxIdleConns(10)
	key := "default:test-cluster:test-get"
	goredis.String(c.Do("set", key, "1234"))

	b.Run("bench-get-withlock", func(b *testing.B) {
		b.ReportAllocs()
		start := time.Now()
		var wg sync.WaitGroup
		for i := 0; i < 30; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				conn, err := c.Get()
				if err != nil {
					panic(err)
				}
				defer conn.Close()
				for i := 0; i < b.N; i++ {
					v, err := goredis.String(conn.Do("get", key))
					if err != nil {
						panic(err)
					}
					if v != "1234" {
						panic(v)
					}
				}
			}()
		}
		wg.Wait()
		cost := time.Since(start)
		b.Logf("cost time: %v for %v", cost, b.N)
	})

	b.Run("bench-get-nolock", func(b *testing.B) {
		b.ReportAllocs()
		start := time.Now()
		var wg sync.WaitGroup
		for i := 0; i < 30; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				conn, err := c.Get()
				if err != nil {
					panic(err)
				}
				defer conn.Close()
				for i := 0; i < b.N; i++ {
					v, err := goredis.String(conn.Do("getnolock", key))
					if err != nil {
						panic(err)
					}
					if v != "1234" {
						panic(v)
					}
				}
			}()
		}
		wg.Wait()
		cost := time.Since(start)
		b.Logf("get nolock cost time: %v for %v", cost, b.N)
	})
}
