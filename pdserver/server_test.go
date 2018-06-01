package pdserver

import (
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/siddontang/goredis"
	"github.com/youzan/ZanRedisDB/cluster/datanode_coord"
	zanredisdb "github.com/youzan/go-zanredisdb"

	"github.com/stretchr/testify/assert"
	"github.com/youzan/ZanRedisDB/cluster/pdnode_coord"
	"github.com/youzan/ZanRedisDB/node"
	ds "github.com/youzan/ZanRedisDB/server"
)

func TestMain(m *testing.M) {
	pdnode_coord.ChangeIntervalForTest()
	datanode_coord.ChangeIntervalForTest()

	ret := m.Run()

	cleanAllCluster(ret)
	os.Exit(ret)
}

func enableAutoBalance(t *testing.T, pduri string, enable bool) {
	uri := fmt.Sprintf("%s/cluster/balance?enable=true", pduri)
	if !enable {
		uri = fmt.Sprintf("%s/cluster/balance?enable=false", pduri)
	}
	rsp, err := http.Post(uri, "", nil)
	assert.Nil(t, err)
	if rsp.StatusCode != 200 {
		assert.FailNow(t, rsp.Status)
	}
	assert.Equal(t, 200, rsp.StatusCode)
	rsp.Body.Close()
	//gpdServer.pdCoord.SetBalanceInterval(0, 24)
}

func waitForLeader(t *testing.T, ns string, part int) (dataNodeWrapper, *node.NamespaceNode) {
	start := time.Now()
	for {
		if time.Since(start) > time.Minute {
			t.Errorf("timeout while waiting leader")
			break
		}
		for _, kv := range gkvList {
			nsNode := kv.s.GetNamespaceFromFullName(ns + "-" + strconv.Itoa(part))
			if nsNode == nil {
				continue
			}
			assert.NotNil(t, nsNode)
			if nsNode.Node.IsLead() {
				return kv, nsNode
			}
		}
		time.Sleep(time.Millisecond * 100)
	}
	return dataNodeWrapper{}, nil
}

func waitForAllFullReady(t *testing.T, ns string, part int) {
	start := time.Now()
	for {
		if time.Since(start) > time.Minute {
			t.Errorf("timeout while waiting full ready")
			break
		}
		allInfo, _, err := gpdServer.pdCoord.GetAllNamespaces()
		assert.Nil(t, err)
		nsInfo, ok := allInfo[ns]
		assert.True(t, ok)
		nsPartInfo, ok := nsInfo[part]
		assert.True(t, ok)

		ready, err := pdnode_coord.IsAllISRFullReady(&nsPartInfo)
		if err != nil || !ready {
			time.Sleep(time.Millisecond * 100)
			continue
		}
		break
	}
}

func waitMarkAsRemoving(t *testing.T, ns string, part int, leaderID string) {
	start := time.Now()
	for {
		if time.Since(start) > time.Minute {
			t.Errorf("timeout while waiting mark removing")
			break
		}
		allInfo, _, err := gpdServer.pdCoord.GetAllNamespaces()
		assert.Nil(t, err)
		nsInfo, ok := allInfo[ns]
		assert.True(t, ok)
		nsPartInfo, ok := nsInfo[part]
		assert.True(t, ok)
		_, ok = nsPartInfo.Removings[leaderID]
		if !ok {
			time.Sleep(time.Millisecond * 100)
			continue
		}
		break
	}
}

func waitRemoveFromRemoving(t *testing.T, ns string, part int) {
	start := time.Now()
	for {
		if time.Since(start) > time.Minute {
			t.Errorf("timeout while waiting remove removing node")
			break
		}
		allInfo, _, err := gpdServer.pdCoord.GetAllNamespaces()
		assert.Nil(t, err)
		nsInfo, ok := allInfo[ns]
		assert.True(t, ok)
		nsPartInfo, ok := nsInfo[part]
		assert.True(t, ok)
		if len(nsPartInfo.Removings) > 0 {
			time.Sleep(time.Millisecond * 100)
			continue
		}
		break
	}
}

func waitEnoughReplica(t *testing.T, ns string, part int) {
	start := time.Now()
	for {
		if time.Since(start) > time.Minute {
			t.Errorf("timeout while waiting enough replicas")
			break
		}
		allInfo, _, err := gpdServer.pdCoord.GetAllNamespaces()
		assert.Nil(t, err)
		nsInfo, ok := allInfo[ns]
		assert.True(t, ok)
		nsPartInfo, ok := nsInfo[part]
		assert.True(t, ok)
		if len(nsPartInfo.GetISR()) < nsPartInfo.Replica {
			time.Sleep(time.Millisecond * 100)
			continue
		}
		break
	}
}

func waitBalancedLeader(t *testing.T, ns string, part int) {
	start := time.Now()
	for {
		if time.Since(start) > time.Minute {
			t.Errorf("timeout while waiting balanced leader become real leader")
			break
		}
		allInfo, _, err := gpdServer.pdCoord.GetAllNamespaces()
		assert.Nil(t, err)
		nsInfo, ok := allInfo[ns]
		assert.True(t, ok)
		nsPartInfo, ok := nsInfo[part]
		assert.True(t, ok)
		if nsPartInfo.GetRealLeader() != nsPartInfo.GetISR()[0] {
			time.Sleep(time.Millisecond * 100)
			continue
		}
		break
	}
}

func waitBalancedAndExpectedLeader(t *testing.T, ns string, part int, expected string) {
	start := time.Now()
	for {
		if time.Since(start) > time.Minute {
			t.Errorf("timeout while waiting expected leader become real leader ")
			break
		}
		allInfo, _, err := gpdServer.pdCoord.GetAllNamespaces()
		assert.Nil(t, err)
		nsInfo, ok := allInfo[ns]
		assert.True(t, ok)
		nsPartInfo, ok := nsInfo[part]
		assert.True(t, ok)
		if nsPartInfo.GetRealLeader() != nsPartInfo.GetISR()[0] || nsPartInfo.GetRealLeader() != expected {
			time.Sleep(time.Millisecond * 100)
			continue
		}
		break
	}
}

func waitBalancedAndJoined(t *testing.T, ns string, part int, expected string) {
	start := time.Now()
	for {
		if time.Since(start) > time.Minute*2 {
			t.Errorf("timeout while waiting expected node become isr")
			break
		}
		allInfo, _, err := gpdServer.pdCoord.GetAllNamespaces()
		assert.Nil(t, err)
		nsInfo, ok := allInfo[ns]
		assert.True(t, ok)
		nsPartInfo, ok := nsInfo[part]
		assert.True(t, ok)
		if len(nsPartInfo.GetISR()) == nsPartInfo.Replica && len(nsPartInfo.Removings) == 0 {
			t.Log(nsPartInfo.GetISR())
			for _, nid := range nsPartInfo.GetISR() {
				if nid == expected {
					ok, _ := pdnode_coord.IsAllISRFullReady(&nsPartInfo)
					if ok {
						return
					}
					break
				}
			}
		}
		time.Sleep(time.Millisecond * 100)
	}
}

func getFollowerNode(t *testing.T, ns string, part int) (*ds.Server, *node.NamespaceNode) {
	for _, kv := range gkvList {
		nsNode := kv.s.GetNamespaceFromFullName(ns + "-" + strconv.Itoa(part))
		if nsNode == nil {
			continue
		}
		assert.NotNil(t, nsNode)
		if nsNode.Node.IsLead() {
			continue
		}
		return kv.s, nsNode
	}
	return nil, nil
}

func TestClusterBalanceAcrossMultiDC(t *testing.T) {
	// TODO:
}

func TestClusterRemoveNode(t *testing.T) {
	// TODO: remove a node from api
}

func getTestRedisConn(t *testing.T, port int) *goredis.PoolConn {
	c := goredis.NewClient("127.0.0.1:"+strconv.Itoa(port), "")
	c.SetMaxIdleConns(4)
	conn, err := c.Get()
	if err != nil {
		t.Fatal(err)
	}
	return conn
}

func TestRWMultiPartOnDifferentNodes(t *testing.T) {
	ensureClusterReady(t)

	time.Sleep(time.Second)
	ns := "test_multi_part_rw"
	partNum := 4

	pduri := "http://127.0.0.1:" + pdHttpPort

	ensureDataNodesReady(t, pduri, len(gkvList))
	enableAutoBalance(t, pduri, true)
	ensureNamespace(t, pduri, ns, partNum, 2)
	defer ensureDeleteNamespace(t, pduri, ns)

	for i := 0; i < partNum; i++ {
		leader, _ := waitForLeader(t, ns, i)
		assert.NotNil(t, leader.s)
	}
	time.Sleep(time.Second)
	// test should write to different parts
	table := "test_set_kv_multi"
	zanClient := getTestClient(t, ns)
	for i := 0; i < 20; i++ {
		k := []byte(fmt.Sprintf("kv%d", i))
		err := zanClient.KVSet(table, k, k)
		assert.Nil(t, err)
		v, err := zanClient.KVGet(table, k)
		assert.Nil(t, err)
		assert.Equal(t, k, v)
	}
	for i := 0; i < partNum; i++ {
		leader, nsNode := waitForLeader(t, ns, i)
		assert.NotNil(t, leader)
		stats := nsNode.Node.GetStats()
		for _, st := range stats.TStats {
			assert.Equal(t, table, st.Name)
			t.Log(st)
			assert.True(t, st.KeyNum > 3)
		}
	}
	// test write to server with no part and not leader part should return error
	for i := 0; i < 20; i++ {
		k := []byte(fmt.Sprintf("%v:kv%d", table, i))
		pid := zanredisdb.GetHashedPartitionID(k, partNum)
		leader, _ := waitForLeader(t, ns, pid)
		t.Logf("pk %v hash to pid: %v, leader is %v", string(k), pid, leader.redisPort)
		for _, srv := range gkvList {
			if srv.redisPort == leader.redisPort {
				continue
			}
			conn := getTestRedisConn(t, srv.redisPort)
			assert.NotNil(t, conn)
			_, err := goredis.String(conn.Do("get", ns+":"+string(k)))
			// get should not send to non-leader
			t.Log(err)
			t.Logf("pk %v send to node: %v", string(k), srv.redisPort)
			assert.NotNil(t, err)
			nsNode := srv.s.GetNamespaceFromFullName(ns + "-" + strconv.Itoa(pid))
			if nsNode == nil {
				_, err := conn.Do("set", ns+":"+string(k), []byte(k))
				t.Log(err)
				assert.NotNil(t, err)
			} else {
				// set can be handled by non-leader
				_, err := conn.Do("set", ns+":"+string(k), []byte(k))
				assert.Nil(t, err)
			}
		}
	}
}

func TestLeaderLost(t *testing.T) {
	// leader is lost and mark leader as removing
	ensureClusterReady(t)

	time.Sleep(time.Second)
	ns := "test_leader_lost"
	partNum := 1

	pduri := "http://127.0.0.1:" + pdHttpPort

	ensureDataNodesReady(t, pduri, len(gkvList))
	enableAutoBalance(t, pduri, true)
	ensureNamespace(t, pduri, ns, partNum, 3)
	defer ensureDeleteNamespace(t, pduri, ns)

	nodeWrapper, nsNode := waitForLeader(t, ns, 0)
	leader := nodeWrapper.s
	assert.NotNil(t, leader)
	dcoord := leader.GetCoord()
	leaderID := dcoord.GetMyID()
	oldRaftReplicaID := nsNode.Node.GetLocalMemberInfo().ID
	// call this to propose some request to write raft logs
	for i := 0; i < 50; i++ {
		nsNode.Node.OptimizeDB("")
	}
	leader.Stop()

	waitMarkAsRemoving(t, ns, 0, leaderID)

	waitRemoveFromRemoving(t, ns, 0)
	waitEnoughReplica(t, ns, 0)
	waitForAllFullReady(t, ns, 0)
	// wait balance
	waitBalancedLeader(t, ns, 0)
	nodeWrapper, _ = waitForLeader(t, ns, 0)
	newLeader := nodeWrapper.s
	assert.NotNil(t, newLeader)
	newLeaderID := newLeader.GetCoord().GetMyID()
	assert.NotEqual(t, leaderID, newLeaderID)
	// restart old leader and wait balance
	// this will test the old replica on the same node should be removed
	// and join with the new replica id on the node.
	time.Sleep(time.Second * 5)
	leader.Start()
	nsNode = leader.GetNamespaceFromFullName(ns + "-0")
	if nsNode != nil {
		assert.False(t, nsNode.Node.IsLead())
	}

	waitBalancedAndExpectedLeader(t, ns, 0, leaderID)

	// should keep leader
	nsNode = leader.GetNamespaceFromFullName(ns + "-0")

	assert.NotNil(t, nsNode)
	assert.True(t, nsNode.Node.IsLead())
	assert.NotEqual(t, oldRaftReplicaID, nsNode.Node.GetLocalMemberInfo().ID)
}

func TestFollowerLost(t *testing.T) {
	// test follower lost should keep old leader
	ensureClusterReady(t)

	time.Sleep(time.Second)
	ns := "test_follower_lost"
	partNum := 1

	pduri := "http://127.0.0.1:" + pdHttpPort

	ensureDataNodesReady(t, pduri, len(gkvList))
	enableAutoBalance(t, pduri, true)
	ensureNamespace(t, pduri, ns, partNum, 3)
	defer ensureDeleteNamespace(t, pduri, ns)
	dnw, nsNode := waitForLeader(t, ns, 0)
	leader := dnw.s
	assert.NotNil(t, leader)
	// call this to propose some request to write raft logs
	for i := 0; i < 50; i++ {
		nsNode.Node.OptimizeDB("")
	}
	follower, followerNode := getFollowerNode(t, ns, 0)
	oldFollowerReplicaID := followerNode.GetRaftID()
	followerID := follower.GetCoord().GetMyID()
	follower.Stop()

	// should keep leader
	assert.True(t, nsNode.Node.IsLead())
	waitMarkAsRemoving(t, ns, 0, followerID)
	// should keep leader
	assert.True(t, nsNode.Node.IsLead())

	waitRemoveFromRemoving(t, ns, 0)
	assert.True(t, nsNode.Node.IsLead())
	waitEnoughReplica(t, ns, 0)

	waitForAllFullReady(t, ns, 0)

	// restart old follower and wait balance
	// the follower should be balanced to join with different replica id
	time.Sleep(time.Second * 5)
	follower.Start()

	waitBalancedAndJoined(t, ns, 0, followerID)

	// should have different replica id
	followerNode = follower.GetNamespaceFromFullName(ns + "-0")
	assert.NotEqual(t, followerNode.GetRaftID(), oldFollowerReplicaID)
}

func TestAddRemoteClusterLogSyncLearner(t *testing.T) {
	node.EnableForTest()
	ensureClusterReady(t)

	time.Sleep(time.Second)
	ns := "test_add_learner"
	partNum := 1

	pduri := "http://127.0.0.1:" + pdHttpPort
	ensureDataNodesReady(t, pduri, len(gkvList))
	enableAutoBalance(t, pduri, true)
	ensureNamespace(t, pduri, ns, partNum, 3)
	defer ensureDeleteNamespace(t, pduri, ns)

	dnw, leaderNode := waitForLeader(t, ns, 0)
	leader := dnw.s
	assert.NotNil(t, leader)

	remotePD, remoteSrvs, remoteTmpDir := startRemoteSyncTestCluster(t, 2)
	defer func() {
		for _, kv := range remoteSrvs {
			kv.s.Stop()
		}
		if remotePD != nil {
			remotePD.Stop()
		}
		if strings.Contains(remoteTmpDir, "rocksdb-test") {
			t.Logf("removing: %v", remoteTmpDir)
			os.RemoveAll(remoteTmpDir)
		}
	}()
	pduri = "http://127.0.0.1:" + pdRemoteHttpPort
	ensureDataNodesReady(t, pduri, len(remoteSrvs))
	enableAutoBalance(t, pduri, true)
	ensureNamespace(t, pduri, ns, partNum, 2)
	defer ensureDeleteNamespace(t, pduri, ns)

	learnerPD, learnerSrvs, tmpDir := startTestClusterForLearner(t, 2)
	defer func() {
		for _, kv := range learnerSrvs {
			kv.s.Stop()
		}
		if learnerPD != nil {
			learnerPD.Stop()
		}
		if strings.Contains(tmpDir, "learner-test") {
			t.Logf("removing: %v", tmpDir)
			os.RemoveAll(tmpDir)
		}
	}()
	start := time.Now()
	remoteNode := remoteSrvs[0].s.GetNamespaceFromFullName(ns + "-0")
	assert.NotNil(t, remoteNode)
	time.Sleep(time.Second * 3)
	for {
		time.Sleep(time.Second)
		if time.Since(start) > time.Minute {
			t.Errorf("timeout waiting add learner")
			break
		}
		commitID := leaderNode.Node.GetCommittedIndex()
		done := 0
		for _, srv := range learnerSrvs {
			nsNode := srv.s.GetNamespaceFromFullName(ns + "-0")
			if nsNode != nil {
				lrns := nsNode.GetLearners()
				t.Log(lrns)
				if len(lrns) == len(learnerSrvs) {
					found := false
					for _, l := range lrns {
						t.Log(*l)
						if l.NodeID == srv.s.GetCoord().GetMyRegID() {
							found = true
							assert.Equal(t, nsNode.GetRaftID(), l.ID)
						}
					}
					assert.True(t, found, "should found myself in learners")
					_, remoteIndex, _ := remoteNode.Node.GetRemoteClusterSyncedRaft(TestClusterName)
					learnerCI := nsNode.Node.GetCommittedIndex()
					t.Logf("commit %v , current remote :%v, learner: %v", commitID, remoteIndex, learnerCI)
					if remoteIndex >= commitID && learnerCI == remoteIndex {
						time.Sleep(time.Second)
						done++
					}
				} else {
					break
				}
			}
		}
		if done >= len(learnerSrvs) {
			break
		}
	}
	commitID := leaderNode.Node.GetCommittedIndex()
	for _, srv := range learnerSrvs {
		nsNode := srv.s.GetNamespaceFromFullName(ns + "-0")
		assert.Equal(t, commitID, nsNode.Node.GetCommittedIndex())
		stats := nsNode.Node.GetStats()
		assert.Equal(t, commitID, stats.InternalStats["synced_index"].(uint64))
	}
}

func TestMigrateLeader(t *testing.T) {
	// add new node and mark leader as removing.
	// leader should transfer leader first and then propose remove self
}

func TestMigrateFollower(t *testing.T) {
	// add new node and mark follower as removing.
	// removing node should propose remove self
}

func TestTransferLeaderWhileReplicaNotReady(t *testing.T) {
	// TODO: test transfer leader while replica is restarting and not catchup fully.
	// should only transfer leader when replica has almost the newest raft logs
}

func TestMarkAsRemovingWhileNotEnoughAlives(t *testing.T) {
	// TODO:
	// should not mark as remove while there is not enough for replica
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

func TestInstallSnapshotFailed(t *testing.T) {
	// TODO: test the follower fall behind too much, and the leader send the snapshot to follower,
	// However, the follower failed to pull the snapshot data from leader. So the raft node should stop
	// and restart later.

	// test case should make sure the snap will be not persisted to the stable storage since the snapshot data is failed to pull.
}

func TestClusterBalanceWhileNewNodeAdding(t *testing.T) {
	// TODO: while replica is not enough, we will add new replica node while check namespace,
	// and then it should wait the new replica is raft synced before we can start to balance
}

func TestClusterAddReplicaOneByOne(t *testing.T) {
	// TODO: while replica is not enough, we will add new replica node while check namespace.
	// If two replica are removed, we need add new replica one by one to avoid 2 failed node in raft.
}
