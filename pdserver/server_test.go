package pdserver

import (
	"flag"
	"fmt"
	"math"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/siddontang/goredis"
	"github.com/youzan/ZanRedisDB/cluster"
	"github.com/youzan/ZanRedisDB/cluster/datanode_coord"
	"github.com/youzan/ZanRedisDB/common"
	"github.com/youzan/ZanRedisDB/internal/test"
	zanredisdb "github.com/youzan/go-zanredisdb"

	"github.com/stretchr/testify/assert"
	"github.com/youzan/ZanRedisDB/cluster/pdnode_coord"
	"github.com/youzan/ZanRedisDB/node"
)

func TestMain(m *testing.M) {
	pdnode_coord.ChangeIntervalForTest()
	datanode_coord.ChangeIntervalForTest()
	flag.Parse()
	if testing.Verbose() {
		cluster.SetLogLevel(int(common.LOG_DEBUG))
	}

	ret := m.Run()

	cleanAllCluster(ret)
	os.Exit(ret)
}

func enableStaleRead(t *testing.T, addr string, enable bool) {
	uri := fmt.Sprintf("%s/staleread?allow=true", addr)
	if !enable {
		uri = fmt.Sprintf("%s/staleread?allow=false", addr)
	}
	rsp, err := http.Post(uri, "", nil)
	assert.Nil(t, err)
	if rsp.StatusCode != 200 {
		assert.FailNow(t, rsp.Status)
	}
	assert.Equal(t, 200, rsp.StatusCode)
	rsp.Body.Close()
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

func waitForLeaderFromNodes(t *testing.T, ns string, part int, nodeList []dataNodeWrapper) (dataNodeWrapper, *node.NamespaceNode) {
	start := time.Now()
	for {
		if time.Since(start) > time.Minute*3 {
			t.Errorf("timeout while waiting leader")
			break
		}
		for _, kv := range nodeList {
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

func waitForLeader(t *testing.T, ns string, part int) (dataNodeWrapper, *node.NamespaceNode) {
	return waitForLeaderFromNodes(t, ns, part, gkvList)
}

func waitForAllFullReady(t *testing.T, ns string, part int) {
	start := time.Now()
	for {
		if time.Since(start) > time.Minute*3 {
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
		if time.Since(start) > time.Minute*3 {
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

func waitMarkAsRemovingUntilTimeout(t *testing.T, ns string, part int, until time.Duration) []string {
	start := time.Now()
	removed := make([]string, 0)
	for {
		if time.Since(start) > until {
			break
		}
		allInfo, _, err := gpdServer.pdCoord.GetAllNamespaces()
		assert.Nil(t, err)
		nsInfo, ok := allInfo[ns]
		assert.True(t, ok)
		nsPartInfo, ok := nsInfo[part]
		assert.True(t, ok)
		if len(nsPartInfo.Removings) > 0 {
			for nid, _ := range nsPartInfo.Removings {
				removed = append(removed, nid)
			}
			break
		}
		time.Sleep(time.Millisecond * 100)
	}
	return removed
}

func waitRemoveFromRemoving(t *testing.T, ns string, part int) {
	start := time.Now()
	for {
		if time.Since(start) > time.Minute*3 {
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
		if time.Since(start) > time.Minute*3 {
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

func getNsInfo(t *testing.T, ns string, part int) cluster.PartitionMetaInfo {
	allInfo, _, err := gpdServer.pdCoord.GetAllNamespaces()
	assert.Nil(t, err)
	nsInfo, ok := allInfo[ns]
	assert.True(t, ok)
	nsPartInfo, ok := nsInfo[part]
	assert.True(t, ok)
	return nsPartInfo
}

func getCurrentPartitionNodes(t *testing.T, ns string) [][]string {
	allInfo, _, err := gpdServer.pdCoord.GetAllNamespaces()
	assert.Nil(t, err)
	nsInfo, ok := allInfo[ns]
	assert.True(t, ok)
	partNodes := make([][]string, len(nsInfo))
	for pid, partInfo := range nsInfo {
		partNodes[pid] = partInfo.GetISR()
	}
	return partNodes
}

func waitBalancedLeader(t *testing.T, ns string, part int) {
	start := time.Now()
	for {
		if time.Since(start) > time.Minute*3 {
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
		if time.Since(start) > time.Minute*3 {
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
		if time.Since(start) > time.Minute {
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

func getFollowerNode(t *testing.T, ns string, part int) (dataNodeWrapper, *node.NamespaceNode) {
	for _, kv := range gkvList {
		nsNode := kv.s.GetNamespaceFromFullName(ns + "-" + strconv.Itoa(part))
		if nsNode == nil {
			continue
		}
		assert.NotNil(t, nsNode)
		if nsNode.Node.IsLead() {
			continue
		}
		return kv, nsNode
	}
	return dataNodeWrapper{}, nil
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

func checkPartitionNodesBalance(t *testing.T, balanceVer string, partitionNodes [][]string) bool {
	replicaNodesMap := make(map[string]int)
	leaderNodesMap := make(map[string]int)
	for _, nlist := range partitionNodes {
		l := nlist[0]
		cnt, ok := leaderNodesMap[l]
		if !ok {
			cnt = 0
		}
		cnt++
		leaderNodesMap[l] = cnt
		nameMap := make(map[string]bool)
		for _, n := range nlist {
			nameMap[n] = true
			cnt, ok = replicaNodesMap[n]
			if !ok {
				cnt = 0
			}
			cnt++
			replicaNodesMap[n] = cnt
		}
		assert.Equal(t, len(nlist), len(nameMap), nlist)
	}

	maxL := 0
	minL := math.MaxInt32
	for _, cnt := range leaderNodesMap {
		if cnt > maxL {
			maxL = cnt
		}
		if cnt < minL {
			minL = cnt
		}
	}
	t.Logf("leader max vs min: %v, %v", maxL, minL)
	balanced := true
	if maxL-minL <= 1 {
		assert.True(t, maxL-minL <= 1, partitionNodes)
	} else {
		balanced = false
	}

	maxL = 0
	minL = math.MaxInt32
	for _, cnt := range replicaNodesMap {
		if cnt > maxL {
			maxL = cnt
		}
		if cnt < minL {
			minL = cnt
		}
	}
	t.Logf("replica max vs min: %v, %v", maxL, minL)
	if balanceVer == "" {
		// default balance may not have balanced replicas
		if maxL-minL <= 3 {
			assert.True(t, maxL-minL <= 3, partitionNodes)
		} else {
			balanced = false
		}
		return balanced
	}
	if maxL-minL <= 1 {
		assert.True(t, maxL-minL <= 1, partitionNodes)
	} else {
		balanced = false
	}
	return balanced
}

func TestRWMultiPartOnDifferentNodes(t *testing.T) {
	ensureClusterReady(t, 4)

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
	defer zanClient.Stop()
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
		stats := nsNode.Node.GetStats("", true)
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
	ensureClusterReady(t, 4)

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

	if balanceVer == "v2" {
		time.Sleep(time.Second * 10)
		waitBalancedLeader(t, ns, 0)
		nodeWrapper, _ := waitForLeader(t, ns, 0)
		nleader := nodeWrapper.s
		assert.NotNil(t, nleader)
		if nleader.GetCoord().GetMyID() == leaderID {
			nsNode := nleader.GetNamespaceFromFullName(ns + "-0")
			assert.NotNil(t, nsNode)
			assert.NotEqual(t, oldRaftReplicaID, nsNode.Node.GetLocalMemberInfo().ID)
		}
	} else {
		waitBalancedAndExpectedLeader(t, ns, 0, leaderID)
		// should keep leader
		nsNode = leader.GetNamespaceFromFullName(ns + "-0")
		assert.NotNil(t, nsNode)
		assert.True(t, nsNode.Node.IsLead())
		assert.NotEqual(t, oldRaftReplicaID, nsNode.Node.GetLocalMemberInfo().ID)
	}
}

func TestFollowerLost(t *testing.T) {
	// test follower lost should keep old leader
	ensureClusterReady(t, 4)
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
	followerWrap, followerNode := getFollowerNode(t, ns, 0)
	follower := followerWrap.s
	oldFollowerReplicaID := followerNode.GetRaftID()
	followerID := follower.GetCoord().GetMyID()
	t.Logf("stopping %v, old replica id: %v", followerID, oldFollowerReplicaID)
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

	// stop for a while and wait the data migrate to others
	// and then start this node to join the cluster and wait
	// data migrate back to this node

	// restart old follower and wait balance
	// the follower should be balanced to join with different replica id
	time.Sleep(time.Second * 5)
	allInfo, _, err := gpdServer.pdCoord.GetAllNamespaces()
	assert.Nil(t, err)
	t.Logf("all namespace: %v", allInfo)
	follower.Start()

	time.Sleep(time.Second * 5)
	waitForAllFullReady(t, ns, 0)

	if balanceVer == "v2" {
		// it may not move back to the origin node for v2 balance
		time.Sleep(time.Second * 10)
	} else {
		waitBalancedAndJoined(t, ns, 0, followerID)
	}
	// should have different replica id
	followerNode = follower.GetNamespaceFromFullName(ns + "-0")
	if balanceVer == "v2" && followerNode == nil {
		return
	}
	assert.NotEqual(t, followerNode.GetRaftID(), oldFollowerReplicaID)
}

func waitRemoteClusterSync(t *testing.T, ns string, leaderNode *node.NamespaceNode, learnerSrvs []dataNodeWrapper, remoteSrvs []dataNodeWrapper) {
	start := time.Now()
	for {
		time.Sleep(time.Second)
		if time.Since(start) > time.Minute {
			t.Errorf("timeout waiting add learner")
			return
		}
		commitID := leaderNode.Node.GetAppliedIndex()
		done := 0
		for _, srv := range learnerSrvs {
			nsNode := srv.s.GetNamespaceFromFullName(ns + "-0")
			if nsNode != nil {
				lrns := nsNode.GetLearners()
				t.Logf("current learners: %v", lrns)
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
					checkAllOK := true
					for _, remote := range remoteSrvs {
						remoteNode := remote.s.GetNamespaceFromFullName(ns + "-0")
						assert.NotNil(t, remoteNode)
						_, remoteIndex, _ := remoteNode.Node.GetRemoteClusterSyncedRaft(TestClusterName)
						learnerCI := nsNode.Node.GetAppliedIndex()
						t.Logf("commit %v , current remote :%v, learner: %v", commitID, remoteIndex, learnerCI)
						if remoteIndex >= commitID && learnerCI == remoteIndex {
							time.Sleep(time.Second)
						} else {
							checkAllOK = false
						}
					}
					if checkAllOK {
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
	commitID := leaderNode.Node.GetAppliedIndex()
	for _, srv := range learnerSrvs {
		nsNode := srv.s.GetNamespaceFromFullName(ns + "-0")
		assert.Equal(t, commitID, nsNode.Node.GetAppliedIndex())
		stats := nsNode.Node.GetStats("", false)
		assert.Equal(t, commitID, stats.InternalStats["synced_index"].(uint64))
	}
}

func TestAddRemoteClusterLogSyncLearner(t *testing.T) {
	node.EnableForTest()
	ensureClusterReady(t, 4)

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
	time.Sleep(time.Second * 3)

	waitRemoteClusterSync(t, ns, leaderNode, learnerSrvs, remoteSrvs)
}

func TestRemoteClusterLearnerContinueAfterSrcRestart(t *testing.T) {
	// test restart and leader changed will continue syncer
	node.EnableForTest()
	ensureClusterReady(t, 3)

	time.Sleep(time.Second)
	ns := "test_learner_continue_after_restart"
	partNum := 1

	pduri := "http://127.0.0.1:" + pdHttpPort
	ensureDataNodesReady(t, pduri, len(gkvList))
	enableAutoBalance(t, pduri, true)
	ensureNamespace(t, pduri, ns, partNum, 3)
	defer ensureDeleteNamespace(t, pduri, ns)

	dnw, leaderNode := waitForLeader(t, ns, 0)
	leader := dnw.s
	assert.NotNil(t, leader)

	node.SetSyncerOnly(true)
	defer node.SetSyncerOnly(false)
	remotePD, remoteSrvs, remoteTmpDir := startRemoteSyncTestCluster(t, 1)
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
	for _, lrnSrv := range remoteSrvs {
		lrnSrv.s.GetCoord().UpdateSyncerWriteOnly(true)
	}
	ensureDataNodesReady(t, pduri, len(remoteSrvs))
	enableAutoBalance(t, pduri, true)
	ensureNamespace(t, pduri, ns, partNum, 1)
	defer ensureDeleteNamespace(t, pduri, ns)

	leaderNode.Node.OptimizeDB("")

	learnerPD, learnerSrvs, tmpDir := startTestClusterForLearner(t, 1)
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
	time.Sleep(time.Second * 3)

	leaderNode.Node.OptimizeDB("")

	t.Logf("begin wait first before restart")
	waitRemoteClusterSync(t, ns, leaderNode, learnerSrvs, remoteSrvs)

	// restart leader
	leader.Stop()
	time.Sleep(time.Second)

	leader.Start()
	// start will reset syncer write only state from remote, so we need set it again to allow learner node running
	node.SetSyncerOnly(true)
	waitEnoughReplica(t, ns, 0)
	waitForAllFullReady(t, ns, 0)

	dnw, leaderNode = waitForLeader(t, ns, 0)
	leader = dnw.s
	assert.NotNil(t, leader)
	leaderNode.Node.OptimizeDB("")
	time.Sleep(time.Second * 3)

	t.Logf("begin wait after source restart")
	waitRemoteClusterSync(t, ns, leaderNode, learnerSrvs, remoteSrvs)
}

func TestRemoteClusterLearnerRestartAndRestoreBackup(t *testing.T) {
	// restart, backup and restore from backup should keep remote cluster term-index
	node.EnableForTest()
	ensureClusterReady(t, 3)

	time.Sleep(time.Second)
	ns := "test_restart_learner"
	partNum := 1

	pduri := "http://127.0.0.1:" + pdHttpPort
	ensureDataNodesReady(t, pduri, len(gkvList))
	enableAutoBalance(t, pduri, true)
	ensureNamespace(t, pduri, ns, partNum, 3)
	defer ensureDeleteNamespace(t, pduri, ns)

	dnw, leaderNode := waitForLeader(t, ns, 0)
	leader := dnw.s
	assert.NotNil(t, leader)

	leaderNode.Node.OptimizeDB("")
	c := getTestRedisConn(t, gkvList[0].redisPort)
	defer c.Close()
	key := fmt.Sprintf("%s:%s", ns, "test_remote_syncer:k1")
	rsp, err := goredis.String(c.Do("set", key, key))
	assert.Nil(t, err)
	assert.Equal(t, "OK", rsp)
	key2 := fmt.Sprintf("%s:%s", ns, "test_remote_syncer:k2")
	rsp, err = goredis.String(c.Do("set", key2, key2))
	assert.Nil(t, err)
	assert.Equal(t, "OK", rsp)

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

	leaderNode.Node.OptimizeDB("")

	learnerPD, learnerSrvs, tmpDir := startTestClusterForLearner(t, 1)
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

	leaderNode.Node.OptimizeDB("")
	time.Sleep(time.Second * 3)

	waitRemoteClusterSync(t, ns, leaderNode, learnerSrvs, remoteSrvs)

	// restart remote cluster node one by one
	for _, kv := range remoteSrvs {
		kv.s.Stop()
		time.Sleep(time.Second)
		kv.s.Start()
		time.Sleep(time.Second)
	}

	leaderNode.Node.OptimizeDB("")
	waitRemoteClusterSync(t, ns, leaderNode, learnerSrvs, remoteSrvs)

	addr := fmt.Sprintf("http://127.0.0.1:%v", remoteSrvs[0].httpPort)
	enableStaleRead(t, addr, true)
	defer enableStaleRead(t, addr, false)
	remoteC := getTestRedisConn(t, remoteSrvs[0].redisPort)
	defer remoteC.Close()
	rsp, err = goredis.String(remoteC.Do("get", key))
	assert.Nil(t, err)
	assert.Equal(t, key, rsp)
	rsp, err = goredis.String(remoteC.Do("get", key2))
	assert.Nil(t, err)
	assert.Equal(t, key2, rsp)
}

func TestRemoteClusterLearnerNotIgnoreDeleteRangeAsConfig(t *testing.T) {
	testRemoteClusterLearnerIgnoreDeleteRangeAsConfig(t, false)
}
func TestRemoteClusterLearnerIgnoreDeleteRangeAsConfig(t *testing.T) {
	testRemoteClusterLearnerIgnoreDeleteRangeAsConfig(t, true)
}
func testRemoteClusterLearnerIgnoreDeleteRangeAsConfig(t *testing.T, ignoreDelRange bool) {
	// restart, backup and restore from backup should keep remote cluster term-index
	node.EnableForTest()
	ensureClusterReady(t, 3)

	time.Sleep(time.Second)
	ns := "test_remote_cluster_syncer_delrange"
	partNum := 1

	pduri := "http://127.0.0.1:" + pdHttpPort
	ensureDataNodesReady(t, pduri, len(gkvList))
	enableAutoBalance(t, pduri, true)
	ensureNamespace(t, pduri, ns, partNum, 3)
	defer ensureDeleteNamespace(t, pduri, ns)

	dnw, leaderNode := waitForLeader(t, ns, 0)
	leader := dnw.s
	assert.NotNil(t, leader)

	leaderNode.Node.OptimizeDB("")

	// we set value before the remote syncer started, since the node syncer only is global var which will be changed if another is running as syncer
	c := getTestRedisConn(t, gkvList[0].redisPort)
	defer c.Close()
	key := fmt.Sprintf("%s:%s", ns, "test_remote_syncer:k1")
	rsp, err := goredis.String(c.Do("set", key, key))
	assert.Nil(t, err)
	assert.Equal(t, "OK", rsp)
	key2 := fmt.Sprintf("%s:%s", ns, "test_remote_syncer:k2")
	rsp, err = goredis.String(c.Do("set", key2, key2))
	assert.Nil(t, err)
	assert.Equal(t, "OK", rsp)

	remotePD, remoteSrvs, remoteTmpDir := startRemoteSyncTestCluster(t, 1)
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
	ensureNamespace(t, pduri, ns, partNum, 1)
	defer ensureDeleteNamespace(t, pduri, ns)

	learnerPD, learnerSrvs, tmpDir := startTestClusterForLearner(t, 1)
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

	leaderNode.Node.OptimizeDB("")

	time.Sleep(time.Second * 3)

	waitRemoteClusterSync(t, ns, leaderNode, learnerSrvs, remoteSrvs)

	addr := fmt.Sprintf("http://127.0.0.1:%v", remoteSrvs[0].httpPort)
	enableStaleRead(t, addr, true)
	defer enableStaleRead(t, addr, false)
	remoteC := getTestRedisConn(t, remoteSrvs[0].redisPort)
	defer remoteC.Close()
	rsp, err = goredis.String(remoteC.Do("get", key))
	assert.Nil(t, err)
	assert.Equal(t, key, rsp)
	rsp, err = goredis.String(remoteC.Do("get", key2))
	assert.Nil(t, err)
	assert.Equal(t, key2, rsp)

	err = leaderNode.Node.DeleteRange(node.DeleteTableRange{
		Table:                   "test_remote_syncer",
		DeleteAll:               true,
		NoReplayToRemoteCluster: ignoreDelRange,
	})
	assert.Nil(t, err)
	time.Sleep(time.Second)
	waitRemoteClusterSync(t, ns, leaderNode, learnerSrvs, remoteSrvs)

	rsp, err = goredis.String(c.Do("get", key))
	assert.Equal(t, goredis.ErrNil, err)
	assert.Equal(t, "", rsp)
	rsp, err = goredis.String(c.Do("get", key2))
	assert.Equal(t, goredis.ErrNil, err)
	assert.Equal(t, "", rsp)

	rsp, err = goredis.String(remoteC.Do("get", key))
	if ignoreDelRange {
		assert.Nil(t, err)
		assert.Equal(t, key, rsp)
	} else {
		assert.Equal(t, goredis.ErrNil, err)
		assert.Equal(t, "", rsp)
	}
	rsp, err = goredis.String(remoteC.Do("get", key2))
	if ignoreDelRange {
		assert.Nil(t, err)
		assert.Equal(t, key2, rsp)
	} else {
		assert.Equal(t, goredis.ErrNil, err)
		assert.Equal(t, "", rsp)
	}
}

func TestClusterBalanceAcrossMultiDC(t *testing.T) {
	// TODO:
}

func TestClusterRemoveNodeNotLast(t *testing.T) {
	testClusterRemoveNode(t, 4, "test_cluster_remove_node_by_api")
}

func testClusterRemoveNode(t *testing.T, leftNodeN int, ns string) {
	// remove a node from api and wait all data balanced to others
	ensureClusterReady(t, leftNodeN)

	time.Sleep(time.Second)
	partNum := 4

	pduri := "http://127.0.0.1:" + pdHttpPort

	ensureDataNodesReady(t, pduri, len(gkvList))
	enableAutoBalance(t, pduri, true)

	newDataNodes, dataDir := addMoreTestDataNodeToCluster(t, 1)
	defer cleanDataNodes(newDataNodes, dataDir)
	time.Sleep(time.Second)

	ensureNamespace(t, pduri, ns, partNum, 3)
	defer ensureDeleteNamespace(t, pduri, ns)
	dnw, nsNode := waitForLeader(t, ns, 0)
	leader := dnw.s
	assert.NotNil(t, leader)
	// call this to propose some request to write raft logs
	for i := 0; i < 10; i++ {
		nsNode.Node.OptimizeDB("")
	}
	oldNsList := make([]cluster.PartitionMetaInfo, 0)
	for i := 0; i < partNum; i++ {
		oldNs := getNsInfo(t, ns, i)
		t.Logf("part %v isr is %v", i, oldNs.GetISR())
		oldNsList = append(oldNsList, oldNs)
		waitBalancedLeader(t, ns, i)
	}

	nsNum := 0
	for i := 0; i < partNum; i++ {
		nsNode := newDataNodes[0].s.GetNamespaceFromFullName(ns + "-" + strconv.Itoa(i))
		if nsNode != nil {
			nsNum++
		}
	}
	assert.True(t, nsNum > 0)
	// remove node from api
	removedNodeID := newDataNodes[0].s.GetCoord().GetMyID()
	gpdServer.pdCoord.MarkNodeAsRemoving(removedNodeID)
	// wait balance
	start := time.Now()
	for i := 0; i < partNum; i++ {
		for {
			if time.Since(start) > time.Minute*time.Duration(partNum) {
				t.Errorf("timeout wait removing partition %v on removed node", i)
				break
			}
			time.Sleep(time.Second * 5)
			nsInfo := getNsInfo(t, ns, i)
			if len(nsInfo.Removings) > 0 {
				continue
			}
			if len(nsInfo.GetISR()) != 3 {
				continue
			}
			waitRemove := false
			for _, nid := range nsInfo.GetISR() {
				if nid == removedNodeID {
					waitRemove = true
					t.Logf("still waiting remove node: %v, %v", nsInfo.GetDesp(), nsInfo.GetISR())
					break
				}
			}
			if waitRemove {
				continue
			}
			break
		}
		waitBalancedLeader(t, ns, i)
	}

	time.Sleep(time.Second * 5)
	for i := 0; i < partNum; i++ {
		for {
			time.Sleep(time.Second)
			waitRemoveFromRemoving(t, ns, i)
			waitEnoughReplica(t, ns, i)
			waitForAllFullReady(t, ns, i)
			waitBalancedLeader(t, ns, i)
			newNs := getNsInfo(t, ns, i)
			newISR := newNs.GetISR()
			if len(newISR) != 3 || len(newNs.Removings) > 0 {
				// wait remove unneed replica
				continue
			}
			break
		}
		nsInfo := getNsInfo(t, ns, i)
		for _, nid := range nsInfo.GetISR() {
			assert.NotEqual(t, nid, removedNodeID)
		}
	}
	for i := 0; i < partNum; i++ {
		nsNode := newDataNodes[0].s.GetNamespaceFromFullName(ns + "-" + strconv.Itoa(i))
		assert.Nil(t, nsNode)
	}
}

func TestClusterRemoveNodeForLast(t *testing.T) {
	testClusterRemoveNode(t, 3, "test_cluster_remove_lastnode_by_api")
}

func TestClusterNodeFailedTooLongBalance(t *testing.T) {
	// one failed node and trigger rebalance on left nodes
	ensureClusterReady(t, 3)

	time.Sleep(time.Second)
	ns := "test_cluster_failed_node_balance"
	partNum := 8
	replicator := 3
	pduri := "http://127.0.0.1:" + pdHttpPort

	ensureDataNodesReady(t, pduri, len(gkvList))
	enableAutoBalance(t, pduri, true)

	newDataNodes, dataDir := addMoreTestDataNodeToCluster(t, 1)
	defer cleanDataNodes(newDataNodes, dataDir)
	time.Sleep(time.Second)

	ensureNamespace(t, pduri, ns, partNum, replicator)
	defer ensureDeleteNamespace(t, pduri, ns)
	dnw, nsNode := waitForLeader(t, ns, 0)
	leader := dnw.s
	assert.NotNil(t, leader)
	// call this to propose some request to write raft logs
	for i := 0; i < 10; i++ {
		nsNode.Node.OptimizeDB("")
	}
	oldNsList := make([]cluster.PartitionMetaInfo, 0)
	for i := 0; i < partNum; i++ {
		oldNs := getNsInfo(t, ns, i)
		t.Logf("part %v isr is %v", i, oldNs.GetISR())
		oldNsList = append(oldNsList, oldNs)
		waitBalancedLeader(t, ns, i)
	}

	nsNum := 0
	for i := 0; i < partNum; i++ {
		nsNode := newDataNodes[0].s.GetNamespaceFromFullName(ns + "-" + strconv.Itoa(i))
		if nsNode != nil {
			nsNum++
		}
	}
	assert.True(t, nsNum > 0)
	// stop node to trigger balance
	removedNodeID := newDataNodes[0].s.GetCoord().GetMyID()
	newDataNodes[0].s.Stop()
	time.Sleep(time.Second * 30)
	// wait balance
	start := time.Now()
	for i := 0; i < partNum; i++ {
		for {
			if time.Since(start) > time.Minute*time.Duration(partNum/2) {
				t.Errorf("timeout wait removing partition %v on removed node", i)
				break
			}
			time.Sleep(time.Second * 5)
			nsInfo := getNsInfo(t, ns, i)
			if len(nsInfo.Removings) > 0 {
				continue
			}
			if len(nsInfo.GetISR()) != replicator {
				continue
			}
			waitRemove := false
			for _, nid := range nsInfo.GetISR() {
				if nid == removedNodeID {
					waitRemove = true
					t.Logf("still waiting remove node: %v, %v", nsInfo.GetDesp(), nsInfo.GetISR())
					break
				}
			}
			if waitRemove {
				continue
			}
			break
		}
		waitBalancedLeader(t, ns, i)
	}

	time.Sleep(time.Second * 5)
	start = time.Now()
	for i := 0; i < partNum; i++ {
		for {
			if time.Since(start) > time.Minute*time.Duration(partNum/2) {
				t.Errorf("timeout wait balance partition %v ", i)
				break
			}
			time.Sleep(time.Second)
			waitRemoveFromRemoving(t, ns, i)
			waitEnoughReplica(t, ns, i)
			waitForAllFullReady(t, ns, i)
			waitBalancedLeader(t, ns, i)
			newNs := getNsInfo(t, ns, i)
			newISR := newNs.GetISR()
			if len(newISR) != replicator || len(newNs.Removings) > 0 {
				// wait remove unneed replica
				continue
			}
			break
		}
		nsInfo := getNsInfo(t, ns, i)
		t.Logf("after stopped, isr: %v", nsInfo.GetISR())
		for _, nid := range nsInfo.GetISR() {
			assert.NotEqual(t, nid, removedNodeID)
		}
		assert.Equal(t, 3, len(nsInfo.GetISR()))
		assert.Equal(t, 0, len(nsInfo.Removings))
	}
	assert.True(t, checkPartitionNodesBalance(t, "v2", getCurrentPartitionNodes(t, ns)), "should balanced after stopped")
	// start the failed node to make sure balance again
	newDataNodes[0].s.Start()
	time.Sleep(time.Second * 10)
	start = time.Now()
	for {
		found := 0
		for i := 0; i < partNum; i++ {
			time.Sleep(time.Second)
			waitEnoughReplica(t, ns, i)
			waitForAllFullReady(t, ns, i)
			waitBalancedLeader(t, ns, i)
			newNs := getNsInfo(t, ns, i)
			newISR := newNs.GetISR()
			if len(newISR) != replicator || len(newNs.Removings) > 0 {
				// wait remove unneed replica
				continue
			}
			t.Logf("after restart stopped node, isr: %v", newNs.GetISR())
			if cluster.FindSlice(newISR, removedNodeID) != -1 {
				found++
				t.Logf("found restarted node in part: %v", i)
			}
			assert.Equal(t, 3, len(newNs.GetISR()))
			assert.Equal(t, 0, len(newNs.Removings))
		}
		t.Logf("found %v part for restarted node", found)
		if time.Since(start) > time.Minute {
			t.Errorf("timeout wait balance")
			break
		}
		if found > partNum/2 {
			localNsList, err := newDataNodes[0].s.GetNsMgr().GetNamespaceNodes(ns, false)
			assert.Nil(t, err)
			t.Logf("found %v part for restarted node, and local loaded: %v", found, localNsList)
			if len(localNsList) < found {
				continue
			}
			if checkPartitionNodesBalance(t, "v2", getCurrentPartitionNodes(t, ns)) {
				break
			}
		}
	}
}

// It should wait raft synced before we can start to balance
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

func TestTransferLeaderWhileReplicaApplyingSnapshot(t *testing.T) {
	// apply snapshot and transfer leader should fail
	defer node.EnableSnapBlockingForTest(false)

	ensureClusterReady(t, 3)
	time.Sleep(time.Second)
	ns := "test_cluster_transfer_leader_snap_applying"
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
	for i := 0; i < 5; i++ {
		nsNode.Node.OptimizeDB("")
	}
	oldNs := getNsInfo(t, ns, 0)
	t.Logf("old isr is: %v", oldNs)
	assert.Equal(t, 3, len(oldNs.GetISR()))

	foWrap, _ := getFollowerNode(t, ns, 0)
	foWrap.s.Stop()

	for i := 0; i < 50; i++ {
		nsNode.Node.OptimizeDB("")
	}
	c := getTestRedisConn(t, dnw.redisPort)
	defer c.Close()
	key := fmt.Sprintf("%s:%s", ns, "snap_apply:k1")
	rsp, err := goredis.String(c.Do("set", key, "1234"))
	assert.Nil(t, err)
	assert.Equal(t, "OK", rsp)

	for i := 0; i < 50; i++ {
		nsNode.Node.OptimizeDB("")
	}
	leaderV, err := goredis.String(c.Do("get", key))
	assert.True(t, err == nil || err == goredis.ErrNil)
	assert.Equal(t, "1234", leaderV)
	time.Sleep(time.Second * 5)

	// make sure the snapshot applying is blocked
	// and then transfer leader to this follower
	node.EnableSnapBlockingForTest(true)
	foWrap.s.Start()
	time.Sleep(time.Second)
	node.PutSnapBlockingTime(time.Second * 20)
	fn := foWrap.s.GetNamespaceFromFullName(ns + "-0")
	assert.True(t, fn.Node.IsApplyingSnapshot())
	foRaftID := fn.GetRaftID()
	err = nsNode.Node.TransferLeadership(foRaftID)
	assert.NotNil(t, err)
	nsInfo := getNsInfo(t, ns, 0)
	transferOK := leader.GetCoord().TransferMyNamespaceLeader(&nsInfo, foWrap.s.GetCoord().GetMyID(), false, true)
	assert.False(t, transferOK, "should not transfer while snapshot applying")
	transferOK = leader.GetCoord().TransferMyNamespaceLeader(&nsInfo, foWrap.s.GetCoord().GetMyID(), false, false)
	assert.False(t, transferOK, "should not transfer while snapshot applying")
	transferOK = leader.GetCoord().TransferMyNamespaceLeader(&nsInfo, foWrap.s.GetCoord().GetMyID(), true, false)
	assert.False(t, transferOK, "should not transfer while snapshot applying")

	time.Sleep(time.Second * 20)
	assert.False(t, fn.Node.IsApplyingSnapshot())
	transferOK = leader.GetCoord().TransferMyNamespaceLeader(&nsInfo, foWrap.s.GetCoord().GetMyID(), false, true)
	assert.True(t, transferOK, "should transfer ok")
	_, newLeaderNode := waitForLeader(t, ns, 0)
	assert.Equal(t, foRaftID, newLeaderNode.GetRaftID())

	waitForAllFullReady(t, ns, 0)
	followerConn := getTestRedisConn(t, foWrap.redisPort)
	defer followerConn.Close()

	getV, err := goredis.String(followerConn.Do("get", key))
	assert.True(t, err == nil || err == goredis.ErrNil)
	assert.Equal(t, "1234", getV)
}

func TestTransferLeaderWhileReplicaLagToomuch(t *testing.T) {
}
func TestClusterRestartNodeCatchup(t *testing.T) {
	// test restarted node catchup while writing
	ensureClusterReady(t, 3)

	time.Sleep(time.Second)
	ns := "test_cluster_restart_catchup"
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
	oldNs := getNsInfo(t, ns, 0)
	gkvList[0].s.Stop()
	done := make(chan bool, 0)
	go func() {
		for {
			nsNode.Node.OptimizeDB("")
			time.Sleep(time.Millisecond)
			select {
			case <-done:
				return
			default:
			}
		}
	}()

	time.Sleep(time.Second * 3)

	gkvList[0].s.Start()

	waitEnoughReplica(t, ns, 0)
	waitForAllFullReady(t, ns, 0)
	close(done)
	waitBalancedAndExpectedLeader(t, ns, 0, leader.GetCoord().GetMyID())

	c := getTestRedisConn(t, gkvList[0].redisPort)
	defer c.Close()
	key := fmt.Sprintf("%s:%s", ns, "restart_catchup:k1")
	rsp, err := goredis.String(c.Do("set", key, "1234"))
	assert.Nil(t, err)
	assert.Equal(t, "OK", rsp)
	time.Sleep(time.Second)

	for i := 0; i < len(gkvList); i++ {
		addr := fmt.Sprintf("http://127.0.0.1:%v", gkvList[i].httpPort)
		enableStaleRead(t, addr, true)
		followerConn := getTestRedisConn(t, gkvList[i].redisPort)
		for i := 0; i < 10; i++ {
			getV, err := goredis.String(followerConn.Do("get", key))
			assert.True(t, err == nil || err == goredis.ErrNil)
			t.Logf("read follower : %v", getV)
			assert.True(t, getV == "1234")
		}
		enableStaleRead(t, addr, false)
		followerConn.Close()
	}

	newNs := getNsInfo(t, ns, 0)
	test.Equal(t, oldNs.GetISR(), newNs.GetISR())
	test.Equal(t, oldNs.GetRealLeader(), newNs.GetRealLeader())
}

func TestMarkAsRemovingWhileNotEnoughAlives(t *testing.T) {
	// should not mark as remove while there is not enough for replica (more than half is dead)
	ensureClusterReady(t, 4)
	newNodes, dataDir := addMoreTestDataNodeToCluster(t, 1)
	defer cleanDataNodes(newNodes, dataDir)

	pduri := "http://127.0.0.1:" + pdHttpPort
	ensureDataNodesReady(t, pduri, len(gkvList)+1)

	time.Sleep(time.Second)
	ns := "test_mark_removing_no_enough"
	partNum := 1

	enableAutoBalance(t, pduri, true)
	ensureNamespace(t, pduri, ns, partNum, 3)
	defer ensureDeleteNamespace(t, pduri, ns)

	allNodes := []dataNodeWrapper{}
	allNodes = append(allNodes, gkvList...)
	allNodes = append(allNodes, newNodes...)
	nodeWrapper, nsNode := waitForLeaderFromNodes(t, ns, 0, allNodes)
	followerWrap, _ := getFollowerNode(t, ns, 0)
	follower := followerWrap.s
	leader := nodeWrapper.s
	assert.NotNil(t, leader)
	dcoord := leader.GetCoord()
	leaderID := dcoord.GetMyID()
	assert.NotEqual(t, leaderID, follower.GetCoord().GetMyID())
	// call this to propose some request to write raft logs
	for i := 0; i < 5; i++ {
		nsNode.Node.OptimizeDB("")
	}
	oldNsInfo := getNsInfo(t, ns, 0)
	time.Sleep(time.Second)
	// make half replicas down and check if removing will happen
	t.Logf("stopping follower node: %v", follower.GetCoord().GetMyID())
	follower.Stop()
	time.Sleep(time.Second)
	t.Logf("stopping leader node: %v", leaderID)
	leader.Stop()
	gpdServer.pdCoord.SetClusterStableNodeNum(2)

	removed := waitMarkAsRemovingUntilTimeout(t, ns, 0, time.Minute)
	assert.Equal(t, 0, len(removed))
	follower.Start()
	leader.Start()

	waitEnoughReplica(t, ns, 0)
	waitForAllFullReady(t, ns, 0)

	waitBalancedLeader(t, ns, 0)
	newNsInfo := getNsInfo(t, ns, 0)
	oldISR := oldNsInfo.GetISR()
	sort.Strings(oldISR)
	newISR := newNsInfo.GetISR()
	sort.Strings(newISR)
	assert.Equal(t, oldISR, newISR)
	nodeWrapper, _ = waitForLeaderFromNodes(t, ns, 0, allNodes)
	newLeader := nodeWrapper.s
	assert.NotNil(t, newLeader)
	if balanceVer == "v2" {
		// v2 balance may change the leader after node restarted
	} else {
		newLeaderID := newLeader.GetCoord().GetMyID()
		assert.Equal(t, leaderID, newLeaderID)
	}
}

func TestMarkAsRemovingWhileOthersNotSynced(t *testing.T) {
	// should not mark any failed node as removed while the other raft replicas are not synced (or have no leader)
	ensureClusterReady(t, 4)
	newNodes, dataDir := addMoreTestDataNodeToCluster(t, 1)
	defer cleanDataNodes(newNodes, dataDir)

	pduri := "http://127.0.0.1:" + pdHttpPort
	ensureDataNodesReady(t, pduri, len(gkvList)+1)
	// stop 2 node in cluster to make sure the replica will be placed on the new added node
	for i := 0; i < 2; i++ {
		gkvList[i].s.Stop()
	}

	time.Sleep(time.Second)
	ns := "test_mark_removing_not_synced"
	partNum := 1

	enableAutoBalance(t, pduri, true)
	ensureNamespace(t, pduri, ns, partNum, 3)
	defer ensureDeleteNamespace(t, pduri, ns)

	leaderWrapper, leaderNode := waitForLeader(t, ns, 0)
	followerWrap, _ := getFollowerNode(t, ns, 0)
	follower := followerWrap.s
	leader := leaderWrapper.s
	assert.NotNil(t, leader)
	dcoord := leader.GetCoord()
	leaderID := dcoord.GetMyID()
	assert.NotEqual(t, leaderID, follower.GetCoord().GetMyID())
	// call this to propose some request to write raft logs
	for i := 0; i < 5; i++ {
		leaderNode.Node.OptimizeDB("")
	}
	oldNsInfo := getNsInfo(t, ns, 0)
	origNodes, _ := gpdServer.pdCoord.GetAllDataNodes()
	time.Sleep(time.Second)
	newNodeID := newNodes[0].s.GetCoord().GetMyID()
	// stop the new node and another node
	stoppedNode := follower
	if newNodeID != follower.GetCoord().GetMyID() {
		t.Logf("stopping follower node: %v", follower.GetCoord().GetMyID())
		follower.Stop()
		time.Sleep(time.Second)
	} else if leaderID != newNodeID {
		t.Logf("stopping leader node: %v", leader.GetCoord().GetMyID())
		leader.Stop()
		stoppedNode = leader
		time.Sleep(time.Second)
	}
	allNodes, _ := gpdServer.pdCoord.GetAllDataNodes()
	assert.Equal(t, len(origNodes)-1, len(allNodes))
	// here we just stop the raft node and remove local data but keep the server running, this
	// can make the raft group is not stable
	t.Logf("stopping raft namespace node: %v", newNodeID)
	newNodes[0].s.GetNamespaceFromFullName(ns + "-0").Destroy()
	gpdServer.pdCoord.SetClusterStableNodeNum(2)

	// should not remove any node since not full synced
	removed := waitMarkAsRemovingUntilTimeout(t, ns, 0, time.Minute)
	assert.Equal(t, 0, len(removed))
	allNodes, _ = gpdServer.pdCoord.GetAllDataNodes()
	assert.Equal(t, len(origNodes)-1, len(allNodes))

	newNsInfo := getNsInfo(t, ns, 0)
	assert.Equal(t, oldNsInfo.GetISR(), newNsInfo.GetISR())

	stoppedNode.Start()
	waitEnoughReplica(t, ns, 0)
	allNodes, _ = gpdServer.pdCoord.GetAllDataNodes()
	assert.Equal(t, len(origNodes), len(allNodes))

	newNsInfo = getNsInfo(t, ns, 0)
	assert.Equal(t, oldNsInfo.GetISR(), newNsInfo.GetISR())
	nodeWrapper, _ := waitForLeader(t, ns, 0)
	newLeader := nodeWrapper.s
	assert.NotNil(t, newLeader)

	newNodes[0].s.Stop()
	for i := 0; i < 2; i++ {
		gkvList[i].s.Start()
	}

	waitMarkAsRemoving(t, ns, 0, newNodeID)
	waitRemoveFromRemoving(t, ns, 0)

	waitForAllFullReady(t, ns, 0)
	waitBalancedLeader(t, ns, 0)
	newNsInfo = getNsInfo(t, ns, 0)
	nodeWrapper, _ = waitForLeader(t, ns, 0)
	newLeader = nodeWrapper.s
	assert.NotNil(t, newLeader)
	assert.Equal(t, newNsInfo.GetISR()[0], newLeader.GetCoord().GetMyID())
	assert.NotEqual(t, oldNsInfo.GetISR(), newNsInfo.GetISR())
	for _, nid := range newNsInfo.GetISR() {
		assert.NotEqual(t, nid, newNodeID)
	}
}

func TestRestartCluster(t *testing.T) {
	// stop all nodes in cluster and start one by one
	ensureClusterReady(t, 4)

	time.Sleep(time.Second)
	ns := "test_cluster_restart_all"
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
	oldNs := getNsInfo(t, ns, 0)
	for _, kv := range gkvList {
		kv.s.Stop()
	}

	time.Sleep(time.Second * 10)

	for _, kv := range gkvList {
		kv.s.Start()
	}

	waitEnoughReplica(t, ns, 0)
	waitForAllFullReady(t, ns, 0)
	waitBalancedAndJoined(t, ns, 0, leader.GetCoord().GetMyID())

	newNs := getNsInfo(t, ns, 0)
	test.Equal(t, oldNs.GetISR(), newNs.GetISR())
	if balanceVer == "v2" {
		// it may happend the leader changed after restart all
		assert.True(t, newNs.GetRealLeader() != "", newNs.GetRealLeader())
	} else {
		test.Equal(t, oldNs.GetRealLeader(), newNs.GetRealLeader())
	}
}

func getDeletedNs(t *testing.T, scanDir string) map[string]int64 {
	// scan all local undeleted ns-part dirs and read the magic code
	dirList, err := filepath.Glob(path.Join(scanDir, "*-*"))
	assert.Nil(t, err)
	magicList := make(map[string]int64)
	for _, dir := range dirList {
		t.Logf("found local dir in data root: %v", dir)
		grpName := path.Base(dir)
		if strings.Contains(grpName, "deleted") {
			code, _ := node.LoadMagicCode(path.Join(dir, "magic_"+grpName))
			magicList[grpName] = code
		}
	}
	return magicList
}

func TestClusterBalanceToNewNodeAndBack(t *testing.T) {
	// It should wait raft synced before we can start to balance
	// and new data should be balanced to new node
	ensureClusterReady(t, 4)

	time.Sleep(time.Second)
	ns := "test_cluster_balance_add_new_node"
	partNum := 4

	pduri := "http://127.0.0.1:" + pdHttpPort

	ensureDataNodesReady(t, pduri, len(gkvList))
	enableAutoBalance(t, pduri, true)
	ensureNamespace(t, pduri, ns, partNum, 3)
	defer ensureDeleteNamespace(t, pduri, ns)
	dnw, nsNode := waitForLeader(t, ns, 0)
	leader := dnw.s
	assert.NotNil(t, leader)
	// call this to propose some request to write raft logs
	for i := 0; i < 10; i++ {
		nsNode.Node.OptimizeDB("")
	}
	oldNsList := make([]cluster.PartitionMetaInfo, 0)
	for i := 0; i < partNum; i++ {
		oldNs := getNsInfo(t, ns, i)
		t.Logf("part %v isr is %v", i, oldNs.GetISR())
		oldNsList = append(oldNsList, oldNs)
	}
	t.Logf("cluster data dir: %v", gtmpDir)
	serverDatas := make(map[string]map[string]int64)
	for _, srv := range gkvList {
		serverDatas[srv.dataPath] = srv.s.GetNsMgr().CheckLocalNamespaces()
		deletedNs := getDeletedNs(t, srv.dataPath)
		t.Logf("%v deleted ns: %v", srv.s.GetCoord().GetMyID(), deletedNs)
		assert.Equal(t, 0, len(deletedNs))
	}
	t.Logf("server datas: %v", serverDatas)

	newDataNodes, dataDir := addMoreTestDataNodeToCluster(t, 2)
	defer cleanDataNodes(newDataNodes, dataDir)

	// wait balance
	time.Sleep(time.Second * 10)

	start := time.Now()
	for {
		time.Sleep(time.Second)
		needWait := false
		for _, nn := range newDataNodes {
			hasReplica := false
			for i := 0; i < partNum; i++ {
				nsNode := nn.s.GetNamespaceFromFullName(ns + "-" + strconv.Itoa(i))
				if nsNode != nil {
					hasReplica = true
					t.Logf("new node %v has replica for namespace: %v", nn.s.GetCoord().GetMyID(), nsNode.FullName())
					break
				}
			}
			if !hasReplica {
				needWait = true
				t.Logf("node %v has no replica for namespace", nn.s.GetCoord().GetMyID())
			}
		}
		if !needWait {
			break
		}
		if time.Since(start) > time.Minute {
			t.Errorf("timeout wait cluster balance")
			break
		}
	}

	for i := 0; i < partNum; i++ {
		waitEnoughReplica(t, ns, i)
		waitForAllFullReady(t, ns, i)
		waitBalancedLeader(t, ns, i)
	}
	newNsList := make([]cluster.PartitionMetaInfo, 0)
	notChangedPart := 0
	for i := 0; i < partNum; i++ {
		for {
			time.Sleep(time.Second * 3)
			newNs := getNsInfo(t, ns, i)
			t.Logf("part %v new isr is %v", i, newNs.GetISR())
			newNsList = append(newNsList, newNs)
			oldISR := oldNsList[i].GetISR()
			sort.Sort(sort.StringSlice(oldISR))
			newISR := newNs.GetISR()
			sort.Sort(sort.StringSlice(newISR))
			if len(newISR) != 3 || len(newNs.Removings) > 0 {
				// wait remove unneed replica
				continue
			}
			// maybe some part can be un moved
			eq := assert.ObjectsAreEqual(oldISR, newISR)
			if eq {
				// not moved partition
				notChangedPart++
				t.Logf("un moved partition: %v, %v", i, newISR)
			}
			break
		}
	}
	assert.True(t, notChangedPart <= partNum/2, "half partitions should be balanced to new")
	time.Sleep(time.Second * 5)
	for i := 0; i < partNum; i++ {
		for {
			waitRemoveFromRemoving(t, ns, i)
			waitEnoughReplica(t, ns, i)
			waitForAllFullReady(t, ns, i)
			waitBalancedLeader(t, ns, i)
			newNs := getNsInfo(t, ns, i)
			newISR := newNs.GetISR()
			if len(newISR) != 3 || len(newNs.Removings) > 0 {
				// wait remove unneed replica
				continue
			}
			break
		}
	}
	totalDeleted := 0
	for _, srv := range gkvList {
		deletedNs := getDeletedNs(t, srv.dataPath)
		t.Logf("%v deleted ns: %v", srv.s.GetCoord().GetMyID(), deletedNs)
		totalDeleted += len(deletedNs)
	}

	t.Logf("after balanced server datas deleted: %v", totalDeleted)
	assert.True(t, totalDeleted > 1)

	for _, nn := range newDataNodes {
		t.Logf("begin stopping new added node: %v", nn.s.GetCoord().GetMyID())
		nn.s.Stop()

		for i := 0; i < partNum; i++ {
			start := time.Now()
			for {
				if time.Since(start) > time.Minute*2 {
					t.Errorf("timeout wait cluster balance for stopped node")
					break
				}
				time.Sleep(time.Second * 5)
				needWait := false
				nsInfo := getNsInfo(t, ns, i)
				newISR := nsInfo.GetISR()
				for _, nid := range nsInfo.GetISR() {
					if nid == nn.s.GetCoord().GetMyID() {
						needWait = true
						t.Logf("stopped new node %v still has replica for namespace: %v, %v", nn.s.GetCoord().GetMyID(), nsInfo.GetISR(), nsInfo.GetDesp())
						break
					}
				}
				if _, ok := nsInfo.Removings[nn.s.GetCoord().GetMyID()]; ok {
					needWait = true
					t.Logf("stopped new node %v still waiting removing for namespace: %v", nn.s.GetCoord().GetMyID(), nsInfo.GetDesp())
				}
				if !needWait {
					if len(newISR) != 3 || len(nsInfo.Removings) > 0 {
						// wait remove unneed replica
						continue
					}
					t.Logf("%v balanced isr: %v", nsInfo.GetDesp(), newISR)
					break
				}
			}
			waitBalancedLeader(t, ns, i)
		}

		time.Sleep(time.Second * 5)
		for i := 0; i < partNum; i++ {
			start := time.Now()
			for {
				if time.Since(start) > time.Minute*2 {
					t.Errorf("timeout waiting balance for stopped")
					break
				}
				waitRemoveFromRemoving(t, ns, i)
				waitEnoughReplica(t, ns, i)
				waitForAllFullReady(t, ns, i)
				waitBalancedLeader(t, ns, i)
				newNs := getNsInfo(t, ns, i)
				newISR := newNs.GetISR()
				if len(newISR) != 3 || len(newNs.Removings) > 0 {
					// wait remove unneed replica
					continue
				}
				t.Logf("%v balanced isr: %v", newNs.GetDesp(), newISR)
				break
			}
		}
	}

	time.Sleep(time.Second * 5)
	for i := 0; i < partNum; i++ {
		waitEnoughReplica(t, ns, i)
		waitForAllFullReady(t, ns, i)
		waitBalancedLeader(t, ns, i)
	}
	newNsList = make([]cluster.PartitionMetaInfo, 0)
	for i := 0; i < partNum; i++ {
		newNs := getNsInfo(t, ns, i)
		t.Logf("part %v final new isr is %v", i, newNs.GetISR())
		newNsList = append(newNsList, newNs)
		oldISR := oldNsList[i].GetISR()
		sort.Sort(sort.StringSlice(oldISR))
		newISR := newNs.GetISR()
		sort.Sort(sort.StringSlice(newISR))
		if balanceVer == "v2" {
			// to reduce data migrate in v2, it may happend the old isr is not the same
			// so we only check if any old not removed
			for _, nn := range newDataNodes {
				nid := nn.s.GetCoord().GetMyID()
				for _, n := range newISR {
					assert.NotEqual(t, n, nid)
				}
			}
		} else {
			assert.Equal(t, oldISR, newISR)
		}
	}

	for i := 0; i < partNum; i++ {
		waitBalancedLeader(t, ns, i)
		newNs := getNsInfo(t, ns, i)
		t.Logf("new info for part %v: %v, %v", i, newNs.GetRealLeader(), newNs.GetISR())
	}
}

func TestClusterIncrReplicaOneByOne(t *testing.T) {
	// While increase replicas, we need add new replica one by one to avoid 2 failed node in raft.
	ensureClusterReady(t, 4)

	time.Sleep(time.Second)
	ns := "test_cluster_increase_replicas"
	partNum := 1

	pduri := "http://127.0.0.1:" + pdHttpPort

	ensureDataNodesReady(t, pduri, len(gkvList))
	enableAutoBalance(t, pduri, true)
	ensureNamespace(t, pduri, ns, partNum, 2)
	defer ensureDeleteNamespace(t, pduri, ns)
	dnw, nsNode := waitForLeader(t, ns, 0)
	leader := dnw.s
	assert.NotNil(t, leader)
	// call this to propose some request to write raft logs
	for i := 0; i < 50; i++ {
		nsNode.Node.OptimizeDB("")
	}
	oldNs := getNsInfo(t, ns, 0)
	t.Logf("old isr is: %v", oldNs)
	assert.Equal(t, 2, len(oldNs.GetISR()))

	err := gpdServer.pdCoord.ChangeNamespaceMetaParam(ns, 4, "", 0)
	assert.Nil(t, err)

	lastNs := oldNs
	for {
		time.Sleep(time.Second)
		newNs := getNsInfo(t, ns, 0)
		t.Logf("new isr is: %v", newNs)
		assert.True(t, len(newNs.GetISR()) <= len(lastNs.GetISR())+1)
		lastNs = newNs
		waitForAllFullReady(t, ns, 0)
		if len(newNs.GetISR()) == 4 {
			break
		}
	}
	waitEnoughReplica(t, ns, 0)
	waitForAllFullReady(t, ns, 0)
	waitBalancedLeader(t, ns, 0)

	newNs := getNsInfo(t, ns, 0)
	t.Logf("new isr is: %v", newNs)
	assert.Equal(t, 4, len(newNs.GetISR()))
	for _, old := range oldNs.GetISR() {
		found := false
		for _, nid := range newNs.GetISR() {
			if old == nid {
				found = true
				break
			}
		}
		assert.True(t, found)
	}
}

func TestClusterDecrReplicaOneByOne(t *testing.T) {
	// While decrease replicas, we need remove replica one by one to avoid 2 failed node in raft.
	ensureClusterReady(t, 4)

	time.Sleep(time.Second)
	ns := "test_cluster_decrease_replicas"
	partNum := 1

	pduri := "http://127.0.0.1:" + pdHttpPort

	ensureDataNodesReady(t, pduri, len(gkvList))
	enableAutoBalance(t, pduri, true)
	ensureNamespace(t, pduri, ns, partNum, 4)
	defer ensureDeleteNamespace(t, pduri, ns)
	dnw, nsNode := waitForLeader(t, ns, 0)
	leader := dnw.s
	assert.NotNil(t, leader)
	// call this to propose some request to write raft logs
	for i := 0; i < 50; i++ {
		nsNode.Node.OptimizeDB("")
	}
	oldNs := getNsInfo(t, ns, 0)
	t.Logf("old isr is: %v", oldNs)
	assert.Equal(t, 4, len(oldNs.GetISR()))

	err := gpdServer.pdCoord.ChangeNamespaceMetaParam(ns, 2, "", 0)
	assert.Nil(t, err)

	lastNs := oldNs
	for {
		time.Sleep(time.Second)
		newNs := getNsInfo(t, ns, 0)
		t.Logf("new isr is: %v", newNs)
		assert.True(t, len(newNs.GetISR()) >= len(lastNs.GetISR())-1)
		lastNs = newNs
		if len(newNs.Removings) > 0 {
			continue
		}
		waitForAllFullReady(t, ns, 0)
		if len(newNs.GetISR()) == 2 {
			break
		}
	}
	waitEnoughReplica(t, ns, 0)
	waitForAllFullReady(t, ns, 0)
	waitBalancedLeader(t, ns, 0)

	newNs := getNsInfo(t, ns, 0)
	t.Logf("new isr is: %v", newNs)
	assert.Equal(t, 2, len(newNs.GetISR()))
	assert.Equal(t, 0, len(newNs.Removings))
	for _, nid := range newNs.GetISR() {
		found := false
		for _, old := range oldNs.GetISR() {
			if old == nid {
				found = true
				break
			}
		}
		assert.True(t, found)
	}
}

func TestRestartWithForceAloneWithLearnerAndRemovedNode(t *testing.T) {
	// test force restart with alone
	// test force start as alone for normal node and for learner node
	// and test force restart as alone for have removed node before
	node.EnableForTest()
	ensureClusterReady(t, 3)

	time.Sleep(time.Second)
	ns := "test_force_restart_alone_after_add_learner_remove_node"
	partNum := 1

	pduri := "http://127.0.0.1:" + pdHttpPort
	ensureDataNodesReady(t, pduri, len(gkvList))
	enableAutoBalance(t, pduri, true)
	ensureNamespace(t, pduri, ns, partNum, 3)
	defer ensureDeleteNamespace(t, pduri, ns)

	dnw, leaderNode := waitForLeader(t, ns, 0)
	leader := dnw.s
	assert.NotNil(t, leader)
	leaderNode.Node.OptimizeDB("")

	node.SetSyncerOnly(true)
	defer node.SetSyncerOnly(false)
	remotePD, remoteSrvs, remoteTmpDir := startRemoteSyncTestCluster(t, 1)
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
	for _, lrnSrv := range remoteSrvs {
		lrnSrv.s.GetCoord().UpdateSyncerWriteOnly(true)
	}
	ensureDataNodesReady(t, pduri, len(remoteSrvs))
	enableAutoBalance(t, pduri, true)
	ensureNamespace(t, pduri, ns, partNum, 1)
	defer ensureDeleteNamespace(t, pduri, ns)

	learnerPD, learnerSrvs, tmpDir := startTestClusterForLearner(t, 1)
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
	time.Sleep(time.Second * 3)

	t.Logf("begin wait first before restart")
	waitRemoteClusterSync(t, ns, leaderNode, learnerSrvs, remoteSrvs)
	oldNs := getNsInfo(t, ns, 0)
	t.Logf("new isr is: %v", oldNs)
	assert.Equal(t, 1, len(oldNs.LearnerNodes))

	err := gpdServer.pdCoord.ChangeNamespaceMetaParam(ns, 1, "", 0)
	assert.Nil(t, err)

	time.Sleep(time.Second * 10)
	for {
		time.Sleep(time.Second)
		newNs := getNsInfo(t, ns, 0)
		t.Logf("new isr is: %v", newNs)
		waitForAllFullReady(t, ns, 0)
		if len(newNs.GetISR()) < 2 {
			break
		}
	}

	waitEnoughReplica(t, ns, 0)
	waitForAllFullReady(t, ns, 0)
	waitBalancedLeader(t, ns, 0)

	// restart leader as alone
	leader.RestartAsStandalone(common.GetNsDesp(ns, 0))
	time.Sleep(time.Second)

	waitEnoughReplica(t, ns, 0)
	waitForAllFullReady(t, ns, 0)

	dnw, leaderNode = waitForLeader(t, ns, 0)
	leader = dnw.s
	assert.NotNil(t, leader)
	leaderNode.Node.OptimizeDB("")
	time.Sleep(time.Second * 3)

	err = gpdServer.pdCoord.ChangeNamespaceMetaParam(ns, 2, "", 0)
	assert.Nil(t, err)

	for {
		time.Sleep(time.Second)
		newNs := getNsInfo(t, ns, 0)
		t.Logf("new isr is: %v", newNs)
		waitForAllFullReady(t, ns, 0)
		if len(newNs.GetISR()) == 2 {
			break
		}
	}
	waitEnoughReplica(t, ns, 0)
	waitForAllFullReady(t, ns, 0)
	waitBalancedLeader(t, ns, 0)
}

func TestInstallSnapshotOnFollower(t *testing.T) {
	// Test the follower fall behind too much, and the leader send the snapshot to follower,
	ensureClusterReady(t, 4)

	time.Sleep(time.Second)
	ns := "test_cluster_snap_install"
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
	for i := 0; i < 5; i++ {
		nsNode.Node.OptimizeDB("")
	}
	c := getTestRedisConn(t, dnw.redisPort)
	defer c.Close()
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("%s:%s:%v", ns, "snap_transfer:k1", i)
		rsp, err := goredis.String(c.Do("set", key, key))
		assert.Nil(t, err)
		assert.Equal(t, "OK", rsp)
	}
	for i := 0; i < 50; i++ {
		nsNode.Node.OptimizeDB("")
	}
	oldNs := getNsInfo(t, ns, 0)
	t.Logf("old isr is: %v", oldNs)
	assert.Equal(t, 3, len(oldNs.GetISR()))
	foWrap, _ := getFollowerNode(t, ns, 0)
	foWrap.s.Stop()

	for i := 0; i < 50; i++ {
		nsNode.Node.OptimizeDB("")
	}
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("%s:%s:%v", ns, "snap_transfer:k1", i)
		rsp, err := goredis.String(c.Do("set", key, "updated"+key))
		assert.Nil(t, err)
		assert.Equal(t, "OK", rsp)
	}
	for i := 0; i < 50; i++ {
		nsNode.Node.OptimizeDB("")
	}
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("%s:%s:%v", ns, "snap_transfer:k1", i)
		leaderV, err := goredis.String(c.Do("get", key))
		assert.True(t, err == nil || err == goredis.ErrNil)
		if i < 50 {
			assert.Equal(t, "updated"+key, leaderV)
		} else {
			assert.Equal(t, key, leaderV)
		}
	}

	foWrap.s.Start()
	time.Sleep(time.Second * 10)
	addr := fmt.Sprintf("http://127.0.0.1:%v", foWrap.httpPort)
	enableStaleRead(t, addr, true)
	followerConn := getTestRedisConn(t, foWrap.redisPort)
	defer followerConn.Close()

	waitForAllFullReady(t, ns, 0)
	time.Sleep(time.Second * 3)

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("%s:%s:%v", ns, "snap_transfer:k1", i)
		getV, err := goredis.String(followerConn.Do("get", key))
		assert.True(t, err == nil || err == goredis.ErrNil)
		if i < 50 {
			assert.Equal(t, "updated"+key, getV)
		} else {
			assert.Equal(t, key, getV)
		}
	}
	enableStaleRead(t, addr, false)
}

func TestInstallSnapshotTransferFailed(t *testing.T) {
	// Test the follower fall behind too much, and the leader send the snapshot to follower,
	// However, the follower failed to pull the snapshot data from leader. So the raft node should stop
	// and restart pull snapshot data later.

	// test case should make sure the snap will be not persisted to the stable storage since the snapshot data is failed to pull.
	// check data write after snapshot should not be read until the snapshot fail is recovered
	node.EnableSnapForTest(true, false, false, false)
	defer node.EnableSnapForTest(false, false, false, false)

	ensureClusterReady(t, 4)

	time.Sleep(time.Second)
	ns := "test_cluster_snap_transfer_failed"
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
	for i := 0; i < 5; i++ {
		nsNode.Node.OptimizeDB("")
	}
	oldNs := getNsInfo(t, ns, 0)
	t.Logf("old isr is: %v", oldNs)
	assert.Equal(t, 3, len(oldNs.GetISR()))
	foWrap, _ := getFollowerNode(t, ns, 0)
	foWrap.s.Stop()

	for i := 0; i < 50; i++ {
		nsNode.Node.OptimizeDB("")
	}
	c := getTestRedisConn(t, dnw.redisPort)
	defer c.Close()
	key := fmt.Sprintf("%s:%s", ns, "snap_transfer:k1")
	rsp, err := goredis.String(c.Do("set", key, "1234"))
	assert.Nil(t, err)
	assert.Equal(t, "OK", rsp)

	for i := 0; i < 50; i++ {
		nsNode.Node.OptimizeDB("")
	}
	leaderV, err := goredis.String(c.Do("get", key))
	assert.True(t, err == nil || err == goredis.ErrNil)
	assert.Equal(t, "1234", leaderV)

	foWrap.s.Start()
	time.Sleep(time.Second * 10)
	addr := fmt.Sprintf("http://127.0.0.1:%v", foWrap.httpPort)
	enableStaleRead(t, addr, true)
	// snapshort should failed
	followerConn := getTestRedisConn(t, foWrap.redisPort)
	defer followerConn.Close()
	for i := 0; i < 10; i++ {
		getV, err := goredis.String(followerConn.Do("get", key))
		assert.NotNil(t, err)
		t.Logf("read follower should failed: %v", err.Error())
		assert.True(t, getV == "")
		time.Sleep(time.Second)
	}

	node.EnableSnapForTest(false, false, false, false)
	waitForAllFullReady(t, ns, 0)
	time.Sleep(time.Second * 3)

	getV, err := goredis.String(followerConn.Do("get", key))
	assert.True(t, err == nil || err == goredis.ErrNil)
	assert.Equal(t, "1234", getV)
	enableStaleRead(t, addr, false)
}

func TestInstallSnapshotSaveRaftFailed(t *testing.T) {
	// test the snapshot transfer to follower success, but the follower save snapshot meta to raft storage failed
	// should restart to re-apply
	// the hardstate and snapshot saving is not atomic, enable this test if we can make that.
	defer node.EnableSnapForTest(false, false, false, false)

	ensureClusterReady(t, 4)

	time.Sleep(time.Second)
	ns := "test_cluster_snap_save_failed"
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
	for i := 0; i < 5; i++ {
		nsNode.Node.OptimizeDB("")
	}
	oldNs := getNsInfo(t, ns, 0)
	t.Logf("old isr is: %v", oldNs)
	assert.Equal(t, 3, len(oldNs.GetISR()))

	foWrap, _ := getFollowerNode(t, ns, 0)
	foWrap.s.Stop()

	for i := 0; i < 50; i++ {
		nsNode.Node.OptimizeDB("")
	}
	c := getTestRedisConn(t, dnw.redisPort)
	defer c.Close()
	key := fmt.Sprintf("%s:%s", ns, "snap_save:k1")
	rsp, err := goredis.String(c.Do("set", key, "1234"))
	assert.Nil(t, err)
	assert.Equal(t, "OK", rsp)

	for i := 0; i < 50; i++ {
		nsNode.Node.OptimizeDB("")
	}
	leaderV, err := goredis.String(c.Do("get", key))
	assert.True(t, err == nil || err == goredis.ErrNil)
	assert.Equal(t, "1234", leaderV)
	time.Sleep(time.Second * 5)

	node.EnableSnapForTest(false, true, false, false)
	foWrap.s.Start()
	time.Sleep(time.Second * 10)
	addr := fmt.Sprintf("http://127.0.0.1:%v", foWrap.httpPort)
	enableStaleRead(t, addr, true)
	// snapshort should failed
	followerConn := getTestRedisConn(t, foWrap.redisPort)
	defer followerConn.Close()
	for i := 0; i < 10; i++ {
		getV, err := goredis.String(followerConn.Do("get", key))
		assert.NotNil(t, err)
		t.Logf("read follower should failed: %v", err.Error())
		assert.True(t, getV == "")
		time.Sleep(time.Second)
	}

	node.EnableSnapForTest(false, false, false, false)
	waitForAllFullReady(t, ns, 0)
	time.Sleep(time.Second * 3)

	getV, err := goredis.String(followerConn.Do("get", key))
	assert.True(t, err == nil || err == goredis.ErrNil)
	assert.Equal(t, "1234", getV)
	enableStaleRead(t, addr, false)
}

func TestInstallSnapshotApplyFailed(t *testing.T) {
	// test the snapshot transfer to follower success, but the follower apply failed
	// should restart to re-apply, while restart the snapshot will be restored success and no need apply in raft loop
	defer node.EnableSnapForTest(false, false, false, false)

	ensureClusterReady(t, 4)

	time.Sleep(time.Second)
	ns := "test_cluster_snap_apply_failed"
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
	for i := 0; i < 5; i++ {
		nsNode.Node.OptimizeDB("")
	}
	oldNs := getNsInfo(t, ns, 0)
	t.Logf("old isr is: %v", oldNs)
	assert.Equal(t, 3, len(oldNs.GetISR()))

	foWrap, _ := getFollowerNode(t, ns, 0)
	foWrap.s.Stop()

	for i := 0; i < 50; i++ {
		nsNode.Node.OptimizeDB("")
	}
	c := getTestRedisConn(t, dnw.redisPort)
	defer c.Close()
	key := fmt.Sprintf("%s:%s", ns, "snap_apply:k1")
	rsp, err := goredis.String(c.Do("set", key, "1234"))
	assert.Nil(t, err)
	assert.Equal(t, "OK", rsp)

	for i := 0; i < 50; i++ {
		nsNode.Node.OptimizeDB("")
	}
	leaderV, err := goredis.String(c.Do("get", key))
	assert.True(t, err == nil || err == goredis.ErrNil)
	assert.Equal(t, "1234", leaderV)
	time.Sleep(time.Second * 5)

	node.EnableSnapForTest(false, false, true, false)
	foWrap.s.Start()
	// apply failed and restart success, so we can be ready after restart
	waitForAllFullReady(t, ns, 0)
	time.Sleep(time.Second * 3)
	addr := fmt.Sprintf("http://127.0.0.1:%v", foWrap.httpPort)
	enableStaleRead(t, addr, true)
	followerConn := getTestRedisConn(t, foWrap.redisPort)
	defer followerConn.Close()

	getV, err := goredis.String(followerConn.Do("get", key))
	assert.True(t, err == nil || err == goredis.ErrNil)
	assert.Equal(t, "1234", getV)
	enableStaleRead(t, addr, false)
}

func TestInstallSnapshotApplyRestoreFailed(t *testing.T) {
	// restore failed will make sure apply snapshot and restart both failed to restore
	defer node.EnableSnapForTest(false, false, false, false)

	ensureClusterReady(t, 4)

	time.Sleep(time.Second)
	ns := "test_cluster_snap_apply_restore_failed"
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
	for i := 0; i < 5; i++ {
		nsNode.Node.OptimizeDB("")
	}
	oldNs := getNsInfo(t, ns, 0)
	t.Logf("old isr is: %v", oldNs)
	assert.Equal(t, 3, len(oldNs.GetISR()))

	foWrap, _ := getFollowerNode(t, ns, 0)
	foWrap.s.Stop()

	for i := 0; i < 50; i++ {
		nsNode.Node.OptimizeDB("")
	}
	c := getTestRedisConn(t, dnw.redisPort)
	defer c.Close()
	key := fmt.Sprintf("%s:%s", ns, "snap_apply:k1")
	rsp, err := goredis.String(c.Do("set", key, "1234"))
	assert.Nil(t, err)
	assert.Equal(t, "OK", rsp)

	for i := 0; i < 50; i++ {
		nsNode.Node.OptimizeDB("")
	}
	leaderV, err := goredis.String(c.Do("get", key))
	assert.True(t, err == nil || err == goredis.ErrNil)
	assert.Equal(t, "1234", leaderV)
	time.Sleep(time.Second * 5)

	node.EnableSnapForTest(false, false, false, true)
	foWrap.s.Start()
	time.Sleep(time.Second * 10)

	addr := fmt.Sprintf("http://127.0.0.1:%v", foWrap.httpPort)
	enableStaleRead(t, addr, true)
	followerConn := getTestRedisConn(t, foWrap.redisPort)
	defer followerConn.Close()

	for i := 0; i < 10; i++ {
		getV, err := goredis.String(followerConn.Do("get", key))
		assert.NotNil(t, err)
		t.Logf("read follower should failed: %v", err.Error())
		assert.True(t, getV == "")
		time.Sleep(time.Second)
	}

	node.EnableSnapForTest(false, false, false, false)
	// apply failed and restart success, so we can be ready after restart
	waitForAllFullReady(t, ns, 0)
	time.Sleep(time.Second * 5)

	getV, err := goredis.String(followerConn.Do("get", key))
	assert.True(t, err == nil || err == goredis.ErrNil)
	assert.Equal(t, "1234", getV)
	enableStaleRead(t, addr, false)
}

func TestSyncerWriteOnlyInitTrueLoadFromRegister(t *testing.T) {
	testSyncerWriteOnlyLoadFromRegister(t, true)
}

func TestSyncerWriteOnlyInitFalseLoadFromRegister(t *testing.T) {
	testSyncerWriteOnlyLoadFromRegister(t, false)
}

func testSyncerWriteOnlyLoadFromRegister(t *testing.T, syncerOnly bool) {
	clusterName := "unit-test_syncer_write_only"
	pd, dataNodes, dataDir := startTestCluster(t, syncerOnly, clusterName, pdHttpPort, 1, baseRedisPort+2000)
	defer os.RemoveAll(dataDir)
	defer pd.Stop()
	defer dataNodes[0].s.Stop()

	time.Sleep(time.Second)
	ns := "test_cluster_start_init_syncer_write_only"
	partNum := 1
	pduri := "http://127.0.0.1:" + pdHttpPort

	ensureDataNodesReady(t, pduri, len(dataNodes))
	enableAutoBalance(t, pduri, true)
	ensureNamespace(t, pduri, ns, partNum, 1)
	defer ensureDeleteNamespace(t, pduri, ns)
	dnw, _ := waitForLeaderFromNodes(t, ns, 0, dataNodes)
	leader := dnw.s
	assert.NotNil(t, leader)
	test.Equal(t, syncerOnly, node.IsSyncerOnly())
	// test restart (should ignore local config)
	dataNodes[0].s.Stop()
	dataNodes[0].s.Start()
	dnw, _ = waitForLeaderFromNodes(t, ns, 0, dataNodes)
	leader = dnw.s
	assert.NotNil(t, leader)
	test.Equal(t, syncerOnly, node.IsSyncerOnly())

	dataNodes[0].s.Stop()
	dataNodes[0].s.GetCoord().UpdateSyncerWriteOnly(!syncerOnly)
	dataNodes[0].s.Start()
	dnw, _ = waitForLeaderFromNodes(t, ns, 0, dataNodes)
	leader = dnw.s
	assert.NotNil(t, leader)
	test.Equal(t, !syncerOnly, node.IsSyncerOnly())
	dataNodes[0].s.Stop()

	dataNodes[0].s.Start()
	dnw, _ = waitForLeaderFromNodes(t, ns, 0, dataNodes)
	leader = dnw.s
	assert.NotNil(t, leader)
	test.Equal(t, !syncerOnly, node.IsSyncerOnly())
}

func TestNamespaceMagicCodeChangedAfterRecreate(t *testing.T) {
	ensureClusterReady(t, 3)
	time.Sleep(time.Second)
	ns := "test_cluster_ns_recreate"
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
	for i := 0; i < 10; i++ {
		nsNode.Node.OptimizeDB("")
	}
	serverDatas := make(map[string]map[string]int64)
	for _, srv := range gkvList {
		serverDatas[srv.dataPath] = srv.s.GetNsMgr().CheckLocalNamespaces()
	}
	t.Logf("server datas: %v", serverDatas)

	ensureDeleteNamespace(t, pduri, ns)
	time.Sleep(time.Second * 5)

	serverDatas2 := make(map[string]map[string]int64)
	for _, srv := range gkvList {
		serverDatas2[srv.dataPath] = srv.s.GetNsMgr().CheckLocalNamespaces()
		assert.Equal(t, 0, len(serverDatas2[srv.dataPath]))
		deletedNs := getDeletedNs(t, srv.dataPath)
		t.Logf("%v server ns deleted: %v", srv.dataPath, deletedNs)
		assert.Equal(t, len(serverDatas[srv.dataPath]), len(deletedNs))
	}
	t.Logf("server datas after ns deleted: %v", serverDatas2)

	ensureNamespace(t, pduri, ns, partNum, 3)
	waitForLeader(t, ns, 0)

	for _, srv := range gkvList {
		recreatedNsList := srv.s.GetNsMgr().CheckLocalNamespaces()
		oldNs := serverDatas[srv.dataPath]
		for name, nsMagic := range recreatedNsList {
			oldMagic := oldNs[name]
			t.Logf("ns %v magic: %v, %v", name, oldMagic, nsMagic)
			assert.NotEqual(t, int64(0), oldMagic)
			assert.NotEqual(t, oldMagic, nsMagic)
		}
		assert.True(t, len(recreatedNsList) > 0)
	}
}
