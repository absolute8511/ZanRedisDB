package pdserver

import (
	"fmt"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/absolute8511/ZanRedisDB/cluster/datanode_coord"

	"github.com/absolute8511/ZanRedisDB/cluster/pdnode_coord"
	"github.com/absolute8511/ZanRedisDB/node"
	ds "github.com/absolute8511/ZanRedisDB/server"
	"github.com/stretchr/testify/assert"
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

func waitForLeader(t *testing.T, ns string, part int) (*ds.Server, *node.NamespaceNode) {
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
				return kv.s, nsNode
			}
		}
		time.Sleep(time.Millisecond * 100)
	}
	return nil, nil
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

func TestLeaderLost(t *testing.T) {
	// leader is lost and mark leader as removing
	ensureClusterReady(t)

	time.Sleep(time.Second)
	ns := "test_leader_lost"
	partNum := 1

	pduri := "http://127.0.0.1:" + pdHttpPort

	ensureDataNodesReady(t, pduri, len(gkvList))
	enableAutoBalance(t, pduri, true)
	ensureNamespace(t, pduri, ns, partNum)
	defer ensureDeleteNamespace(t, pduri, ns)

	leader, nsNode := waitForLeader(t, ns, 0)
	assert.NotNil(t, leader)
	dcoord := leader.GetCoord()
	leaderID := dcoord.GetMyID()
	oldRaftReplicaID := nsNode.Node.GetLocalMemberInfo().ID
	// call this to propose some request to write raft logs
	for i := 0; i < 50; i++ {
		nsNode.Node.OptimizeDB()
	}
	leader.Stop()

	waitMarkAsRemoving(t, ns, 0, leaderID)

	waitRemoveFromRemoving(t, ns, 0)
	waitEnoughReplica(t, ns, 0)
	waitForAllFullReady(t, ns, 0)
	// wait balance
	waitBalancedLeader(t, ns, 0)
	newLeader, _ := waitForLeader(t, ns, 0)
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
	ensureNamespace(t, pduri, ns, partNum)
	defer ensureDeleteNamespace(t, pduri, ns)

	leader, nsNode := waitForLeader(t, ns, 0)
	assert.NotNil(t, leader)
	// call this to propose some request to write raft logs
	for i := 0; i < 50; i++ {
		nsNode.Node.OptimizeDB()
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
