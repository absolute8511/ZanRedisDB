package pdnode_coord

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/youzan/ZanRedisDB/cluster"
)

type testLogger struct {
	t *testing.T
}

func newTestLogger(t *testing.T) *testLogger {
	return &testLogger{t: t}
}

func (l *testLogger) Output(maxdepth int, s string) error {
	l.t.Logf("%v:%v", time.Now().UnixNano(), s)
	return nil
}

func (l *testLogger) OutputErr(maxdepth int, s string) error {
	l.t.Logf("%v:%v", time.Now().UnixNano(), s)
	return nil
}

func (l *testLogger) OutputWarning(maxdepth int, s string) error {
	l.t.Logf("%v:%v", time.Now().UnixNano(), s)
	return nil
}

func checkPartitionNodesBalance(t *testing.T, balanceVer string, partitionNodes [][]string) {
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
	assert.True(t, maxL-minL <= 1, partitionNodes)

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
		assert.True(t, maxL-minL <= 3, partitionNodes)
		return
	}
	assert.True(t, maxL-minL <= 1, partitionNodes)
}

func TestClusterNodesPlacementAcrossDCV1(t *testing.T) {
	testClusterNodesPlacementAcrossDC(t, "")
}

func TestClusterNodesPlacementAcrossDCV2(t *testing.T) {
	testClusterNodesPlacementAcrossDC(t, "v2")
}

func testClusterNodesPlacementAcrossDC(t *testing.T, balanceVer string) {
	cluster.SetLogger(2, newTestLogger(t))
	nodes := make(map[string]cluster.NodeInfo)

	dc1Nodes := SortableStrings{"11", "12", "13", "14", "15", "16"}
	for _, nid := range dc1Nodes {
		var n cluster.NodeInfo
		n.ID = nid
		n.Tags = make(map[string]interface{})
		n.Tags[cluster.DCInfoTag] = "1"
		nodes[nid] = n
	}
	dc2Nodes := SortableStrings{"21", "22", "23", "24", "25", "26"}
	for _, nid := range dc2Nodes {
		var n cluster.NodeInfo
		n.ID = nid
		n.Tags = make(map[string]interface{})
		n.Tags[cluster.DCInfoTag] = "2"
		nodes[nid] = n
	}
	partitionNum := 8
	replicator := 3
	nodeNameList := getNodeNameList(nodes)
	assert.Equal(t, nodeNameList, getNodeNameList(nodes))
	t.Log(nodeNameList)
	placementNodes, err := getRebalancedPartitionsFromNameList("test",
		partitionNum, replicator, nil, nodeNameList, balanceVer)
	assert.Nil(t, err)
	t.Log(placementNodes)
	t.Log(nodeNameList)
	assert.Equal(t, partitionNum, len(placementNodes))
	checkPartitionNodesBalance(t, balanceVer, placementNodes)

	placementNodes2, err := getRebalancedPartitionsFromNameList("test",
		partitionNum, replicator, placementNodes, nodeNameList, balanceVer)
	assert.Nil(t, err)
	t.Log(placementNodes2)
	assert.Equal(t, placementNodes, placementNodes2)
	t.Log(nodeNameList)
	checkPartitionNodesBalance(t, balanceVer, placementNodes2)

	for _, v := range placementNodes {
		assert.Equal(t, replicator, len(v))
		dc1Cnt := 0
		dc2Cnt := 0
		for _, n := range v {
			if cluster.FindSlice(dc1Nodes, n) != -1 {
				dc1Cnt++
			}
			if cluster.FindSlice(dc2Nodes, n) != -1 {
				dc2Cnt++
			}
		}
		assert.Equal(t, true, dc1Cnt > 0, v)
		assert.Equal(t, true, dc2Cnt > 0, v)
		diff := dc2Cnt - dc1Cnt
		if diff < 0 {
			diff = -1 * diff
		}
		assert.Equal(t, 1, diff, v)
	}
	dc1Nodes = SortableStrings{"11", "12", "13", "14", "15", "16"}
	dc2Nodes = SortableStrings{"21", "22", "23", "24", "25", "26"}
	dc3Nodes := SortableStrings{"31", "32", "33", "34", "35", "36"}

	nodeNameList = make([]SortableStrings, 0)
	nodeNameList = append(nodeNameList, dc1Nodes, dc2Nodes, dc3Nodes)
	placementNodes, err = getRebalancedPartitionsFromNameList("test",
		partitionNum, replicator, nil, nodeNameList, balanceVer)
	assert.Nil(t, err)
	t.Log(placementNodes)
	assert.Equal(t, partitionNum, len(placementNodes))
	checkPartitionNodesBalance(t, balanceVer, placementNodes)
	for _, v := range placementNodes {
		assert.Equal(t, replicator, len(v))
		dc1Cnt := 0
		dc2Cnt := 0
		dc3Cnt := 0
		for _, n := range v {
			if cluster.FindSlice(dc1Nodes, n) != -1 {
				dc1Cnt++
			}
			if cluster.FindSlice(dc2Nodes, n) != -1 {
				dc2Cnt++
			}
			if cluster.FindSlice(dc3Nodes, n) != -1 {
				dc3Cnt++
			}
		}
		assert.Equal(t, true, dc1Cnt > 0)
		assert.Equal(t, true, dc2Cnt > 0)
		assert.Equal(t, true, dc3Cnt > 0)
		assert.Equal(t, dc1Cnt, dc2Cnt)
		assert.Equal(t, dc1Cnt, dc3Cnt)
	}
	dc1Nodes = SortableStrings{"11", "12", "13", "14", "15", "16"}
	dc2Nodes = SortableStrings{"21", "22", "23"}

	nodeNameList = make([]SortableStrings, 0)
	nodeNameList = append(nodeNameList, dc1Nodes, dc2Nodes)
	placementNodes, err = getRebalancedPartitionsFromNameList("test",
		partitionNum, replicator, nil, nodeNameList, balanceVer)
	assert.Nil(t, err)
	t.Log(placementNodes)
	assert.Equal(t, partitionNum, len(placementNodes))
	checkPartitionNodesBalance(t, balanceVer, placementNodes)
	dc2NoReplicas := 0
	for _, v := range placementNodes {
		assert.Equal(t, replicator, len(v))
		dc1Cnt := 0
		dc2Cnt := 0
		for _, n := range v {
			if cluster.FindSlice(dc1Nodes, n) != -1 {
				dc1Cnt++
			}
			if cluster.FindSlice(dc2Nodes, n) != -1 {
				dc2Cnt++
			}
		}
		assert.Equal(t, true, dc1Cnt > 0)
		if dc2NoReplicas >= 3 {
			assert.Equal(t, true, dc2Cnt > 0)
		}
		if dc2Cnt == 0 {
			dc2NoReplicas++
		}
	}
	assert.True(t, dc2NoReplicas >= 2)
	assert.True(t, dc2NoReplicas <= 3)

	dc1Nodes = SortableStrings{"11", "12", "13", "14", "15", "16"}
	dc2Nodes = SortableStrings{}

	nodeNameList = make([]SortableStrings, 0)
	nodeNameList = append(nodeNameList, dc1Nodes, dc2Nodes)
	placementNodes, err = getRebalancedPartitionsFromNameList("test",
		partitionNum, replicator, nil, nodeNameList, balanceVer)
	assert.Nil(t, err)
	t.Log(placementNodes)
	assert.Equal(t, partitionNum, len(placementNodes))
	checkPartitionNodesBalance(t, balanceVer, placementNodes)
	for _, v := range placementNodes {
		assert.Equal(t, replicator, len(v))
	}

	dc1Nodes = SortableStrings{"11", "12", "13", "14", "15", "16"}
	nodeNameList = make([]SortableStrings, 0)
	nodeNameList = append(nodeNameList, dc1Nodes)
	placementNodes2, err = getRebalancedPartitionsFromNameList("test",
		partitionNum, replicator, nil, nodeNameList, balanceVer)
	t.Log(placementNodes2)
	assert.Nil(t, err)
	assert.Equal(t, partitionNum, len(placementNodes2))
	checkPartitionNodesBalance(t, balanceVer, placementNodes2)
	for _, v := range placementNodes2 {
		assert.Equal(t, replicator, len(v))
	}
	assert.Equal(t, placementNodes, placementNodes2)
	checkPartitionNodesBalance(t, balanceVer, placementNodes2)
}

func TestClusterNodesPlacementWithMigrateV1(t *testing.T) {
	testClusterNodesPlacementWithMigrateIn1DC(t, "")
	testClusterNodesPlacementWithMigrateIn2DC(t, "")
}
func TestClusterNodesPlacementWithMigrateV2(t *testing.T) {
	testClusterNodesPlacementWithMigrateIn1DC(t, "v2")
	testClusterNodesPlacementWithMigrateIn2DC(t, "v2")
}

func computeTheMigrateCost(oldParts [][]string, newParts [][]string) (int, int, int) {
	leaderChanged := 0
	replicaAdded := 0
	replicaDeleted := 0
	for pid := 0; pid < len(oldParts); pid++ {
		olist := oldParts[pid]
		nlist := newParts[pid]
		if olist[0] != nlist[0] {
			leaderChanged++
		}
		// replica may more or less while change the replicator
		omap := make(map[string]bool)
		for _, o := range olist {
			omap[o] = true
		}
		// remove the same
		unchanged := 0
		for _, n := range nlist {
			_, ok := omap[n]
			if ok {
				delete(omap, n)
				unchanged++
			}
		}
		replicaDeleted += len(omap)
		replicaAdded += len(nlist) - unchanged
	}
	return leaderChanged, replicaAdded, replicaDeleted
}

func genClusterNodes(names SortableStrings, dc string, cnodes map[string]cluster.NodeInfo) map[string]cluster.NodeInfo {
	if cnodes == nil {
		cnodes = make(map[string]cluster.NodeInfo)
	}
	for _, nid := range names {
		var n cluster.NodeInfo
		n.ID = nid
		n.Tags = make(map[string]interface{})
		n.Tags[cluster.DCInfoTag] = dc
		cnodes[nid] = n
	}
	return cnodes
}

func testClusterNodesPlacementWithMigrateIn2DC(t *testing.T, balanceVer string) {
	dc1Nodes := SortableStrings{"110", "130", "150", "170", "190"}
	dc2Nodes := SortableStrings{"210", "230", "250", "270", "290"}
	addedList1 := SortableStrings{"100", "120", "140", "160", "180", "199"}
	addedList2 := SortableStrings{"200", "220", "240", "260", "280", "299"}
	testClusterNodesPlacementWithMigrate(t, balanceVer, dc1Nodes, dc2Nodes, addedList1, addedList2)
}

func testClusterNodesPlacementWithMigrateIn1DC(t *testing.T, balanceVer string) {
	dc1Nodes := SortableStrings{"110", "120", "130", "140", "150"}
	dc2Nodes := SortableStrings{}
	addedList1 := SortableStrings{"100", "115", "125", "135", "145", "155"}
	addedList2 := SortableStrings{}
	testClusterNodesPlacementWithMigrate(t, balanceVer, dc1Nodes, dc2Nodes, addedList1, addedList2)
}

func testClusterNodesPlacementWithMigrate(t *testing.T, balanceVer string,
	dc1Nodes SortableStrings, dc2Nodes SortableStrings,
	addedList1 SortableStrings, addedList2 SortableStrings) {

	cluster.SetLogger(2, newTestLogger(t))
	nodes := make(map[string]cluster.NodeInfo)
	nodes = genClusterNodes(dc1Nodes, "1", nodes)
	nodes = genClusterNodes(dc2Nodes, "2", nodes)

	partitionNum := 32
	replicator := 3
	nodeNameList := getNodeNameList(nodes)
	placementNodes, err := getRebalancedPartitionsFromNameList("test",
		partitionNum, replicator, nil, nodeNameList, balanceVer)
	t.Log(placementNodes)
	assert.Nil(t, err)
	assert.Equal(t, partitionNum, len(placementNodes))
	checkPartitionNodesBalance(t, balanceVer, placementNodes)

	allNodes := make([]cluster.NodeInfo, 0)
	for _, nlist := range nodeNameList {
		for _, n := range nlist {
			allNodes = append(allNodes, nodes[n])
		}
	}
	// try remove any of node, and check the migrated partitions
	for removeIndex := 0; removeIndex < len(allNodes); removeIndex++ {
		newNodes := make(map[string]cluster.NodeInfo)
		for i, v := range allNodes {
			if i == removeIndex {
				continue
			}
			newNodes[v.ID] = v
		}
		nodeNameList := getNodeNameList(newNodes)
		placementNodes2, err := getRebalancedPartitionsFromNameList("test",
			partitionNum, replicator, placementNodes, nodeNameList, balanceVer)
		t.Log(placementNodes2)
		assert.Nil(t, err)
		assert.Equal(t, partitionNum, len(placementNodes2))
		checkPartitionNodesBalance(t, balanceVer, placementNodes2)
		lcost, fadded, deleted := computeTheMigrateCost(placementNodes, placementNodes2)
		t.Logf("%v remove, leader changed: %v, replica added: %v, deleted: %v", removeIndex, lcost, fadded, deleted)
	}
	// try add node to any of the sorted position, and check the migrated partitions
	for _, added := range addedList1 {
		newNodes := make(map[string]cluster.NodeInfo)
		for _, v := range allNodes {
			newNodes[v.ID] = v
		}
		var n cluster.NodeInfo
		n.ID = added
		n.Tags = make(map[string]interface{})
		n.Tags[cluster.DCInfoTag] = "1"
		newNodes[added] = n
		nodeNameList := getNodeNameList(newNodes)
		placementNodes2, err := getRebalancedPartitionsFromNameList("test",
			partitionNum, replicator, placementNodes, nodeNameList, balanceVer)
		t.Log(placementNodes2)
		assert.Nil(t, err)
		assert.Equal(t, partitionNum, len(placementNodes2))
		checkPartitionNodesBalance(t, balanceVer, placementNodes2)
		lcost, fadded, deleted := computeTheMigrateCost(placementNodes, placementNodes2)
		t.Logf("%v added, leader changed: %v, replica added: %v, deleted: %v", added, lcost, fadded, deleted)
	}
	for _, added := range addedList2 {
		newNodes := make(map[string]cluster.NodeInfo)
		for _, v := range allNodes {
			newNodes[v.ID] = v
		}
		var n cluster.NodeInfo
		n.ID = added
		n.Tags = make(map[string]interface{})
		n.Tags[cluster.DCInfoTag] = "2"
		newNodes[added] = n
		nodeNameList := getNodeNameList(newNodes)
		placementNodes2, err := getRebalancedPartitionsFromNameList("test",
			partitionNum, replicator, placementNodes, nodeNameList, balanceVer)
		t.Log(placementNodes2)
		assert.Nil(t, err)
		assert.Equal(t, partitionNum, len(placementNodes2))
		checkPartitionNodesBalance(t, balanceVer, placementNodes2)
		lcost, fadded, deleted := computeTheMigrateCost(placementNodes, placementNodes2)
		t.Logf("%v added, leader changed: %v, replica added: %v, deleted: %v", added, lcost, fadded, deleted)
	}

	// try increase replica, and check the migrated partitions
	placementNodes2, err := getRebalancedPartitionsFromNameList("test",
		partitionNum, replicator+1, placementNodes, nodeNameList, balanceVer)
	t.Log(placementNodes2)
	assert.Nil(t, err)
	assert.Equal(t, partitionNum, len(placementNodes2))
	checkPartitionNodesBalance(t, balanceVer, placementNodes2)
	lcost, fadded, deleted := computeTheMigrateCost(placementNodes, placementNodes2)
	t.Logf("replica increased, leader changed: %v, replica added: %v, deleted: %v", lcost, fadded, deleted)
	// try decrease replica, and check the migrated partitions
	placementNodes2, err = getRebalancedPartitionsFromNameList("test",
		partitionNum, replicator-1, placementNodes, nodeNameList, balanceVer)
	t.Log(placementNodes2)
	assert.Nil(t, err)
	assert.Equal(t, partitionNum, len(placementNodes2))
	checkPartitionNodesBalance(t, balanceVer, placementNodes2)
	lcost, fadded, deleted = computeTheMigrateCost(placementNodes, placementNodes2)
	t.Logf("replica decreased, leader changed: %v, replica added: %v, deleted: %v", lcost, fadded, deleted)
}

func TestClusterMigrateWhileBalanceChanged(t *testing.T) {
	// check change the default balance to v2
	cluster.SetLogger(2, newTestLogger(t))
	nodes := make(map[string]cluster.NodeInfo)
	dc1Nodes := SortableStrings{"110", "120", "130", "140", "150"}
	nodes = genClusterNodes(dc1Nodes, "1", nodes)

	partitionNum := 8
	replicator := 3
	nodeNameList := getNodeNameList(nodes)
	placementNodes, err := getRebalancedPartitionsFromNameList("test",
		partitionNum, replicator, nil, nodeNameList, "")
	t.Log(placementNodes)
	assert.Nil(t, err)
	assert.Equal(t, partitionNum, len(placementNodes))
	checkPartitionNodesBalance(t, "", placementNodes)

	placementNodes2, err := getRebalancedPartitionsFromNameList("test",
		partitionNum, replicator, placementNodes, nodeNameList, "v2")
	t.Log(placementNodes2)
	assert.Nil(t, err)
	assert.Equal(t, partitionNum, len(placementNodes2))
	checkPartitionNodesBalance(t, "v2", placementNodes2)
	lcost, fadded, deleted := computeTheMigrateCost(placementNodes, placementNodes2)
	t.Logf("leader changed: %v, replica added: %v, deleted: %v", lcost, fadded, deleted)
	assert.True(t, lcost <= 0, lcost)
	assert.True(t, fadded <= 1, fadded)
	assert.True(t, deleted <= 1, deleted)
	assert.True(t, fadded+deleted > 0, fadded, deleted)
}
