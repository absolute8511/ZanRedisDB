package pdnode_coord

import (
	"testing"

	"github.com/absolute8511/ZanRedisDB/cluster"
	"github.com/stretchr/testify/assert"
)

func TestClusterNodesPlacementAcrossDC(t *testing.T) {
	dc1Nodes := SortableStrings{"11", "12", "13", "14", "15", "16"}
	dc2Nodes := SortableStrings{"21", "22", "23", "24", "25", "26"}

	nodeNameList := make([]SortableStrings, 0)
	nodeNameList = append(nodeNameList, dc1Nodes, dc2Nodes)
	placementNodes, err := getRebalancedPartitionsFromNameList("test",
		8, 3, nodeNameList)
	assert.Nil(t, err)
	assert.Equal(t, 8, len(placementNodes))
	t.Log(placementNodes)

	for _, v := range placementNodes {
		assert.Equal(t, 3, len(v))
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
		assert.Equal(t, true, dc2Cnt > 0)
		diff := dc2Cnt - dc1Cnt
		if diff < 0 {
			diff = -1 * diff
		}
		assert.Equal(t, 1, diff)
	}
	dc1Nodes = SortableStrings{"11", "12", "13", "14", "15", "16"}
	dc2Nodes = SortableStrings{"21", "22", "23", "24", "25", "26"}
	dc3Nodes := SortableStrings{"31", "32", "33", "34", "35", "36"}

	nodeNameList = make([]SortableStrings, 0)
	nodeNameList = append(nodeNameList, dc1Nodes, dc2Nodes, dc3Nodes)
	placementNodes, err = getRebalancedPartitionsFromNameList("test",
		8, 3, nodeNameList)
	assert.Nil(t, err)
	assert.Equal(t, 8, len(placementNodes))
	t.Log(placementNodes)
	for _, v := range placementNodes {
		assert.Equal(t, 3, len(v))
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
		8, 3, nodeNameList)
	assert.Nil(t, err)
	assert.Equal(t, 8, len(placementNodes))
	t.Log(placementNodes)
	dc2NoReplicas := 0
	for _, v := range placementNodes {
		assert.Equal(t, 3, len(v))
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
		if dc2NoReplicas >= 2 {
			assert.Equal(t, true, dc2Cnt > 0)
		}
		if dc2Cnt == 0 {
			dc2NoReplicas++
		}
	}
	assert.Equal(t, 2, dc2NoReplicas)

	dc1Nodes = SortableStrings{"11", "12", "13", "14", "15", "16"}
	dc2Nodes = SortableStrings{}

	nodeNameList = make([]SortableStrings, 0)
	nodeNameList = append(nodeNameList, dc1Nodes, dc2Nodes)
	placementNodes, err = getRebalancedPartitionsFromNameList("test",
		8, 3, nodeNameList)
	assert.Nil(t, err)
	assert.Equal(t, 8, len(placementNodes))
	t.Log(placementNodes)
	for _, v := range placementNodes {
		assert.Equal(t, 3, len(v))
	}

	dc1Nodes = SortableStrings{"11", "12", "13", "14", "15", "16"}
	nodeNameList = make([]SortableStrings, 0)
	nodeNameList = append(nodeNameList, dc1Nodes)
	placementNodes2, err := getRebalancedPartitionsFromNameList("test",
		8, 3, nodeNameList)
	t.Log(placementNodes2)
	assert.Nil(t, err)
	assert.Equal(t, 8, len(placementNodes2))
	for _, v := range placementNodes2 {
		assert.Equal(t, 3, len(v))
	}
	assert.Equal(t, placementNodes, placementNodes2)
}
