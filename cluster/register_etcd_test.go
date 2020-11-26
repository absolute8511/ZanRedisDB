package cluster

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var testEtcdServers = "http://127.0.0.1:2379"

func TestRegisterEtcd(t *testing.T) {
	clusterID := "unittest-zankv-cluster-test-register"
	reg, err := NewPDEtcdRegister(testEtcdServers)
	assert.Nil(t, err)
	reg.InitClusterID(clusterID)
	nodeInfo := &NodeInfo{
		NodeIP: "127.0.0.1",
	}
	nodeInfo.ID = GenNodeID(nodeInfo, "pd")
	reg.Register(nodeInfo)
	defer reg.Unregister(nodeInfo)
	reg.Start()

	ns := "test-ns"
	reg.DeleteWholeNamespace(ns)

	part := 0
	err = reg.CreateNamespacePartition(ns, part)
	assert.Nil(t, err)
	minGID, err := reg.PrepareNamespaceMinGID()
	assert.Nil(t, err)

	err = reg.CreateNamespace(ns, &NamespaceMetaInfo{
		PartitionNum: 2,
		Replica:      3,
		MinGID:       minGID,
	})
	assert.Nil(t, err)
	replicaInfo := &PartitionReplicaInfo{
		RaftNodes: []string{"127.0.0.1:111"},
		RaftIDs:   map[string]uint64{"127.0.0.1:111": 1},
	}
	err = reg.UpdateNamespacePartReplicaInfo(ns, part, replicaInfo, 0)
	assert.Nil(t, err)
	time.Sleep(time.Second)
	allNs, epoch, err := reg.GetAllNamespaces()
	assert.Nil(t, err)
	t.Logf("ns: %v, epoch: %v", allNs, epoch)
	assert.Equal(t, 1, len(allNs))
	assert.Equal(t, 1, len(allNs[ns]))
	assert.Equal(t, part, allNs[ns][part].Partition)

	part1 := 1
	err = reg.CreateNamespacePartition(ns, part1)
	assert.Nil(t, err)
	err = reg.UpdateNamespacePartReplicaInfo(ns, part1, replicaInfo, 0)
	assert.Nil(t, err)

	start := time.Now()
	for {
		allNs, nepoch, err := reg.GetAllNamespaces()
		assert.Nil(t, err)
		assert.Equal(t, 1, len(allNs))
		t.Logf("ns: %v, epoch for ns: %v", allNs, nepoch)
		if nepoch > epoch {
			epoch = nepoch
			assert.Equal(t, 2, len(allNs[ns]))
			assert.Equal(t, part, allNs[ns][part].Partition)
			assert.Equal(t, part1, allNs[ns][part1].Partition)
			break
		}
		time.Sleep(time.Millisecond)
		if time.Since(start) > time.Second*5 {
			t.Errorf("epoch not increased: %v, %v", epoch, nepoch)
		}
	}
	// should not changed if no change anymore
	time.Sleep(time.Millisecond)
	_, nepoch, err := reg.GetAllNamespaces()
	assert.Equal(t, epoch, nepoch)

	reg.DeleteNamespacePart(ns, part)

	start = time.Now()
	for {
		allNs, nepoch, err := reg.GetAllNamespaces()
		assert.Nil(t, err)
		assert.Equal(t, 1, len(allNs))
		t.Logf("ns: %v, epoch for ns: %v", allNs, nepoch)
		if nepoch > epoch {
			epoch = nepoch
			assert.Equal(t, 1, len(allNs[ns]))
			assert.Equal(t, part1, allNs[ns][part1].Partition)
			break
		}
		time.Sleep(time.Millisecond)
		if time.Since(start) > time.Second*5 {
			t.Errorf("epoch not increased: %v, %v", epoch, nepoch)
		}
	}

	// should not changed if no change anymore
	time.Sleep(time.Millisecond)
	_, nepoch, err = reg.GetAllNamespaces()
	assert.Equal(t, epoch, nepoch)

	reg.DeleteWholeNamespace(ns)
	start = time.Now()
	for {
		allNs, nepoch, err := reg.GetAllNamespaces()
		assert.Nil(t, err)
		t.Logf("ns: %v, epoch for ns: %v", allNs, nepoch)
		if nepoch > epoch {
			assert.Equal(t, 0, len(allNs))
			epoch = nepoch
			break
		}
		time.Sleep(time.Millisecond)
		if time.Since(start) > time.Second*5 {
			t.Errorf("epoch not increased: %v, %v", epoch, nepoch)
		}
	}

	// should not changed if no change anymore
	time.Sleep(time.Millisecond)
	_, nepoch, err = reg.GetAllNamespaces()
	assert.Equal(t, epoch, nepoch)
}
