package cluster

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var testEtcdServers = "http://127.0.0.1:2379"

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

func TestEtcdRegisterGetSetTimeout(t *testing.T) {
	client, err := NewEClient(testEtcdServers)
	assert.Nil(t, err)
	client.timeout = time.Microsecond

	testKey := "/zankv_test/unittest-zankv-cluster-test-register/timeouttest"
	_, err = client.Set(testKey, "testvalue", 10)
	assert.Equal(t, err, context.DeadlineExceeded)
	_, err = client.Get(testKey, false, false)
	assert.Equal(t, err, context.DeadlineExceeded)
}

func TestRegisterWatchKeepAliveTimeoutInDeadConn(t *testing.T) {
	WatchEtcdTimeout = time.Second
	defer func() {
		WatchEtcdTimeout = time.Second * time.Duration(EtcdTTL*2)
	}()
	clusterID := "unittest-zankv-cluster-test-register"
	reg, err := NewPDEtcdRegister(testEtcdServers)
	assert.Nil(t, err)
	reg.InitClusterID(clusterID)
	nodeInfo := &NodeInfo{
		NodeIP: "127.0.0.1",
	}
	nodeInfo.ID = GenNodeID(nodeInfo, "pd")
	reg.Start()
	defer reg.Stop()
	reg.Register(nodeInfo)
	defer reg.Unregister(nodeInfo)

	stopC := make(chan struct{}, 1)
	leaderChan := make(chan *NodeInfo, 10)
	// watch pd leader and acquire leader
	reg.AcquireAndWatchLeader(leaderChan, stopC)
	pdLeaderChanged := int32(0)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case n := <-leaderChan:
				t.Logf("in pd register pd leader changed to : %v", n)
				atomic.AddInt32(&pdLeaderChanged, 1)
			case <-stopC:
				return
			}
		}
	}()
	// watch data nodes
	dnChan := make(chan []NodeInfo, 10)
	wg.Add(1)
	go func() {
		defer wg.Done()
		reg.WatchDataNodes(dnChan, stopC)
	}()
	dataNodeChanged := int32(0)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case n, ok := <-dnChan:
				if !ok {
					return
				}
				t.Logf("nodes changed to %v", n)
				atomic.AddInt32(&dataNodeChanged, 1)
			case <-stopC:
				return
			}
		}
	}()

	// watch pd leader
	nodeReg, err := NewDNEtcdRegister(testEtcdServers)
	assert.Nil(t, err)
	nodeReg.InitClusterID(clusterID)
	nodeInfo2 := &NodeInfo{
		NodeIP: "127.0.0.1",
	}
	nodeInfo2.ID = GenNodeID(nodeInfo2, "datanode")
	nodeReg.Start()
	defer nodeReg.Stop()
	nodeReg.Register(nodeInfo2)
	defer nodeReg.Unregister(nodeInfo2)
	leaderChan2 := make(chan *NodeInfo, 10)
	wg.Add(1)
	go func() {
		defer wg.Done()
		nodeReg.WatchPDLeader(leaderChan2, stopC)
	}()
	timer := time.NewTimer(time.Second * time.Duration(EtcdTTL+1))
	for {
		select {
		case n, ok := <-leaderChan2:
			if !ok {
				return
			}
			t.Logf("in data register pd leader changed to %v", n)
			atomic.AddInt32(&pdLeaderChanged, 1)
		case <-timer.C:
			close(stopC)
			wg.Wait()
			t.Logf("changed: %v , %v", atomic.LoadInt32(&pdLeaderChanged), atomic.LoadInt32(&dataNodeChanged))
			assert.True(t, atomic.LoadInt32(&dataNodeChanged) >= 1)
			assert.True(t, atomic.LoadInt32(&dataNodeChanged) <= 4)
			assert.True(t, atomic.LoadInt32(&pdLeaderChanged) <= 8)
			assert.True(t, atomic.LoadInt32(&pdLeaderChanged) >= 2)
			return
		}
	}
}

func TestRegisterWatchExpired(t *testing.T) {
	// TODO: test watch when etcd returned index cleared
}
