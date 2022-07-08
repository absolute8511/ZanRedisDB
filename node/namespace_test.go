package node

import (
	"fmt"
	io "io"
	"io/ioutil"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/youzan/ZanRedisDB/common"
	"github.com/youzan/ZanRedisDB/raft"
	"github.com/youzan/ZanRedisDB/raft/raftpb"
	"github.com/youzan/ZanRedisDB/rockredis"
	"github.com/youzan/ZanRedisDB/stats"
	"github.com/youzan/ZanRedisDB/transport/rafthttp"
	"golang.org/x/net/context"
)

type fakeRaft struct {
}

func (fr *fakeRaft) SaveDBFrom(r io.Reader, m raftpb.Message) (int64, error) {
	return 0, nil
}
func (fr *fakeRaft) Process(ctx context.Context, m raftpb.Message) error {
	return nil
}
func (fr *fakeRaft) IsPeerRemoved(id uint64) bool {
	return false
}
func (fr *fakeRaft) ReportUnreachable(id uint64, group raftpb.Group) {
}
func (fr *fakeRaft) ReportSnapshot(id uint64, group raftpb.Group, status raft.SnapshotStatus) {
}

func getTestNamespaceMgr(t *testing.T) (*NamespaceMgr, string) {
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("rocksdb-test-%d", time.Now().UnixNano()))
	if err != nil {
		t.Fatal(err)
	}
	mconf := &MachineConfig{
		DataRootDir:    tmpDir,
		TickMs:         100,
		ElectionTick:   10,
		UseRocksWAL:    true,
		SharedRocksWAL: true,
	}
	ts := &stats.TransportStats{}
	ts.Initialize()
	raftTransport := &rafthttp.Transport{
		DialTimeout: time.Second * 5,
		ClusterID:   "",
		Raft:        &fakeRaft{},
		Snapshotter: &fakeRaft{},
		TrStats:     ts,
		PeersStats:  stats.NewPeersStats(),
		ErrorC:      nil,
	}
	nsMgr := NewNamespaceMgr(raftTransport, mconf)
	return nsMgr, tmpDir
}

func TestInitNodeWithSharedRocksdb(t *testing.T) {
	nsMgr, tmpDir := getTestNamespaceMgr(t)
	defer os.RemoveAll(tmpDir)
	nsMgr.Start()
	defer nsMgr.Stop()

	var replica ReplicaInfo
	replica.NodeID = 1
	replica.ReplicaID = 1
	replica.RaftAddr = "127.0.0.1:1111"

	nsConf := NewNSConfig()
	nsConf.Name = "default-0"
	nsConf.BaseName = "default"
	nsConf.EngType = rockredis.EngType
	nsConf.PartitionNum = 3
	nsConf.Replicator = 1
	nsConf.RaftGroupConf.GroupID = 1000
	nsConf.RaftGroupConf.SeedNodes = append(nsConf.RaftGroupConf.SeedNodes, replica)

	n0, err := nsMgr.InitNamespaceNode(nsConf, 1, false)
	assert.Nil(t, err)

	nsConf1 := NewNSConfig()
	nsConf1.Name = "default-1"
	nsConf1.BaseName = "default"
	nsConf1.EngType = rockredis.EngType
	nsConf1.PartitionNum = 3
	nsConf1.Replicator = 1
	nsConf1.RaftGroupConf.GroupID = 1000
	nsConf1.RaftGroupConf.SeedNodes = append(nsConf.RaftGroupConf.SeedNodes, replica)
	n1, err := nsMgr.InitNamespaceNode(nsConf1, 1, false)
	assert.Nil(t, err)

	nsConf2 := NewNSConfig()
	nsConf2.Name = "default-2"
	nsConf2.BaseName = "default"
	nsConf2.EngType = rockredis.EngType
	nsConf2.PartitionNum = 3
	nsConf2.Replicator = 1
	nsConf2.RaftGroupConf.GroupID = 1000
	nsConf2.RaftGroupConf.SeedNodes = append(nsConf.RaftGroupConf.SeedNodes, replica)
	n2, err := nsMgr.InitNamespaceNode(nsConf2, 1, false)
	assert.Nil(t, err)
	// test if raft storage is rocksdb
	n0Rock, ok := n0.Node.rn.raftStorage.(*raft.RocksStorage)
	assert.True(t, ok)
	n1Rock, ok := n1.Node.rn.raftStorage.(*raft.RocksStorage)
	assert.True(t, ok)
	n2Rock, ok := n2.Node.rn.raftStorage.(*raft.RocksStorage)
	assert.True(t, ok)
	assert.Equal(t, n0Rock.Eng(), n1Rock.Eng())
	assert.Equal(t, n0Rock.Eng(), n2Rock.Eng())
}

func TestLoadLocalMagicList(t *testing.T) {
	nsMgr, tmpDir := getTestNamespaceMgr(t)
	defer os.RemoveAll(tmpDir)
	nsMgr.Start()
	defer nsMgr.Stop()

	var replica ReplicaInfo
	replica.NodeID = 1
	replica.ReplicaID = 1
	replica.RaftAddr = "127.0.0.1:1111"

	nsConf := NewNSConfig()
	nsConf.Name = "default-0"
	nsConf.BaseName = "default"
	nsConf.EngType = rockredis.EngType
	nsConf.PartitionNum = 3
	nsConf.Replicator = 1
	nsConf.RaftGroupConf.GroupID = 1000
	nsConf.RaftGroupConf.SeedNodes = append(nsConf.RaftGroupConf.SeedNodes, replica)

	n0, err := nsMgr.InitNamespaceNode(nsConf, 1, false)
	assert.Nil(t, err)
	magicCode := 12345
	err = n0.SetMagicCode(int64(magicCode))
	assert.Nil(t, err)
	// set same should be ok
	err = n0.SetMagicCode(int64(magicCode))
	assert.Nil(t, err)
	// set different should be error
	err = n0.SetMagicCode(int64(magicCode + 1))
	assert.NotNil(t, err)

	nsConf1 := NewNSConfig()
	nsConf1.Name = "default-1"
	nsConf1.BaseName = "default"
	nsConf1.EngType = rockredis.EngType
	nsConf1.PartitionNum = 3
	nsConf1.Replicator = 1
	nsConf1.RaftGroupConf.GroupID = 1001
	nsConf1.RaftGroupConf.SeedNodes = append(nsConf.RaftGroupConf.SeedNodes, replica)
	n1, err := nsMgr.InitNamespaceNode(nsConf1, 1, false)
	assert.Nil(t, err)
	err = n1.SetMagicCode(int64(magicCode))
	assert.Nil(t, err)

	nsConf2 := NewNSConfig()
	nsConf2.Name = "default-2"
	nsConf2.BaseName = "default"
	nsConf2.EngType = rockredis.EngType
	nsConf2.PartitionNum = 3
	nsConf2.Replicator = 1
	nsConf2.RaftGroupConf.GroupID = 1002
	nsConf2.RaftGroupConf.SeedNodes = append(nsConf.RaftGroupConf.SeedNodes, replica)
	n2, err := nsMgr.InitNamespaceNode(nsConf2, 1, false)
	assert.Nil(t, err)
	err = n2.SetMagicCode(int64(magicCode))
	assert.Nil(t, err)

	magicList := nsMgr.CheckLocalNamespaces()
	assert.Equal(t, 3, len(magicList))
	for ns, code := range magicList {
		baseNs, part := common.GetNamespaceAndPartition(ns)
		assert.Equal(t, "default", baseNs)
		assert.True(t, part < 3, part)
		assert.True(t, part >= 0, part)
		assert.Equal(t, int64(magicCode), code)
	}
}

type testMapGet struct {
	mutex    sync.RWMutex
	kvNodes  map[string]*NamespaceNode
	nsMetas  map[string]*NamespaceMeta
	kvNodes2 sync.Map
	nsMetas2 sync.Map
}

func (m *testMapGet) getNode(nsBaseName string, pk []byte) *NamespaceNode {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	v, ok := m.nsMetas[nsBaseName]
	if !ok {
		return nil
	}
	pid := GetHashedPartitionID(pk, v.PartitionNum)
	fullName := common.GetNsDesp(nsBaseName, pid)
	n, ok := m.kvNodes[fullName]
	if !ok {
		return nil
	}
	return n
}

func (m *testMapGet) getNode2(nsBaseName string, pk []byte) *NamespaceNode {
	v, ok := m.nsMetas2.Load(nsBaseName)
	if !ok {
		return nil
	}
	vv := v.(*NamespaceMeta)
	pid := GetHashedPartitionID(pk, vv.PartitionNum)
	fullName := common.GetNsDesp(nsBaseName, pid)
	n, ok := m.kvNodes2.Load(fullName)
	if !ok {
		return nil
	}
	return n.(*NamespaceNode)
}

func BenchmarkNamespaceMgr_GetNamespaceNodeWithPrimaryKey(b *testing.B) {
	nsm := &testMapGet{
		kvNodes: make(map[string]*NamespaceNode),
		nsMetas: make(map[string]*NamespaceMeta),
	}
	nsm.nsMetas["test"] = &NamespaceMeta{
		PartitionNum: 16,
	}
	nsm.nsMetas2.Store("test", &NamespaceMeta{
		PartitionNum: 16,
	})
	for i := 0; i < 16; i++ {
		k := fmt.Sprintf("test-%v", i)
		nsm.kvNodes[k] = &NamespaceNode{
			ready: 1,
		}
		nsm.kvNodes2.Store(k, &NamespaceNode{
			ready: 1,
		})
	}
	b.ResetTimer()
	var wg sync.WaitGroup
	for g := 0; g < 1000; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < b.N; i++ {
				got := nsm.getNode2("test", []byte(strconv.Itoa(i)))
				if got == nil {
					panic("failed get node")
				}
			}
		}()
	}
	wg.Wait()
}
