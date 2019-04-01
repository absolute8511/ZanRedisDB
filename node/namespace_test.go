package node

import (
	"fmt"
	io "io"
	"io/ioutil"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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

func TestInitNodeWithSharedRocksdb(t *testing.T) {
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
