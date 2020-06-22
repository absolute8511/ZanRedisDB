package server

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"path"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/siddontang/goredis"
	"github.com/stretchr/testify/assert"
	"github.com/youzan/ZanRedisDB/common"
	"github.com/youzan/ZanRedisDB/node"
	"github.com/youzan/ZanRedisDB/rockredis"
)

var testOnce sync.Once
var gkvs *Server
var gredisport int
var OK = "OK"
var gtmpDir string

func changeLogLevel(t *testing.T, l int, port int) {
	url := fmt.Sprintf("http://127.0.0.1:%v/loglevel/set?loglevel=%d", port, l)
	rsp, err := http.Post(url, "json", nil)
	if err != nil {
		t.Error(err)
	}
	if rsp.StatusCode != http.StatusOK {
		t.Error(rsp.Status)
	}
}

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

func startTestServer(t *testing.T, port int) (*Server, int, string) {
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("rocksdb-test-%d", time.Now().UnixNano()))
	assert.Nil(t, err)
	t.Logf("dir:%v\n", tmpDir)
	ioutil.WriteFile(
		path.Join(tmpDir, "myid"),
		[]byte(strconv.FormatInt(int64(1), 10)),
		common.FILE_PERM)
	redisport := port
	raftAddr := "http://127.0.0.1:" + strconv.Itoa(port+2)
	var replica node.ReplicaInfo
	replica.NodeID = 1
	replica.ReplicaID = 1
	replica.RaftAddr = raftAddr
	kvOpts := ServerConfig{
		ClusterID:      "test",
		DataDir:        tmpDir,
		RedisAPIPort:   redisport,
		HttpAPIPort:    redisport + 1,
		LocalRaftAddr:  raftAddr,
		BroadcastAddr:  "127.0.0.1",
		TickMs:         100,
		ElectionTick:   5,
		UseRocksWAL:    true,
		SharedRocksWAL: true,
		UseRedisV2:     true,
	}
	kvOpts.RocksDBOpts.EnablePartitionedIndexFilter = true
	kvOpts.WALRocksDBOpts.EngineType = "pebble"

	nsConf := node.NewNSConfig()
	nsConf.Name = "default-0"
	nsConf.BaseName = "default"
	nsConf.EngType = rockredis.EngType
	nsConf.PartitionNum = 1
	nsConf.Replicator = 1
	nsConf.RaftGroupConf.GroupID = 1000
	nsConf.RaftGroupConf.SeedNodes = append(nsConf.RaftGroupConf.SeedNodes, replica)
	//nsConf.ExpirationPolicy = common.ConsistencyDeletionExpirationPolicy
	nsConf.ExpirationPolicy = common.WaitCompactExpirationPolicy
	nsConf.DataVersion = common.ValueHeaderV1Str
	kv := NewServer(kvOpts)
	if _, err := kv.InitKVNamespace(1, nsConf, false); err != nil {
		t.Fatalf("failed to init namespace: %v", err)
	}

	kv.Start()
	time.Sleep(time.Second)
	return kv, redisport, tmpDir
}

func waitServerForLeader(t *testing.T, w time.Duration) {
	start := time.Now()
	for {
		replicaNode := gkvs.GetNamespaceFromFullName("default-0")
		assert.NotNil(t, replicaNode)
		if replicaNode.Node.IsLead() {
			return
		}
		if time.Since(start) > w {
			t.Fatalf("\033[31m timed out %v for wait leader \033[39m\n", time.Since(start))
			break
		}
		time.Sleep(time.Second)
	}
}

func getTestConn(t *testing.T) *goredis.PoolConn {
	testOnce.Do(func() {
		gkvs, gredisport, gtmpDir = startTestServer(t, 12345)
		waitServerForLeader(t, time.Second*10)
	},
	)
	c := goredis.NewClient("127.0.0.1:"+strconv.Itoa(gredisport), "")
	c.SetMaxIdleConns(4)
	conn, err := c.Get()
	if err != nil {
		t.Fatal(err)
	}
	return conn
}
