package pdserver

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/absolute8511/ZanRedisDB/cluster"
	"github.com/absolute8511/ZanRedisDB/common"
	ds "github.com/absolute8511/ZanRedisDB/server"
	"github.com/absolute8511/go-zanredisdb"
	"github.com/stretchr/testify/assert"
)

const (
	TEST_CLUSTER_NAME = "unit-test"
)

var testEtcdServers = "http://etcd0-qa.s.qima-inc.com:2379"

type dataNodeWrapper struct {
	s         *ds.Server
	redisPort int
	httpPort  int
	dataPath  string
}

func startTestCluster(t *testing.T, n int) (*Server, []dataNodeWrapper, string) {
	serverAddrList := strings.Split(testEtcdServers, ",")
	rootPath := serverAddrList[0] + "/v2/keys/" + cluster.ROOT_DIR + "/" + TEST_CLUSTER_NAME + "/Namespaces?recursive=true"
	if !strings.HasPrefix(rootPath, "http://") {
		rootPath = "http://" + rootPath
	}

	req, _ := http.NewRequest("DELETE",
		rootPath,
		nil,
	)
	rsp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("clean cluster failed: %v", err)
	} else {
		rsp.Body.Close()
	}

	kvList := make([]dataNodeWrapper, 0, n)
	clusterTmpDir, err := ioutil.TempDir("", fmt.Sprintf("rocksdb-test-%d", time.Now().UnixNano()))
	assert.Nil(t, err)
	t.Logf("dir:%v\n", clusterTmpDir)

	opts := NewServerConfig()
	opts.HTTPAddress = "127.0.0.1:18001"
	opts.BroadcastAddr = "127.0.0.1"
	opts.ClusterID = "unit-test"
	opts.ClusterLeadershipAddresses = testEtcdServers
	pd := NewServer(opts)
	pd.Start()

	for i := 0; i < n; i++ {
		tmpDir := path.Join(clusterTmpDir, strconv.Itoa(i))
		os.MkdirAll(tmpDir, common.DIR_PERM)
		raftAddr := "http://127.0.0.1:" + strconv.Itoa(12345+i*100)
		redisPort := 22345 + i*100
		httpPort := 32345 + i*100
		kvOpts := ds.ServerConfig{
			ClusterID:            "unit-test",
			EtcdClusterAddresses: testEtcdServers,
			DataDir:              tmpDir,
			RedisAPIPort:         redisPort,
			LocalRaftAddr:        raftAddr,
			BroadcastAddr:        "127.0.0.1",
			HttpAPIPort:          httpPort,
			TickMs:               100,
			ElectionTick:         5,
		}
		kv := ds.NewServer(kvOpts)
		kv.Start()
		time.Sleep(time.Second)
		kvList = append(kvList, dataNodeWrapper{kv, redisPort, httpPort, tmpDir})
	}
	return pd, kvList, clusterTmpDir
}

func getTestClient(t *testing.T) *zanredisdb.ZanRedisClient {
	conf := &zanredisdb.Conf{
		DialTimeout:  time.Second * 15,
		ReadTimeout:  0,
		WriteTimeout: 0,
		TendInterval: 10,
		Namespace:    "unit-test-ns",
	}
	conf.LookupList = append(conf.LookupList, "127.0.0.1:18001")
	c := zanredisdb.NewZanRedisClient(conf)
	c.Start()
	return c
	// c.Stop()
}

func TestStartCluster(t *testing.T) {
	pd, kvList, tmpDir := startTestCluster(t, 3)
	defer func() {
		for _, kv := range kvList {
			kv.s.Stop()
		}
		pd.Stop()
		os.RemoveAll(tmpDir)
	}()

	client := getTestClient(t)
	defer client.Stop()

	time.Sleep(time.Second)
	rsp, err := http.Get("http://127.0.0.1:18001/datanodes")
	assert.Nil(t, err)
	t.Log(rsp)
	type dataNodeResp struct {
		Nodes []cluster.NodeInfo `json:"nodes"`
	}
	var mapValue dataNodeResp
	b, err := ioutil.ReadAll(rsp.Body)
	rsp.Body.Close()
	assert.Nil(t, err)
	err = json.Unmarshal(b, &mapValue)
	assert.Nil(t, err)
	t.Log(mapValue)
	nodes := mapValue.Nodes
	assert.Equal(t, 3, len(nodes))
	rsp, err = http.Get("http://127.0.0.1:18001/listpd")
	assert.Nil(t, err)
	t.Log(rsp)
	b, err = ioutil.ReadAll(rsp.Body)
	assert.Nil(t, err)
	rsp.Body.Close()
	type listpdResp struct {
		PDLeader cluster.NodeInfo   `json:"pdleader"`
		PDNodes  []cluster.NodeInfo `json:"pdnodes"`
	}
	var pdRspValue listpdResp
	err = json.Unmarshal(b, &pdRspValue)
	assert.Nil(t, err)
	t.Log(pdRspValue)
	assert.Equal(t, 1, len(pdRspValue.PDNodes))
	pdleader := pdRspValue.PDLeader
	assert.Equal(t, "127.0.0.1", pdleader.NodeIP)
	assert.Equal(t, "18001", pdleader.HttpPort)
}
