package pdserver

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/absolute8511/ZanRedisDB/cluster"
	"github.com/absolute8511/ZanRedisDB/common"
	ds "github.com/absolute8511/ZanRedisDB/server"
	"github.com/absolute8511/go-zanredisdb"
	"github.com/stretchr/testify/assert"
)

const (
	TestClusterName           = "unit-test"
	TestRemoteSyncClusterName = "unit-test-remote-sync"
	pdHttpPort                = "18007"
	pdRemoteHttpPort          = "18008"
	pdLearnerHttpPort         = "18009"
)

var testEtcdServers = "http://127.0.0.1:2379"
var testOnce sync.Once
var gpdServer *Server
var gkvList []dataNodeWrapper
var gtmpDir string

type dataNodeWrapper struct {
	s         *ds.Server
	redisPort int
	httpPort  int
	dataPath  string
}

func startTestClusterForLearner(t *testing.T, n int) (*Server, []dataNodeWrapper, string) {
	kvList := make([]dataNodeWrapper, 0, n)
	clusterTmpDir, err := ioutil.TempDir("", fmt.Sprintf("learner-test-%d", time.Now().UnixNano()))
	assert.Nil(t, err)
	t.Logf("dir:%v\n", clusterTmpDir)

	opts := NewServerConfig()
	opts.HTTPAddress = "127.0.0.1:" + pdLearnerHttpPort
	opts.BroadcastAddr = "127.0.0.1"
	opts.ClusterID = "unit-test"
	opts.ClusterLeadershipAddresses = testEtcdServers
	opts.BalanceInterval = []string{"0", "24"}
	opts.LearnerRole = common.LearnerRoleLogSyncer
	pd := NewServer(opts)
	pd.Start()

	for i := 0; i < n; i++ {
		tmpDir := path.Join(clusterTmpDir, strconv.Itoa(i))
		os.MkdirAll(tmpDir, common.DIR_PERM)
		raftAddr := "http://127.0.0.1:" + strconv.Itoa(15345+i*100)
		redisPort := 25345 + i*100
		httpPort := 35345 + i*100
		kvOpts := ds.ServerConfig{
			ClusterID:            TestClusterName,
			EtcdClusterAddresses: testEtcdServers,
			DataDir:              tmpDir,
			RedisAPIPort:         redisPort,
			LocalRaftAddr:        raftAddr,
			BroadcastAddr:        "127.0.0.1",
			HttpAPIPort:          httpPort,
			TickMs:               100,
			ElectionTick:         5,
			LearnerRole:          common.LearnerRoleLogSyncer,
			RemoteSyncCluster:    "http://127.0.0.1:" + pdRemoteHttpPort,
		}
		kv := ds.NewServer(kvOpts)
		kv.Start()
		time.Sleep(time.Second)
		kvList = append(kvList, dataNodeWrapper{kv, redisPort, httpPort, tmpDir})
	}
	return pd, kvList, clusterTmpDir
}

func startRemoteSyncTestCluster(t *testing.T, n int) (*Server, []dataNodeWrapper, string) {
	return startTestCluster(t, TestRemoteSyncClusterName, pdRemoteHttpPort, n, 16345)
}

func startDefaultTestCluster(t *testing.T, n int) (*Server, []dataNodeWrapper, string) {
	return startTestCluster(t, TestClusterName, pdHttpPort, n, 17345)
}

func startTestCluster(t *testing.T, clusterName string, pdPort string, n int, basePort int) (*Server, []dataNodeWrapper, string) {
	serverAddrList := strings.Split(testEtcdServers, ",")
	rootPath := serverAddrList[0] + "/v2/keys/" + cluster.ROOT_DIR + "/" + clusterName + "/?recursive=true"
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
	opts.HTTPAddress = "127.0.0.1:" + pdPort
	opts.BroadcastAddr = "127.0.0.1"
	opts.ClusterID = clusterName
	opts.ClusterLeadershipAddresses = testEtcdServers
	opts.BalanceInterval = []string{"0", "24"}
	pd := NewServer(opts)
	pd.Start()

	for i := 0; i < n; i++ {
		tmpDir := path.Join(clusterTmpDir, strconv.Itoa(i))
		os.MkdirAll(tmpDir, common.DIR_PERM)
		raftAddr := "http://127.0.0.1:" + strconv.Itoa(basePort+i*100)
		redisPort := basePort + 10000 + i*100
		httpPort := basePort + 20000 + i*100
		rpcPort := basePort + 22000 + i*100
		kvOpts := ds.ServerConfig{
			ClusterID:            clusterName,
			EtcdClusterAddresses: testEtcdServers,
			DataDir:              tmpDir,
			RedisAPIPort:         redisPort,
			LocalRaftAddr:        raftAddr,
			BroadcastAddr:        "127.0.0.1",
			HttpAPIPort:          httpPort,
			GrpcAPIPort:          rpcPort,
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

func cleanAllCluster(ret int) {
	for _, kv := range gkvList {
		kv.s.Stop()
	}
	if gpdServer != nil {
		gpdServer.Stop()
	}

	if ret == 0 {
		if strings.Contains(gtmpDir, "rocksdb-test") {
			fmt.Println("removing: ", gtmpDir)
			os.RemoveAll(gtmpDir)
		}
	}
}

func getTestClient(t *testing.T, ns string) *zanredisdb.ZanRedisClient {
	conf := &zanredisdb.Conf{
		DialTimeout:  time.Second * 15,
		ReadTimeout:  0,
		WriteTimeout: 0,
		TendInterval: 10,
		Namespace:    ns,
	}
	conf.LookupList = append(conf.LookupList, "127.0.0.1:"+pdHttpPort)
	c := zanredisdb.NewZanRedisClient(conf)
	c.Start()
	return c
}

func startTestClusterAndCheck(t *testing.T) (*Server, []dataNodeWrapper, string) {
	pd, kvList, tmpDir := startDefaultTestCluster(t, 4)
	time.Sleep(time.Second)
	pduri := "http://127.0.0.1:" + pdHttpPort
	uri := fmt.Sprintf("%s/datanodes", pduri)
	start := time.Now()
	type dataNodeResp struct {
		Nodes []cluster.NodeInfo `json:"nodes"`
	}
	for {
		if time.Since(start) > time.Second*60 {
			assert.FailNow(t, "should discovery all nodes")
			break
		}
		rsp, err := http.Get(uri)
		assert.Nil(t, err)
		if rsp.StatusCode != 200 {
			assert.FailNow(t, rsp.Status)
		}

		var nodesRsp dataNodeResp
		d, err := ioutil.ReadAll(rsp.Body)
		assert.Nil(t, err)
		rsp.Body.Close()
		err = json.Unmarshal(d, &nodesRsp)
		if err != nil {
			assert.FailNow(t, "nodes rsp error: "+string(d)+err.Error())
		}
		assert.Nil(t, err)
		if len(nodesRsp.Nodes) != len(kvList) {
			time.Sleep(time.Second)
			t.Logf(string(d))
			continue
		}
		break
	}

	rsp, err := http.Get("http://127.0.0.1:" + pdHttpPort + "/listpd")
	assert.Nil(t, err)
	t.Log(rsp)
	b, err := ioutil.ReadAll(rsp.Body)
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
	// assert.Equal(t, "127.0.0.1", pdleader.NodeIP)
	assert.Equal(t, pdHttpPort, pdleader.HttpPort)
	return pd, kvList, tmpDir
}

func ensureClusterReady(t *testing.T) {
	testOnce.Do(func() {
		gpdServer, gkvList, gtmpDir = startTestClusterAndCheck(t)
	},
	)
}

func ensureDataNodesReady(t *testing.T, pduri string, expect int) {
	uri := fmt.Sprintf("%s/datanodes", pduri)
	start := time.Now()
	type dataNodeResp struct {
		Nodes []cluster.NodeInfo `json:"nodes"`
	}
	for {
		if time.Since(start) > time.Second*10 {
			assert.FailNow(t, "should discovery all nodes")
			break
		}
		rsp, err := http.Get(uri)
		assert.Nil(t, err)
		if rsp.StatusCode != 200 {
			assert.FailNow(t, rsp.Status)
		}

		var nodesRsp dataNodeResp
		d, err := ioutil.ReadAll(rsp.Body)
		assert.Nil(t, err)
		rsp.Body.Close()
		err = json.Unmarshal(d, &nodesRsp)
		if err != nil {
			assert.FailNow(t, "nodes rsp error: "+string(d)+err.Error())
		}
		assert.Nil(t, err)
		t.Log(nodesRsp)
		if len(nodesRsp.Nodes) != expect {
			time.Sleep(time.Second)
			t.Logf(string(d))
			continue
		}
		break
	}
}

func ensureNamespace(t *testing.T, pduri string, ns string, partNum int) {
	uri := fmt.Sprintf("%s/cluster/namespace/create?namespace=%s&partition_num=%d&replicator=3", pduri, ns, partNum)
	rsp, err := http.Post(uri, "", nil)
	assert.Nil(t, err)
	if rsp.StatusCode != 200 {
		assert.FailNow(t, rsp.Status)
	}
	assert.Equal(t, 200, rsp.StatusCode)
	rsp.Body.Close()
	time.Sleep(time.Second)
	start := time.Now()
	var nsList map[string][]string
	for {
		if time.Since(start) > time.Minute {
			assert.FailNow(t, "should init namespace")
			break
		}
		rsp, err := http.Get(pduri + "/namespaces")
		assert.Nil(t, err)
		if rsp.StatusCode != 200 {
			assert.FailNow(t, rsp.Status)
		}
		d, err := ioutil.ReadAll(rsp.Body)
		assert.Nil(t, err)
		rsp.Body.Close()
		err = json.Unmarshal(d, &nsList)
		assert.Nil(t, err)
		if len(nsList["namespaces"]) == 0 {
			time.Sleep(time.Second)
			continue
		}
		t.Log(nsList)
		found := false
		for _, n := range nsList["namespaces"] {
			if ns == n {
				found = true
				break
			}
		}
		if !found {
			time.Sleep(time.Second)
			continue
		}
		break
	}
	// query the namespace datanodes
	type queryNsInfo struct {
		Epoch     int64                     `json:"epoch"`
		PNum      int                       `json:"partition_num"`
		EngType   string                    `json:"eng_type"`
		PartNodes map[int]PartitionNodeInfo `json:"partitions"`
	}
	start = time.Now()
	for {
		if time.Since(start) > time.Minute {
			assert.FailNow(t, "should discovery namespace on all replica nodes")
			break
		}
		rsp, err := http.Get(pduri + "/query/" + ns)
		assert.Nil(t, err)
		if rsp.StatusCode != 200 {
			assert.FailNow(t, "query ns: failed "+rsp.Status)
			time.Sleep(time.Second)
			continue
		}
		assert.Equal(t, 200, rsp.StatusCode)
		d, err := ioutil.ReadAll(rsp.Body)
		assert.Nil(t, err)
		rsp.Body.Close()
		var queryRsp queryNsInfo
		err = json.Unmarshal(d, &queryRsp)
		assert.Nil(t, err)
		t.Log(queryRsp)
		if len(queryRsp.PartNodes) != partNum {
			time.Sleep(time.Second)
			continue
		}
		assert.Equal(t, 3, len(queryRsp.PartNodes[0].Replicas))
		break
	}
}

func ensureDeleteNamespace(t *testing.T, pduri string, ns string) {
	uri := fmt.Sprintf("%s/cluster/namespace/delete?namespace=%s&partition=**", pduri, ns)
	req, err := http.NewRequest("DELETE", uri, nil)
	assert.Nil(t, err)
	rsp, err := http.DefaultClient.Do(req)
	assert.Nil(t, err)
	assert.Equal(t, 200, rsp.StatusCode)
	rsp.Body.Close()
}

func TestClusterInitStart(t *testing.T) {
	ensureClusterReady(t)
}
func TestClusterSchemaAddIndex(t *testing.T) {
	ensureClusterReady(t)

	time.Sleep(time.Second)
	ns := "test_schema_ns"
	partNum := 2

	pduri := "http://127.0.0.1:" + pdHttpPort
	uri := fmt.Sprintf("%s/datanodes", pduri)

	ensureDataNodesReady(t, pduri, len(gkvList))
	ensureNamespace(t, pduri, ns, partNum)

	indexTable := "test_hash_table"
	uri = fmt.Sprintf("%s/cluster/schema/index/add?namespace=%s&table=%s&indextype=hash_secondary", pduri, ns, indexTable)
	var hindex common.HsetIndexSchema
	hindex.Name = "test_hash_index"
	hindex.IndexField = "hash_index_field"
	hindex.ValueType = common.Int64V
	hindexData, _ := json.Marshal(hindex)
	rsp, err := http.Post(uri, "", bytes.NewBuffer(hindexData))
	assert.Nil(t, err)
	assert.Equal(t, 200, rsp.StatusCode)
	rsp.Body.Close()
	time.Sleep(time.Second)
	start := time.Now()
	for {
		if time.Since(start) > time.Second*60 {
			assert.Fail(t, "should init table index schema")
			break
		}
		allReady := true
		for _, kv := range gkvList {
			notReady := false
			for pid := 0; pid < partNum; pid++ {
				n := kv.s.GetNamespaceFromFullName(common.GetNsDesp(ns, pid))
				if n == nil {
					continue
				}
				allIndexes, err := n.Node.GetIndexSchema(indexTable)
				if err != nil {
					t.Logf("kv %v index not found %v", kv.redisPort, pid)
					time.Sleep(time.Millisecond * 100)
					notReady = true
					break
				}
				localIndex, ok := allIndexes[indexTable]
				assert.True(t, ok, "index table should be ok")
				assert.Equal(t, 1, len(localIndex.HsetIndexes))
				assert.Equal(t, hindex.Name, localIndex.HsetIndexes[0].Name)
				assert.Equal(t, hindex.ValueType, localIndex.HsetIndexes[0].ValueType)
				if localIndex.HsetIndexes[0].State != common.ReadyIndex {
					t.Logf("kv %v index not ready %v, %v", kv.redisPort, pid, localIndex.HsetIndexes[0])
					notReady = true
					time.Sleep(time.Millisecond * 100)
					break
				}
				t.Logf("kv %v index ready %v, %v", kv.redisPort, pid, localIndex.HsetIndexes[0])
			}
			if notReady {
				allReady = false
				time.Sleep(time.Millisecond * 100)
				break
			}
		}
		if allReady {
			break
		}
	}
	uri = fmt.Sprintf("%s/cluster/schema/index/del?namespace=%s&table=%s&indextype=hash_secondary&indexname=%s", pduri, ns, indexTable, hindex.Name)
	req, err := http.NewRequest("DELETE", uri, nil)
	assert.Nil(t, err)
	rsp, err = http.DefaultClient.Do(req)
	assert.Nil(t, err)
	assert.Equal(t, 200, rsp.StatusCode)
	rsp.Body.Close()

	ensureDeleteNamespace(t, pduri, ns)
}
