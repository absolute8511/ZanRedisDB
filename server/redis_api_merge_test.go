package server

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/absolute8511/ZanRedisDB/node"
	"github.com/absolute8511/ZanRedisDB/rockredis"
	"github.com/siddontang/goredis"
	"github.com/stretchr/testify/assert"
)

var testOnceMerge sync.Once
var kvsMerge *Server
var redisportMerge int
var testNamespaces = make(map[string]*node.NamespaceNode)

func startMergeTestServer(t *testing.T) (*Server, int, string) {
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("rocksdb-test-%d", time.Now().UnixNano()))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("dir:%v\n", tmpDir)
	ioutil.WriteFile(
		path.Join(tmpDir, "myid"),
		[]byte(strconv.FormatInt(int64(1), 10)),
		common.FILE_PERM)
	raftAddr := "http://127.0.0.1:32345"
	redisportMerge := 42345
	var replica node.ReplicaInfo
	replica.NodeID = 1
	replica.ReplicaID = 1
	replica.RaftAddr = raftAddr
	kvOpts := ServerConfig{
		ClusterID:     "test",
		DataDir:       tmpDir,
		RedisAPIPort:  redisportMerge,
		LocalRaftAddr: raftAddr,
		BroadcastAddr: "127.0.0.1",
		TickMs:        100,
		ElectionTick:  5,
	}
	nsConf := node.NewNSConfig()
	nsConf.Name = "default-0"
	nsConf.BaseName = "default"
	nsConf.EngType = rockredis.EngType
	nsConf.PartitionNum = 3
	nsConf.Replicator = 1
	nsConf.RaftGroupConf.GroupID = 1000
	nsConf.RaftGroupConf.SeedNodes = append(nsConf.RaftGroupConf.SeedNodes, replica)
	kv := NewServer(kvOpts)
	n, err := kv.InitKVNamespace(1, nsConf, false)
	if err != nil {
		t.Fatalf("failed to init namespace: %v", err)
	}
	nsConf1 := node.NewNSConfig()
	nsConf1.Name = "default-1"
	nsConf1.BaseName = "default"
	nsConf1.EngType = rockredis.EngType
	nsConf1.PartitionNum = 3
	nsConf1.Replicator = 1
	nsConf1.RaftGroupConf.GroupID = 1000
	nsConf1.RaftGroupConf.SeedNodes = append(nsConf.RaftGroupConf.SeedNodes, replica)
	n1, err := kv.InitKVNamespace(1, nsConf1, false)
	if err != nil {
		t.Fatalf("failed to init namespace: %v", err)
	}

	nsConf2 := node.NewNSConfig()
	nsConf2.Name = "default-2"
	nsConf2.BaseName = "default"
	nsConf2.EngType = rockredis.EngType
	nsConf2.PartitionNum = 3
	nsConf2.Replicator = 1
	nsConf2.RaftGroupConf.GroupID = 1000
	nsConf2.RaftGroupConf.SeedNodes = append(nsConf.RaftGroupConf.SeedNodes, replica)
	n2, err := kv.InitKVNamespace(1, nsConf2, false)
	if err != nil {
		t.Fatalf("failed to init namespace: %v", err)
	}

	kv.Start()
	time.Sleep(time.Second)
	testNamespaces[nsConf.Name] = n
	testNamespaces[nsConf1.Name] = n1
	testNamespaces[nsConf2.Name] = n2
	return kv, redisportMerge, tmpDir
}

func getMergeTestConn(t *testing.T) *goredis.PoolConn {
	testOnceMerge.Do(func() {
		kvsMerge, redisportMerge, _ = startMergeTestServer(t)
	},
	)
	c := goredis.NewClient("127.0.0.1:"+strconv.Itoa(redisportMerge), "")
	c.SetMaxIdleConns(4)
	conn, err := c.Get()
	if err != nil {
		t.Fatal(err)
	}
	return conn
}

func checkMergeScanValues(t *testing.T, ay interface{}, values ...interface{}) {
	a, err := goredis.Strings(ay, nil)
	if err != nil {
		t.Fatal(err)
	}

	if len(a) != len(values) {
		t.Fatal(fmt.Sprintf("len %d != %d", len(a), len(values)))
	}
	equalCount := 0

	for _, v := range a {
		for j, _ := range values {
			if string(v) == fmt.Sprintf("%v", values[j]) {
				equalCount++
			}
		}
	}

	if equalCount != len(a) {
		t.Fatal("not all equal")
	}
}

func checkMergeAdvanceScan(t *testing.T, c *goredis.PoolConn, tp string) {
	if ay, err := goredis.Values(c.Do("ADVSCAN", "default:testscanmerge:", tp, "count", 5)); err != nil {
		t.Fatal(err)
	} else if len(ay) != 2 {
		t.Fatal(len(ay))
	} else if n := ay[0].([]byte); string(n) != "MDpNVEU9OzE6TVE9PTsyOk1BPT07" &&
		string(n) != "MDpNVEU9OzI6TUE9PTsxOk1RPT07" &&
		string(n) != "MTpNUT09OzA6TVRFPTsyOk1BPT07" &&
		string(n) != "MTpNUT09OzI6TUE9PTswOk1URT07" &&
		string(n) != "MjpNQT09OzA6TVRFPTsxOk1RPT07" &&
		string(n) != "MjpNQT09OzE6TVE9PTswOk1URT07" {
		t.Fatal(string(n))
	} else {
		checkMergeScanValues(t, ay[1], "0", "1", "11")
	}

	if ay, err := goredis.Values(c.Do("ADVSCAN", "default:testscanmerge:MDpNVEU9OzE6TVE9PTsyOk1BPT07", tp, "count", 6)); err != nil {
		t.Fatal(err)
	} else if len(ay) != 2 {
		t.Fatal(len(ay))
	} else if n := ay[0].([]byte); string(n) != "MDpOUT09OzE6TVRjPTsyOk1UST07" &&
		string(n) != "MDpOUT09OzI6TVRJPTsxOk1UYz07" &&
		string(n) != "MTpNVGM9OzA6TlE9PTsyOk1UST07" &&
		string(n) != "MTpNVGM9OzI6TVRJPTswOk5RPT07" &&
		string(n) != "MjpNVEk9OzA6TlE9PTsxOk1UYz07" &&
		string(n) != "MjpNVEk9OzE6TVRjPTswOk5RPT07" {
		t.Fatal(string(n))
	} else {
		checkMergeScanValues(t, ay[1], "5", "16", "17", "10", "12", "18")
	}

	if ay, err := goredis.Values(c.Do("ADVSCAN", "default:testscanmerge:MDpOUT09OzE6TVRjPTsyOk1UST07", tp, "count", 8)); err != nil {
		t.Fatal(err)
	} else if len(ay) != 2 {
		t.Fatal(len(ay))
	} else if n := ay[0].([]byte); string(n) != "MTpPUT09OzI6TVRRPTs=" &&
		string(n) != "MjpNVFE9OzE6T1E9PTs=" {
		t.Fatal(string(n))
	} else {
		if len(ay[1].([]interface{})) != 0 {
			checkMergeScanValues(t, ay[1], "3", "9", "13", "14")
		}
	}

	if ay, err := goredis.Values(c.Do("ADVSCAN", "default:testscanmerge:MTpPUT09OzI6TVRRPTs=", tp, "count", 5)); err != nil {
		t.Fatal(err)
	} else if len(ay) != 2 {
		t.Fatal(len(ay))
	} else if n := ay[0].([]byte); string(n) != "MjpNVGs9Ow==" {
		t.Fatal(string(n))
	} else {
		if len(ay[1].([]interface{})) != 0 {
			checkMergeScanValues(t, ay[1], "15", "19")
		}
	}

	if ay, err := goredis.Values(c.Do("ADVSCAN", "default:testscanmerge:MjpNVGs9Ow==", tp, "count", 8)); err != nil {
		t.Fatal(err)
	} else if len(ay) != 2 {
		t.Fatal(len(ay))
	} else if n := ay[0].([]byte); string(n) != "" &&
		string(n) != "" {
		t.Fatal(string(n))
	} else {
		if len(ay[1].([]interface{})) != 0 {
			checkMergeScanValues(t, ay[1], "2", "4", "6", "7", "8")
		}
	}

	if _, err := goredis.Values(c.Do("ADVSCAN", "default::MDpkR1Z6ZEhOallXNDZOQT09OzI6ZEdWemRITmpZVzQ2T1E9PTs=", tp, "count", 8)); err == nil {
		t.Fatal("want err, get nil ")
	}

	if ay, err := goredis.Values(c.Do("ADVSCAN", "default:testscan1:MDpkR1Z6ZEhOallDZOQT09OzI6ZEdWemRITmpZVzQ2T1E9PTs=", tp, "count", 8)); err == nil {
		fmt.Println(string(ay[0].([]byte)))
		t.Fatal("want err, get nil ")
	}

	if _, err := goredis.Values(c.Do("ADVSCAN", "default:testscanmerge:dGVzdHNjYW46NA==", tp, "count", 0)); err == nil {
		t.Fatal("want err, get nil")
	}
}

func TestMergeScan(t *testing.T) {
	c := getMergeTestConn(t)
	defer c.Close()

	testMergekvsMergeScan(t, c)
	testMergeHashKeyScan(t, c)
	testMergeListKeyScan(t, c)
	testMergeZSetKeyScan(t, c)
	testMergeSetKeyScan(t, c)
}

func testMergekvsMergeScan(t *testing.T, c *goredis.PoolConn) {
	for i := 0; i < 20; i++ {
		value := fmt.Sprintf("value_%d", i)
		if _, err := c.Do("set", "default:testscanmerge:"+fmt.Sprintf("%d", i), []byte(value)); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 20; i++ {
		value := fmt.Sprintf("value_%d", i)
		if _, err := c.Do("set", "default:testscanmerge1:"+fmt.Sprintf("%d", i), []byte(value)); err != nil {
			t.Fatal(err)
		}
	}
	checkMergeAdvanceScan(t, c, "KV")
}

func testMergeHashKeyScan(t *testing.T, c *goredis.PoolConn) {
	for i := 0; i < 20; i++ {
		if _, err := c.Do("hset", "default:testscanmerge:"+fmt.Sprintf("%d", i), fmt.Sprintf("%d", i), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 20; i++ {
		if _, err := c.Do("hset", "default:testscanmerge1:"+fmt.Sprintf("%d", i), fmt.Sprintf("%d", i), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}
	checkMergeAdvanceScan(t, c, "HASH")
}

func testMergeListKeyScan(t *testing.T, c *goredis.PoolConn) {
	for i := 0; i < 20; i++ {
		if _, err := c.Do("lpush", "default:testscanmerge:"+fmt.Sprintf("%d", i), fmt.Sprintf("%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 20; i++ {
		if _, err := c.Do("lpush", "default:testscanmerge1:"+fmt.Sprintf("%d", i), fmt.Sprintf("%d", i)); err != nil {
			t.Fatal(err)
		}
	}

	checkMergeAdvanceScan(t, c, "LIST")
}

func testMergeZSetKeyScan(t *testing.T, c *goredis.PoolConn) {
	for i := 0; i < 20; i++ {
		if _, err := c.Do("zadd", "default:testscanmerge:"+fmt.Sprintf("%d", i), i, []byte("value")); err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < 20; i++ {
		if _, err := c.Do("zadd", "default:testscanmerge1:"+fmt.Sprintf("%d", i), i, []byte("value")); err != nil {
			t.Fatal(err)
		}
	}

	checkMergeAdvanceScan(t, c, "ZSET")
}

func testMergeSetKeyScan(t *testing.T, c *goredis.PoolConn) {
	for i := 0; i < 20; i++ {
		if _, err := c.Do("sadd", "default:testscanmerge:"+fmt.Sprintf("%d", i), fmt.Sprintf("%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 20; i++ {
		if _, err := c.Do("sadd", "default:testscanmerge1:"+fmt.Sprintf("%d", i), fmt.Sprintf("%d", i)); err != nil {
			t.Fatal(err)
		}
	}

	checkMergeAdvanceScan(t, c, "SET")
}

func TestKVMergeScanCrossTable(t *testing.T) {

	c := getMergeTestConn(t)
	defer c.Close()

	for i := 0; i < 20; i++ {
		if _, err := c.Do("set", "default:testscanmerge:"+fmt.Sprintf("%d", i), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < 20; i++ {
		if _, err := c.Do("set", "default:testscanmerge1:"+fmt.Sprintf("%d", i), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}
	if ay, err := goredis.Values(c.Do("ADVSCAN", "default:testscanmerge1:", "KV", "COUNT", 1000)); err != nil {
		t.Fatal(err)
	} else if len(ay) != 2 {
		t.Fatal(len(ay))
	} else {
		a, err := goredis.Strings(ay[1], nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(a) != 20 {
			t.Fatal("want 20 get ", len(a))
		}
	}
}

func TestStrHindexMergeSearch(t *testing.T) {
	c := getMergeTestConn(t)
	defer c.Close()

	ns := "default"
	table := "test_strhashindex"
	indexField := "test_f"
	sc := &node.SchemaChange{
		Type:       node.SchemaChangeAddHsetIndex,
		Table:      table,
		SchemaData: nil,
	}
	hindex := &common.HsetIndexSchema{
		Name:       "strhindex_test",
		IndexField: indexField,
		ValueType:  common.StringV,
		State:      common.InitIndex,
	}
	sc.SchemaData, _ = json.Marshal(hindex)
	for _, nsNode := range testNamespaces {
		nsNode.Node.ProposeChangeTableSchema(table, sc)
	}
	time.Sleep(time.Second)

	sc.Type = node.SchemaChangeUpdateHsetIndex
	hindex.State = common.BuildingIndex
	sc.SchemaData, _ = json.Marshal(hindex)
	for _, nsNode := range testNamespaces {
		nsNode.Node.ProposeChangeTableSchema(table, sc)
	}

	time.Sleep(time.Second)

	hindex.State = common.ReadyIndex
	sc.SchemaData, _ = json.Marshal(hindex)
	for _, nsNode := range testNamespaces {
		nsNode.Node.ProposeChangeTableSchema(table, sc)
	}

	time.Sleep(time.Second)
	for i := 0; i < 20; i++ {
		_, err := c.Do("hset", ns+":"+table+":"+fmt.Sprintf("%d", i), "test_f", []byte(fmt.Sprintf("%d", i)))
		assert.Nil(t, err)
		_, err = c.Do("hset", ns+":"+table+":"+fmt.Sprintf("%d", i), "test_f2", []byte(fmt.Sprintf("%d", i+20)))
		assert.Nil(t, err)
	}

	ay, err := goredis.Values(c.Do("hidx.from", ns+":"+table, "where", "\"test_f=1\""))
	assert.Nil(t, err)
	t.Log(ay)
	assert.Equal(t, 2, len(ay))
	assert.Equal(t, []byte("1"), ay[0].([]byte))
	assert.Equal(t, []byte("1"), ay[1].([]byte))

	ay, err = goredis.Values(c.Do("hidx.from", ns+":"+table, "where", "\"test_f=1\"", "hgetall", "$"))
	assert.Nil(t, err)
	t.Log(ay)
	assert.Equal(t, 3, len(ay))
	assert.Equal(t, []byte("1"), ay[0].([]byte))
	assert.Equal(t, []byte("1"), ay[1].([]byte))
	fvs, err := goredis.Strings(ay[2], nil)
	assert.Nil(t, err)
	assert.Equal(t, 4, len(fvs))
	assert.Equal(t, "test_f", fvs[0])
	assert.Equal(t, "1", fvs[1])
	assert.Equal(t, "test_f2", fvs[2])
	assert.Equal(t, "21", fvs[3])

	ay, err = goredis.Values(c.Do("hidx.from", ns+":"+table, "where", "\"test_f=1\"", "hget", "$", "test_f2"))
	assert.Nil(t, err)
	t.Log(ay)
	assert.Equal(t, 3, len(ay))
	assert.Equal(t, []byte("1"), ay[0].([]byte))
	assert.Equal(t, []byte("1"), ay[1].([]byte))
	assert.Equal(t, []byte("21"), ay[2].([]byte))

	ay, err = goredis.Values(c.Do("hidx.from", ns+":"+table, "where", "\"test_f=1\"", "hmget", "$", "test_f", "test_f2"))
	assert.Nil(t, err)
	t.Log(ay)
	assert.Equal(t, 3, len(ay))
	assert.Equal(t, []byte("1"), ay[0].([]byte))
	assert.Equal(t, []byte("1"), ay[1].([]byte))
	vv, err := goredis.Strings(ay[2], nil)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(vv))
	assert.Equal(t, "1", vv[0])
	assert.Equal(t, "21", vv[1])

	ay, err = goredis.Values(c.Do("hidx.from", ns+":"+table, "where", "\"test_f>1\""))
	assert.Nil(t, err)
	t.Log(ay)
	assert.Equal(t, 18*2, len(ay))
	for _, v := range ay {
		iv, _ := strconv.Atoi(string(v.([]byte)))
		assert.True(t, iv > 1)
	}

	ay, err = goredis.Values(c.Do("hidx.from", ns+":"+table, "where", "\"test_f>2\"", "hget", "$", "test_f2"))
	assert.Nil(t, err)
	t.Log(ay)
	// for string, the order should be "0, 1, 10, 11, ..., 19, 2, 3, 4....."
	assert.Equal(t, 7*3, len(ay))
	for i := 0; i < len(ay)-1; i = i + 3 {
		iv, _ := strconv.Atoi(string(ay[i].([]byte)))
		assert.True(t, iv > 2)
		iv, _ = strconv.Atoi(string(ay[i+1].([]byte)))
		assert.True(t, iv > 2)
		f2iv, _ := strconv.Atoi(string(ay[i+2].([]byte)))
		assert.Equal(t, iv+20, f2iv)
	}

	ay, err = goredis.Values(c.Do("hidx.from", ns+":"+table, "where", "\"test_f>1 and test_f<11\""))
	assert.Nil(t, err)
	t.Log(ay)
	assert.Equal(t, 1*2, len(ay))
	assert.Equal(t, "10", string(ay[0].([]byte)))
	assert.Equal(t, "10", string(ay[1].([]byte)))
}

func TestIntHindexMergeSearch(t *testing.T) {
	c := getMergeTestConn(t)
	defer c.Close()

	ns := "default"
	table := "test_inthashindex"
	indexField := "test_f"
	sc := &node.SchemaChange{
		Type:       node.SchemaChangeAddHsetIndex,
		Table:      table,
		SchemaData: nil,
	}
	hindex := &common.HsetIndexSchema{
		Name:       "inthindex_test",
		IndexField: indexField,
		ValueType:  common.Int32V,
		State:      common.InitIndex,
	}
	sc.SchemaData, _ = json.Marshal(hindex)
	for _, nsNode := range testNamespaces {
		nsNode.Node.ProposeChangeTableSchema(table, sc)
	}
	time.Sleep(time.Second)

	sc.Type = node.SchemaChangeUpdateHsetIndex
	hindex.State = common.BuildingIndex
	sc.SchemaData, _ = json.Marshal(hindex)
	for _, nsNode := range testNamespaces {
		nsNode.Node.ProposeChangeTableSchema(table, sc)
	}

	time.Sleep(time.Second)

	hindex.State = common.ReadyIndex
	sc.SchemaData, _ = json.Marshal(hindex)
	for _, nsNode := range testNamespaces {
		nsNode.Node.ProposeChangeTableSchema(table, sc)
	}

	time.Sleep(time.Second)
	for i := 0; i < 20; i++ {
		_, err := c.Do("hset", ns+":"+table+":"+fmt.Sprintf("%d", i), "test_f", []byte(fmt.Sprintf("%d", i)))
		assert.Nil(t, err)
		_, err = c.Do("hset", ns+":"+table+":"+fmt.Sprintf("%d", i), "test_f2", []byte(fmt.Sprintf("%d", i+20)))
		assert.Nil(t, err)
	}

	ay, err := goredis.Values(c.Do("hidx.from", ns+":"+table, "where", "\"test_f=1\""))
	assert.Nil(t, err)
	t.Log(ay)
	assert.Equal(t, 2, len(ay))
	assert.Equal(t, []byte("1"), ay[0].([]byte))
	assert.Equal(t, int64(1), ay[1].(int64))

	ay, err = goredis.Values(c.Do("hidx.from", ns+":"+table, "where", "\"test_f=1\"", "hgetall", "$"))
	assert.Nil(t, err)
	t.Log(ay)
	assert.Equal(t, 3, len(ay))
	assert.Equal(t, []byte("1"), ay[0].([]byte))
	assert.Equal(t, int64(1), ay[1].(int64))
	fvs, err := goredis.Strings(ay[2], nil)
	assert.Nil(t, err)
	assert.Equal(t, 4, len(fvs))
	assert.Equal(t, "test_f", fvs[0])
	assert.Equal(t, "1", fvs[1])
	assert.Equal(t, "test_f2", fvs[2])
	assert.Equal(t, "21", fvs[3])

	ay, err = goredis.Values(c.Do("hidx.from", ns+":"+table, "where", "\"test_f=1\"", "hget", "$", "test_f2"))
	assert.Nil(t, err)
	t.Log(ay)
	assert.Equal(t, 3, len(ay))
	assert.Equal(t, []byte("1"), ay[0].([]byte))
	assert.Equal(t, int64(1), ay[1].(int64))
	assert.Equal(t, []byte("21"), ay[2].([]byte))

	ay, err = goredis.Values(c.Do("hidx.from", ns+":"+table, "where", "\"test_f=1\"", "hmget", "$", "test_f", "test_f2"))
	assert.Nil(t, err)
	t.Log(ay)
	assert.Equal(t, 3, len(ay))
	assert.Equal(t, []byte("1"), ay[0].([]byte))
	assert.Equal(t, int64(1), ay[1].(int64))
	vv, err := goredis.Strings(ay[2], nil)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(vv))
	assert.Equal(t, "1", vv[0])
	assert.Equal(t, "21", vv[1])

	ay, err = goredis.Values(c.Do("hidx.from", ns+":"+table, "where", "\"test_f>1\""))
	assert.Nil(t, err)
	t.Log(ay)
	assert.Equal(t, 18*2, len(ay))
	for i := 0; i < len(ay)-1; i = i + 2 {
		iv, _ := strconv.Atoi(string(ay[i].([]byte)))
		assert.True(t, iv > 1)
		nv, _ := ay[i+1].(int64)
		assert.True(t, nv > 1)
	}

	ay, err = goredis.Values(c.Do("hidx.from", ns+":"+table, "where", "\"test_f>1\"", "hget", "$", "test_f2"))
	assert.Nil(t, err)
	t.Log(ay)
	assert.Equal(t, 18*3, len(ay))
	for i := 0; i < len(ay)-1; i = i + 3 {
		iv, _ := strconv.Atoi(string(ay[i].([]byte)))
		assert.True(t, iv > 1)
		nv, _ := ay[i+1].(int64)
		assert.True(t, nv > 1)
		f2iv, _ := strconv.Atoi(string(ay[i+2].([]byte)))
		assert.Equal(t, iv+20, f2iv)
	}

	ay, err = goredis.Values(c.Do("hidx.from", ns+":"+table, "where", "\"test_f>1 and test_f<10\""))
	assert.Nil(t, err)
	t.Log(ay)
	assert.Equal(t, 8*2, len(ay))
	for i := 0; i < len(ay)-1; i = i + 2 {
		iv, _ := strconv.Atoi(string(ay[i].([]byte)))
		assert.True(t, iv > 1)
		assert.True(t, iv < 10)
		nv, _ := ay[i+1].(int64)
		assert.True(t, nv > 1)
		assert.True(t, nv < 10)
	}
}
