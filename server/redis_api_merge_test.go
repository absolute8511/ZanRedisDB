package server

import (
	"bytes"
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
)

var testOnceMerge sync.Once
var kvsMerge *Server
var redisportMerge int

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
	_, err = kv.InitKVNamespace(1, nsConf, false)
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
	_, err = kv.InitKVNamespace(1, nsConf1, false)
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
	_, err = kv.InitKVNamespace(1, nsConf2, false)
	if err != nil {
		t.Fatalf("failed to init namespace: %v", err)
	}

	kv.Start()
	time.Sleep(time.Second)
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

func checkKVFullScanValues(t *testing.T, ay interface{}, values map[string]string) {
	a, err := goredis.MultiBulk(ay, nil)
	if err != nil {
		t.Fatal(err)
	}

	var equalCount, totalCount int

	for idx, _ := range a {
		item := a[idx].([]interface{})
		if len(item) != 2 {
			t.Fatal("item length is not 2. len:", len(item))
		}
		totalCount++
		key := item[0].([]byte)
		if val, ok := values[string(key)]; ok {
			value := item[1].([]byte)
			if string(value) == val {
				equalCount++
			} else {
				t.Fatal("value not equal, key:", string(key), "want value:", val, ", get ", string(value))
			}
		} else {
			t.Fatal("can not find key:", string(key))
		}
	}

	if equalCount != totalCount {
		t.Fatal("equal count not equal")
	}
}

func checkHashFullScanValues(t *testing.T, ay interface{}, values map[string]map[string]string) {
	a, err := goredis.MultiBulk(ay, nil)
	if err != nil {
		t.Fatal(err)
	}

	var equalCount, totalCount int

	for idx, _ := range a {
		item := a[idx].([]interface{})
		length := len(item)
		totalCount += length - 1
		key := item[0].([]byte)
		if val, ok := values[string(key)]; ok {
			for i := 1; i < length; i++ {
				fv := item[i].([]interface{})
				field := fv[0].([]byte)
				if v, ok1 := val[string(field)]; ok1 {
					value := fv[1].([]byte)
					if string(value) == v {
						equalCount++
					} else {
						t.Fatal("value not equal, key:", string(key), "; field:", string(field))
					}

				} else {
					t.Fatal("can not find field:", string(field), ";key:", string(key))
				}
			}
		} else {
			t.Fatal("can not find key:", string(key))
		}
	}
	if equalCount != totalCount {
		t.Fatal("equal count not equal")
	}

}

func checkListOrSetFullScanValues(t *testing.T, ay interface{}, values map[string][]string) {
	a, err := goredis.MultiBulk(ay, nil)
	if err != nil {
		t.Fatal(err)
	}

	var equalCount, totalCount int

	for idx, _ := range a {
		item := a[idx].([]interface{})
		if len(item) < 2 {
			t.Fatal("item length is less than 2. len:", len(item))
		}
		itemLen := len(item)
		totalCount += itemLen - 1
		key := item[0].([]byte)
		if val, ok := values[string(key)]; ok {
			for i := 1; i < itemLen; i++ {
				for _, v := range val {
					value := item[i].([]byte)
					splits := bytes.Split(value, []byte("_"))
					if len(splits) != 3 {
						t.Fatal("value format error. value:", string(value))
					}
					if string(value) == v {
						equalCount++
					}
				}
			}
		} else {
			t.Fatal("can not find key:", string(key))
		}
	}

	if equalCount != totalCount {
		t.Fatal("equal count not equal")
	}

}

func checkZSetFullScanValues(t *testing.T, ay interface{}, values map[string]map[string]string) {
	a, err := goredis.MultiBulk(ay, nil)
	if err != nil {
		t.Fatal(err)
	}

	var equalCount, totalCount int

	for idx, _ := range a {
		item := a[idx].([]interface{})
		length := len(item)
		totalCount += length - 1
		key := item[0].([]byte)
		if val, ok := values[string(key)]; ok {
			for i := 1; i < length; i++ {
				p := item[i].([]interface{})
				zvalue := p[0].([]byte)
				if v, ok1 := val[string(zvalue)]; ok1 {
					zscore := p[1].([]byte)
					if string(zscore) == v {
						equalCount++
					} else {
						t.Fatal("score not equal, key:", string(key), "; member:", string(zvalue))
					}
				} else {
					t.Fatal("can not find member:", string(zvalue), " ;key:", string(key))
				}
			}
		} else {
			t.Fatal("can not find key:", string(key))
		}
	}

	if equalCount != totalCount {
		t.Fatal("equal count not equal", equalCount, len(values))
	}

}
