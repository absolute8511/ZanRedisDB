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
	} else if n := ay[0].([]byte); string(n) != "MDpkR1Z6ZEhOallXNXRaWEpuWlRveE1RPT07MTpkR1Z6ZEhOallXNXRaWEpuWlRveDsyOmRHVnpkSE5qWVc1dFpYSm5aVG93Ow==" &&
		string(n) != "MDpkR1Z6ZEhOallXNXRaWEpuWlRveE1RPT07MjpkR1Z6ZEhOallXNXRaWEpuWlRvdzsxOmRHVnpkSE5qWVc1dFpYSm5aVG94Ow==" &&
		string(n) != "MTpkR1Z6ZEhOallXNXRaWEpuWlRveDswOmRHVnpkSE5qWVc1dFpYSm5aVG94TVE9PTsyOmRHVnpkSE5qWVc1dFpYSm5aVG93Ow==" &&
		string(n) != "MTpkR1Z6ZEhOallXNXRaWEpuWlRveDsyOmRHVnpkSE5qWVc1dFpYSm5aVG93OzA6ZEdWemRITmpZVzV0WlhKblpUb3hNUT09Ow==" &&
		string(n) != "MjpkR1Z6ZEhOallXNXRaWEpuWlRvdzswOmRHVnpkSE5qWVc1dFpYSm5aVG94TVE9PTsxOmRHVnpkSE5qWVc1dFpYSm5aVG94Ow==" &&
		string(n) != "MjpkR1Z6ZEhOallXNXRaWEpuWlRvdzsxOmRHVnpkSE5qWVc1dFpYSm5aVG94OzA6ZEdWemRITmpZVzV0WlhKblpUb3hNUT09Ow==" {
		t.Fatal(string(n))
	} else {
		checkMergeScanValues(t, ay[1], "testscanmerge:0", "testscanmerge:1", "testscanmerge:11")
	}

	if ay, err := goredis.Values(c.Do("ADVSCAN", "default:testscanmerge:MDpkR1Z6ZEhOallXNXRaWEpuWlRveE1RPT07MTpkR1Z6ZEhOallXNXRaWEpuWlRveDsyOmRHVnpkSE5qWVc1dFpYSm5aVG93Ow==", tp, "count", 6)); err != nil {
		t.Fatal(err)
	} else if len(ay) != 2 {
		t.Fatal(len(ay))
	} else if n := ay[0].([]byte); string(n) != "MDpkR1Z6ZEhOallXNXRaWEpuWlRvMTsxOmRHVnpkSE5qWVc1dFpYSm5aVG94Tnc9PTsyOmRHVnpkSE5qWVc1dFpYSm5aVG94TWc9PTs=" &&
		string(n) != "MDpkR1Z6ZEhOallXNXRaWEpuWlRvMTsyOmRHVnpkSE5qWVc1dFpYSm5aVG94TWc9PTsxOmRHVnpkSE5qWVc1dFpYSm5aVG94Tnc9PTs=" &&
		string(n) != "MTpkR1Z6ZEhOallXNXRaWEpuWlRveE53PT07MDpkR1Z6ZEhOallXNXRaWEpuWlRvMTsyOmRHVnpkSE5qWVc1dFpYSm5aVG94TWc9PTs=" &&
		string(n) != "MTpkR1Z6ZEhOallXNXRaWEpuWlRveE53PT07MjpkR1Z6ZEhOallXNXRaWEpuWlRveE1nPT07MDpkR1Z6ZEhOallXNXRaWEpuWlRvMTs=" &&
		string(n) != "MjpkR1Z6ZEhOallXNXRaWEpuWlRveE1nPT07MDpkR1Z6ZEhOallXNXRaWEpuWlRvMTsxOmRHVnpkSE5qWVc1dFpYSm5aVG94Tnc9PTs=" &&
		string(n) != "MjpkR1Z6ZEhOallXNXRaWEpuWlRveE1nPT07MTpkR1Z6ZEhOallXNXRaWEpuWlRveE53PT07MDpkR1Z6ZEhOallXNXRaWEpuWlRvMTs=" {
		t.Fatal(string(n))
	} else {
		checkMergeScanValues(t, ay[1], "testscanmerge:5", "testscanmerge:16", "testscanmerge:17", "testscanmerge:10", "testscanmerge:12", "testscanmerge:18")
	}

	if ay, err := goredis.Values(c.Do("ADVSCAN", "default:testscanmerge:MDpkR1Z6ZEhOallXNXRaWEpuWlRvMTsxOmRHVnpkSE5qWVc1dFpYSm5aVG94Tnc9PTsyOmRHVnpkSE5qWVc1dFpYSm5aVG94TWc9PTs=", tp, "count", 8)); err != nil {
		t.Fatal(err)
	} else if len(ay) != 2 {
		t.Fatal(len(ay))
	} else if n := ay[0].([]byte); string(n) != "MTpkR1Z6ZEhOallXNXRaWEpuWlRvNTsyOmRHVnpkSE5qWVc1dFpYSm5aVG94TkE9PTs=" &&
		string(n) != "MjpkR1Z6ZEhOallXNXRaWEpuWlRveE5BPT07MTpkR1Z6ZEhOallXNXRaWEpuWlRvNTs=" {
		t.Fatal(string(n))
	} else {
		if len(ay[1].([]interface{})) != 0 {
			checkMergeScanValues(t, ay[1], "testscanmerge:3", "testscanmerge:9", "testscanmerge:13", "testscanmerge:14")
		}
	}

	if ay, err := goredis.Values(c.Do("ADVSCAN", "default:testscanmerge:MTpkR1Z6ZEhOallXNXRaWEpuWlRvNTsyOmRHVnpkSE5qWVc1dFpYSm5aVG94TkE9PTs=", tp, "count", 5)); err != nil {
		t.Fatal(err)
	} else if len(ay) != 2 {
		t.Fatal(len(ay))
	} else if n := ay[0].([]byte); string(n) != "MjpkR1Z6ZEhOallXNXRaWEpuWlRveE9RPT07" {
		t.Fatal(string(n))
	} else {
		if len(ay[1].([]interface{})) != 0 {
			checkMergeScanValues(t, ay[1], "testscanmerge:15", "testscanmerge:19")
		}
	}

	if ay, err := goredis.Values(c.Do("ADVSCAN", "default:testscanmerge:MjpkR1Z6ZEhOallXNXRaWEpuWlRveE9RPT07", tp, "count", 8)); err != nil {
		t.Fatal(err)
	} else if len(ay) != 2 {
		t.Fatal(len(ay))
	} else if n := ay[0].([]byte); string(n) != "" &&
		string(n) != "" {
		t.Fatal(string(n))
	} else {
		if len(ay[1].([]interface{})) != 0 {
			checkMergeScanValues(t, ay[1], "testscanmerge:2", "testscanmerge:4", "testscanmerge:6", "testscanmerge:7", "testscanmerge:8")
		}
	}

	if _, err := goredis.Values(c.Do("ADVSCAN", "default::MDpkR1Z6ZEhOallXNDZOQT09OzI6ZEdWemRITmpZVzQ2T1E9PTs=", tp, "count", 8)); err == nil {
		t.Fatal("want err, get nil ")
	}

	if _, err := goredis.Values(c.Do("ADVSCAN", "default:testscan1:MDpkR1Z6ZEhOallXNDZOQT09OzI6ZEdWemRITmpZVzQ2T1E9PTs=", tp, "count", 8)); err == nil {
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

func checkKVFullScan(t *testing.T, c *goredis.PoolConn) {
	if ay, err := goredis.Values(c.Do("FULLSCAN", "default:testscanmerge:", "KV", "count", 5)); err != nil {
		t.Fatal(err)
	} else if len(ay) != 2 {
		t.Fatal(len(ay))
	} else if n := ay[0].([]byte); string(n) != "MDpUVlJGUFRvPTsxOlRWRTlQVG89OzI6VFVFOVBUbz07" &&
		string(n) != "MDpUVlJGUFRvPTsyOlRVRTlQVG89OzE6VFZFOVBUbz07" &&
		string(n) != "MTpUVkU5UFRvPTswOlRWUkZQVG89OzI6VFVFOVBUbz07" &&
		string(n) != "MTpUVkU5UFRvPTsyOlRVRTlQVG89OzA6VFZSRlBUbz07" &&
		string(n) != "MjpUVUU5UFRvPTswOlRWUkZQVG89OzE6VFZFOVBUbz07" &&
		string(n) != "MjpUVUU5UFRvPTsxOlRWRTlQVG89OzA6VFZSRlBUbz07" {
		t.Fatal(string(n))
	} else {
		values := map[string]string{
			"1":  "value_1",
			"0":  "value_0",
			"11": "value_11",
		}
		checkKVFullScanValues(t, ay[1], values)
	}

	if ay, err := goredis.Values(c.Do("FULLSCAN", "default:testscanmerge:MDpUVlJGUFRvPTsxOlRWRTlQVG89OzI6VFVFOVBUbz07", "KV", "count", 6)); err != nil {
		t.Fatal(err)
	} else if len(ay) != 2 {
		t.Fatal(len(ay))
	} else if n := ay[0].([]byte); string(n) != "MDpUbEU5UFRvPTsxOlRWUmpQVG89OzI6VFZSSlBUbz07" &&
		string(n) != "MDpUbEU5UFRvPTsyOlRWUkpQVG89OzE6VFZSalBUbz07" &&
		string(n) != "MTpUVlJqUFRvPTswOlRsRTlQVG89OzI6VFZSSlBUbz07" &&
		string(n) != "MTpUVlJqUFRvPTsyOlRWUkpQVG89OzA6VGxFOVBUbz07" &&
		string(n) != "MjpUVlJKUFRvPTswOlRsRTlQVG89OzE6VFZSalBUbz07" &&
		string(n) != "MjpUVlJKUFRvPTsxOlRWUmpQVG89OzA6VGxFOVBUbz07" {
		t.Fatal(string(n))
	} else {
		values := map[string]string{
			"18": "value_18",
			"5":  "value_5",
			"16": "value_16",
			"17": "value_17",
			"10": "value_10",
			"12": "value_12",
		}
		checkKVFullScanValues(t, ay[1], values)
	}

	if ay, err := goredis.Values(c.Do("FULLSCAN", "default:testscanmerge:MDpUbEU5UFRvPTsxOlRWUmpQVG89OzI6VFZSSlBUbz07", "KV", "count", 12)); err != nil {
		t.Fatal(err)
	} else if len(ay) != 2 {
		t.Fatal(len(ay))
	} else if n := ay[0].([]byte); string(n) != "MjpUVlJyUFRvPTs=" {
		t.Fatal(string(n))
	} else {
		values := map[string]string{
			"13": "value_13",
			"14": "value_14",
			"15": "value_15",
			"19": "value_19",
			"3":  "value_3",
			"9":  "value_9",
		}
		checkKVFullScanValues(t, ay[1], values)
	}

	if ay, err := goredis.Values(c.Do("FULLSCAN", "default:testscanmerge:MjpUVlJyUFRvPTs=", "KV", "count", 6)); err != nil {
		t.Fatal(err)
	} else if len(ay) != 2 {
		t.Fatal(len(ay))
	} else if n := ay[0].([]byte); string(n) != "" {
		t.Fatal(string(n))
	} else {
		values := map[string]string{
			"2": "value_2",
			"4": "value_4",
			"6": "value_6",
			"7": "value_7",
			"8": "value_8",
		}
		checkKVFullScanValues(t, ay[1], values)
	}

	if _, err := goredis.Values(c.Do("FULLSCAN", "default::MDpNVEU9OzE6TVE9PTsyOk1BPT07", "KV", "count", 8)); err == nil {
		t.Fatal("want err, get nil ")
	}

	if _, err := goredis.Values(c.Do("FULLSCAN", "default:testscanmerge:dGVzdHNjYW46NA==", "KV", "count", 0)); err == nil {
		t.Fatal("want err, get nil")
	}
}

func checkFullScanValues(t *testing.T, ay interface{}, values ...interface{}) {

}

func checkHashFullScan(t *testing.T, c *goredis.PoolConn) {
	if ay, err := goredis.Values(c.Do("FULLSCAN", "default:testscanmerge:", "HASH", "count", 5)); err != nil {
		t.Fatal(err)
	} else if len(ay) != 2 {
		t.Fatal(len(ay))
	} else if n := ay[0].([]byte); string(n) != "MDpUbEU5UFRwTlFUMDk7MTpUVkU5UFRwTlFUMDk7MjpUVUU5UFRwTlFUMDk7" &&
		string(n) != "MDpUbEU5UFRwTlFUMDk7MjpUVUU5UFRwTlFUMDk7MTpUVkU5UFRwTlFUMDk7" &&
		string(n) != "MTpUVkU5UFRwTlFUMDk7MDpUbEU5UFRwTlFUMDk7MjpUVUU5UFRwTlFUMDk7" &&
		string(n) != "MTpUVkU5UFRwTlFUMDk7MjpUVUU5UFRwTlFUMDk7MDpUbEU5UFRwTlFUMDk7" &&
		string(n) != "MjpUVUU5UFRwTlFUMDk7MDpUbEU5UFRwTlFUMDk7MTpUVkU5UFRwTlFUMDk7" &&
		string(n) != "MjpUVUU5UFRwTlFUMDk7MTpUVkU5UFRwTlFUMDk7MDpUbEU5UFRwTlFUMDk7" {
		t.Fatal(string(n))
	} else {
		values := map[string]map[string]string{
			"0": {
				"0": "value_0_0",
			},
			"1": {
				"0": "value_1_0",
			},
			"5": {
				"0": "value_5_0",
			},
		}
		checkHashFullScanValues(t, ay[1], values)
	}

	if ay, err := goredis.Values(c.Do("FULLSCAN", "default:testscanmerge:MDpUbEU5UFRwTlFUMDk7MTpUVkU5UFRwTlFUMDk7MjpUVUU5UFRwTlFUMDk7", "HASH", "count", 6)); err != nil {
		t.Fatal(err)
	} else if len(ay) != 2 {
		t.Fatal(len(ay))
	} else if n := ay[0].([]byte); string(n) != "MDpUbEU5UFRwTlp6MDk7MTpUWGM5UFRwTlFUMDk7MjpUV2M5UFRwTlVUMDk7" &&
		string(n) != "MDpUbEU5UFRwTlp6MDk7MjpUV2M5UFRwTlVUMDk7MTpUWGM5UFRwTlFUMDk7" &&
		string(n) != "MTpUWGM5UFRwTlFUMDk7MDpUbEU5UFRwTlp6MDk7MjpUV2M5UFRwTlVUMDk7" &&
		string(n) != "MTpUWGM5UFRwTlFUMDk7MjpUV2M5UFRwTlVUMDk7MDpUbEU5UFRwTlp6MDk7" &&
		string(n) != "MjpUV2M5UFRwTlVUMDk7MDpUbEU5UFRwTlp6MDk7MTpUWGM5UFRwTlFUMDk7" &&
		string(n) != "MjpUV2M5UFRwTlVUMDk7MTpUWGM5UFRwTlFUMDk7MDpUbEU5UFRwTlp6MDk7" {
		t.Fatal(string(n))
	} else {
		values := map[string]map[string]string{
			"2": {
				"0": "value_2_0",
				"1": "value_2_1",
			},
			"5": {
				"1": "value_5_1",
				"2": "value_5_2",
			},
			"1": {
				"1": "value_1_1",
			},
			"3": {
				"0": "value_3_0",
			},
		}
		checkHashFullScanValues(t, ay[1], values)
	}

	if ay, err := goredis.Values(c.Do("FULLSCAN", "default:testscanmerge:MDpUbEU5UFRwTlp6MDk7MTpUWGM5UFRwTlFUMDk7MjpUV2M5UFRwTlVUMDk7", "HASH", "count", 8)); err != nil {
		t.Fatal(err)
	} else if len(ay) != 2 {
		t.Fatal(len(ay))
	} else if n := ay[0].([]byte); string(n) != "MDpUbEU5UFRwT1FUMDk7MTpUWGM5UFRwTlp6MDk7MjpUa0U5UFRwTlFUMDk7" &&
		string(n) != "MDpUbEU5UFRwT1FUMDk7MjpUa0U5UFRwTlFUMDk7MTpUWGM5UFRwTlp6MDk7" &&
		string(n) != "MTpUWGM5UFRwTlp6MDk7MDpUbEU5UFRwT1FUMDk7MjpUa0U5UFRwTlFUMDk7" &&
		string(n) != "MTpUWGM5UFRwTlp6MDk7MjpUa0U5UFRwTlFUMDk7MDpUbEU5UFRwT1FUMDk7" &&
		string(n) != "MjpUa0U5UFRwTlFUMDk7MDpUbEU5UFRwT1FUMDk7MTpUWGM5UFRwTlp6MDk7" &&
		string(n) != "MjpUa0U5UFRwTlFUMDk7MTpUWGM5UFRwTlp6MDk7MDpUbEU5UFRwT1FUMDk7" {
		t.Fatal(string(n))
	} else {
		values := map[string]map[string]string{
			"3": {
				"1": "value_3_1",
				"2": "value_3_2",
			},
			"5": {
				"3": "value_5_3",
				"4": "value_5_4",
			},
			"2": {
				"2": "value_2_2",
			},
			"4": {
				"0": "value_4_0",
			},
		}
		checkHashFullScanValues(t, ay[1], values)
	}

	if ay, err := goredis.Values(c.Do("FULLSCAN", "default:testscanmerge:MDpUbEU5UFRwT1FUMDk7MTpUWGM5UFRwTlp6MDk7MjpUa0U5UFRwTlFUMDk7", "HASH", "count", 12)); err != nil {
		t.Fatal(err)
	} else if len(ay) != 2 {
		t.Fatal(len(ay))
	} else if n := ay[0].([]byte); string(n) != "MDpUVlJGUFRwTlZFRTk7MTpUMUU5UFRwTlp6MDk7MjpUa0U5UFRwT1FUMDk7" &&
		string(n) != "MDpUVlJGUFRwTlZFRTk7MjpUa0U5UFRwT1FUMDk7MTpUMUU5UFRwTlp6MDk7" &&
		string(n) != "MTpUMUU5UFRwTlp6MDk7MDpUVlJGUFRwTlZFRTk7MjpUa0U5UFRwT1FUMDk7" &&
		string(n) != "MTpUMUU5UFRwTlp6MDk7MjpUa0U5UFRwT1FUMDk7MDpUVlJGUFRwTlZFRTk7" &&
		string(n) != "MjpUa0U5UFRwT1FUMDk7MDpUVlJGUFRwTlZFRTk7MTpUMUU5UFRwTlp6MDk7" &&
		string(n) != "MjpUa0U5UFRwT1FUMDk7MTpUMUU5UFRwTlp6MDk7MDpUVlJGUFRwTlZFRTk7" {
		t.Fatal(string(n))
	} else {
		values := map[string]map[string]string{
			"5": {
				"5": "value_5_5",
			},
			"11": {
				"0":  "value_11_0",
				"1":  "value_11_1",
				"10": "value_11_10",
			},
			"3": {
				"3": "value_3_3",
			},
			"9": {
				"0": "value_9_0",
				"1": "value_9_1",
				"2": "value_9_2",
			},
			"4": {
				"1": "value_4_1",
				"2": "value_4_2",
				"3": "value_4_3",
				"4": "value_4_4",
			},
		}
		checkHashFullScanValues(t, ay[1], values)
	}

	if ay, err := goredis.Values(c.Do("FULLSCAN", "default:testscanmerge:MDpUVlJGUFRwTlZFRTk7MTpUMUU5UFRwTlp6MDk7MjpUa0U5UFRwT1FUMDk7", "HASH", "count", 120)); err != nil {
		t.Fatal(err)
	} else if len(ay) != 2 {
		t.Fatal(len(ay))
	} else if n := ay[0].([]byte); string(n) != "MTpUVlJqUFRwT2R6MDk7MjpUVlJKUFRwTlZFazk7" &&
		string(n) != "MjpUVlJKUFRwTlZFazk7MTpUVlJqUFRwT2R6MDk7" {
		t.Fatal(string(n))
	} else {
		values := map[string]map[string]string{
			"11": {
				"2":  "value_11_2",
				"3":  "value_11_3",
				"4":  "value_11_4",
				"5":  "value_11_5",
				"6":  "value_11_6",
				"7":  "value_11_7",
				"8":  "value_11_8",
				"9":  "value_11_9",
				"11": "value_11_11",
			},
			"18": {
				"0":  "value_18_0",
				"1":  "value_18_1",
				"2":  "value_18_2",
				"3":  "value_18_3",
				"4":  "value_18_4",
				"5":  "value_18_5",
				"6":  "value_18_6",
				"7":  "value_18_7",
				"8":  "value_18_8",
				"9":  "value_18_9",
				"10": "value_18_10",
				"11": "value_18_11",
				"12": "value_18_12",
				"13": "value_18_13",
				"14": "value_18_14",
				"15": "value_18_15",
				"16": "value_18_16",
				"17": "value_18_17",
				"18": "value_18_18",
			},
			"9": {
				"3": "value_9_3",
				"4": "value_9_4",
				"5": "value_9_5",
				"6": "value_9_6",
				"7": "value_9_7",
				"8": "value_9_8",
				"9": "value_9_9",
			},
			"16": {
				"0":  "value_16_0",
				"1":  "value_16_1",
				"2":  "value_16_2",
				"3":  "value_16_3",
				"4":  "value_16_4",
				"5":  "value_16_5",
				"6":  "value_16_6",
				"7":  "value_16_7",
				"8":  "value_16_8",
				"9":  "value_16_9",
				"10": "value_16_10",
				"11": "value_16_11",
				"12": "value_16_12",
				"13": "value_16_13",
				"14": "value_16_14",
				"15": "value_16_15",
				"16": "value_16_16",
			},
			"17": {
				"0":  "value_17_0",
				"1":  "value_17_1",
				"2":  "value_17_2",
				"3":  "value_17_3",
				"4":  "value_17_4",
				"5":  "value_17_5",
				"6":  "value_17_6",
				"7":  "value_17_7",
				"10": "value_17_10",
				"11": "value_17_11",
				"12": "value_17_12",
				"13": "value_17_13",
				"14": "value_17_14",
				"15": "value_17_15",
				"16": "value_17_16",
				"17": "value_17_17",
			},
			"6": {
				"0": "value_6_0",
				"1": "value_6_1",
				"2": "value_6_2",
				"3": "value_6_3",
				"4": "value_6_4",
				"5": "value_6_5",
				"6": "value_6_6",
			},
			"7": {
				"0": "value_7_0",
				"1": "value_7_1",
				"2": "value_7_2",
				"3": "value_7_3",
				"4": "value_7_4",
				"5": "value_7_5",
				"6": "value_7_6",
				"7": "value_7_7",
			},
			"8": {
				"0": "value_8_0",
				"1": "value_8_1",
				"2": "value_8_2",
				"3": "value_8_3",
				"4": "value_8_4",
				"5": "value_8_5",
				"6": "value_8_6",
				"7": "value_8_7",
				"8": "value_8_8",
			},
			"10": {
				"0":  "value_10_0",
				"1":  "value_10_1",
				"2":  "value_10_2",
				"3":  "value_10_3",
				"4":  "value_10_4",
				"5":  "value_10_5",
				"6":  "value_10_6",
				"7":  "value_10_7",
				"8":  "value_10_8",
				"9":  "value_10_9",
				"10": "value_10_10",
			},
			"12": {
				"0":  "value_12_0",
				"1":  "value_12_1",
				"10": "value_12_10",
				"11": "value_12_11",
				"12": "value_12_12",
			},
		}
		checkHashFullScanValues(t, ay[1], values)
	}

	if ay, err := goredis.Values(c.Do("FULLSCAN", "default:testscanmerge:MTpUVlJqUFRwT2R6MDk7MjpUVlJKUFRwTlZFazk7", "HASH", "count", 10)); err != nil {
		t.Fatal(err)
	} else if len(ay) != 2 {
		t.Fatal(len(ay))
	} else if n := ay[0].([]byte); string(n) != "MjpUVlJKUFRwT1p6MDk7" {
		t.Fatal(string(n))
	} else {
		values := map[string]map[string]string{
			"17": {
				"8": "value_17_8",
				"9": "value_17_9",
			},
			"12": {
				"2": "value_12_2",
				"3": "value_12_3",
				"4": "value_12_4",
				"5": "value_12_5",
				"6": "value_12_6",
			},
		}
		checkHashFullScanValues(t, ay[1], values)
	}

	if ay, err := goredis.Values(c.Do("FULLSCAN", "default:testscanmerge:MjpUVlJKUFRwT1p6MDk7", "HASH", "count", 100)); err != nil {
		t.Fatal(err)
	} else if len(ay) != 2 {
		t.Fatal(len(ay))
	} else if n := ay[0].([]byte); string(n) != "" {
		t.Fatal(string(n))
	} else {
		values := map[string]map[string]string{
			"12": {
				"7": "value_12_7",
				"8": "value_12_8",
				"9": "value_12_9",
			},
			"13": {
				"0":  "value_13_0",
				"1":  "value_13_1",
				"2":  "value_13_2",
				"3":  "value_13_3",
				"4":  "value_13_4",
				"5":  "value_13_5",
				"6":  "value_13_6",
				"7":  "value_13_7",
				"8":  "value_13_8",
				"9":  "value_13_9",
				"10": "value_13_10",
				"11": "value_13_11",
				"12": "value_13_12",
				"13": "value_13_13",
			},
			"14": {
				"0":  "value_14_0",
				"1":  "value_14_1",
				"2":  "value_14_2",
				"3":  "value_14_3",
				"4":  "value_14_4",
				"5":  "value_14_5",
				"6":  "value_14_6",
				"7":  "value_14_7",
				"8":  "value_14_8",
				"9":  "value_14_9",
				"10": "value_14_10",
				"11": "value_14_11",
				"12": "value_14_12",
				"13": "value_14_13",
				"14": "value_14_14",
			},
			"15": {
				"0":  "value_15_0",
				"1":  "value_15_1",
				"2":  "value_15_2",
				"3":  "value_15_3",
				"4":  "value_15_4",
				"5":  "value_15_5",
				"6":  "value_15_6",
				"7":  "value_15_7",
				"8":  "value_15_8",
				"9":  "value_15_9",
				"10": "value_15_10",
				"11": "value_15_11",
				"12": "value_15_12",
				"13": "value_15_13",
				"14": "value_15_14",
				"15": "value_15_15",
			},
			"19": {
				"0":  "value_19_0",
				"1":  "value_19_1",
				"2":  "value_19_2",
				"3":  "value_19_3",
				"4":  "value_19_4",
				"5":  "value_19_5",
				"6":  "value_19_6",
				"7":  "value_19_7",
				"8":  "value_19_8",
				"9":  "value_19_9",
				"10": "value_19_10",
				"11": "value_19_11",
				"12": "value_19_12",
				"13": "value_19_13",
				"14": "value_19_14",
				"15": "value_19_15",
				"16": "value_19_16",
				"17": "value_19_17",
				"18": "value_19_18",
				"19": "value_19_19",
			},
		}
		checkHashFullScanValues(t, ay[1], values)
	}

	if _, err := goredis.Values(c.Do("FULLSCAN", "default::MDpkR1Z6ZEhOallXNDZOQT09OzI6ZEdWemRITmpZVzQ2T1E9PTs=", "HASH", "count", 8)); err == nil {
		t.Fatal("want err, get nil ")
	}

	if _, err := goredis.Values(c.Do("FULLSCAN", "default:testscan1:MDpkR1Z6ZEhOallXNDZOQT09OzI6ZEdWemRITmpZVzQ2T1E9PTs=", "HASH", "count", 8)); err == nil {
		t.Fatal("want err, get nil ")
	}

	if _, err := goredis.Values(c.Do("FULLSCAN", "default:testscanmerge:dGVzdHNjYW46NA==", "HASH", "count", 0)); err == nil {
		t.Fatal("want err, get nil")
	}
}

func checkListFullScan(t *testing.T, c *goredis.PoolConn) {
	if ay, err := goredis.Values(c.Do("FULLSCAN", "default:testscanmerge:", "LIST", "count", 5)); err != nil {
		t.Fatal(err)
	} else if len(ay) != 2 {
		t.Fatal(len(ay))
	} else if n := ay[0].([]byte); string(n) != "MDpUbEU5UFRwSUx5OHZMeTh2THk4dmN6MD07MTpUVkU5UFRwSUx5OHZMeTh2THk4dk9EMD07MjpUVUU5UFRwSlFVRkJRVUZCUVVGQlFUMD07" &&
		string(n) != "MDpUbEU5UFRwSUx5OHZMeTh2THk4dmN6MD07MjpUVUU5UFRwSlFVRkJRVUZCUVVGQlFUMD07MTpUVkU5UFRwSUx5OHZMeTh2THk4dk9EMD07" &&
		string(n) != "MTpUVkU5UFRwSUx5OHZMeTh2THk4dk9EMD07MDpUbEU5UFRwSUx5OHZMeTh2THk4dmN6MD07MjpUVUU5UFRwSlFVRkJRVUZCUVVGQlFUMD07" &&
		string(n) != "MTpUVkU5UFRwSUx5OHZMeTh2THk4dk9EMD07MjpUVUU5UFRwSlFVRkJRVUZCUVVGQlFUMD07MDpUbEU5UFRwSUx5OHZMeTh2THk4dmN6MD07" &&
		string(n) != "MjpUVUU5UFRwSlFVRkJRVUZCUVVGQlFUMD07MDpUbEU5UFRwSUx5OHZMeTh2THk4dmN6MD07MTpUVkU5UFRwSUx5OHZMeTh2THk4dk9EMD07" &&
		string(n) != "MjpUVUU5UFRwSlFVRkJRVUZCUVVGQlFUMD07MTpUVkU5UFRwSUx5OHZMeTh2THk4dk9EMD07MDpUbEU5UFRwSUx5OHZMeTh2THk4dmN6MD07" {
		t.Fatal(string(n))
	} else {
		values := map[string][]string{
			"0": {"value_0_0"},
			"1": {"value_1_1"},
			"5": {"value_5_5"},
		}
		checkListOrSetFullScanValues(t, ay[1], values)
	}

	if ay, err := goredis.Values(c.Do("FULLSCAN", "default:testscanmerge:MDpUbEU5UFRwSUx5OHZMeTh2THk4dmN6MD07MTpUVkU5UFRwSUx5OHZMeTh2THk4dk9EMD07MjpUVUU5UFRwSlFVRkJRVUZCUVVGQlFUMD07", "LIST", "count", 6)); err != nil {
		t.Fatal(err)
	} else if len(ay) != 2 {
		t.Fatal(len(ay))
	} else if n := ay[0].([]byte); string(n) != "MDpUbEU5UFRwSUx5OHZMeTh2THk4dk1EMD07MTpUWGM5UFRwSUx5OHZMeTh2THk4dk1EMD07MjpUV2M5UFRwSUx5OHZMeTh2THk4dk9EMD07" &&
		string(n) != "MDpUbEU5UFRwSUx5OHZMeTh2THk4dk1EMD07MjpUV2M5UFRwSUx5OHZMeTh2THk4dk9EMD07MTpUWGM5UFRwSUx5OHZMeTh2THk4dk1EMD07" &&
		string(n) != "MTpUWGM5UFRwSUx5OHZMeTh2THk4dk1EMD07MDpUbEU5UFRwSUx5OHZMeTh2THk4dk1EMD07MjpUV2M5UFRwSUx5OHZMeTh2THk4dk9EMD07" &&
		string(n) != "MTpUWGM5UFRwSUx5OHZMeTh2THk4dk1EMD07MjpUV2M5UFRwSUx5OHZMeTh2THk4dk9EMD07MDpUbEU5UFRwSUx5OHZMeTh2THk4dk1EMD07" &&
		string(n) != "MjpUV2M5UFRwSUx5OHZMeTh2THk4dk9EMD07MDpUbEU5UFRwSUx5OHZMeTh2THk4dk1EMD07MTpUWGM5UFRwSUx5OHZMeTh2THk4dk1EMD07" &&
		string(n) != "MjpUV2M5UFRwSUx5OHZMeTh2THk4dk9EMD07MTpUWGM5UFRwSUx5OHZMeTh2THk4dk1EMD07MDpUbEU5UFRwSUx5OHZMeTh2THk4dk1EMD07" {
		t.Fatal(string(n))
	} else {
		values := map[string][]string{
			"2": {"value_2_2", "value_2_1"},
			"5": {"value_5_4", "value_5_3"},
			"1": {"value_1_0"},
			"3": {"value_3_3"},
		}
		checkListOrSetFullScanValues(t, ay[1], values)
	}

	if ay, err := goredis.Values(c.Do("FULLSCAN", "default:testscanmerge:MDpUbEU5UFRwSUx5OHZMeTh2THk4dk1EMD07MTpUWGM5UFRwSUx5OHZMeTh2THk4dk1EMD07MjpUV2M5UFRwSUx5OHZMeTh2THk4dk9EMD07", "LIST", "count", 8)); err != nil {
		t.Fatal(err)
	} else if len(ay) != 2 {
		t.Fatal(len(ay))
	} else if n := ay[0].([]byte); string(n) != "MDpUbEU5UFRwSUx5OHZMeTh2THk4dk9EMD07MTpUWGM5UFRwSUx5OHZMeTh2THk4dk9EMD07MjpUa0U5UFRwSUx5OHZMeTh2THk4dmR6MD07" &&
		string(n) != "MDpUbEU5UFRwSUx5OHZMeTh2THk4dk9EMD07MjpUa0U5UFRwSUx5OHZMeTh2THk4dmR6MD07MTpUWGM5UFRwSUx5OHZMeTh2THk4dk9EMD07" &&
		string(n) != "MTpUWGM5UFRwSUx5OHZMeTh2THk4dk9EMD07MDpUbEU5UFRwSUx5OHZMeTh2THk4dk9EMD07MjpUa0U5UFRwSUx5OHZMeTh2THk4dmR6MD07" &&
		string(n) != "MTpUWGM5UFRwSUx5OHZMeTh2THk4dk9EMD07MjpUa0U5UFRwSUx5OHZMeTh2THk4dmR6MD07MDpUbEU5UFRwSUx5OHZMeTh2THk4dk9EMD07" &&
		string(n) != "MjpUa0U5UFRwSUx5OHZMeTh2THk4dmR6MD07MDpUbEU5UFRwSUx5OHZMeTh2THk4dk9EMD07MTpUWGM5UFRwSUx5OHZMeTh2THk4dk9EMD07" &&
		string(n) != "MjpUa0U5UFRwSUx5OHZMeTh2THk4dmR6MD07MTpUWGM5UFRwSUx5OHZMeTh2THk4dk9EMD07MDpUbEU5UFRwSUx5OHZMeTh2THk4dk9EMD07" {
		t.Fatal(string(n))
	} else {
		values := map[string][]string{
			"2": {"value_2_0"},
			"5": {"value_5_2", "value_5_1"},
			"4": {"value_4_4"},
			"3": {"value_3_2", "value_3_1"},
		}
		checkListOrSetFullScanValues(t, ay[1], values)
	}

	if ay, err := goredis.Values(c.Do("FULLSCAN", "default:testscanmerge:MDpUbEU5UFRwSUx5OHZMeTh2THk4dk9EMD07MTpUWGM5UFRwSUx5OHZMeTh2THk4dk9EMD07MjpUa0U5UFRwSUx5OHZMeTh2THk4dmR6MD07", "LIST", "count", 12)); err != nil {
		t.Fatal(err)
	} else if len(ay) != 2 {
		t.Fatal(len(ay))
	} else if n := ay[0].([]byte); string(n) != "" &&
		string(n) != "MDpUVlJGUFRwSUx5OHZMeTh2THk4dll6MD07MTpUMUU5UFRwSUx5OHZMeTh2THk4dmF6MD07MjpUa0U5UFRwSlFVRkJRVUZCUVVGQlFUMD07" &&
		string(n) != "MDpUVlJGUFRwSUx5OHZMeTh2THk4dll6MD07MjpUa0U5UFRwSlFVRkJRVUZCUVVGQlFUMD07MTpUMUU5UFRwSUx5OHZMeTh2THk4dmF6MD07" &&
		string(n) != "MTpUMUU5UFRwSUx5OHZMeTh2THk4dmF6MD07MDpUVlJGUFRwSUx5OHZMeTh2THk4dll6MD07MjpUa0U5UFRwSlFVRkJRVUZCUVVGQlFUMD07" &&
		string(n) != "MTpUMUU5UFRwSUx5OHZMeTh2THk4dmF6MD07MjpUa0U5UFRwSlFVRkJRVUZCUVVGQlFUMD07MDpUVlJGUFRwSUx5OHZMeTh2THk4dll6MD07" &&
		string(n) != "MjpUa0U5UFRwSlFVRkJRVUZCUVVGQlFUMD07MDpUVlJGUFRwSUx5OHZMeTh2THk4dll6MD07MTpUMUU5UFRwSUx5OHZMeTh2THk4dmF6MD07" &&
		string(n) != "" {
		t.Fatal(string(n))
	} else {
		values := map[string][]string{
			"11": {"value_11_11", "value_11_10", "value_11_9"},
			"5":  {"value_5_0"},
			"4":  {"value_4_3", "value_4_2", "value_4_1", "value_4_0"},
			"3":  {"value_3_0"},
			"9":  {"value_9_9", "value_9_8", "value_9_7"},
		}
		checkListOrSetFullScanValues(t, ay[1], values)
	}

	if ay, err := goredis.Values(c.Do("FULLSCAN", "default:testscanmerge:MDpUVlJGUFRwSUx5OHZMeTh2THk4dll6MD07MTpUMUU5UFRwSUx5OHZMeTh2THk4dmF6MD07MjpUa0U5UFRwSlFVRkJRVUZCUVVGQlFUMD07", "LIST", "count", 120)); err != nil {
		t.Fatal(err)
	} else if len(ay) != 2 {
		t.Fatal(len(ay))
	} else if n := ay[0].([]byte); string(n) != "MTpUVlJqUFRwSUx5OHZMeTh2THk4dk5EMD07MjpUVlJKUFRwSUx5OHZMeTh2THk4dlp6MD07" &&
		string(n) != "MjpUVlJKUFRwSUx5OHZMeTh2THk4dlp6MD07MTpUVlJqUFRwSUx5OHZMeTh2THk4dk5EMD07" {
		t.Fatal(string(n))
	} else {
		values := map[string][]string{
			"11": {"value_11_8", "value_11_7", "value_11_6", "value_11_5", "value_11_4", "value_11_3", "value_11_2", "value_11_1", "value_11_0"},
			"18": {"value_18_18", "value_18_17", "value_18_16", "value_18_15", "value_18_14", "value_18_13", "value_18_12", "value_18_11", "value_18_10", "value_18_9", "value_18_8", "value_18_7", "value_18_6", "value_18_5", "value_18_4", "value_18_3", "value_18_2", "value_18_1", "value_18_0"},
			"16": {"value_16_16", "value_16_15", "value_16_14", "value_16_13", "value_16_12", "value_16_11", "value_16_10", "value_16_9", "value_16_8", "value_16_7", "value_16_6", "value_16_5", "value_16_4", "value_16_3", "value_16_2", "value_16_1", "value_16_0"},
			"17": {"value_17_17", "value_17_16", "value_17_15", "value_17_14", "value_17_13", "value_17_12", "value_17_11", "value_17_10", "value_17_9", "value_17_8", "value_17_7", "value_17_6", "value_17_5", "value_17_4", "value_17_3", "value_17_2"},
			"9":  {"value_9_6", "value_9_5", "value_9_4", "value_9_3", "value_9_2", "value_9_1", "value_9_0"},
			"6":  {"value_6_6", "value_6_5", "value_6_4", "value_6_3", "value_6_2", "value_6_1", "value_6_0"},
			"7":  {"value_7_7", "value_7_6", "value_7_5", "value_7_4", "value_7_3", "value_7_2", "value_7_1", "value_7_0"},
			"8":  {"value_8_8", "value_8_7", "value_8_6", "value_8_5", "value_8_4", "value_8_3", "value_8_2", "value_8_1", "value_8_0"},
			"10": {"value_10_10", "value_10_9", "value_10_8", "value_10_7", "value_10_6", "value_10_5", "value_10_4", "value_10_3", "value_10_2", "value_10_1", "value_10_0"},
			"12": {"value_12_12", "value_12_11", "value_12_10", "value_12_9", "value_12_8"},
		}
		checkListOrSetFullScanValues(t, ay[1], values)
	}

	if ay, err := goredis.Values(c.Do("FULLSCAN", "default:testscanmerge:MTpUVlJqUFRwSUx5OHZMeTh2THk4dk5EMD07MjpUVlJKUFRwSUx5OHZMeTh2THk4dlp6MD07", "LIST", "count", 10)); err != nil {
		t.Fatal(err)
	} else if len(ay) != 2 {
		t.Fatal(len(ay))
	} else if n := ay[0].([]byte); string(n) != "MjpUVlJKUFRwSUx5OHZMeTh2THk4dk1EMD07" {
		t.Fatal(string(n))
	} else {
		values := map[string][]string{
			"17": {"value_17_1", "value_17_0"},
			"12": {"value_12_7", "value_12_6", "value_12_5", "value_12_4", "value_12_3"},
		}
		checkListOrSetFullScanValues(t, ay[1], values)
	}

	if ay, err := goredis.Values(c.Do("FULLSCAN", "default:testscanmerge:MjpUVlJKUFRwSUx5OHZMeTh2THk4dk1EMD07", "LIST", "count", 100)); err != nil {
		t.Fatal(err)
	} else if len(ay) != 2 {
		t.Fatal(len(ay))
	} else if n := ay[0].([]byte); string(n) != "" {
		t.Fatal(string(n))
	} else {
		values := map[string][]string{
			"12": {"value_12_2", "value_12_1", "value_12_0"},
			"13": {"value_13_13", "value_13_12", "value_13_11", "value_13_10", "value_13_9", "value_13_8", "value_13_7", "value_13_6", "value_13_5", "value_13_4", "value_13_3", "value_13_2", "value_13_1", "value_13_0"},
			"14": {"value_14_14", "value_14_13", "value_14_12", "value_14_11", "value_14_10", "value_14_9", "value_14_8", "value_14_7", "value_14_6", "value_14_5", "value_14_4", "value_14_3", "value_14_2", "value_14_1", "value_14_0"},
			"15": {"value_15_15", "value_15_14", "value_15_13", "value_15_12", "value_15_11", "value_15_10", "value_15_9", "value_15_8", "value_15_7", "value_15_6", "value_15_5", "value_15_4", "value_15_3", "value_15_2", "value_15_1", "value_15_0"},
			"19": {"value_19_19", "value_19_18", "value_19_17", "value_19_16", "value_19_15", "value_19_14", "value_19_13", "value_19_12", "value_19_11", "value_19_10", "value_19_9", "value_19_8", "value_19_7", "value_19_6", "value_19_5", "value_19_4", "value_19_3", "value_19_2", "value_19_1", "value_19_0"},
		}
		checkListOrSetFullScanValues(t, ay[1], values)
	}

	if _, err := goredis.Values(c.Do("FULLSCAN", "default::MDpkR1Z6ZEhOallXNDZOQT09OzI6ZEdWemRITmpZVzQ2T1E9PTs=", "LIST", "count", 8)); err == nil {
		t.Fatal("want err, get nil ")
	}

	if _, err := goredis.Values(c.Do("FULLSCAN", "default:testscan1:MDpkR1Z6ZEhOallXNDZOQT09OzI6ZEdWemRITmpZVzQ2T1E9PTs=", "LIST", "count", 8)); err == nil {
		t.Fatal("want err, get nil ")
	}

	if _, err := goredis.Values(c.Do("FULLSCAN", "default:testscanmerge:dGVzdHNjYW46NA==", "LIST", "count", 0)); err == nil {
		t.Fatal("want err, get nil")
	}
}

func checkSetFullScan(t *testing.T, c *goredis.PoolConn) {
	if ay, err := goredis.Values(c.Do("FULLSCAN", "default:testscanmerge:", "SET", "count", 5)); err != nil {
		t.Fatal(err)
	} else if len(ay) != 2 {
		t.Fatal(len(ay))
	} else if n := ay[0].([]byte); string(n) != "MDpUbEU5UFRwa2JVWnpaRmRXWms1V09IYz07MTpUVkU5UFRwa2JVWnpaRmRXWmsxV09IYz07MjpUVUU5UFRwa2JVWnpaRmRXWmsxR09IYz07" &&
		string(n) != "MDpUbEU5UFRwa2JVWnpaRmRXWms1V09IYz07MjpUVUU5UFRwa2JVWnpaRmRXWmsxR09IYz07MTpUVkU5UFRwa2JVWnpaRmRXWmsxV09IYz07" &&
		string(n) != "MTpUVkU5UFRwa2JVWnpaRmRXWmsxV09IYz07MDpUbEU5UFRwa2JVWnpaRmRXWms1V09IYz07MjpUVUU5UFRwa2JVWnpaRmRXWmsxR09IYz07" &&
		string(n) != "MTpUVkU5UFRwa2JVWnpaRmRXWmsxV09IYz07MjpUVUU5UFRwa2JVWnpaRmRXWmsxR09IYz07MDpUbEU5UFRwa2JVWnpaRmRXWms1V09IYz07" &&
		string(n) != "MjpUVUU5UFRwa2JVWnpaRmRXWmsxR09IYz07MDpUbEU5UFRwa2JVWnpaRmRXWms1V09IYz07MTpUVkU5UFRwa2JVWnpaRmRXWmsxV09IYz07" &&
		string(n) != "MjpUVUU5UFRwa2JVWnpaRmRXWmsxR09IYz07MTpUVkU5UFRwa2JVWnpaRmRXWmsxV09IYz07MDpUbEU5UFRwa2JVWnpaRmRXWms1V09IYz07" {
		t.Fatal(string(n))
	} else {
		values := map[string][]string{
			"0": {"value_0_0"},
			"1": {"value_1_0"},
			"5": {"value_5_0"},
		}
		checkListOrSetFullScanValues(t, ay[1], values)
	}

	if ay, err := goredis.Values(c.Do("FULLSCAN", "default:testscanmerge:MDpUbEU5UFRwa2JVWnpaRmRXWms1V09IYz07MTpUVkU5UFRwa2JVWnpaRmRXWmsxV09IYz07MjpUVUU5UFRwa2JVWnpaRmRXWmsxR09IYz07", "SET", "count", 6)); err != nil {
		t.Fatal(err)
	} else if len(ay) != 2 {
		t.Fatal(len(ay))
	} else if n := ay[0].([]byte); string(n) != "MDpUbEU5UFRwa2JVWnpaRmRXWms1V09Iaz07MTpUWGM5UFRwa2JVWnpaRmRXWmsweE9IYz07MjpUV2M5UFRwa2JVWnpaRmRXWmsxc09IZz07" &&
		string(n) != "MDpUbEU5UFRwa2JVWnpaRmRXWms1V09Iaz07MjpUV2M5UFRwa2JVWnpaRmRXWmsxc09IZz07MTpUWGM5UFRwa2JVWnpaRmRXWmsweE9IYz07" &&
		string(n) != "MTpUWGM5UFRwa2JVWnpaRmRXWmsweE9IYz07MDpUbEU5UFRwa2JVWnpaRmRXWms1V09Iaz07MjpUV2M5UFRwa2JVWnpaRmRXWmsxc09IZz07" &&
		string(n) != "MTpUWGM5UFRwa2JVWnpaRmRXWmsweE9IYz07MjpUV2M5UFRwa2JVWnpaRmRXWmsxc09IZz07MDpUbEU5UFRwa2JVWnpaRmRXWms1V09Iaz07" &&
		string(n) != "MjpUV2M5UFRwa2JVWnpaRmRXWmsxc09IZz07MDpUbEU5UFRwa2JVWnpaRmRXWms1V09Iaz07MTpUWGM5UFRwa2JVWnpaRmRXWmsweE9IYz07" &&
		string(n) != "MjpUV2M5UFRwa2JVWnpaRmRXWmsxc09IZz07MTpUWGM5UFRwa2JVWnpaRmRXWmsweE9IYz07MDpUbEU5UFRwa2JVWnpaRmRXWms1V09Iaz07" {
		t.Fatal(string(n))
	} else {
		values := map[string][]string{
			"2": {"value_2_0", "value_2_1"},
			"5": {"value_5_1", "value_5_2"},
			"1": {"value_1_1"},
			"3": {"value_3_0"},
		}
		checkListOrSetFullScanValues(t, ay[1], values)
	}
	if ay, err := goredis.Values(c.Do("FULLSCAN", "default:testscanmerge:MDpUbEU5UFRwa2JVWnpaRmRXWms1V09Iaz07MTpUWGM5UFRwa2JVWnpaRmRXWmsweE9IYz07MjpUV2M5UFRwa2JVWnpaRmRXWmsxc09IZz07", "SET", "count", 8)); err != nil {
		t.Fatal(err)
	} else if len(ay) != 2 {
		t.Fatal(len(ay))
	} else if n := ay[0].([]byte); string(n) != "MDpUbEU5UFRwa2JVWnpaRmRXWms1V09EQT07MTpUWGM5UFRwa2JVWnpaRmRXWmsweE9Iaz07MjpUa0U5UFRwa2JVWnpaRmRXWms1R09IYz07" &&
		string(n) != "MDpUbEU5UFRwa2JVWnpaRmRXWms1V09EQT07MjpUa0U5UFRwa2JVWnpaRmRXWms1R09IYz07MTpUWGM5UFRwa2JVWnpaRmRXWmsweE9Iaz07" &&
		string(n) != "MTpUWGM5UFRwa2JVWnpaRmRXWmsweE9Iaz07MDpUbEU5UFRwa2JVWnpaRmRXWms1V09EQT07MjpUa0U5UFRwa2JVWnpaRmRXWms1R09IYz07" &&
		string(n) != "MTpUWGM5UFRwa2JVWnpaRmRXWmsweE9Iaz07MjpUa0U5UFRwa2JVWnpaRmRXWms1R09IYz07MDpUbEU5UFRwa2JVWnpaRmRXWms1V09EQT07" &&
		string(n) != "MjpUa0U5UFRwa2JVWnpaRmRXWms1R09IYz07MDpUbEU5UFRwa2JVWnpaRmRXWms1V09EQT07MTpUWGM5UFRwa2JVWnpaRmRXWmsweE9Iaz07" &&
		string(n) != "MjpUa0U5UFRwa2JVWnpaRmRXWms1R09IYz07MTpUWGM5UFRwa2JVWnpaRmRXWmsweE9Iaz07MDpUbEU5UFRwa2JVWnpaRmRXWms1V09EQT07" {
		t.Fatal(string(n))
	} else {
		values := map[string][]string{
			"2": {"value_2_2"},
			"5": {"value_5_3", "value_5_4"},
			"4": {"value_4_0"},
			"3": {"value_3_2", "value_3_1"},
		}
		checkListOrSetFullScanValues(t, ay[1], values)
	}

	if ay, err := goredis.Values(c.Do("FULLSCAN", "default:testscanmerge:MDpUbEU5UFRwa2JVWnpaRmRXWms1V09EQT07MTpUWGM5UFRwa2JVWnpaRmRXWmsweE9Iaz07MjpUa0U5UFRwa2JVWnpaRmRXWms1R09IYz07", "SET", "count", 12)); err != nil {
		t.Fatal(err)
	} else if len(ay) != 2 {
		t.Fatal(len(ay))
	} else if n := ay[0].([]byte); string(n) != "" &&
		string(n) != "MDpUVlJGUFRwa2JVWnpaRmRXWmsxVVJtWk5WRUU5OzE6VDFFOVBUcGtiVVp6WkZkV1prOVdPSGs9OzI6VGtFOVBUcGtiVVp6WkZkV1prNUdPREE9Ow==" &&
		string(n) != "MDpUVlJGUFRwa2JVWnpaRmRXWmsxVVJtWk5WRUU5OzI6VGtFOVBUcGtiVVp6WkZkV1prNUdPREE9OzE6VDFFOVBUcGtiVVp6WkZkV1prOVdPSGs9Ow==" &&
		string(n) != "MTpUMUU5UFRwa2JVWnpaRmRXWms5V09Iaz07MDpUVlJGUFRwa2JVWnpaRmRXWmsxVVJtWk5WRUU5OzI6VGtFOVBUcGtiVVp6WkZkV1prNUdPREE9Ow==" &&
		string(n) != "MTpUMUU5UFRwa2JVWnpaRmRXWms5V09Iaz07MjpUa0U5UFRwa2JVWnpaRmRXWms1R09EQT07MDpUVlJGUFRwa2JVWnpaRmRXWmsxVVJtWk5WRUU5Ow==" &&
		string(n) != "MjpUa0U5UFRwa2JVWnpaRmRXWms1R09EQT07MDpUVlJGUFRwa2JVWnpaRmRXWmsxVVJtWk5WRUU5OzE6VDFFOVBUcGtiVVp6WkZkV1prOVdPSGs9Ow==" &&
		string(n) != "MjpUa0U5UFRwa2JVWnpaRmRXWms1R09EQT07MTpUMUU5UFRwa2JVWnpaRmRXWms5V09Iaz07MDpUVlJGUFRwa2JVWnpaRmRXWmsxVVJtWk5WRUU5Ow==" {
		t.Fatal(string(n))
	} else {
		values := map[string][]string{
			"11": {"value_11_1", "value_11_10", "value_11_0"},
			"5":  {"value_5_5"},
			"4":  {"value_4_3", "value_4_2", "value_4_1", "value_4_4"},
			"3":  {"value_3_3"},
			"9":  {"value_9_0", "value_9_1", "value_9_2"},
		}
		checkListOrSetFullScanValues(t, ay[1], values)
	}

	if ay, err := goredis.Values(c.Do("FULLSCAN", "default:testscanmerge:MDpUVlJGUFRwa2JVWnpaRmRXWmsxVVJtWk5WRUU5OzE6VDFFOVBUcGtiVVp6WkZkV1prOVdPSGs9OzI6VGtFOVBUcGtiVVp6WkZkV1prNUdPREE9Ow==", "SET", "count", 120)); err != nil {
		t.Fatal(err)
	} else if len(ay) != 2 {
		t.Fatal(len(ay))
	} else if n := ay[0].([]byte); string(n) != "MTpUVlJqUFRwa2JVWnpaRmRXWmsxVVpHWk9kejA5OzI6VFZSSlBUcGtiVVp6WkZkV1prMVVTbVpOVkVrOTs=" &&
		string(n) != "MjpUVlJKUFRwa2JVWnpaRmRXWmsxVVNtWk5WRWs5OzE6VFZSalBUcGtiVVp6WkZkV1prMVVaR1pPZHowOTs=" {
		t.Fatal(string(n))
	} else {
		values := map[string][]string{
			"11": {"value_11_8", "value_11_7", "value_11_6", "value_11_5", "value_11_4", "value_11_3", "value_11_2", "value_11_9", "value_11_11"},
			"18": {"value_18_18", "value_18_17", "value_18_16", "value_18_15", "value_18_14", "value_18_13", "value_18_12", "value_18_11", "value_18_10", "value_18_9", "value_18_8", "value_18_7", "value_18_6", "value_18_5", "value_18_4", "value_18_3", "value_18_2", "value_18_1", "value_18_0"},
			"16": {"value_16_16", "value_16_15", "value_16_14", "value_16_13", "value_16_12", "value_16_11", "value_16_10", "value_16_9", "value_16_8", "value_16_7", "value_16_6", "value_16_5", "value_16_4", "value_16_3", "value_16_2", "value_16_1", "value_16_0"},
			"17": {"value_17_17", "value_17_16", "value_17_15", "value_17_14", "value_17_13", "value_17_12", "value_17_11", "value_17_10", "value_17_7", "value_17_6", "value_17_5", "value_17_4", "value_17_3", "value_17_2", "value_17_1", "value_17_0"},
			"9":  {"value_9_6", "value_9_5", "value_9_4", "value_9_3", "value_9_7", "value_9_8", "value_9_9"},
			"6":  {"value_6_6", "value_6_5", "value_6_4", "value_6_3", "value_6_2", "value_6_1", "value_6_0"},
			"7":  {"value_7_7", "value_7_6", "value_7_5", "value_7_4", "value_7_3", "value_7_2", "value_7_1", "value_7_0"},
			"8":  {"value_8_8", "value_8_7", "value_8_6", "value_8_5", "value_8_4", "value_8_3", "value_8_2", "value_8_1", "value_8_0"},
			"10": {"value_10_10", "value_10_9", "value_10_8", "value_10_7", "value_10_6", "value_10_5", "value_10_4", "value_10_3", "value_10_2", "value_10_1", "value_10_0"},
			"12": {"value_12_12", "value_12_11", "value_12_10", "value_12_0", "value_12_1"},
		}
		checkListOrSetFullScanValues(t, ay[1], values)
	}

	if ay, err := goredis.Values(c.Do("FULLSCAN", "default:testscanmerge:MTpUVlJqUFRwa2JVWnpaRmRXWmsxVVpHWk9kejA5OzI6VFZSSlBUcGtiVVp6WkZkV1prMVVTbVpOVkVrOTs=", "SET", "count", 10)); err != nil {
		t.Fatal(err)
	} else if len(ay) != 2 {
		t.Fatal(len(ay))
	} else if n := ay[0].([]byte); string(n) != "MjpUVlJKUFRwa2JVWnpaRmRXWmsxVVNtWk9aejA5Ow==" {
		t.Fatal(string(n))
	} else {
		values := map[string][]string{
			"17": {"value_17_9", "value_17_8"},
			"12": {"value_12_7", "value_12_6", "value_12_5", "value_12_4", "value_12_3", "value_12_2"},
		}
		checkListOrSetFullScanValues(t, ay[1], values)
	}

	if ay, err := goredis.Values(c.Do("FULLSCAN", "default:testscanmerge:MjpUVlJKUFRwa2JVWnpaRmRXWmsxVVNtWk9aejA5Ow==", "SET", "count", 100)); err != nil {
		t.Fatal(err)
	} else if len(ay) != 2 {
		t.Fatal(len(ay))
	} else if n := ay[0].([]byte); string(n) != "" {
		t.Fatal(string(n))
	} else {
		values := map[string][]string{
			"12": {"value_12_7", "value_12_8", "value_12_9"},
			"13": {"value_13_13", "value_13_12", "value_13_11", "value_13_10", "value_13_9", "value_13_8", "value_13_7", "value_13_6", "value_13_5", "value_13_4", "value_13_3", "value_13_2", "value_13_1", "value_13_0"},
			"14": {"value_14_14", "value_14_13", "value_14_12", "value_14_11", "value_14_10", "value_14_9", "value_14_8", "value_14_7", "value_14_6", "value_14_5", "value_14_4", "value_14_3", "value_14_2", "value_14_1", "value_14_0"},
			"15": {"value_15_15", "value_15_14", "value_15_13", "value_15_12", "value_15_11", "value_15_10", "value_15_9", "value_15_8", "value_15_7", "value_15_6", "value_15_5", "value_15_4", "value_15_3", "value_15_2", "value_15_1", "value_15_0"},
			"19": {"value_19_19", "value_19_18", "value_19_17", "value_19_16", "value_19_15", "value_19_14", "value_19_13", "value_19_12", "value_19_11", "value_19_10", "value_19_9", "value_19_8", "value_19_7", "value_19_6", "value_19_5", "value_19_4", "value_19_3", "value_19_2", "value_19_1", "value_19_0"},
		}
		checkListOrSetFullScanValues(t, ay[1], values)
	}

	if _, err := goredis.Values(c.Do("FULLSCAN", "default::MDpkR1Z6ZEhOallXNDZOQT09OzI6ZEdWemRITmpZVzQ2T1E9PTs=", "SET", "count", 8)); err == nil {
		t.Fatal("want err, get nil ")
	}

	if _, err := goredis.Values(c.Do("FULLSCAN", "default:testscan1:MDpkR1Z6ZEhOallXNDZOQT09OzI6ZEdWemRITmpZVzQ2T1E9PTs=", "SET", "count", 8)); err == nil {
		t.Fatal("want err, get nil ")
	}

	if _, err := goredis.Values(c.Do("FULLSCAN", "default:testscanmerge:dGVzdHNjYW46NA==", "SET", "count", 0)); err == nil {
		t.Fatal("want err, get nil")
	}

}

func checkZSetFullScan(t *testing.T, c *goredis.PoolConn) {
	if ay, err := goredis.Values(c.Do("FULLSCAN", "default:testscanmerge:", "ZSET", "count", 5)); err != nil {
		t.Fatal(err)
	} else if len(ay) != 2 {
		t.Fatal(len(ay))
	} else if n := ay[0].([]byte); string(n) != "MDpUbEU5UFRwa2JVWnpaRmRXWms1V09IYz07MTpUVkU5UFRwa2JVWnpaRmRXWmsxV09IYz07MjpUVUU5UFRwa2JVWnpaRmRXWmsxR09IYz07" &&
		string(n) != "MDpUbEU5UFRwa2JVWnpaRmRXWms1V09IYz07MjpUVUU5UFRwa2JVWnpaRmRXWmsxR09IYz07MTpUVkU5UFRwa2JVWnpaRmRXWmsxV09IYz07" &&
		string(n) != "MTpUVkU5UFRwa2JVWnpaRmRXWmsxV09IYz07MDpUbEU5UFRwa2JVWnpaRmRXWms1V09IYz07MjpUVUU5UFRwa2JVWnpaRmRXWmsxR09IYz07" &&
		string(n) != "MTpUVkU5UFRwa2JVWnpaRmRXWmsxV09IYz07MjpUVUU5UFRwa2JVWnpaRmRXWmsxR09IYz07MDpUbEU5UFRwa2JVWnpaRmRXWms1V09IYz07" &&
		string(n) != "MjpUVUU5UFRwa2JVWnpaRmRXWmsxR09IYz07MDpUbEU5UFRwa2JVWnpaRmRXWms1V09IYz07MTpUVkU5UFRwa2JVWnpaRmRXWmsxV09IYz07" &&
		string(n) != "MjpUVUU5UFRwa2JVWnpaRmRXWmsxR09IYz07MTpUVkU5UFRwa2JVWnpaRmRXWmsxV09IYz07MDpUbEU5UFRwa2JVWnpaRmRXWms1V09IYz07" {
		t.Fatal(string(n))
	} else {
		values := map[string]map[string]string{
			"0": {
				"value_0_0": "0",
			},
			"1": {
				"value_1_0": "0",
			},
			"5": {
				"value_5_0": "0",
			},
		}
		checkZSetFullScanValues(t, ay[1], values)
	}

	if ay, err := goredis.Values(c.Do("FULLSCAN", "default:testscanmerge:MDpUbEU5UFRwa2JVWnpaRmRXWms1V09IYz07MTpUVkU5UFRwa2JVWnpaRmRXWmsxV09IYz07MjpUVUU5UFRwa2JVWnpaRmRXWmsxR09IYz07", "ZSET", "count", 6)); err != nil {
		t.Fatal(err)
	} else if len(ay) != 2 {
		t.Fatal(len(ay))
	} else if n := ay[0].([]byte); string(n) != "MDpUbEU5UFRwa2JVWnpaRmRXWms1V09Iaz07MTpUWGM5UFRwa2JVWnpaRmRXWmsweE9IYz07MjpUV2M5UFRwa2JVWnpaRmRXWmsxc09IZz07" &&
		string(n) != "MDpUbEU5UFRwa2JVWnpaRmRXWms1V09Iaz07MjpUV2M5UFRwa2JVWnpaRmRXWmsxc09IZz07MTpUWGM5UFRwa2JVWnpaRmRXWmsweE9IYz07" &&
		string(n) != "MTpUWGM5UFRwa2JVWnpaRmRXWmsweE9IYz07MDpUbEU5UFRwa2JVWnpaRmRXWms1V09Iaz07MjpUV2M5UFRwa2JVWnpaRmRXWmsxc09IZz07" &&
		string(n) != "MTpUWGM5UFRwa2JVWnpaRmRXWmsweE9IYz07MjpUV2M5UFRwa2JVWnpaRmRXWmsxc09IZz07MDpUbEU5UFRwa2JVWnpaRmRXWms1V09Iaz07" &&
		string(n) != "MjpUV2M5UFRwa2JVWnpaRmRXWmsxc09IZz07MDpUbEU5UFRwa2JVWnpaRmRXWms1V09Iaz07MTpUWGM5UFRwa2JVWnpaRmRXWmsweE9IYz07" &&
		string(n) != "MjpUV2M5UFRwa2JVWnpaRmRXWmsxc09IZz07MTpUWGM5UFRwa2JVWnpaRmRXWmsweE9IYz07MDpUbEU5UFRwa2JVWnpaRmRXWms1V09Iaz07" {
		t.Fatal(string(n))
	} else {
		values := map[string]map[string]string{
			"2": {
				"value_2_0": "0",
				"value_2_1": "1",
			},
			"5": {
				"value_5_1": "1",
				"value_5_2": "2",
			},
			"1": {
				"value_1_1": "1",
			},
			"3": {
				"value_3_0": "0",
			},
		}
		checkZSetFullScanValues(t, ay[1], values)
	}
	if ay, err := goredis.Values(c.Do("FULLSCAN", "default:testscanmerge:MDpUbEU5UFRwa2JVWnpaRmRXWms1V09Iaz07MTpUWGM5UFRwa2JVWnpaRmRXWmsweE9IYz07MjpUV2M5UFRwa2JVWnpaRmRXWmsxc09IZz07", "ZSET", "count", 8)); err != nil {
		t.Fatal(err)
	} else if len(ay) != 2 {
		t.Fatal(len(ay))
	} else if n := ay[0].([]byte); string(n) != "MDpUbEU5UFRwa2JVWnpaRmRXWms1V09EQT07MTpUWGM5UFRwa2JVWnpaRmRXWmsweE9Iaz07MjpUa0U5UFRwa2JVWnpaRmRXWms1R09IYz07" &&
		string(n) != "MDpUbEU5UFRwa2JVWnpaRmRXWms1V09EQT07MjpUa0U5UFRwa2JVWnpaRmRXWms1R09IYz07MTpUWGM5UFRwa2JVWnpaRmRXWmsweE9Iaz07" &&
		string(n) != "MTpUWGM5UFRwa2JVWnpaRmRXWmsweE9Iaz07MDpUbEU5UFRwa2JVWnpaRmRXWms1V09EQT07MjpUa0U5UFRwa2JVWnpaRmRXWms1R09IYz07" &&
		string(n) != "MTpUWGM5UFRwa2JVWnpaRmRXWmsweE9Iaz07MjpUa0U5UFRwa2JVWnpaRmRXWms1R09IYz07MDpUbEU5UFRwa2JVWnpaRmRXWms1V09EQT07" &&
		string(n) != "MjpUa0U5UFRwa2JVWnpaRmRXWms1R09IYz07MDpUbEU5UFRwa2JVWnpaRmRXWms1V09EQT07MTpUWGM5UFRwa2JVWnpaRmRXWmsweE9Iaz07" &&
		string(n) != "MjpUa0U5UFRwa2JVWnpaRmRXWms1R09IYz07MTpUWGM5UFRwa2JVWnpaRmRXWmsweE9Iaz07MDpUbEU5UFRwa2JVWnpaRmRXWms1V09EQT07" {
		t.Fatal(string(n))
	} else {
		values := map[string]map[string]string{
			"3": {
				"value_3_1": "1",
				"value_3_2": "2",
			},
			"5": {
				"value_5_3": "3",
				"value_5_4": "4",
			},
			"2": {
				"value_2_2": "2",
			},
			"4": {
				"value_4_0": "0",
			},
		}
		checkZSetFullScanValues(t, ay[1], values)
	}

	if ay, err := goredis.Values(c.Do("FULLSCAN", "default:testscanmerge:MDpUbEU5UFRwa2JVWnpaRmRXWms1V09EQT07MTpUWGM5UFRwa2JVWnpaRmRXWmsweE9Iaz07MjpUa0U5UFRwa2JVWnpaRmRXWms1R09IYz07", "ZSET", "count", 12)); err != nil {
		t.Fatal(err)
	} else if len(ay) != 2 {
		t.Fatal(len(ay))
	} else if n := ay[0].([]byte); string(n) != "" &&
		string(n) != "MDpUVlJGUFRwa2JVWnpaRmRXWmsxVVJtWk5WRUU5OzE6VDFFOVBUcGtiVVp6WkZkV1prOVdPSGs9OzI6VGtFOVBUcGtiVVp6WkZkV1prNUdPREE9Ow==" &&
		string(n) != "MDpUVlJGUFRwa2JVWnpaRmRXWmsxVVJtWk5WRUU5OzI6VGtFOVBUcGtiVVp6WkZkV1prNUdPREE9OzE6VDFFOVBUcGtiVVp6WkZkV1prOVdPSGs9Ow==" &&
		string(n) != "MTpUMUU5UFRwa2JVWnpaRmRXWms5V09Iaz07MDpUVlJGUFRwa2JVWnpaRmRXWmsxVVJtWk5WRUU5OzI6VGtFOVBUcGtiVVp6WkZkV1prNUdPREE9Ow==" &&
		string(n) != "MTpUMUU5UFRwa2JVWnpaRmRXWms5V09Iaz07MjpUa0U5UFRwa2JVWnpaRmRXWms1R09EQT07MDpUVlJGUFRwa2JVWnpaRmRXWmsxVVJtWk5WRUU5Ow==" &&
		string(n) != "MjpUa0U5UFRwa2JVWnpaRmRXWms1R09EQT07MDpUVlJGUFRwa2JVWnpaRmRXWmsxVVJtWk5WRUU5OzE6VDFFOVBUcGtiVVp6WkZkV1prOVdPSGs9Ow==" &&
		string(n) != "MjpUa0U5UFRwa2JVWnpaRmRXWms1R09EQT07MTpUMUU5UFRwa2JVWnpaRmRXWms5V09Iaz07MDpUVlJGUFRwa2JVWnpaRmRXWmsxVVJtWk5WRUU5Ow==" {
		t.Fatal(string(n))
	} else {
		values := map[string]map[string]string{
			"5": {
				"value_5_5": "5",
			},
			"11": {
				"value_11_0":  "0",
				"value_11_1":  "1",
				"value_11_10": "10",
			},
			"3": {
				"value_3_3": "3",
			},
			"9": {
				"value_9_0": "0",
				"value_9_1": "1",
				"value_9_2": "2",
			},
			"4": {
				"value_4_1": "1",
				"value_4_2": "2",
				"value_4_3": "3",
				"value_4_4": "4",
			},
		}
		checkZSetFullScanValues(t, ay[1], values)
	}

	if ay, err := goredis.Values(c.Do("FULLSCAN", "default:testscanmerge:MDpUVlJGUFRwa2JVWnpaRmRXWmsxVVJtWk5WRUU5OzE6VDFFOVBUcGtiVVp6WkZkV1prOVdPSGs9OzI6VGtFOVBUcGtiVVp6WkZkV1prNUdPREE9Ow==", "ZSET", "count", 120)); err != nil {
		t.Fatal(err)
	} else if len(ay) != 2 {
		t.Fatal(len(ay))
	} else if n := ay[0].([]byte); string(n) != "MTpUVlJqUFRwa2JVWnpaRmRXWmsxVVpHWk9kejA5OzI6VFZSSlBUcGtiVVp6WkZkV1prMVVTbVpOVkVrOTs=" &&
		string(n) != "MjpUVlJKUFRwa2JVWnpaRmRXWmsxVVNtWk5WRWs5OzE6VFZSalBUcGtiVVp6WkZkV1prMVVaR1pPZHowOTs=" {
		t.Fatal(string(n))
	} else {
		values := map[string]map[string]string{
			"11": {
				"value_11_2":  "2",
				"value_11_3":  "3",
				"value_11_4":  "4",
				"value_11_5":  "5",
				"value_11_6":  "6",
				"value_11_7":  "7",
				"value_11_8":  "8",
				"value_11_9":  "9",
				"value_11_11": "11",
			},
			"18": {
				"value_18_0":  "0",
				"value_18_1":  "1",
				"value_18_2":  "2",
				"value_18_3":  "3",
				"value_18_4":  "4",
				"value_18_5":  "5",
				"value_18_6":  "6",
				"value_18_7":  "7",
				"value_18_8":  "8",
				"value_18_9":  "9",
				"value_18_10": "10",
				"value_18_11": "11",
				"value_18_12": "12",
				"value_18_13": "13",
				"value_18_14": "14",
				"value_18_15": "15",
				"value_18_16": "16",
				"value_18_17": "17",
				"value_18_18": "18",
			},
			"9": {
				"value_9_3": "3",
				"value_9_4": "4",
				"value_9_5": "5",
				"value_9_6": "6",
				"value_9_7": "7",
				"value_9_8": "8",
				"value_9_9": "9",
			},
			"16": {
				"value_16_0":  "0",
				"value_16_1":  "1",
				"value_16_2":  "2",
				"value_16_3":  "3",
				"value_16_4":  "4",
				"value_16_5":  "5",
				"value_16_6":  "6",
				"value_16_7":  "7",
				"value_16_8":  "8",
				"value_16_9":  "9",
				"value_16_10": "10",
				"value_16_11": "11",
				"value_16_12": "12",
				"value_16_13": "13",
				"value_16_14": "14",
				"value_16_15": "15",
				"value_16_16": "16",
			},
			"17": {
				"value_17_0":  "0",
				"value_17_1":  "1",
				"value_17_2":  "2",
				"value_17_3":  "3",
				"value_17_4":  "4",
				"value_17_5":  "5",
				"value_17_6":  "6",
				"value_17_7":  "7",
				"value_17_10": "10",
				"value_17_11": "11",
				"value_17_12": "12",
				"value_17_13": "13",
				"value_17_14": "14",
				"value_17_15": "15",
				"value_17_16": "16",
				"value_17_17": "17",
			},
			"6": {
				"value_6_0": "0",
				"value_6_1": "1",
				"value_6_2": "2",
				"value_6_3": "3",
				"value_6_4": "4",
				"value_6_5": "5",
				"value_6_6": "6",
			},
			"7": {
				"value_7_0": "0",
				"value_7_1": "1",
				"value_7_2": "2",
				"value_7_3": "3",
				"value_7_4": "4",
				"value_7_5": "5",
				"value_7_6": "6",
				"value_7_7": "7",
			},
			"8": {
				"value_8_0": "0",
				"value_8_1": "1",
				"value_8_2": "2",
				"value_8_3": "3",
				"value_8_4": "4",
				"value_8_5": "5",
				"value_8_6": "6",
				"value_8_7": "7",
				"value_8_8": "8",
			},
			"10": {
				"value_10_0":  "0",
				"value_10_1":  "1",
				"value_10_2":  "2",
				"value_10_3":  "3",
				"value_10_4":  "4",
				"value_10_5":  "5",
				"value_10_6":  "6",
				"value_10_7":  "7",
				"value_10_8":  "8",
				"value_10_9":  "9",
				"value_10_10": "10",
			},
			"12": {
				"value_12_0":  "0",
				"value_12_1":  "1",
				"value_12_10": "10",
				"value_12_11": "11",
				"value_12_12": "12",
			},
		}
		checkZSetFullScanValues(t, ay[1], values)
	}

	if ay, err := goredis.Values(c.Do("FULLSCAN", "default:testscanmerge:MTpUVlJqUFRwa2JVWnpaRmRXWmsxVVpHWk9kejA5OzI6VFZSSlBUcGtiVVp6WkZkV1prMVVTbVpOVkVrOTs=", "ZSET", "count", 10)); err != nil {
		t.Fatal(err)
	} else if len(ay) != 2 {
		t.Fatal(len(ay))
	} else if n := ay[0].([]byte); string(n) != "MjpUVlJKUFRwa2JVWnpaRmRXWmsxVVNtWk9aejA5Ow==" {
		t.Fatal(string(n))
	} else {
		values := map[string]map[string]string{
			"17": {
				"value_17_8": "8",
				"value_17_9": "9",
			},
			"12": {
				"value_12_2": "2",
				"value_12_3": "3",
				"value_12_4": "4",
				"value_12_5": "5",
				"value_12_6": "6",
			},
		}
		checkZSetFullScanValues(t, ay[1], values)
	}

	if ay, err := goredis.Values(c.Do("FULLSCAN", "default:testscanmerge:MjpUVlJKUFRwa2JVWnpaRmRXWmsxVVNtWk9aejA5Ow==", "ZSET", "count", 100)); err != nil {
		t.Fatal(err)
	} else if len(ay) != 2 {
		t.Fatal(len(ay))
	} else if n := ay[0].([]byte); string(n) != "" {
		t.Fatal(string(n))
	} else {
		values := map[string]map[string]string{
			"12": {
				"value_12_7": "7",
				"value_12_8": "8",
				"value_12_9": "9",
			},
			"13": {
				"value_13_0":  "0",
				"value_13_1":  "1",
				"value_13_2":  "2",
				"value_13_3":  "3",
				"value_13_4":  "4",
				"value_13_5":  "5",
				"value_13_6":  "6",
				"value_13_7":  "7",
				"value_13_8":  "8",
				"value_13_9":  "9",
				"value_13_10": "10",
				"value_13_11": "11",
				"value_13_12": "12",
				"value_13_13": "13",
			},
			"14": {
				"value_14_0":  "0",
				"value_14_1":  "1",
				"value_14_2":  "2",
				"value_14_3":  "3",
				"value_14_4":  "4",
				"value_14_5":  "5",
				"value_14_6":  "6",
				"value_14_7":  "7",
				"value_14_8":  "8",
				"value_14_9":  "9",
				"value_14_10": "10",
				"value_14_11": "11",
				"value_14_12": "12",
				"value_14_13": "13",
				"value_14_14": "14",
			},
			"15": {
				"value_15_0":  "0",
				"value_15_1":  "1",
				"value_15_2":  "2",
				"value_15_3":  "3",
				"value_15_4":  "4",
				"value_15_5":  "5",
				"value_15_6":  "6",
				"value_15_7":  "7",
				"value_15_8":  "8",
				"value_15_9":  "9",
				"value_15_10": "10",
				"value_15_11": "11",
				"value_15_12": "12",
				"value_15_13": "13",
				"value_15_14": "14",
				"value_15_15": "15",
			},
			"19": {
				"value_19_0":  "0",
				"value_19_1":  "1",
				"value_19_2":  "2",
				"value_19_3":  "3",
				"value_19_4":  "4",
				"value_19_5":  "5",
				"value_19_6":  "6",
				"value_19_7":  "7",
				"value_19_8":  "8",
				"value_19_9":  "9",
				"value_19_10": "10",
				"value_19_11": "11",
				"value_19_12": "12",
				"value_19_13": "13",
				"value_19_14": "14",
				"value_19_15": "15",
				"value_19_16": "16",
				"value_19_17": "17",
				"value_19_18": "18",
				"value_19_19": "19",
			},
		}
		checkHashFullScanValues(t, ay[1], values)
	}

	if _, err := goredis.Values(c.Do("FULLSCAN", "default::MDpkR1Z6ZEhOallXNDZOQT09OzI6ZEdWemRITmpZVzQ2T1E9PTs=", "ZSET", "count", 8)); err == nil {
		t.Fatal("want err, get nil ")
	}

	if _, err := goredis.Values(c.Do("FULLSCAN", "default:testscan1:MDpkR1Z6ZEhOallXNDZOQT09OzI6ZEdWemRITmpZVzQ2T1E9PTs=", "ZSET", "count", 8)); err == nil {
		t.Fatal("want err, get nil ")
	}

	if _, err := goredis.Values(c.Do("FULLSCAN", "default:testscanmerge:dGVzdHNjYW46NA==", "ZSET", "count", 0)); err == nil {
		t.Fatal("want err, get nil")
	}

}

func TestFullScan(t *testing.T) {
	c := getMergeTestConn(t)
	defer c.Close()

	testKVFullScan(t, c)
	testHashFullScan(t, c)
	testListFullScan(t, c)
	testSetFullScan(t, c)
	testZSetFullScan(t, c)
}

func testKVFullScan(t *testing.T, c *goredis.PoolConn) {
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
	checkKVFullScan(t, c)
	fmt.Println("KV FullScan success")
}

func testHashFullScan(t *testing.T, c *goredis.PoolConn) {
	for i := 0; i < 20; i++ {
		for j := 0; j <= i; j++ {
			value := fmt.Sprintf("value_%d_%d", i, j)
			if _, err := c.Do("hset", "default:testscanmerge:"+fmt.Sprintf("%d", i), fmt.Sprintf("%d", j), []byte(value)); err != nil {
				t.Fatal(err)
			}
		}
	}
	for i := 0; i < 20; i++ {
		for j := 0; j <= i; j++ {
			value := fmt.Sprintf("value_%d_%d", i, j)
			if _, err := c.Do("hset", "default:testscanmerge1:"+fmt.Sprintf("%d", i), fmt.Sprintf("%d", j), []byte(value)); err != nil {
				t.Fatal(err)
			}
		}
	}
	checkHashFullScan(t, c)
	fmt.Println("Hash FullScan success")
}

func testListFullScan(t *testing.T, c *goredis.PoolConn) {
	for i := 0; i < 20; i++ {
		for j := 0; j <= i; j++ {
			value := fmt.Sprintf("value_%d_%d", i, j)
			if _, err := c.Do("lpush", "default:testscanmerge:"+fmt.Sprintf("%d", i), value); err != nil {
				t.Fatal(err)
			}
		}
	}
	for i := 0; i < 20; i++ {
		for j := 0; j <= i; j++ {
			value := fmt.Sprintf("value_%d_%d", i, j)
			if _, err := c.Do("lpush", "default:testscanmerge1:"+fmt.Sprintf("%d", i), value); err != nil {
				t.Fatal(err)
			}
		}
	}

	checkListFullScan(t, c)

	fmt.Println("List FullScan success")
}

func testSetFullScan(t *testing.T, c *goredis.PoolConn) {
	for i := 0; i < 20; i++ {
		for j := 0; j <= i; j++ {
			value := fmt.Sprintf("value_%d_%d", i, j)
			if _, err := c.Do("sadd", "default:testscanmerge:"+fmt.Sprintf("%d", i), value); err != nil {
				t.Fatal(err)
			}
		}
	}
	for i := 0; i < 20; i++ {
		for j := 0; j <= i; j++ {
			value := fmt.Sprintf("value_%d_%d", i, j)
			if _, err := c.Do("sadd", "default:testscanmerge1:"+fmt.Sprintf("%d", i), value); err != nil {
				t.Fatal(err)
			}
		}
	}

	checkSetFullScan(t, c)
	fmt.Println("Set FullScan success")
}

func testZSetFullScan(t *testing.T, c *goredis.PoolConn) {
	for i := 0; i < 20; i++ {
		for j := 0; j <= i; j++ {
			value := fmt.Sprintf("value_%d_%d", i, j)
			if _, err := c.Do("zadd", "default:testscanmerge:"+fmt.Sprintf("%d", i), j, []byte(value)); err != nil {
				t.Fatal(err)
			}
		}
	}

	for i := 0; i < 20; i++ {
		for j := 0; j <= i; j++ {
			value := fmt.Sprintf("value_%d_%d", i, j)
			if _, err := c.Do("zadd", "default:testscanmerge1:"+fmt.Sprintf("%d", i), j, []byte(value)); err != nil {
				t.Fatal(err)
			}
		}
	}

	checkZSetFullScan(t, c)

	fmt.Println("ZSet FullScan success")
}
