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

func checkKVFullScanValues(t *testing.T, ay interface{}, values []interface{}) {
	a, err := goredis.MultiBulk(ay, nil)
	if err != nil {
		t.Fatal(err)
	}

	if len(a) != len(values) {
		t.Fatal(fmt.Sprintf("len %d != %d", len(a), len(values)))
	}
	var equalCount int

	for _, val := range values {
		for idx, _ := range a {
			item := a[idx].([]interface{})
			if len(item) != 2 {
				t.Fatal("item length is not 2. len:", len(item))
			}
			key := item[0].([]byte)
			value := item[1].([]byte)
			if val.(string) == string(key) {
				equalCount++
				v := fmt.Sprintf("value_%s", key)
				if v != string(value) {
					t.Fatal("value is not right, key:", string(key), "; value:", string(value))
				}
			}
		}
	}

	if equalCount != len(values) {
		t.Fatal("equal count not equal")
	}
}

func checkListFullScanValues(t *testing.T, ay interface{}, values []interface{}) {
	a, err := goredis.MultiBulk(ay, nil)
	if err != nil {
		t.Fatal(err)
	}

	var equalCount int

	for _, val := range values {
		for idx, _ := range a {
			item := a[idx].([]interface{})
			if len(item) < 2 {
				t.Fatal("item length is not 3. len:", len(item))
			}
			itemLen := len(item)
			key := item[0].([]byte)
			if val.(string) == string(key) {
				equalCount++
				for i := 1; i < itemLen; i++ {
					value := item[i].([]byte)
					splits := bytes.Split(value, []byte("_"))
					if len(splits) != 3 {
						t.Fatal("value format error. value:", string(value))
					}

					if !bytes.Equal(key, splits[1]) {
						t.Fatal("key:", string(key), "; value:", string(value))
					}
				}
				break
			}
		}
	}

	if equalCount != len(values) {
		t.Fatal("equal count not equal")
	}

}

func checkHashFullScanValues(t *testing.T, ay interface{}, values []interface{}) {
	a, err := goredis.MultiBulk(ay, nil)
	if err != nil {
		t.Fatal(err)
	}

	if len(a) != len(values) {
		t.Fatal(fmt.Sprintf("len %d != %d", len(a), len(values)))
	}

	var equalCount int

	for _, val := range values {
		for idx, _ := range a {
			item := a[idx].([]interface{})
			if len(item) != 3 {
				t.Fatal("item length is not 3. len:", len(item))
			}
			key := item[0].([]byte)
			field := item[1].([]byte)
			value := item[2].([]byte)
			if val.(string) == string(key) {
				equalCount++
				splits := bytes.Split(value, []byte("_"))
				if len(splits) != 3 {
					t.Fatal("value format error. value:", string(value))
				}

				if !bytes.Equal(key, splits[1]) || !bytes.Equal(field, splits[2]) {
					t.Fatal("key:", string(key), "; field:", string(field), "; value:", string(value))
				}
				break
			}
		}
	}

	if equalCount != len(values) {
		t.Fatal("equal count not equal")
	}

}

func checkSetFullScanValues(t *testing.T, ay interface{}, values []interface{}) {
	a, err := goredis.MultiBulk(ay, nil)
	if err != nil {
		t.Fatal(err)
	}

	if len(a) != len(values)*2 {
		t.Fatal(fmt.Sprintf("len %d != %d", len(a), len(values)*2))
	}

	var equalCount int
	length := len(a)
	for _, val := range values {
		for i := 0; i < length; i = i + 2 {
			k := a[i].([]byte)
			if val.(string) == string(k) {
				equalCount++
				splits := bytes.SplitN(k, []byte(":"), 2)
				if len(splits) != 2 {
					t.Fatal("invlid key format. key:", string(k))
				}

				v := a[i+1].([]interface{})
				length := len(v)
				for j := 0; j < length; j++ {
					svalue := v[j].([]byte)
					svalue_splits := bytes.SplitN(svalue, []byte("_"), 3)
					if len(svalue_splits) != 3 {
						t.Fatal("invlid value format. svalue:", string(svalue))
					}

					if string(splits[1]) != string(svalue_splits[1]) {
						t.Fatal("invlid value format. svalue:", string(svalue), "; i:", string(splits[1]), "; j:", j)
					}
				}
			}
		}
	}
	if equalCount != len(values) {
		t.Fatal("equal count not equal")
	}

}

func checkZSetFullScanValues(t *testing.T, ay interface{}, values []interface{}) {
	a, err := goredis.MultiBulk(ay, nil)
	if err != nil {
		t.Fatal(err)
	}

	if len(a) != len(values)*2 {
		t.Fatal(fmt.Sprintf("len %d != %d", len(a), len(values)*2))
	}

	var equalCount int
	length := len(a)

	for _, val := range values {
		for i := 0; i < length; i = i + 2 {
			k := a[i].([]byte)
			if val.(string) == string(k) {
				equalCount++
				splits := bytes.SplitN(k, []byte(":"), 2)
				if len(splits) != 2 {
					t.Fatal("key format error. key:", string(k))
				}

				fieldcount, err := strconv.Atoi(string(splits[1]))
				if err != nil {
					t.Fatal(err)
				}

				v := a[i+1].([]interface{})
				for j := 0; j <= fieldcount*2; j = j + 2 {
					zvalue := v[j].([]byte)
					zscore := v[j+1].([]byte)

					zvalue_splits := bytes.SplitN(zvalue, []byte("_"), 3)
					if len(zvalue_splits) != 3 {
						t.Fatal("zvalue value format error")
					}

					if string(zvalue_splits[1]) != string(splits[1]) {
						t.Fatal("zvalue error. zvalue:", string(zvalue))
					}

					if string(zvalue_splits[2]) != string(zscore) {
						t.Fatal("zcore error. zvalue:", string(zvalue), "; zscore:", string(zscore))
					}
				}

			}
		}
	}

	if equalCount != len(values) {
		t.Fatal("equal count not equal")
	}

}

func checkFullScanValues(t *testing.T, ay interface{}, tp string, values ...interface{}) {
	switch tp {
	case "KV":
		checkKVFullScanValues(t, ay, values)
	case "LIST", "SET":
		checkListFullScanValues(t, ay, values)
	case "HASH":
		checkHashFullScanValues(t, ay, values)
		/*
			case "SET":
				checkSetFullScanValues(t, ay, values)
		*/
	case "ZSET":
		checkZSetFullScanValues(t, ay, values)
	}

}

func checkKVFullScan(t *testing.T, c *goredis.PoolConn, tp string) {
	if ay, err := goredis.Values(c.Do("FULLSCAN", "default:testscanmerge:", tp, "count", 5)); err != nil {
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
		checkFullScanValues(t, ay[1], tp, "1", "0", "11")
	}

	if ay, err := goredis.Values(c.Do("FULLSCAN", "default:testscanmerge:MDpUVlJGUFRvPTsxOlRWRTlQVG89OzI6VFVFOVBUbz07", tp, "count", 6)); err != nil {
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
		checkFullScanValues(t, ay[1], tp, "18", "5", "16", "17", "10", "12")
	}

	if ay, err := goredis.Values(c.Do("FULLSCAN", "default:testscanmerge:MDpUbEU5UFRvPTsxOlRWUmpQVG89OzI6VFZSSlBUbz07", tp, "count", 12)); err != nil {
		t.Fatal(err)
	} else if len(ay) != 2 {
		t.Fatal(len(ay))
	} else if n := ay[0].([]byte); string(n) != "MjpUVlJyUFRvPTs=" {
		t.Fatal(string(n))
	} else {
		if len(ay[1].([]interface{})) != 0 {
			checkFullScanValues(t, ay[1], tp, "13", "14", "3", "15", "9", "19")
		}
	}

	if ay, err := goredis.Values(c.Do("FULLSCAN", "default:testscanmerge:MjpUVlJyUFRvPTs=", tp, "count", 6)); err != nil {
		t.Fatal(err)
	} else if len(ay) != 2 {
		t.Fatal(len(ay))
	} else if n := ay[0].([]byte); string(n) != "" {
		t.Fatal(string(n))
	} else {
		if len(ay[1].([]interface{})) != 0 {
			checkFullScanValues(t, ay[1], tp, "2", "4", "6", "7", "8")
		}
	}

	if _, err := goredis.Values(c.Do("FULLSCAN", "default::MDpNVEU9OzE6TVE9PTsyOk1BPT07", tp, "count", 8)); err == nil {
		t.Fatal("want err, get nil ")
	}

	if _, err := goredis.Values(c.Do("FULLSCAN", "default:testscanmerge:dGVzdHNjYW46NA==", tp, "count", 0)); err == nil {
		t.Fatal("want err, get nil")
	}
}

func checkHashFullScan(t *testing.T, c *goredis.PoolConn, tp string) {
	if ay, err := goredis.Values(c.Do("FULLSCAN", "default:testscanmerge:", tp, "count", 5)); err != nil {
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
		checkFullScanValues(t, ay[1], tp, "0", "1", "5")
	}

	if ay, err := goredis.Values(c.Do("FULLSCAN", "default:testscanmerge:MDpUbEU5UFRwTlFUMDk7MTpUVkU5UFRwTlFUMDk7MjpUVUU5UFRwTlFUMDk7", tp, "count", 6)); err != nil {
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
		checkFullScanValues(t, ay[1], tp, "2", "2", "5", "5", "1", "3")
	}

	if ay, err := goredis.Values(c.Do("FULLSCAN", "default:testscanmerge:MDpUbEU5UFRwTlp6MDk7MTpUWGM5UFRwTlFUMDk7MjpUV2M5UFRwTlVUMDk7", tp, "count", 8)); err != nil {
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
		if len(ay[1].([]interface{})) != 0 {
			checkFullScanValues(t, ay[1], tp, "5", "5", "3", "3", "2", "4")
		}
	}

	if ay, err := goredis.Values(c.Do("FULLSCAN", "default:testscanmerge:MDpUbEU5UFRwT1FUMDk7MTpUWGM5UFRwTlp6MDk7MjpUa0U5UFRwTlFUMDk7", tp, "count", 12)); err != nil {
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
		if len(ay[1].([]interface{})) != 0 {
			checkFullScanValues(t, ay[1], tp, "5", "11", "11", "11", "3", "9", "9", "9", "4", "4", "4", "4")
		}
	}

	if ay, err := goredis.Values(c.Do("FULLSCAN", "default:testscanmerge:MDpUVlJGUFRwTlZFRTk7MTpUMUU5UFRwTlp6MDk7MjpUa0U5UFRwT1FUMDk7", tp, "count", 120)); err != nil {
		t.Fatal(err)
	} else if len(ay) != 2 {
		t.Fatal(len(ay))
	} else if n := ay[0].([]byte); string(n) != "MTpUVlJqUFRwT2R6MDk7MjpUVlJKUFRwTlZFazk7" &&
		string(n) != "MjpUVlJKUFRwTlZFazk7MTpUVlJqUFRwT2R6MDk7" {
		t.Fatal(string(n))
	} else {
		if len(ay[1].([]interface{})) != 0 {
			checkFullScanValues(t, ay[1], tp, "11", "11", "11", "11", "11", "11", "11", "11", "11", "18", "18", "18", "18", "18", "18", "18", "18", "18", "18", "18", "18",
				"18", "18", "18", "18", "18", "18", "18", "9", "9", "9", "9", "9", "9", "9", "16", "16", "16", "16", "16", "16", "16", "16", "16", "16", "16", "16", "16",
				"16", "16", "16", "16", "17", "17", "17", "17", "17", "17", "17", "17", "17", "17", "17", "17", "17", "17", "17", "17", "6", "6", "6", "6", "6", "6", "6",
				"7", "7", "7", "7", "7", "7", "7", "7", "8", "8", "8", "8", "8", "8", "8", "8", "8", "10", "10", "10", "10", "10", "10", "10", "10", "10", "10", "10", "12",
				"12", "12", "12", "12")
		}
	}

	if ay, err := goredis.Values(c.Do("FULLSCAN", "default:testscanmerge:MTpUVlJqUFRwT2R6MDk7MjpUVlJKUFRwTlZFazk7", tp, "count", 10)); err != nil {
		t.Fatal(err)
	} else if len(ay) != 2 {
		t.Fatal(len(ay))
	} else if n := ay[0].([]byte); string(n) != "MjpUVlJKUFRwT1p6MDk7" {
		t.Fatal(string(n))
	} else {
		if len(ay[1].([]interface{})) != 0 {
			checkFullScanValues(t, ay[1], tp, "17", "17", "12", "12", "12", "12", "12")
		}
	}

	if ay, err := goredis.Values(c.Do("FULLSCAN", "default:testscanmerge:MjpUVlJKUFRwT1p6MDk7", tp, "count", 100)); err != nil {
		t.Fatal(err)
	} else if len(ay) != 2 {
		t.Fatal(len(ay))
	} else if n := ay[0].([]byte); string(n) != "" {
		t.Fatal(string(n))
	} else {
		if len(ay[1].([]interface{})) != 0 {
			checkFullScanValues(t, ay[1], tp, "12", "12", "12", "13", "13", "13", "13", "13", "13", "13", "13", "13", "13", "13", "13", "13", "13", "14", "14", "14", "14",
				"14", "14", "14", "14", "14", "14", "14", "14", "14", "14", "14", "15", "15", "15", "15", "15", "15", "15", "15", "15", "15", "15", "15", "15", "15", "15",
				"15", "19", "19", "19", "19", "19", "19", "19", "19", "19", "19", "19", "19", "19", "19", "19", "19", "19", "19", "19", "19")
		}
	}

	if _, err := goredis.Values(c.Do("FULLSCAN", "default::MDpkR1Z6ZEhOallXNDZOQT09OzI6ZEdWemRITmpZVzQ2T1E9PTs=", tp, "count", 8)); err == nil {
		t.Fatal("want err, get nil ")
	}

	if _, err := goredis.Values(c.Do("FULLSCAN", "default:testscan1:MDpkR1Z6ZEhOallXNDZOQT09OzI6ZEdWemRITmpZVzQ2T1E9PTs=", tp, "count", 8)); err == nil {
		t.Fatal("want err, get nil ")
	}

	if _, err := goredis.Values(c.Do("FULLSCAN", "default:testscanmerge:dGVzdHNjYW46NA==", tp, "count", 0)); err == nil {
		t.Fatal("want err, get nil")
	}
}

func checkListFullScan(t *testing.T, c *goredis.PoolConn, tp string) {
	if ay, err := goredis.Values(c.Do("FULLSCAN", "default:testscanmerge:", tp, "count", 5)); err != nil {
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
		checkFullScanValues(t, ay[1], tp, "0", "1", "5")
	}

	if ay, err := goredis.Values(c.Do("FULLSCAN", "default:testscanmerge:MDpUbEU5UFRwSUx5OHZMeTh2THk4dmN6MD07MTpUVkU5UFRwSUx5OHZMeTh2THk4dk9EMD07MjpUVUU5UFRwSlFVRkJRVUZCUVVGQlFUMD07", tp, "count", 6)); err != nil {
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
		checkFullScanValues(t, ay[1], tp, "2", "2", "5", "5", "1", "3")
	}
	if ay, err := goredis.Values(c.Do("FULLSCAN", "default:testscanmerge:MDpUbEU5UFRwSUx5OHZMeTh2THk4dk1EMD07MTpUWGM5UFRwSUx5OHZMeTh2THk4dk1EMD07MjpUV2M5UFRwSUx5OHZMeTh2THk4dk9EMD07", tp, "count", 8)); err != nil {
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
		if len(ay[1].([]interface{})) != 0 {
			checkFullScanValues(t, ay[1], tp, "5", "5", "3", "3", "2", "4")
		}
	}

	if ay, err := goredis.Values(c.Do("FULLSCAN", "default:testscanmerge:MDpUbEU5UFRwSUx5OHZMeTh2THk4dk9EMD07MTpUWGM5UFRwSUx5OHZMeTh2THk4dk9EMD07MjpUa0U5UFRwSUx5OHZMeTh2THk4dmR6MD07", tp, "count", 12)); err != nil {
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
		if len(ay[1].([]interface{})) != 0 {
			checkFullScanValues(t, ay[1], tp, "5", "11", "11", "11", "3", "9", "9", "9", "4", "4", "4", "4")
		}
	}

	if ay, err := goredis.Values(c.Do("FULLSCAN", "default:testscanmerge:MDpUVlJGUFRwSUx5OHZMeTh2THk4dll6MD07MTpUMUU5UFRwSUx5OHZMeTh2THk4dmF6MD07MjpUa0U5UFRwSlFVRkJRVUZCUVVGQlFUMD07", tp, "count", 120)); err != nil {
		t.Fatal(err)
	} else if len(ay) != 2 {
		t.Fatal(len(ay))
	} else if n := ay[0].([]byte); string(n) != "MTpUVlJqUFRwSUx5OHZMeTh2THk4dk5EMD07MjpUVlJKUFRwSUx5OHZMeTh2THk4dlp6MD07" &&
		string(n) != "MjpUVlJKUFRwSUx5OHZMeTh2THk4dlp6MD07MTpUVlJqUFRwSUx5OHZMeTh2THk4dk5EMD07" {
		t.Fatal(string(n))
	} else {
		if len(ay[1].([]interface{})) != 0 {
			checkFullScanValues(t, ay[1], tp, "11", "11", "11", "11", "11", "11", "11", "11", "11", "18", "18", "18", "18", "18", "18", "18", "18", "18", "18", "18", "18",
				"18", "18", "18", "18", "18", "18", "18", "9", "9", "9", "9", "9", "9", "9", "16", "16", "16", "16", "16", "16", "16", "16", "16", "16", "16", "16", "16",
				"16", "16", "16", "16", "17", "17", "17", "17", "17", "17", "17", "17", "17", "17", "17", "17", "17", "17", "17", "17", "6", "6", "6", "6", "6", "6", "6",
				"7", "7", "7", "7", "7", "7", "7", "7", "8", "8", "8", "8", "8", "8", "8", "8", "8", "10", "10", "10", "10", "10", "10", "10", "10", "10", "10", "10", "12",
				"12", "12", "12", "12")
		}
	}

	if ay, err := goredis.Values(c.Do("FULLSCAN", "default:testscanmerge:MTpUVlJqUFRwSUx5OHZMeTh2THk4dk5EMD07MjpUVlJKUFRwSUx5OHZMeTh2THk4dlp6MD07", tp, "count", 10)); err != nil {
		t.Fatal(err)
	} else if len(ay) != 2 {
		t.Fatal(len(ay))
	} else if n := ay[0].([]byte); string(n) != "MjpUVlJKUFRwSUx5OHZMeTh2THk4dk1EMD07" {
		t.Fatal(string(n))
	} else {
		if len(ay[1].([]interface{})) != 0 {
			checkFullScanValues(t, ay[1], tp, "17", "17", "12", "12", "12", "12", "12")
		}
	}

	if ay, err := goredis.Values(c.Do("FULLSCAN", "default:testscanmerge:MjpUVlJKUFRwSUx5OHZMeTh2THk4dk1EMD07", tp, "count", 100)); err != nil {
		t.Fatal(err)
	} else if len(ay) != 2 {
		t.Fatal(len(ay))
	} else if n := ay[0].([]byte); string(n) != "" {
		t.Fatal(string(n))
	} else {
		if len(ay[1].([]interface{})) != 0 {
			checkFullScanValues(t, ay[1], tp, "12", "12", "12", "13", "13", "13", "13", "13", "13", "13", "13", "13", "13", "13", "13", "13", "13", "14", "14", "14", "14",
				"14", "14", "14", "14", "14", "14", "14", "14", "14", "14", "14", "15", "15", "15", "15", "15", "15", "15", "15", "15", "15", "15", "15", "15", "15", "15",
				"15", "19", "19", "19", "19", "19", "19", "19", "19", "19", "19", "19", "19", "19", "19", "19", "19", "19", "19", "19", "19")
		}
	}

	if _, err := goredis.Values(c.Do("FULLSCAN", "default::MDpkR1Z6ZEhOallXNDZOQT09OzI6ZEdWemRITmpZVzQ2T1E9PTs=", tp, "count", 8)); err == nil {
		t.Fatal("want err, get nil ")
	}

	if _, err := goredis.Values(c.Do("FULLSCAN", "default:testscan1:MDpkR1Z6ZEhOallXNDZOQT09OzI6ZEdWemRITmpZVzQ2T1E9PTs=", tp, "count", 8)); err == nil {
		t.Fatal("want err, get nil ")
	}

	if _, err := goredis.Values(c.Do("FULLSCAN", "default:testscanmerge:dGVzdHNjYW46NA==", tp, "count", 0)); err == nil {
		t.Fatal("want err, get nil")
	}
}

func checkSetFullScan(t *testing.T, c *goredis.PoolConn, tp string) {
	if ay, err := goredis.Values(c.Do("FULLSCAN", "default:testscanmerge:", tp, "count", 5)); err != nil {
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
		checkFullScanValues(t, ay[1], tp, "0", "1", "5")
	}

	if ay, err := goredis.Values(c.Do("FULLSCAN", "default:testscanmerge:MDpUbEU5UFRwa2JVWnpaRmRXWms1V09IYz07MTpUVkU5UFRwa2JVWnpaRmRXWmsxV09IYz07MjpUVUU5UFRwa2JVWnpaRmRXWmsxR09IYz07", tp, "count", 6)); err != nil {
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
		checkFullScanValues(t, ay[1], tp, "2", "2", "5", "5", "1", "3")
	}
	if ay, err := goredis.Values(c.Do("FULLSCAN", "default:testscanmerge:MDpUbEU5UFRwa2JVWnpaRmRXWms1V09Iaz07MTpUWGM5UFRwa2JVWnpaRmRXWmsweE9IYz07MjpUV2M5UFRwa2JVWnpaRmRXWmsxc09IZz07", tp, "count", 8)); err != nil {
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
		if len(ay[1].([]interface{})) != 0 {
			checkFullScanValues(t, ay[1], tp, "5", "5", "3", "3", "2", "4")
		}
	}

	if ay, err := goredis.Values(c.Do("FULLSCAN", "default:testscanmerge:MDpUbEU5UFRwa2JVWnpaRmRXWms1V09EQT07MTpUWGM5UFRwa2JVWnpaRmRXWmsweE9Iaz07MjpUa0U5UFRwa2JVWnpaRmRXWms1R09IYz07", tp, "count", 12)); err != nil {
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
		if len(ay[1].([]interface{})) != 0 {
			checkFullScanValues(t, ay[1], tp, "5", "11", "11", "11", "3", "9", "9", "9", "4", "4", "4", "4")
		}
	}

	if ay, err := goredis.Values(c.Do("FULLSCAN", "default:testscanmerge:MDpUVlJGUFRwa2JVWnpaRmRXWmsxVVJtWk5WRUU5OzE6VDFFOVBUcGtiVVp6WkZkV1prOVdPSGs9OzI6VGtFOVBUcGtiVVp6WkZkV1prNUdPREE9Ow==", tp, "count", 120)); err != nil {
		t.Fatal(err)
	} else if len(ay) != 2 {
		t.Fatal(len(ay))
	} else if n := ay[0].([]byte); string(n) != "MTpUVlJqUFRwa2JVWnpaRmRXWmsxVVpHWk9kejA5OzI6VFZSSlBUcGtiVVp6WkZkV1prMVVTbVpOVkVrOTs=" &&
		string(n) != "MjpUVlJKUFRwa2JVWnpaRmRXWmsxVVNtWk5WRWs5OzE6VFZSalBUcGtiVVp6WkZkV1prMVVaR1pPZHowOTs=" {
		t.Fatal(string(n))
	} else {
		if len(ay[1].([]interface{})) != 0 {
			checkFullScanValues(t, ay[1], tp, "11", "11", "11", "11", "11", "11", "11", "11", "11", "18", "18", "18", "18", "18", "18", "18", "18", "18", "18", "18", "18",
				"18", "18", "18", "18", "18", "18", "18", "9", "9", "9", "9", "9", "9", "9", "16", "16", "16", "16", "16", "16", "16", "16", "16", "16", "16", "16", "16",
				"16", "16", "16", "16", "17", "17", "17", "17", "17", "17", "17", "17", "17", "17", "17", "17", "17", "17", "17", "17", "6", "6", "6", "6", "6", "6", "6",
				"7", "7", "7", "7", "7", "7", "7", "7", "8", "8", "8", "8", "8", "8", "8", "8", "8", "10", "10", "10", "10", "10", "10", "10", "10", "10", "10", "10", "12",
				"12", "12", "12", "12")
		}
	}

	if ay, err := goredis.Values(c.Do("FULLSCAN", "default:testscanmerge:MTpUVlJqUFRwa2JVWnpaRmRXWmsxVVpHWk9kejA5OzI6VFZSSlBUcGtiVVp6WkZkV1prMVVTbVpOVkVrOTs=", tp, "count", 10)); err != nil {
		t.Fatal(err)
	} else if len(ay) != 2 {
		t.Fatal(len(ay))
	} else if n := ay[0].([]byte); string(n) != "MjpUVlJKUFRwa2JVWnpaRmRXWmsxVVNtWk9aejA5Ow==" {
		t.Fatal(string(n))
	} else {
		if len(ay[1].([]interface{})) != 0 {
			checkFullScanValues(t, ay[1], tp, "17", "17", "12", "12", "12", "12", "12")
		}
	}

	if ay, err := goredis.Values(c.Do("FULLSCAN", "default:testscanmerge:MjpUVlJKUFRwa2JVWnpaRmRXWmsxVVNtWk9aejA5Ow==", tp, "count", 100)); err != nil {
		t.Fatal(err)
	} else if len(ay) != 2 {
		t.Fatal(len(ay))
	} else if n := ay[0].([]byte); string(n) != "" {
		t.Fatal(string(n))
	} else {
		if len(ay[1].([]interface{})) != 0 {
			checkFullScanValues(t, ay[1], tp, "12", "12", "12", "13", "13", "13", "13", "13", "13", "13", "13", "13", "13", "13", "13", "13", "13", "14", "14", "14", "14",
				"14", "14", "14", "14", "14", "14", "14", "14", "14", "14", "14", "15", "15", "15", "15", "15", "15", "15", "15", "15", "15", "15", "15", "15", "15", "15",
				"15", "19", "19", "19", "19", "19", "19", "19", "19", "19", "19", "19", "19", "19", "19", "19", "19", "19", "19", "19", "19")
		}
	}

	if _, err := goredis.Values(c.Do("FULLSCAN", "default::MDpkR1Z6ZEhOallXNDZOQT09OzI6ZEdWemRITmpZVzQ2T1E9PTs=", tp, "count", 8)); err == nil {
		t.Fatal("want err, get nil ")
	}

	if _, err := goredis.Values(c.Do("FULLSCAN", "default:testscan1:MDpkR1Z6ZEhOallXNDZOQT09OzI6ZEdWemRITmpZVzQ2T1E9PTs=", tp, "count", 8)); err == nil {
		t.Fatal("want err, get nil ")
	}

	if _, err := goredis.Values(c.Do("FULLSCAN", "default:testscanmerge:dGVzdHNjYW46NA==", tp, "count", 0)); err == nil {
		t.Fatal("want err, get nil")
	}

}

func checkZSetFullScan(t *testing.T, c *goredis.PoolConn, tp string) {
}

func TestFullScan(t *testing.T) {
	c := getMergeTestConn(t)
	defer c.Close()

	//testKVFullScan(t, c)
	//testHashFullScan(t, c)
	//testListFullScan(t, c)
	testSetFullScan(t, c)
	//		testZSetFullScan(t, c)
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
	checkKVFullScan(t, c, "KV")
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
	checkHashFullScan(t, c, "HASH")
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

	checkListFullScan(t, c, "LIST")

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

	checkSetFullScan(t, c, "SET")
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

	checkZSetFullScan(t, c, "ZSET")

	fmt.Println("ZSet FullScan success")
}
