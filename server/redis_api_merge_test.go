package server

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"path"
	"strconv"
	"strings"
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

func checkKVBackupValues(t *testing.T, ay interface{}, values []interface{}) {
	a, err := goredis.Strings(ay, nil)
	if err != nil {
		t.Fatal(err)
	}

	if len(a) != len(values)*2 {
		t.Fatal(fmt.Sprintf("len %d != %d", len(a), len(values)*2))
	}
	var equalCount int
	success := true
	length := len(a)
FATAL:
	for _, val := range values {

		for i := 0; i < length; i = i + 2 {
			k := a[i]
			if val.(string) == k {
				equalCount++
				splits := strings.SplitN(k, ":", 2)
				if len(splits) != 2 {
					success = false
					break FATAL
				}
				v := fmt.Sprintf("value_%s", splits[1])
				if v != a[i+1] {
					success = false
					break FATAL
				}
			}
		}
	}
	if !success {
		t.Fatal("failed")
	}
	if equalCount != len(values) {
		t.Fatal("equal count not equal")
	}
}

func checkListBackupValues(t *testing.T, ay interface{}, values []interface{}) {
	a, err := goredis.MultiBulk(ay, nil)
	if err != nil {
		t.Fatal(err)
	}

	if len(a) != len(values)*2 {
		t.Fatal(fmt.Sprintf("len %d != %d", len(a), len(values)*2))
	}

	var equalCount int
	success := true
	length := len(a)
FATAL:
	for _, val := range values {
		for i := 0; i < length; i = i + 2 {
			k := a[i].([]byte)
			if val.(string) == string(k) {
				equalCount++
				splits := bytes.SplitN(k, []byte(":"), 2)
				if len(splits) != 2 {
					success = false
					break FATAL
				}

				v := a[i+1].([]interface{})
				if string(splits[1]) != string(v[0].([]byte)) {
					success = false
					break FATAL
				}
			}
		}
	}
	if !success {
		t.Fatal("failed")
	}
	if equalCount != len(values) {
		t.Fatal("equal count not equal")
	}

}

func checkHashBackupValues(t *testing.T, ay interface{}, values []interface{}) {
	a, err := goredis.MultiBulk(ay, nil)
	if err != nil {
		t.Fatal(err)
	}

	if len(a) != len(values)*2 {
		t.Fatal(fmt.Sprintf("len %d != %d", len(a), len(values)*2))
	}

	var equalCount int
	success := true
	length := len(a)
FATAL:
	for _, val := range values {
		for i := 0; i < length; i = i + 2 {
			k := a[i].([]byte)
			if val.(string) == string(k) {
				equalCount++
				splits := bytes.SplitN(k, []byte(":"), 2)
				if len(splits) != 2 {
					success = false
					break FATAL
				}

				v := a[i+1].([]interface{})
				if string(splits[1]) != string(v[0].([]byte)) {
					t.Fatal("hash key error")
				}

				hashvalue := fmt.Sprintf("value_%s", splits[1])
				if hashvalue != string(v[1].([]byte)) {
					success = false
					break FATAL
				}

			}
		}
	}
	if !success {
		t.Fatal("failed")
	}
	if equalCount != len(values) {
		t.Fatal("equal count not equal")
	}

}

func checkZSetBackupValues(t *testing.T, ay interface{}, values []interface{}) {
	a, err := goredis.MultiBulk(ay, nil)
	if err != nil {
		t.Fatal(err)
	}

	if len(a) != len(values)*2 {
		t.Fatal(fmt.Sprintf("len %d != %d", len(a), len(values)*2))
	}

	var equalCount int
	success := true
	length := len(a)
FATAL:
	for _, val := range values {
		for i := 0; i < length; i = i + 2 {
			k := a[i].([]byte)
			if val.(string) == string(k) {
				equalCount++
				splits := bytes.SplitN(k, []byte(":"), 2)
				if len(splits) != 2 {
					success = false
					break FATAL
				}
				v := a[i+1].([]interface{})
				member := fmt.Sprintf("value_%s", splits[1])
				if string(v[0].([]byte)) != member {
					success = false
					break FATAL
				}

				if !bytes.Equal(splits[1], v[1].([]byte)) {
					success = false
					break FATAL
				}

			}
		}
	}
	if !success {
		t.Fatal("failed")
	}
	if equalCount != len(values) {
		t.Fatal("equal count not equal")
	}

}

func checkBackupValues(t *testing.T, ay interface{}, tp string, values ...interface{}) {
	switch tp {
	case "KV":
		checkKVBackupValues(t, ay, values)
	case "LIST", "SET":
		checkListBackupValues(t, ay, values)
	case "HASH":
		checkHashBackupValues(t, ay, values)
	case "ZSET":
		checkZSetBackupValues(t, ay, values)
	}

}

func checkBackup(t *testing.T, c *goredis.PoolConn, tp string) {
	if ay, err := goredis.Values(c.Do("BACKUP", "default:testscanmerge:", tp, "count", 5)); err != nil {
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
		checkBackupValues(t, ay[1], tp, "testscanmerge:1", "testscanmerge:11", "testscanmerge:0")
	}

	if ay, err := goredis.Values(c.Do("BACKUP", "default:testscanmerge:MDpkR1Z6ZEhOallXNXRaWEpuWlRveE1RPT07MTpkR1Z6ZEhOallXNXRaWEpuWlRveDsyOmRHVnpkSE5qWVc1dFpYSm5aVG93Ow==", tp, "count", 6)); err != nil {
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
		checkBackupValues(t, ay[1], tp, "testscanmerge:5", "testscanmerge:10", "testscanmerge:12", "testscanmerge:16", "testscanmerge:17", "testscanmerge:18")
	}

	if ay, err := goredis.Values(c.Do("BACKUP", "default:testscanmerge:MDpkR1Z6ZEhOallXNXRaWEpuWlRvMTsxOmRHVnpkSE5qWVc1dFpYSm5aVG94Tnc9PTsyOmRHVnpkSE5qWVc1dFpYSm5aVG94TWc9PTs=", tp, "count", 8)); err != nil {
		t.Fatal(err)
	} else if len(ay) != 2 {
		t.Fatal(len(ay))
	} else if n := ay[0].([]byte); string(n) != "MTpkR1Z6ZEhOallXNXRaWEpuWlRvNTsyOmRHVnpkSE5qWVc1dFpYSm5aVG94TkE9PTs=" &&
		string(n) != "MjpkR1Z6ZEhOallXNXRaWEpuWlRveE5BPT07MTpkR1Z6ZEhOallXNXRaWEpuWlRvNTs=" {
		t.Fatal(string(n))
	} else {
		if len(ay[1].([]interface{})) != 0 {
			checkBackupValues(t, ay[1], tp, "testscanmerge:3", "testscanmerge:9", "testscanmerge:13", "testscanmerge:14")
		}
	}

	if ay, err := goredis.Values(c.Do("BACKUP", "default:testscanmerge:MTpkR1Z6ZEhOallXNXRaWEpuWlRvNTsyOmRHVnpkSE5qWVc1dFpYSm5aVG94TkE9PTs=", tp, "count", 5)); err != nil {
		t.Fatal(err)
	} else if len(ay) != 2 {
		t.Fatal(len(ay))
	} else if n := ay[0].([]byte); string(n) != "MjpkR1Z6ZEhOallXNXRaWEpuWlRveE9RPT07" {
		t.Fatal(string(n))
	} else {
		if len(ay[1].([]interface{})) != 0 {
			checkBackupValues(t, ay[1], tp, "testscanmerge:15", "testscanmerge:19")
		}
	}

	if ay, err := goredis.Values(c.Do("BACKUP", "default:testscanmerge:MjpkR1Z6ZEhOallXNXRaWEpuWlRveE9RPT07", tp, "count", 8)); err != nil {
		t.Fatal(err)
	} else if len(ay) != 2 {
		t.Fatal(len(ay))
	} else if n := ay[0].([]byte); string(n) != "" &&
		string(n) != "" {
		t.Fatal(string(n))
	} else {
		if len(ay[1].([]interface{})) != 0 {
			checkBackupValues(t, ay[1], tp, "testscanmerge:2", "testscanmerge:4", "testscanmerge:6", "testscanmerge:7", "testscanmerge:8")
		}
	}

	if _, err := goredis.Values(c.Do("BACKUP", "default::MDpkR1Z6ZEhOallXNDZOQT09OzI6ZEdWemRITmpZVzQ2T1E9PTs=", tp, "count", 8)); err == nil {
		t.Fatal("want err, get nil ")
	}

	if _, err := goredis.Values(c.Do("BACKUP", "default:testscan1:MDpkR1Z6ZEhOallXNDZOQT09OzI6ZEdWemRITmpZVzQ2T1E9PTs=", tp, "count", 8)); err == nil {
		t.Fatal("want err, get nil ")
	}

	if _, err := goredis.Values(c.Do("BACKUP", "default:testscanmerge:dGVzdHNjYW46NA==", tp, "count", 0)); err == nil {
		t.Fatal("want err, get nil")
	}
}

func TestBackup(t *testing.T) {
	c := getMergeTestConn(t)
	defer c.Close()

	testKVBackup(t, c)
	testHashBackup(t, c)
	testListBackup(t, c)
	testSetBackup(t, c)
	testZSetBackup(t, c)
}

func testKVBackup(t *testing.T, c *goredis.PoolConn) {
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
	checkBackup(t, c, "KV")
	fmt.Println("KV Backup success")
}

func testHashBackup(t *testing.T, c *goredis.PoolConn) {
	for i := 0; i < 20; i++ {
		value := fmt.Sprintf("value_%d", i)
		if _, err := c.Do("hset", "default:testscanmerge:"+fmt.Sprintf("%d", i), fmt.Sprintf("%d", i), []byte(value)); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 20; i++ {
		value := fmt.Sprintf("value_%d", i)
		if _, err := c.Do("hset", "default:testscanmerge1:"+fmt.Sprintf("%d", i), fmt.Sprintf("%d", i), []byte(value)); err != nil {
			t.Fatal(err)
		}
	}
	checkBackup(t, c, "HASH")
	fmt.Println("Hash Backup success")
}

func testListBackup(t *testing.T, c *goredis.PoolConn) {
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

	checkBackup(t, c, "LIST")

	fmt.Println("List Backup success")
}

func testSetBackup(t *testing.T, c *goredis.PoolConn) {
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

	checkBackup(t, c, "SET")
	fmt.Println("Set Backup success")
}

func testZSetBackup(t *testing.T, c *goredis.PoolConn) {
	for i := 0; i < 20; i++ {
		value := fmt.Sprintf("value_%d", i)
		if _, err := c.Do("zadd", "default:testscanmerge:"+fmt.Sprintf("%d", i), i, []byte(value)); err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < 20; i++ {
		value := fmt.Sprintf("value_%d", i)
		if _, err := c.Do("zadd", "default:testscanmerge1:"+fmt.Sprintf("%d", i), i, []byte(value)); err != nil {
			t.Fatal(err)
		}
	}

	checkBackup(t, c, "ZSET")

	fmt.Println("ZSet Backup success")
}
