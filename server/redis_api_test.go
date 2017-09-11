package server

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"path"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/absolute8511/ZanRedisDB/node"
	"github.com/absolute8511/ZanRedisDB/rockredis"
	"github.com/siddontang/goredis"
	"github.com/stretchr/testify/assert"
)

var testOnce sync.Once
var kvs *Server
var redisport int
var OK = "OK"

func startTestServer(t *testing.T) (*Server, int, string) {
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("rocksdb-test-%d", time.Now().UnixNano()))
	assert.Nil(t, err)
	t.Logf("dir:%v\n", tmpDir)
	ioutil.WriteFile(
		path.Join(tmpDir, "myid"),
		[]byte(strconv.FormatInt(int64(1), 10)),
		common.FILE_PERM)
	raftAddr := "http://127.0.0.1:12345"
	redisport := 22345
	var replica node.ReplicaInfo
	replica.NodeID = 1
	replica.ReplicaID = 1
	replica.RaftAddr = raftAddr
	kvOpts := ServerConfig{
		ClusterID:     "test",
		DataDir:       tmpDir,
		RedisAPIPort:  redisport,
		LocalRaftAddr: raftAddr,
		BroadcastAddr: "127.0.0.1",
		TickMs:        100,
		ElectionTick:  5,
	}
	if testing.Verbose() {
		rockredis.SetLogLevel(4)
		node.SetLogLevel(4)
	}
	nsConf := node.NewNSConfig()
	nsConf.Name = "default-0"
	nsConf.BaseName = "default"
	nsConf.EngType = rockredis.EngType
	nsConf.PartitionNum = 1
	nsConf.Replicator = 1
	nsConf.RaftGroupConf.GroupID = 1000
	nsConf.RaftGroupConf.SeedNodes = append(nsConf.RaftGroupConf.SeedNodes, replica)
	nsConf.ExpirationPolicy = "consistency_deletion"
	kv := NewServer(kvOpts)
	if _, err := kv.InitKVNamespace(1, nsConf, false); err != nil {
		t.Fatalf("failed to init namespace: %v", err)
	}

	kv.Start()
	time.Sleep(time.Second * 3)
	return kv, redisport, tmpDir
}

func getTestConn(t *testing.T) *goredis.PoolConn {
	testOnce.Do(func() {
		kvs, redisport, _ = startTestServer(t)
	},
	)
	c := goredis.NewClient("127.0.0.1:"+strconv.Itoa(redisport), "")
	c.SetMaxIdleConns(4)
	conn, err := c.Get()
	if err != nil {
		t.Fatal(err)
	}
	return conn
}

func TestKV(t *testing.T) {
	c := getTestConn(t)
	defer c.Close()

	key1 := "default:test:a"
	key2 := "default:test:b"
	keyExpire := "default:test:xx"
	if ok, err := goredis.String(c.Do("set", key1, "1234")); err != nil {
		t.Fatal(err)
	} else if ok != OK {
		t.Fatal(ok)
	}

	if n, err := goredis.Int(c.Do("setnx", key1, "123")); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	if n, err := goredis.Int(c.Do("setnx", key2, "123")); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if ok, err := goredis.String(c.Do("setex", keyExpire, 1, "hello world")); err != nil {
		t.Fatal(err)
	} else if ok != OK {
		t.Fatal(ok)
	}
	if v, err := goredis.String(c.Do("get", keyExpire)); err != nil {
		t.Fatal(err)
	} else if v != "hello world" {
		t.Fatal(v)
	}

	time.Sleep(time.Second * 4)
	if v, err := goredis.String(c.Do("get", keyExpire)); err != goredis.ErrNil {
		if err == nil && v == "hello world" {
			time.Sleep(time.Second * 16)
			if v, err := goredis.String(c.Do("get", keyExpire)); err != goredis.ErrNil {
				t.Fatalf("expired key should be expired: %v, %v", v, err)
			}
		} else {
			t.Fatalf("get expired key error: %v, %v", v, err)
		}
	}

	if v, err := goredis.String(c.Do("get", key1)); err != nil {
		t.Fatal(err)
	} else if v != "1234" {
		t.Fatal(v)
	}

	//if v, err := goredis.String(c.Do("getset", "a", "123")); err != nil {
	//	t.Fatal(err)
	//} else if v != "1234" {
	//	t.Fatal(v)
	//}

	//if v, err := goredis.String(c.Do("get", "a")); err != nil {
	//	t.Fatal(err)
	//} else if v != "123" {
	//	t.Fatal(v)
	//}

	if n, err := goredis.Int(c.Do("exists", key1)); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if n, err := goredis.Int(c.Do("exists", "default:test:empty_key_test")); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	if _, err := goredis.Int(c.Do("del", key1, key2)); err != nil {
		t.Fatal(err)
	}

	if n, err := goredis.Int(c.Do("exists", key1)); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	if n, err := goredis.Int(c.Do("exists", key2)); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}
}

func TestKVM(t *testing.T) {
	c := getTestConn(t)
	defer c.Close()

	key1 := "default:test:kvma"
	key2 := "default:test:kvmb"
	key3 := "default:test:kvmc"
	if ok, err := goredis.String(c.Do("mset", key1, "1", key2, "2")); err != nil {
		t.Fatal(err)
	} else if ok != OK {
		t.Fatal(ok)
	}

	if v, err := goredis.String(c.Do("get", key1)); err != nil {
		t.Fatal(err)
	} else if v != "1" {
		t.Error(v)
	}
	if v, err := goredis.String(c.Do("get", key2)); err != nil {
		t.Fatal(err)
	} else if v != "2" {
		t.Error(v)
	}

	if v, err := goredis.MultiBulk(c.Do("mget", key1, key2, key3)); err != nil {
		t.Fatal(err)
	} else if len(v) != 3 {
		t.Fatal(len(v))
	} else {
		if vv, ok := v[0].([]byte); !ok || string(vv) != "1" {
			t.Fatalf("not 1, %v", v)
		}

		if vv, ok := v[1].([]byte); !ok || string(vv) != "2" {
			t.Errorf("not 2, %v", v[1])
		}

		if v[2] != nil {
			t.Errorf("must nil: %v", v[2])
		}
	}
}

func TestKVIncrDecr(t *testing.T) {
	c := getTestConn(t)
	defer c.Close()

	key := "default:test:kv_n"
	if n, err := goredis.Int64(c.Do("incr", key)); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if n, err := goredis.Int64(c.Do("incr", key)); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	//if n, err := goredis.Int64(c.Do("incrby", "n", 10)); err != nil {
	//	t.Fatal(err)
	//} else if n != 11 {
	//	t.Fatal(n)
	//}

	//if n, err := goredis.Int64(c.Do("decrby", "n", 10)); err != nil {
	//	t.Fatal(err)
	//} else if n != 1 {
	//	t.Fatal(n)
	//}
}

func TestKVBatch(t *testing.T) {
	if testing.Verbose() {
		node.SetLogLevel(int(common.LOG_DETAIL))
	}
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			c := getTestConn(t)
			defer c.Close()

			key1 := "default:test:a" + strconv.Itoa(index)
			key2 := "default:test:b" + strconv.Itoa(index)
			key3 := "default:test:c" + strconv.Itoa(index)
			key4 := "default:test:d" + strconv.Itoa(index)
			keyExpire := "default:test:xx" + strconv.Itoa(index)
			if ok, err := goredis.String(c.Do("set", key1, "1234")); err != nil {
				t.Fatal(err)
			} else if ok != OK {
				t.Fatal(ok)
			}

			if n, err := goredis.Int(c.Do("setnx", key1, "123")); err != nil {
				t.Fatal(err)
			} else if n != 0 {
				t.Fatal(n)
			}

			if n, err := goredis.Int(c.Do("setnx", key2, "123")); err != nil {
				t.Fatal(err)
			} else if n != 1 {
				t.Fatal(n)
			}

			if ok, err := goredis.String(c.Do("set", key3, key3)); err != nil {
				t.Fatal(err)
			} else if ok != OK {
				t.Fatal(ok)
			}
			if v, err := goredis.String(c.Do("get", key3)); err != nil {
				t.Fatal(err)
			} else if v != key3 {
				t.Fatal(v)
			}

			if ok, err := goredis.String(c.Do("setex", keyExpire, 1, "hello world")); err != nil {
				t.Fatal(err)
			} else if ok != OK {
				t.Fatal(ok)
			}
			if v, err := goredis.String(c.Do("get", keyExpire)); err != nil {
				t.Fatal(err)
			} else if v != "hello world" {
				t.Fatal(v)
			}

			if ok, err := goredis.String(c.Do("set", key4, key4)); err != nil {
				t.Fatal(err)
			} else if ok != OK {
				t.Fatal(ok)
			}
			if v, err := goredis.String(c.Do("get", key4)); err != nil {
				t.Fatal(err)
			} else if v != key4 {
				t.Fatal(v)
			}

			mkey1 := "default:test:kvma" + strconv.Itoa(index)
			mkey2 := "default:test:kvmb" + strconv.Itoa(index)
			mkey3 := "default:test:kvmc" + strconv.Itoa(index)
			if ok, err := goredis.String(c.Do("mset", mkey1, "1", mkey2, "2")); err != nil {
				t.Fatal(err)
			} else if ok != OK {
				t.Fatal(ok)
			}

			if v, err := goredis.String(c.Do("get", mkey1)); err != nil {
				t.Fatal(err)
			} else if v != "1" {
				t.Error(v)
			}
			if v, err := goredis.String(c.Do("get", mkey2)); err != nil {
				t.Fatal(err)
			} else if v != "2" {
				t.Error(v)
			}

			if v, err := goredis.MultiBulk(c.Do("mget", mkey1, mkey2, mkey3)); err != nil {
				t.Fatal(err)
			} else if len(v) != 3 {
				t.Fatal(len(v))
			} else {
				if vv, ok := v[0].([]byte); !ok || string(vv) != "1" {
					t.Fatalf("not 1, %v", v)
				}

				if vv, ok := v[1].([]byte); !ok || string(vv) != "2" {
					t.Errorf("not 2, %v", v[1])
				}

				if v[2] != nil {
					t.Errorf("must nil: %v", v[2])
				}
			}

			time.Sleep(time.Second * 4)
			if v, err := goredis.String(c.Do("get", keyExpire)); err != goredis.ErrNil {
				if err == nil && v == "hello world" {
					time.Sleep(time.Second * 16)
					if v, err := goredis.String(c.Do("get", keyExpire)); err != goredis.ErrNil {
						t.Fatalf("expired key should be expired: %v, %v", v, err)
					}
				} else {
					t.Fatalf("get expired key error: %v, %v", v, err)
				}
			}

			if v, err := goredis.String(c.Do("get", key1)); err != nil {
				t.Fatal(err)
			} else if v != "1234" {
				t.Fatal(v)
			}

			if n, err := goredis.Int(c.Do("exists", key1)); err != nil {
				t.Fatal(err)
			} else if n != 1 {
				t.Fatal(n)
			}

			if n, err := goredis.Int(c.Do("exists", "default:test:empty_key_test"+strconv.Itoa(index))); err != nil {
				t.Fatal(err)
			} else if n != 0 {
				t.Fatal(n)
			}

			if _, err := goredis.Int(c.Do("del", key1, key2)); err != nil {
				t.Fatal(err)
			}

			if n, err := goredis.Int(c.Do("exists", key1)); err != nil {
				t.Fatal(err)
			} else if n != 0 {
				t.Fatal(n)
			}

			if n, err := goredis.Int(c.Do("exists", key2)); err != nil {
				t.Fatal(err)
			} else if n != 0 {
				t.Fatal(n)
			}
		}(i)
	}
	wg.Wait()
}

func TestKVErrorParams(t *testing.T) {
	c := getTestConn(t)
	defer c.Close()

	key1 := "default:test:kv_erra"
	key2 := "default:test:kv_errb"
	key3 := "default:test:kv_errc"
	_, err := c.Do("get", key1, key2, key3)
	assert.NotNil(t, err)

	_, err = c.Do("set", key1, key2, key3)
	assert.NotNil(t, err)

	_, err = c.Do("getset", key1, key2, key3)
	assert.NotNil(t, err)

	_, err = c.Do("setnx", key1, key2, key3)
	assert.NotNil(t, err)

	_, err = c.Do("exists", key1, key2)
	assert.NotNil(t, err)

	_, err = c.Do("incr", key1, key2)
	assert.NotNil(t, err)

	_, err = c.Do("incrby", key1)
	assert.NotNil(t, err)

	_, err = c.Do("decrby", key1)
	assert.NotNil(t, err)

	_, err = c.Do("del")
	assert.NotNil(t, err)

	_, err = c.Do("mset")
	assert.NotNil(t, err)

	_, err = c.Do("mset", key1, key2, key3)
	assert.NotNil(t, err)

	_, err = c.Do("mget")
	assert.NotNil(t, err)
}

func TestHash(t *testing.T) {
	c := getTestConn(t)
	defer c.Close()

	key := "default:test:hasha"
	//if n, err := goredis.Int(c.Do("hkeyexists", key)); err != nil {
	//	t.Fatal(err)
	//} else if n != 0 {
	//	t.Fatal(n)
	//}

	if n, err := goredis.Int(c.Do("hset", key, 1, 0)); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}
	//if n, err := goredis.Int(c.Do("hkeyexists", key)); err != nil {
	//	t.Fatal(err)
	//} else if n != 1 {
	//	t.Fatal(n)
	//}

	if n, err := goredis.Int(c.Do("hexists", key, 1)); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if n, err := goredis.Int(c.Do("hexists", key, -1)); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	if n, err := goredis.Int(c.Do("hget", key, 1)); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	if n, err := goredis.Int(c.Do("hset", key, 1, 1)); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	if n, err := goredis.Int(c.Do("hget", key, 1)); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if n, err := goredis.Int(c.Do("hlen", key)); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}
}

func testHashArray(ay []interface{}, checkValues ...int) error {
	if len(ay) != len(checkValues) {
		return fmt.Errorf("invalid return number %d != %d", len(ay), len(checkValues))
	}

	for i := 0; i < len(ay); i++ {
		if ay[i] == nil && checkValues[i] != 0 {
			return fmt.Errorf("must nil")
		} else if ay[i] != nil {
			v, ok := ay[i].([]byte)
			if !ok {
				return fmt.Errorf("invalid return data %d %v :%T", i, ay[i], ay[i])
			}

			d, _ := strconv.Atoi(string(v))

			if d != checkValues[i] {
				return fmt.Errorf("invalid data %d %s != %d", i, v, checkValues[i])
			}
		}
	}
	return nil
}

func TestHashM(t *testing.T) {
	c := getTestConn(t)
	defer c.Close()

	key := "default:test:msetb"
	if ok, err := goredis.String(c.Do("hmset", key, 1, 1, 2, 2, 3, 3)); err != nil {
		t.Fatal(err)
	} else if ok != OK {
		t.Fatal(ok)
	}

	if n, err := goredis.Int(c.Do("hlen", key)); err != nil {
		t.Fatal(err)
	} else if n != 3 {
		t.Fatal(n)
	}

	if v, err := goredis.MultiBulk(c.Do("hmget", key, 1, 2, 3, 4)); err != nil {
		t.Fatal(err)
	} else {
		if err := testHashArray(v, 1, 2, 3, 0); err != nil {
			t.Fatal(err)
		}
	}

	if n, err := goredis.Int(c.Do("hdel", key, 1, 2, 3, 4)); err != nil {
		t.Fatal(err)
	} else if n != 3 {
		t.Fatal(n)
	}

	if n, err := goredis.Int(c.Do("hlen", key)); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	if v, err := goredis.MultiBulk(c.Do("hmget", key, 1, 2, 3, 4)); err != nil {
		t.Fatal(err)
	} else {
		if err := testHashArray(v, 0, 0, 0, 0); err != nil {
			t.Fatal(err)
		}
	}

	if n, err := goredis.Int(c.Do("hlen", key)); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}
}

func TestHashIncr(t *testing.T) {
	c := getTestConn(t)
	defer c.Close()

	key := "default:test:hashincr-c"
	if n, err := goredis.Int(c.Do("hincrby", key, 1, 1)); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(err)
	}

	if n, err := goredis.Int(c.Do("hlen", key)); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if n, err := goredis.Int(c.Do("hincrby", key, 1, 10)); err != nil {
		t.Fatal(err)
	} else if n != 11 {
		t.Fatal(err)
	}

	if n, err := goredis.Int(c.Do("hlen", key)); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if n, err := goredis.Int(c.Do("hincrby", key, 1, -11)); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(err)
	}
}

func TestHashGetAll(t *testing.T) {
	c := getTestConn(t)
	defer c.Close()

	key := "default:test:hgetalld"

	if ok, err := goredis.String(c.Do("hmset", key, 1, 1, 2, 2, 3, 3)); err != nil {
		t.Fatal(err)
	} else if ok != OK {
		t.Fatal(ok)
	}

	if v, err := goredis.MultiBulk(c.Do("hgetall", key)); err != nil {
		t.Fatal(err)
	} else {
		if err := testHashArray(v, 1, 1, 2, 2, 3, 3); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := goredis.MultiBulk(c.Do("hkeys", key)); err != nil {
		t.Fatal(err)
	} else {
		if err := testHashArray(v, 1, 2, 3); err != nil {
			t.Fatal(err)
		}
	}

	//if v, err := goredis.MultiBulk(c.Do("hvals", key)); err != nil {
	//	t.Fatal(err)
	//} else {
	//	if err := testHashArray(v, 1, 2, 3); err != nil {
	//		t.Fatal(err)
	//	}
	//}

	if n, err := goredis.Int(c.Do("hclear", key)); err != nil {
		t.Fatal(err)
	} else if n != 3 {
		t.Fatal(n)
	}

	if n, err := goredis.Int(c.Do("hlen", key)); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}
}

func TestHashErrorParams(t *testing.T) {
	c := getTestConn(t)
	defer c.Close()

	key := "default:test:hash_err_param"
	if _, err := c.Do("hset", key); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("hget", key); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("hexists", key); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("hdel", key); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("hlen", key, "a"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("hincrby", key); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("hmset", key); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("hmset", key, "f1", "v1", "f2"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("hmget", key); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("hgetall"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("hkeys"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("hvals"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("hclear"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("hclear", key, "a"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("hmclear"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}
}

func testListIndex(t *testing.T, key string, index int64, v int) error {
	c := getTestConn(t)
	defer c.Close()

	n, err := goredis.Int(c.Do("lindex", key, index))
	if err == goredis.ErrNil && v != 0 {
		return fmt.Errorf("must nil")
	} else if err != nil && err != goredis.ErrNil {
		return err
	} else if n != v {
		return fmt.Errorf("index err number %d != %d", n, v)
	}

	return nil
}

func testListRange(t *testing.T, key string, start int64, stop int64, checkValues ...int) error {
	c := getTestConn(t)
	defer c.Close()

	vs, err := goredis.MultiBulk(c.Do("lrange", key, start, stop))
	if err != nil {
		return err
	}

	if len(vs) != len(checkValues) {
		return fmt.Errorf("invalid return number %d != %d", len(vs), len(checkValues))
	}

	var n int
	for i, v := range vs {
		if d, ok := v.([]byte); ok {
			n, err = strconv.Atoi(string(d))
			if err != nil {
				return err
			} else if n != checkValues[i] {
				return fmt.Errorf("invalid data %d: %d != %d", i, n, checkValues[i])
			}
		} else {
			return fmt.Errorf("invalid data %v %T", v, v)
		}
	}

	return nil
}

func TestList(t *testing.T) {
	c := getTestConn(t)
	defer c.Close()

	key := "default:test:lista"
	//if n, err := goredis.Int(c.Do("lkeyexists", key)); err != nil {
	//	t.Fatal(err)
	//} else if n != 0 {
	//	t.Fatal(n)
	//}

	if n, err := goredis.Int(c.Do("lpush", key, 1)); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	//if n, err := goredis.Int(c.Do("lkeyexists", key)); err != nil {
	//	t.Fatal(err)
	//} else if n != 1 {
	//	t.Fatal(1)
	//}

	if n, err := goredis.Int(c.Do("rpush", key, 2)); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	if n, err := goredis.Int(c.Do("rpush", key, 3)); err != nil {
		t.Fatal(err)
	} else if n != 3 {
		t.Fatal(n)
	}

	if n, err := goredis.Int(c.Do("llen", key)); err != nil {
		t.Fatal(err)
	} else if n != 3 {
		t.Fatal(n)
	}

	//for ledis-cli a 1 2 3
	// 127.0.0.1:6379> lrange a 0 0
	// 1) "1"
	if err := testListRange(t, key, 0, 0, 1); err != nil {
		t.Fatal(err)
	}

	// 127.0.0.1:6379> lrange a 0 1
	// 1) "1"
	// 2) "2"

	if err := testListRange(t, key, 0, 1, 1, 2); err != nil {
		t.Fatal(err)
	}

	// 127.0.0.1:6379> lrange a 0 5
	// 1) "1"
	// 2) "2"
	// 3) "3"
	if err := testListRange(t, key, 0, 5, 1, 2, 3); err != nil {
		t.Fatal(err)
	}

	// 127.0.0.1:6379> lrange a -1 5
	// 1) "3"
	if err := testListRange(t, key, -1, 5, 3); err != nil {
		t.Fatal(err)
	}

	// 127.0.0.1:6379> lrange a -5 -1
	// 1) "1"
	// 2) "2"
	// 3) "3"
	if err := testListRange(t, key, -5, -1, 1, 2, 3); err != nil {
		t.Fatal(err)
	}

	// 127.0.0.1:6379> lrange a -2 -1
	// 1) "2"
	// 2) "3"
	if err := testListRange(t, key, -2, -1, 2, 3); err != nil {
		t.Fatal(err)
	}

	// 127.0.0.1:6379> lrange a -1 -2
	// (empty list or set)
	if err := testListRange(t, key, -1, -2); err != nil {
		t.Fatal(err)
	}

	// 127.0.0.1:6379> lrange a -1 2
	// 1) "3"
	if err := testListRange(t, key, -1, 2, 3); err != nil {
		t.Fatal(err)
	}

	// 127.0.0.1:6379> lrange a -5 5
	// 1) "1"
	// 2) "2"
	// 3) "3"
	if err := testListRange(t, key, -5, 5, 1, 2, 3); err != nil {
		t.Fatal(err)
	}

	// 127.0.0.1:6379> lrange a -1 0
	// (empty list or set)
	if err := testListRange(t, key, -1, 0); err != nil {
		t.Fatal(err)
	}

	if err := testListRange(t, "default:test:empty list", 0, 100); err != nil {
		t.Fatal(err)
	}

	// 127.0.0.1:6379> lrange a -1 -1
	// 1) "3"
	if err := testListRange(t, key, -1, -1, 3); err != nil {
		t.Fatal(err)
	}

	if err := testListIndex(t, key, -1, 3); err != nil {
		t.Fatal(err)
	}

	if err := testListIndex(t, key, 0, 1); err != nil {
		t.Fatal(err)
	}

	if err := testListIndex(t, key, 1, 2); err != nil {
		t.Fatal(err)
	}

	if err := testListIndex(t, key, 2, 3); err != nil {
		t.Fatal(err)
	}

	if err := testListIndex(t, key, 5, 0); err != nil {
		t.Fatal(err)
	}

	if err := testListIndex(t, key, -1, 3); err != nil {
		t.Fatal(err)
	}

	if err := testListIndex(t, key, -2, 2); err != nil {
		t.Fatal(err)
	}

	if err := testListIndex(t, key, -3, 1); err != nil {
		t.Fatal(err)
	}
}

func TestListMPush(t *testing.T) {
	c := getTestConn(t)
	defer c.Close()

	key := "default:test:listmpushb"
	if n, err := goredis.Int(c.Do("rpush", key, 1, 2, 3)); err != nil {
		t.Fatal(err)
	} else if n != 3 {
		t.Fatal(n)
	}

	if err := testListRange(t, key, 0, 3, 1, 2, 3); err != nil {
		t.Fatal(err)
	}

	if n, err := goredis.Int(c.Do("lpush", key, 1, 2, 3)); err != nil {
		t.Fatal(err)
	} else if n != 6 {
		t.Fatal(n)
	}

	if err := testListRange(t, key, 0, 6, 3, 2, 1, 1, 2, 3); err != nil {
		t.Fatal(err)
	}
}

func TestPop(t *testing.T) {
	c := getTestConn(t)
	defer c.Close()

	key := "default:test:c"
	if n, err := goredis.Int(c.Do("rpush", key, 1, 2, 3, 4, 5, 6)); err != nil {
		t.Fatal(err)
	} else if n != 6 {
		t.Fatal(n)
	}

	if v, err := goredis.Int(c.Do("lpop", key)); err != nil {
		t.Fatal(err)
	} else if v != 1 {
		t.Fatal(v)
	}

	if v, err := goredis.Int(c.Do("rpop", key)); err != nil {
		t.Fatal(err)
	} else if v != 6 {
		t.Fatal(v)
	}

	if n, err := goredis.Int(c.Do("lpush", key, 1)); err != nil {
		t.Fatal(err)
	} else if n != 5 {
		t.Fatal(n)
	}

	if err := testListRange(t, key, 0, 5, 1, 2, 3, 4, 5); err != nil {
		t.Fatal(err)
	}

	for i := 1; i <= 5; i++ {
		if v, err := goredis.Int(c.Do("lpop", key)); err != nil {
			t.Fatal(err)
		} else if v != i {
			t.Fatal(v)
		}
	}

	if n, err := goredis.Int(c.Do("llen", key)); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	c.Do("rpush", key, 1, 2, 3, 4, 5)

	if n, err := goredis.Int(c.Do("lclear", key)); err != nil {
		t.Fatal(err)
	} else if n != 5 {
		t.Fatal(n)
	}

	if n, err := goredis.Int(c.Do("llen", key)); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

}

func disableTestTrim(t *testing.T) {
	c := getTestConn(t)
	defer c.Close()

	key := "default:test:d"
	if n, err := goredis.Int(c.Do("rpush", key, 1, 2, 3, 4, 5, 6)); err != nil {
		t.Fatal(err)
	} else if n != 6 {
		t.Fatal(n)
	}

	if n, err := goredis.Int(c.Do("ltrim_front", key, 2)); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	if n, err := goredis.Int(c.Do("llen", key)); err != nil {
		t.Fatal(err)
	} else if n != 4 {
		t.Fatal(n)
	}

	if n, err := goredis.Int(c.Do("ltrim_back", key, 2)); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	if n, err := goredis.Int(c.Do("llen", key)); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	if n, err := goredis.Int(c.Do("ltrim_front", key, 5)); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	if n, err := goredis.Int(c.Do("llen", key)); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	if n, err := goredis.Int(c.Do("rpush", key, 1, 2)); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	if n, err := goredis.Int(c.Do("ltrim_front", key, 2)); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	if n, err := goredis.Int(c.Do("llen", key)); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}
}
func TestListLPushRPop(t *testing.T) {
	c := getTestConn(t)
	c2 := getTestConn(t)
	defer c.Close()
	defer c2.Close()

	k1 := []byte("default:test_lpushrpop:1")
	klist := make([][]byte, 0, 10)
	klist = append(klist, k1)
	for i := 2; i < 9; i++ {
		klist = append(klist, []byte("default:test_lpushrpop:"+strconv.Itoa(i)))
	}

	n, err := goredis.Int(c.Do("llen", k1))
	assert.Nil(t, err)
	assert.Equal(t, 0, n)
	c.Do("lpush", k1, []byte("a"))
	n, err = goredis.Int(c.Do("llen", k1))
	assert.Nil(t, err)
	assert.Equal(t, 1, n)
	c.Do("rpop", k1)
	n, err = goredis.Int(c.Do("llen", k1))
	assert.Nil(t, err)
	assert.Equal(t, 0, n)
	c.Do("rpop", k1)
	n, err = goredis.Int(c.Do("llen", k1))
	assert.Nil(t, err)
	assert.Equal(t, 0, n)
	c.Do("lpush", k1, []byte("a"))
	c.Do("lpush", k1, []byte("a"))
	c.Do("lpush", k1, []byte("a"))
	c.Do("lpush", k1, []byte("a"))
	c.Do("rpop", k1)
	c.Do("rpop", k1)
	c.Do("lpush", k1, []byte("a"))
	c.Do("lpush", k1, []byte("a"))
	c.Do("rpop", k1)
	c.Do("rpop", k1)
	c.Do("lpush", k1, []byte("a"))
	c.Do("rpop", k1)
	c.Do("rpop", k1)
	n, err = goredis.Int(c.Do("llen", k1))
	assert.Nil(t, err)
	assert.Equal(t, 1, n)
	v, err := goredis.Bytes(c.Do("rpop", k1))
	assert.Nil(t, err)
	assert.Equal(t, []byte("a"), v)
	n, err = goredis.Int(c.Do("llen", k1))
	assert.Nil(t, err)
	assert.Equal(t, 0, n)

	pushed := make([]int32, len(klist))
	poped := make([]int32, len(klist))
	connPushList := make([]*goredis.PoolConn, len(klist))
	connPopList := make([]*goredis.PoolConn, len(klist))

	start := time.Now()
	var wg sync.WaitGroup
	for i := range klist {
		connPushList[i] = getTestConn(t)
		connPopList[i] = getTestConn(t)
		wg.Add(2)
		go func(index int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(time.Now().UnixNano()))
			for {
				_, err := connPushList[index].Do("lpush", klist[index], []byte("a"))
				assert.Nil(t, err)
				atomic.AddInt32(&pushed[index], 1)
				time.Sleep(time.Microsecond * time.Duration(r.Int31n(1000)))
				if time.Since(start) > time.Second*10 {
					break
				}
			}
		}(i)
		go func(index int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(time.Now().UnixNano()))
			for {
				v, err := goredis.Bytes(connPopList[index].Do("rpop", klist[index]))
				assert.Nil(t, err)
				if len(v) > 0 {
					assert.Equal(t, []byte("a"), v)
					atomic.AddInt32(&poped[index], 1)
				}
				time.Sleep(time.Microsecond * time.Duration(r.Int31n(1000)))
				if time.Since(start) > time.Second*10 {
					break
				}
			}
		}(i)

	}
	wg.Wait()

	for i, tk := range klist {
		n, err = goredis.Int(c.Do("llen", tk))
		assert.Nil(t, err)
		t.Logf("pushed %v poped %v", atomic.LoadInt32(&pushed[i]), atomic.LoadInt32(&poped[i]))
		assert.True(t, pushed[i] >= poped[i])
		assert.Equal(t, int(pushed[i]-poped[i]), n)
	}
}

func TestListErrorParams(t *testing.T) {
	c := getTestConn(t)
	defer c.Close()

	key := "default:test:list_err_param"
	if _, err := c.Do("lpush", key); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("rpush", key); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("lpop", key, "a"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("rpop", key, "a"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("llen", key, "a"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("lindex", key); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("lrange", key); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("lclear"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("lmclear"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("ltrim_front", key, "-1"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("ltrim_back", key, "a"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}
}

func TestSet(t *testing.T) {
	c := getTestConn(t)
	defer c.Close()

	key1 := "default:test:testdb_cmd_set_1"
	key2 := "default:test:testdb_cmd_set_2"

	if n, err := goredis.Int(c.Do("scard", key1)); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	if n, err := goredis.Int(c.Do("sadd", key1, 0, 1)); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	if n, err := goredis.Int(c.Do("scard", key1)); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	if n, err := goredis.Int(c.Do("sadd", key2, 0, 1, 2, 3)); err != nil {
		t.Fatal(err)
	} else if n != 4 {
		t.Fatal(n)
	}

	if n, err := goredis.Int(c.Do("srem", key1, 0, 1)); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	if n, err := goredis.Int(c.Do("sismember", key2, 0)); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if n, err := goredis.MultiBulk(c.Do("smembers", key2)); err != nil {
		t.Fatal(err)
	} else if len(n) != 4 {
		t.Fatal(n)
	}

	if n, err := goredis.Int(c.Do("sclear", key2)); err != nil {
		t.Fatal(err)
	} else if n != 4 {
		t.Fatal(n)
	}

	c.Do("sadd", key1, 0)
	c.Do("sadd", key2, 1)
	if n, err := goredis.Int(c.Do("smclear", key1, key2)); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}
}

func TestSetErrorParams(t *testing.T) {
	c := getTestConn(t)
	defer c.Close()

	key := "default:test:set_error_param"
	if _, err := c.Do("sadd", key); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("scard"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("scard", key, key); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("sismember", key); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("sismember", key, "m1", "m2"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("smembers"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("smembers", key, key); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("srem"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("srem", key); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("sclear"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("sclear", key, key); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("smclear"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

}

func TestZSet(t *testing.T) {
	c := getTestConn(t)
	defer c.Close()

	key := "default:test:myzset"

	//if n, err := goredis.Int(c.Do("zkeyexists", key)); err != nil {
	//	t.Fatal(err)
	//} else if n != 0 {
	//	t.Fatal(n)
	//}

	if n, err := goredis.Int(c.Do("zadd", key, 3, "a", 4, "b")); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	//if n, err := goredis.Int(c.Do("zkeyexists", key)); err != nil {
	//	t.Fatal(err)
	//} else if n != 1 {
	//	t.Fatal(n)
	//}

	if n, err := goredis.Int(c.Do("zcard", key)); err != nil {
		t.Fatal(n)
	} else if n != 2 {
		t.Fatal(n)
	}

	if n, err := goredis.Int(c.Do("zadd", key, 1, "a", 2, "b")); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	if n, err := goredis.Int(c.Do("zcard", key)); err != nil {
		t.Fatal(n)
	} else if n != 2 {
		t.Fatal(n)
	}

	if n, err := goredis.Int(c.Do("zadd", key, 3, "c", 4, "d")); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	if n, err := goredis.Int(c.Do("zcard", key)); err != nil {
		t.Fatal(err)
	} else if n != 4 {
		t.Fatal(n)
	}

	if s, err := goredis.Int(c.Do("zscore", key, "c")); err != nil {
		t.Fatal(err)
	} else if s != 3 {
		t.Fatal(s)
	}

	if n, err := goredis.Int(c.Do("zrem", key, "d", "e")); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if n, err := goredis.Int(c.Do("zcard", key)); err != nil {
		t.Fatal(err)
	} else if n != 3 {
		t.Fatal(n)
	}

	if n, err := goredis.Int(c.Do("zincrby", key, 4, "c")); err != nil {
		t.Fatal(err)
	} else if n != 7 {
		t.Fatal(n)
	}

	if n, err := goredis.Int(c.Do("zincrby", key, -4, "c")); err != nil {
		t.Fatal(err)
	} else if n != 3 {
		t.Fatal(n)
	}

	if n, err := goredis.Int(c.Do("zincrby", key, 4, "d")); err != nil {
		t.Fatal(err)
	} else if n != 4 {
		t.Fatal(n)
	}

	if n, err := goredis.Int(c.Do("zcard", key)); err != nil {
		t.Fatal(err)
	} else if n != 4 {
		t.Fatal(n)
	}

	if n, err := goredis.Int(c.Do("zrem", key, "a", "b", "c", "d")); err != nil {
		t.Fatal(err)
	} else if n != 4 {
		t.Fatal(n)
	}

	if n, err := goredis.Int(c.Do("zcard", key)); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

}

func TestZSetCount(t *testing.T) {
	c := getTestConn(t)
	defer c.Close()

	key := "default:test:myzset"
	if _, err := goredis.Int(c.Do("zadd", key, 1, "a", 2, "b", 3, "c", 4, "d")); err != nil {
		t.Fatal(err)
	}

	if n, err := goredis.Int(c.Do("zcount", key, 2, 4)); err != nil {
		t.Fatal(err)
	} else if n != 3 {
		t.Fatal(n)
	}

	if n, err := goredis.Int(c.Do("zcount", key, 4, 4)); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if n, err := goredis.Int(c.Do("zcount", key, 4, 3)); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	if n, err := goredis.Int(c.Do("zcount", key, "(2", 4)); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	if n, err := goredis.Int(c.Do("zcount", key, "2", "(4")); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	if n, err := goredis.Int(c.Do("zcount", key, "(2", "(4")); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if n, err := goredis.Int(c.Do("zcount", key, "-inf", "+inf")); err != nil {
		t.Fatal(err)
	} else if n != 4 {
		t.Fatal(n)
	}

	c.Do("zadd", key, 3, "e")

	if n, err := goredis.Int(c.Do("zcount", key, "(2", "(4")); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	c.Do("zrem", key, "a", "b", "c", "d", "e")
}

func TestZSetRank(t *testing.T) {
	c := getTestConn(t)
	defer c.Close()

	key := "default:test:myzset"
	if _, err := goredis.Int(c.Do("zadd", key, 1, "a", 2, "b", 3, "c", 4, "d")); err != nil {
		t.Fatal(err)
	}

	if n, err := goredis.Int(c.Do("zrank", key, "c")); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	if _, err := goredis.Int(c.Do("zrank", key, "e")); err != goredis.ErrNil {
		t.Fatal(err)
	}

	if n, err := goredis.Int(c.Do("zrevrank", key, "c")); err != nil {
		t.Fatalf("cmd error: %v", err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if _, err := goredis.Int(c.Do("zrevrank", key, "e")); err != goredis.ErrNil {
		t.Fatal(err)
	}

	key2 := "default:test:myzset2"
	if _, err := goredis.Int(c.Do("zadd", key2, 0, "val0", 1, "val1", 2, "val2", 3, "val3")); err != nil {
		t.Fatal(err)
	}
	if _, err := goredis.Int(c.Do("zadd", key2, 4, "val4", 5, "val5", 6, "val6")); err != nil {
		t.Fatal(err)
	}
	// this is used to test the case for iterator seek to max may cause seek to the next last data
	keyExpire := "default:test:myexpkey"
	keyExpire2 := "default:test:myexpkey2"
	c.Do("setex", keyExpire, 10, "v1")
	c.Do("setex", keyExpire2, 10, "v1")

	if n, err := goredis.Int(c.Do("zrank", key2, "val3")); err != nil {
		t.Fatal(err)
	} else if n != 3 {
		t.Fatal(n)
	}

	if n, err := goredis.Int(c.Do("zrevrank", key2, "val3")); err != nil {
		t.Fatalf("cmd error: %v", err)
	} else if n != 3 {
		t.Fatal(n)
	}
}

func testZSetRange(ay []interface{}, checkValues ...interface{}) error {
	if len(ay) != len(checkValues) {
		return fmt.Errorf("invalid return number %d != %d", len(ay), len(checkValues))
	}

	for i := 0; i < len(ay); i++ {
		v, ok := ay[i].([]byte)
		if !ok {
			return fmt.Errorf("invalid data %d %v %T", i, ay[i], ay[i])
		}

		switch cv := checkValues[i].(type) {
		case string:
			if string(v) != cv {
				return fmt.Errorf("not equal %s != %s", v, checkValues[i])
			}
		default:
			if s, _ := strconv.Atoi(string(v)); s != checkValues[i] {
				return fmt.Errorf("not equal %s != %v", v, checkValues[i])
			}
		}

	}

	return nil
}

func TestZSetRangeScore(t *testing.T) {
	c := getTestConn(t)
	defer c.Close()

	key := "default:test:myzset_range"
	if _, err := goredis.Int(c.Do("zadd", key, 1, "a", 2, "b", 3, "c", 4, "d")); err != nil {
		t.Fatal(err)
	}

	if v, err := goredis.MultiBulk(c.Do("zrangebyscore", key, 1, 4, "withscores")); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetRange(v, "a", 1, "b", 2, "c", 3, "d", 4); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := goredis.MultiBulk(c.Do("zrangebyscore", key, 1, 4, "withscores", "limit", 1, 2)); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetRange(v, "b", 2, "c", 3); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := goredis.MultiBulk(c.Do("zrangebyscore", key, "-inf", "+inf", "withscores")); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetRange(v, "a", 1, "b", 2, "c", 3, "d", 4); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := goredis.MultiBulk(c.Do("zrangebyscore", key, "(1", "(4")); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetRange(v, "b", "c"); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := goredis.MultiBulk(c.Do("zrevrangebyscore", key, 4, 1, "withscores")); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetRange(v, "d", 4, "c", 3, "b", 2, "a", 1); err != nil {
			t.Fatalf("%v, %v", err, v)
		}
	}

	if v, err := goredis.MultiBulk(c.Do("zrevrangebyscore", key, 4, 1, "withscores", "limit", 1, 2)); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetRange(v, "c", 3, "b", 2); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := goredis.MultiBulk(c.Do("zrevrangebyscore", key, "+inf", "-inf", "withscores")); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetRange(v, "d", 4, "c", 3, "b", 2, "a", 1); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := goredis.MultiBulk(c.Do("zrevrangebyscore", key, "(4", "(1")); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetRange(v, "c", "b"); err != nil {
			t.Fatal(err)
		}
	}

	if n, err := goredis.Int(c.Do("zremrangebyscore", key, 2, 3)); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	if n, err := goredis.Int(c.Do("zcard", key)); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	if v, err := goredis.MultiBulk(c.Do("zrangebyscore", key, 1, 4)); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetRange(v, "a", "d"); err != nil {
			t.Fatal(err)
		}
	}
}

func TestZSetRange(t *testing.T) {
	c := getTestConn(t)
	defer c.Close()

	key := "default:test:myzset_range_rank"
	if _, err := goredis.Int(c.Do("zadd", key, 1, "a", 2, "b", 3, "c", 4, "d")); err != nil {
		t.Fatal(err)
	}

	if v, err := goredis.MultiBulk(c.Do("zrange", key, 0, 3, "withscores")); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetRange(v, "a", 1, "b", 2, "c", 3, "d", 4); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := goredis.MultiBulk(c.Do("zrange", key, 1, 4, "withscores")); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetRange(v, "b", 2, "c", 3, "d", 4); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := goredis.MultiBulk(c.Do("zrange", key, -2, -1, "withscores")); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetRange(v, "c", 3, "d", 4); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := goredis.MultiBulk(c.Do("zrange", key, 0, -1, "withscores")); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetRange(v, "a", 1, "b", 2, "c", 3, "d", 4); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := goredis.MultiBulk(c.Do("zrange", key, -1, -2, "withscores")); err != nil {
		t.Fatal(err)
	} else if len(v) != 0 {
		t.Fatal(len(v))
	}

	if v, err := goredis.MultiBulk(c.Do("zrevrange", key, 0, 4, "withscores")); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetRange(v, "d", 4, "c", 3, "b", 2, "a", 1); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := goredis.MultiBulk(c.Do("zrevrange", key, 0, -1, "withscores")); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetRange(v, "d", 4, "c", 3, "b", 2, "a", 1); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := goredis.MultiBulk(c.Do("zrevrange", key, 2, 3, "withscores")); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetRange(v, "b", 2, "a", 1); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := goredis.MultiBulk(c.Do("zrevrange", key, -2, -1, "withscores")); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetRange(v, "b", 2, "a", 1); err != nil {
			t.Fatal(err)
		}
	}

	if n, err := goredis.Int(c.Do("zremrangebyrank", key, 2, 3)); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	if n, err := goredis.Int(c.Do("zcard", key)); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	if v, err := goredis.MultiBulk(c.Do("zrange", key, 0, 4)); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetRange(v, "a", "b"); err != nil {
			t.Fatal(err)
		}
	}

	if n, err := goredis.Int(c.Do("zclear", key)); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	if n, err := goredis.Int(c.Do("zcard", key)); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

}

func TestZsetErrorParams(t *testing.T) {
	c := getTestConn(t)
	defer c.Close()

	key := "default:test:zset_error_param"
	//zadd
	if _, err := c.Do("zadd", key); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("zadd", key, "a", "b", "c"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("zadd", key, "-a", "a"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("zadd", key, "0.1", "a"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	//zcard
	if _, err := c.Do("zcard"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	//zscore
	if _, err := c.Do("zscore", key); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	//zrem
	if _, err := c.Do("zrem", key); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	//zincrby
	if _, err := c.Do("zincrby", key); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("zincrby", key, 0.1, "a"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	//zcount
	if _, err := c.Do("zcount", key); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("zcount", key, "-inf", "=inf"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("zcount", key, 0.1, 0.1); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	//zrank
	if _, err := c.Do("zrank", key); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	//zrevzrank
	if _, err := c.Do("zrevrank", key); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	//zremrangebyrank
	if _, err := c.Do("zremrangebyrank", key); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("zremrangebyrank", key, 0.1, 0.1); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	//zremrangebyscore
	if _, err := c.Do("zremrangebyscore", key); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("zremrangebyscore", key, "-inf", "a"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("zremrangebyscore", key, 0, "a"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	//zrange
	if _, err := c.Do("zrange", key); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("zrange", key, 0, 1, "withscore"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("zrange", key, 0, 1, "withscores", "a"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	//zrevrange, almost same as zrange
	if _, err := c.Do("zrevrange", key); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	//zrangebyscore

	if _, err := c.Do("zrangebyscore", key); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("zrangebyscore", key, 0, 1, "withscore"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("zrangebyscore", key, 0, 1, "withscores", "limit"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("zrangebyscore", key, 0, 1, "withscores", "limi", 1, 1); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("zrangebyscore", key, 0, 1, "withscores", "limit", "a", 1); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("zrangebyscore", key, 0, 1, "withscores", "limit", 1, "a"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	//zrevrangebyscore, almost same as zrangebyscore
	if _, err := c.Do("zrevrangebyscore", key); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	//zclear
	if _, err := c.Do("zclear"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	//zmclear
	if _, err := c.Do("zmclear"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}
}

func TestZSetLex(t *testing.T) {
	c := getTestConn(t)
	defer c.Close()

	key := "default:test:myzlexset"
	if _, err := c.Do("zadd", key,
		0, "a", 0, "b", 0, "c", 0, "d", 0, "e", 0, "f", 0, "g"); err != nil {
		t.Fatal(err)
	}

	if ay, err := goredis.Strings(c.Do("zrangebylex", key, "-", "[c")); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(ay, []string{"a", "b", "c"}) {
		t.Fatalf("must equal")
	}

	if ay, err := goredis.Strings(c.Do("zrangebylex", key, "-", "(c")); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(ay, []string{"a", "b"}) {
		t.Fatalf("must equal")
	}

	if ay, err := goredis.Strings(c.Do("zrangebylex", key, "[aaa", "(g")); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(ay, []string{"b", "c", "d", "e", "f"}) {
		t.Fatalf("must equal")
	}

	if n, err := goredis.Int64(c.Do("zlexcount", key, "-", "(c")); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	if n, err := goredis.Int64(c.Do("zremrangebylex", key, "[aaa", "(g")); err != nil {
		t.Fatal(err)
	} else if n != 5 {
		t.Fatal(n)
	}

	if n, err := goredis.Int64(c.Do("zlexcount", key, "-", "+")); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}
}

func checkScanValues(t *testing.T, ay interface{}, values ...interface{}) {
	a, err := goredis.Strings(ay, nil)
	if err != nil {
		t.Error(err)
	}

	if len(a) != len(values) {
		t.Error(fmt.Sprintf("len %d != %d", len(a), len(values)))
	}
	for i, v := range a {
		vv := fmt.Sprintf("%v", values[i])
		if string(v) != vv {
			if len(v) == len(vv)+8 {
				if string(v[:len(vv)]) != vv {
					t.Fatal(fmt.Sprintf("%d %s != %v", i, string(v), values[i]))
				}
			} else if len(v)+8 == len(vv) {
				if string(v) != vv[:len(v)] {
					t.Fatal(fmt.Sprintf("%d %s != %v", i, string(v), values[i]))
				}
			} else {
				t.Fatal(fmt.Sprintf("%d %s != %v", i, string(v), values[i]))
			}
		}
	}
}

func checkAdvanceScan(t *testing.T, c *goredis.PoolConn, tp string) {
	if ay, err := goredis.Values(c.Do("ADVSCAN", "default:testscan:"+"", tp, "count", 5)); err != nil {
		t.Error(err)
	} else if len(ay) != 2 {
		t.Fatal(len(ay))
		//} else if n := ay[0].([]byte); string(n) != "MDpkR1Z6ZEhOallXNDZOQT09Ow==" {
	} else if n := ay[0].([]byte); string(n) != "MDpOQT09Ow==" {
		t.Fatal(string(n))
	} else {
		checkScanValues(t, ay[1], "0", "1", "2", "3", "4")
	}

	if ay, err := goredis.Values(c.Do("ADVSCAN", "default:testscan:MDpOQT09Ow==", tp, "count", 6)); err != nil {
		t.Fatal(err)
	} else if len(ay) != 2 {
		t.Fatal(len(ay))
	} else if n := ay[0].([]byte); string(n) != "" {
		t.Fatal(string(n))
	} else {
		checkScanValues(t, ay[1], "5", "6", "7", "8", "9")
	}

	if ay, err := goredis.Values(c.Do("ADVSCAN", "default:testscan:MDpkR1Z6ZEhOallXNDZPUT09Ow==", tp, "count", 0)); err != nil {
		t.Fatal(err)
	} else if len(ay) != 2 {
		t.Fatal(len(ay))
	} else if n := ay[0].([]byte); string(n) != "" {
		t.Fatal(string(n))
	} else {
		if len(ay[1].([]interface{})) != 0 {
			t.Fatal(ay[1])
		}
	}
}

func TestScan(t *testing.T) {
	c := getTestConn(t)
	defer c.Close()

	testKVScan(t, c)
	testHashKeyScan(t, c)
	testListKeyScan(t, c)
	testZSetKeyScan(t, c)
	testSetKeyScan(t, c)
}

func testKVScan(t *testing.T, c *goredis.PoolConn) {
	for i := 0; i < 10; i++ {
		if _, err := c.Do("set", "default:testscan:"+fmt.Sprintf("%d", i), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}
	checkAdvanceScan(t, c, "KV")
}

func testHashKeyScan(t *testing.T, c *goredis.PoolConn) {
	for i := 0; i < 10; i++ {
		if _, err := c.Do("hset", "default:testscan:"+fmt.Sprintf("%d", i), fmt.Sprintf("%d", i), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}

	checkAdvanceScan(t, c, "HASH")
}

func testListKeyScan(t *testing.T, c *goredis.PoolConn) {
	for i := 0; i < 10; i++ {
		if _, err := c.Do("lpush", "default:testscan:"+fmt.Sprintf("%d", i), fmt.Sprintf("%d", i)); err != nil {
			t.Fatal(err)
		}
	}

	checkAdvanceScan(t, c, "LIST")
}

func testZSetKeyScan(t *testing.T, c *goredis.PoolConn) {
	for i := 0; i < 10; i++ {
		if _, err := c.Do("zadd", "default:testscan:"+fmt.Sprintf("%d", i), i, []byte("value")); err != nil {
			t.Fatal(err)
		}
	}

	checkAdvanceScan(t, c, "ZSET")
}

func testSetKeyScan(t *testing.T, c *goredis.PoolConn) {
	for i := 0; i < 10; i++ {
		if _, err := c.Do("sadd", "default:testscan:"+fmt.Sprintf("%d", i), fmt.Sprintf("%d", i)); err != nil {
			t.Fatal(err)
		}
	}

	checkAdvanceScan(t, c, "SET")
}

func TestHashScan(t *testing.T) {
	c := getTestConn(t)
	defer c.Close()

	key := "default:testscan:scan_hash"
	c.Do("HMSET", key, "a", 1, "b", 2)

	if ay, err := goredis.Values(c.Do("HSCAN", key, "")); err != nil {
		t.Fatal(err)
	} else if len(ay) != 2 {
		t.Fatal(len(ay))
	} else {
		checkScanValues(t, ay[1], "a", 1, "b", 2)
	}
}

func TestSetScan(t *testing.T) {
	c := getTestConn(t)
	defer c.Close()

	key := "default:test:scan_set"
	c.Do("SADD", key, "a", "b")

	if ay, err := goredis.Values(c.Do("SSCAN", key, "")); err != nil {
		t.Fatal(err)
	} else if len(ay) != 2 {
		t.Fatal(len(ay))
	} else {
		checkScanValues(t, ay[1], "a", "b")
	}
}

func TestZSetScan(t *testing.T) {
	c := getTestConn(t)
	defer c.Close()

	key := "default:test:scan_zset"
	c.Do("ZADD", key, 1, "a", 2, "b")

	if ay, err := goredis.Values(c.Do("ZSCAN", key, "")); err != nil {
		t.Fatal(err)
	} else if len(ay) != 2 {
		t.Fatal(len(ay))
	} else {
		checkScanValues(t, ay[1], "a", 1, "b", 2)
	}
}

func TestJson(t *testing.T) {
	c := getTestConn(t)
	defer c.Close()

	key := "default:test:jsonapi_a"
	n, err := goredis.Int(c.Do("json.keyexists", key))
	assert.Nil(t, err)
	assert.Equal(t, int(0), n)

	strRet, err := goredis.String(c.Do("json.set", key, ".a", `"str"`))
	assert.Nil(t, err)
	assert.Equal(t, "OK", strRet)

	n, err = goredis.Int(c.Do("json.keyexists", key))
	assert.Nil(t, err)
	assert.Equal(t, int(1), n)

	strRets, err := goredis.Strings(c.Do("json.get", key, ".a"))
	assert.Nil(t, err)
	assert.Equal(t, 1, len(strRets))
	assert.Equal(t, "str", strRets[0])

	strRet, err = goredis.String(c.Do("json.set", key, "1", "3"))
	assert.Nil(t, err)
	assert.Equal(t, "OK", strRet)

	strRets, err = goredis.Strings(c.Do("json.get", key, ""))
	assert.Nil(t, err)
	t.Log(strRets)
	assert.Equal(t, 1, len(strRets))
	assert.True(t, strRets[0] != "")
	t.Log(strRets[0])
	assert.True(t, strRets[0] == `{"a":"str","1":3}` || (strRets[0] == `{"1":3,"a":"str"}`))

	strRets, err = goredis.Strings(c.Do("json.get", key, "a"))
	assert.Nil(t, err)
	assert.Equal(t, 1, len(strRets))
	assert.Equal(t, "str", strRets[0])

	strRets, err = goredis.Strings(c.Do("json.get", key, "1"))
	assert.Nil(t, err)
	assert.Equal(t, 1, len(strRets))
	t.Log(strRets)
	assert.Equal(t, "3", strRets[0])

	strRets, err = goredis.Strings(c.Do("json.get", key, "1", "a"))
	assert.Nil(t, err)
	assert.Equal(t, 2, len(strRets))
	t.Log(strRets)
	assert.Equal(t, "3", strRets[0])
	assert.Equal(t, "str", strRets[1])

	n, err = goredis.Int(c.Do("json.objlen", key))
	assert.Nil(t, err)
	assert.Equal(t, 2, n)
	strRets, err = goredis.Strings(c.Do("json.objkeys", key))
	assert.Nil(t, err)
	assert.Equal(t, 2, len(strRets))
	for _, s := range strRets {
		assert.True(t, s == "a" || s == "1")
	}
	c.Do("json.del", key, "1")
	strRets, err = goredis.Strings(c.Do("json.get", key, "1"))
	assert.Nil(t, err)
	assert.Equal(t, 1, len(strRets))
	assert.Equal(t, "", strRets[0])

	n, err = goredis.Int(c.Do("json.objlen", key))
	assert.Nil(t, err)
	assert.Equal(t, 1, n)
	strRets, err = goredis.Strings(c.Do("json.objkeys", key))
	assert.Nil(t, err)
	assert.Equal(t, 1, len(strRets))
	for _, s := range strRets {
		assert.True(t, s == "a")
	}

	c.Do("json.del", key, "a")
	strRets, err = goredis.Strings(c.Do("json.get", key, ".a"))
	assert.Nil(t, err)
	assert.Equal(t, 1, len(strRets))
	assert.Equal(t, "", strRets[0])

	n, err = goredis.Int(c.Do("json.objlen", key))
	assert.Nil(t, err)
	assert.Equal(t, 0, n)
	strRets, err = goredis.Strings(c.Do("json.objkeys", key))
	assert.Nil(t, err)
	assert.Equal(t, 0, len(strRets))
}

func TestJsonInvalidJson(t *testing.T) {
	c := getTestConn(t)
	defer c.Close()

	key := "default:test:jsonapi_invalid"

	strRet, err := goredis.String(c.Do("json.set", key, ".a", `"str"`))
	assert.Nil(t, err)
	assert.Equal(t, "OK", strRet)

	strRet, err = goredis.String(c.Do("json.set", key, "1", "3"))
	assert.Nil(t, err)
	assert.Equal(t, "OK", strRet)

	_, err = c.Do("json.set", key, "2", "invalid_str")
	assert.NotNil(t, err)
}

func TestJsonSetComplexJson(t *testing.T) {
	c := getTestConn(t)
	defer c.Close()

	key := "default:test:jsonapi_complex"

	strRet, err := goredis.String(c.Do("json.set", key, "", `{
        "address": {
            "street": "2 Avenue",
            "zipcode": "10075",
            "building": "1480",
            "coord": [-73.9557413, 40.7720266]
        },
        "borough": "Manhattan",
        "cuisine": "Italian",
        "grades": [
            {
                "date": "2014-10-01",
                "grade": "A",
                "score": 11
            },
            {
                "date": "2014-01-16",
                "grade": "B",
                "score": 17
            }
        ],
        "name": "Vella",
        "restaurant_id": "41704620"
	}`))

	assert.Nil(t, err)
	assert.Equal(t, "OK", strRet)
	strRets, err := goredis.Strings(c.Do("json.get", key, "borough"))
	assert.Nil(t, err)
	assert.Equal(t, 1, len(strRets))
	assert.Equal(t, "Manhattan", strRets[0])
	strRets, err = goredis.Strings(c.Do("json.get", key, "address.zipcode"))
	assert.Nil(t, err)
	assert.Equal(t, 1, len(strRets))
	assert.Equal(t, "10075", strRets[0])
	strRets, err = goredis.Strings(c.Do("json.get", key, "grades.0.score"))
	assert.Nil(t, err)
	assert.Equal(t, 1, len(strRets))
	assert.Equal(t, "11", strRets[0])
	c.Do("json.set", key, "cuisine", `"American"`)
	c.Do("json.set", key, "address.street", `"East 31st Street"`)
	strRets, err = goredis.Strings(c.Do("json.get", key, "cuisine"))
	assert.Nil(t, err)
	assert.Equal(t, 1, len(strRets))
	assert.Equal(t, "American", strRets[0])
	strRets, err = goredis.Strings(c.Do("json.get", key, "address.street"))
	assert.Nil(t, err)
	assert.Equal(t, 1, len(strRets))
	assert.Equal(t, "East 31st Street", strRets[0])
}

func TestJsonArrayOp(t *testing.T) {
	c := getTestConn(t)
	defer c.Close()

	key := "default:test:json_arrayop_d"
	_, err := c.Do("json.set", key, "", `[1, 2]`)
	assert.Nil(t, err)
	n, err := goredis.Int(c.Do("json.arrappend", key, ".", `{"3":[]}`))
	assert.Nil(t, err)
	assert.Equal(t, 3, n)

	n, err = goredis.Int(c.Do("json.arrappend", key, ".", "4", "5"))
	assert.Nil(t, err)
	assert.Equal(t, 5, n)

	n, err = goredis.Int(c.Do("json.arrlen", key))
	assert.Nil(t, err)
	assert.Equal(t, 5, n)

	n, err = goredis.Int(c.Do("json.arrappend", key, "2.3", "33", "34"))
	assert.Nil(t, err)
	assert.Equal(t, 2, n)

	n, err = goredis.Int(c.Do("json.arrlen", key, "2.3"))
	assert.Nil(t, err)
	assert.Equal(t, 2, n)

	poped, err := goredis.String(c.Do("json.arrpop", key))
	assert.Nil(t, err)
	assert.Equal(t, "5", poped)

	poped, err = goredis.String(c.Do("json.arrpop", key))
	assert.Nil(t, err)
	assert.Equal(t, "4", poped)

	n, err = goredis.Int(c.Do("json.arrlen", key))
	assert.Nil(t, err)
	assert.Equal(t, 3, n)

	poped, err = goredis.String(c.Do("json.arrpop", key, "2.3"))
	assert.Nil(t, err)
	assert.Equal(t, "34", poped)

	n, err = goredis.Int(c.Do("json.arrlen", key, "2.3"))
	assert.Nil(t, err)
	assert.Equal(t, 1, n)

	poped, err = goredis.String(c.Do("json.arrpop", key))
	assert.Nil(t, err)
	assert.Equal(t, `{"3":[33]}`, poped)

	n, err = goredis.Int(c.Do("json.arrlen", key))
	assert.Nil(t, err)
	assert.Equal(t, 2, n)

	poped, err = goredis.String(c.Do("json.arrpop", key))
	assert.Nil(t, err)
	assert.Equal(t, "2", poped)
	poped, err = goredis.String(c.Do("json.arrpop", key))
	assert.Nil(t, err)
	assert.Equal(t, "1", poped)

	n, err = goredis.Int(c.Do("json.arrlen", key))
	assert.Nil(t, err)
	assert.Equal(t, 0, n)

	poped, err = goredis.String(c.Do("json.arrpop", key))
	assert.Nil(t, err)
	assert.Equal(t, "", poped)
}

func TestJsonErrorParams(t *testing.T) {
	c := getTestConn(t)
	defer c.Close()

	key := "default:test:json_err_param"
	if _, err := c.Do("json.set", key); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("json.get", key); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("json.del", key); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("json.arrylen"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("json.arrappend", key, "a"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("json.arrpop"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("json.objkeys"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("json.objlen"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}
}
