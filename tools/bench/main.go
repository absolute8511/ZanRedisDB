package main

import (
	"flag"
	"fmt"
	"math/rand"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/siddontang/goredis"
)

var ip = flag.String("ip", "127.0.0.1", "redis server ip")
var port = flag.Int("port", 6380, "redis server port")
var number = flag.Int("n", 1000, "request number")
var clients = flag.Int("c", 50, "number of clients")
var round = flag.Int("r", 1, "benchmark round number")
var valueSize = flag.Int("vsize", 100, "kv value size")
var tests = flag.String("t", "set,get,randget,del,lpush,lrange,lpop,hset,hget,hdel,zadd,zrange,zrevrange,zdel", "only run the comma separated list of tests")
var primaryKeyCnt = flag.Int("pkn", 100, "primary key count for hash,list,set,zset")
var namespace = flag.String("namespace", "default", "the prefix namespace")
var table = flag.String("table", "test", "the table to write")
var wg sync.WaitGroup

var client *goredis.Client
var loop int = 0

func waitBench(c *goredis.PoolConn, cmd string, args ...interface{}) error {
	v := args[0]
	prefix := *namespace + ":" + *table + ":"
	switch vt := v.(type) {
	case string:
		args[0] = prefix + vt
	case []byte:
		args[0] = []byte(prefix + string(vt))
	case int:
		args[0] = prefix + strconv.Itoa(vt)
	case int64:
		args[0] = prefix + strconv.Itoa(int(vt))
	}
	_, err := c.Do(strings.ToUpper(cmd), args...)
	if err != nil {
		fmt.Printf("do %s error %s\n", cmd, err.Error())
		return err
	}
	return nil
}

func bench(cmd string, f func(c *goredis.PoolConn, cindex int, loopIter int) error) {
	wg.Add(*clients)

	t1 := time.Now()
	for i := 0; i < *clients; i++ {
		go func(clientIndex int) {
			var err error
			c, _ := client.Get()
			for j := 0; j < loop; j++ {
				err = f(c, clientIndex, j)
				if err != nil {
					break
				}
			}
			c.Close()
			wg.Done()
		}(i)
	}

	wg.Wait()

	t2 := time.Now()

	d := t2.Sub(t1)

	fmt.Printf("%s: %s %0.3f micros/op, %0.2fop/s\n",
		cmd,
		d.String(),
		float64(d.Nanoseconds()/1e3)/float64(*number),
		float64(*number)/d.Seconds())
}

var kvSetBase int64 = 0
var kvGetBase int64 = 0
var kvIncrBase int64 = 0
var kvDelBase int64 = 0

func benchSet() {
	valueSample := make([]byte, *valueSize)
	for i := 0; i < len(valueSample); i++ {
		valueSample[i] = byte(i % 255)
	}
	f := func(c *goredis.PoolConn, cindex int, loopi int) error {
		value := make([]byte, *valueSize)
		copy(value, valueSample)
		n := atomic.AddInt64(&kvSetBase, 1)
		copy(value[0:], strconv.Itoa(int(n)))
		return waitBench(c, "SET", n, value)
	}

	bench("set", f)
}

func benchGet() {
	f := func(c *goredis.PoolConn, cindex int, loopi int) error {
		n := atomic.AddInt64(&kvGetBase, 1)
		return waitBench(c, "GET", n)
	}

	bench("get", f)
}

func benchRandGet() {
	f := func(c *goredis.PoolConn, cindex int, loopi int) error {
		n := rand.Int() % *number
		return waitBench(c, "GET", n)
	}

	bench("randget", f)
}

func benchDel() {
	f := func(c *goredis.PoolConn, cindex int, loopi int) error {
		n := atomic.AddInt64(&kvDelBase, 1)
		return waitBench(c, "DEL", n)
	}

	bench("del", f)
}

var listPushBase int64 = 0
var listRange10Base int64 = 0
var listRange50Base int64 = 0
var listRange100Base int64 = 0
var listPopBase int64 = 0

func benchPushList() {
	valueSample := make([]byte, *valueSize)
	for i := 0; i < len(valueSample); i++ {
		valueSample[i] = byte(i % 255)
	}

	f := func(c *goredis.PoolConn, cindex int, loopi int) error {
		value := make([]byte, *valueSize)
		copy(value, valueSample)
		n := atomic.AddInt64(&listPushBase, 1) % int64(*primaryKeyCnt)
		copy(value[0:], strconv.Itoa(int(n)))
		return waitBench(c, "RPUSH", "mytestlist"+strconv.Itoa(int(n)), value)
	}

	bench("rpush", f)
}

func benchRangeList10() {
	f := func(c *goredis.PoolConn, cindex int, loopi int) error {
		n := atomic.AddInt64(&listRange10Base, 1) % int64(*primaryKeyCnt)
		return waitBench(c, "LRANGE", "mytestlist"+strconv.Itoa(int(n)), 0, 10)
	}

	bench("lrange10", f)
}

func benchRangeList50() {
	f := func(c *goredis.PoolConn, cindex int, loopi int) error {
		n := atomic.AddInt64(&listRange50Base, 1) % int64(*primaryKeyCnt)
		return waitBench(c, "LRANGE", "mytestlist"+strconv.Itoa(int(n)), 0, 50)
	}

	bench("lrange50", f)
}

func benchRangeList100() {
	f := func(c *goredis.PoolConn, cindex int, loopi int) error {
		n := atomic.AddInt64(&listRange100Base, 1) % int64(*primaryKeyCnt)
		return waitBench(c, "LRANGE", "mytestlist"+strconv.Itoa(int(n)), 0, 100)
	}

	bench("lrange100", f)
}

func benchPopList() {
	f := func(c *goredis.PoolConn, cindex int, loopi int) error {
		n := atomic.AddInt64(&listPopBase, 1) % int64(*primaryKeyCnt)
		return waitBench(c, "LPOP", "mytestlist"+strconv.Itoa(int(n)))
	}

	bench("lpop", f)
}

var hashPKBase int64 = 0
var hashSetBase int64 = 0
var hashIncrBase int64 = 0
var hashGetBase int64 = 0
var hashDelBase int64 = 0

func benchHset() {
	valueSample := make([]byte, *valueSize)
	for i := 0; i < len(valueSample); i++ {
		valueSample[i] = byte(i % 255)
	}

	atomic.StoreInt64(&hashPKBase, 0)
	subKeyCnt := int64(*number / (*primaryKeyCnt))
	f := func(c *goredis.PoolConn, cindex int, loopi int) error {
		value := make([]byte, *valueSize)
		copy(value, valueSample)

		n := atomic.AddInt64(&hashSetBase, 1)
		pk := n / subKeyCnt
		subkey := n - pk*subKeyCnt
		copy(value[0:], strconv.Itoa(int(n)))
		return waitBench(c, "HSET", "myhashkey"+strconv.Itoa(int(pk)), subkey, value)
	}

	bench("hset", f)
}

func benchHGet() {
	atomic.StoreInt64(&hashPKBase, 0)
	subKeyCnt := int64(*number / (*primaryKeyCnt))
	f := func(c *goredis.PoolConn, cindex int, loopi int) error {
		n := atomic.AddInt64(&hashGetBase, 1)
		pk := n / subKeyCnt
		subkey := n - pk*subKeyCnt
		return waitBench(c, "HGET", "myhashkey"+strconv.Itoa(int(pk)), subkey)
	}

	bench("hget", f)
}

func benchHRandGet() {
	atomic.StoreInt64(&hashPKBase, 0)
	subKeyCnt := int64(*number / (*primaryKeyCnt))
	f := func(c *goredis.PoolConn, cindex int, loopi int) error {
		n := int64(rand.Int() % *number)
		pk := n / subKeyCnt
		subkey := n - pk*subKeyCnt
		return waitBench(c, "HGET", "myhashkey"+strconv.Itoa(int(pk)), subkey)
	}

	bench("hrandget", f)
}

func benchHDel() {
	atomic.StoreInt64(&hashPKBase, 0)
	subKeyCnt := int64(*number / (*primaryKeyCnt))
	f := func(c *goredis.PoolConn, cindex int, loopi int) error {
		n := atomic.AddInt64(&hashDelBase, 1)
		pk := n / subKeyCnt
		subkey := n - pk*subKeyCnt
		return waitBench(c, "HDEL", "myhashkey"+strconv.Itoa(int(pk)), subkey)
	}

	bench("hdel", f)
}

var zsetPKBase int64 = 0
var zsetAddBase int64 = 0
var zsetDelBase int64 = 0

func benchZAdd() {
	atomic.StoreInt64(&zsetPKBase, 0)
	subKeyCnt := int64(*number / (*primaryKeyCnt))
	f := func(c *goredis.PoolConn, cindex int, loopi int) error {
		n := atomic.AddInt64(&zsetAddBase, 1)
		pk := n / subKeyCnt
		subkey := n - pk*subKeyCnt
		member := strconv.Itoa(int(subkey))
		return waitBench(c, "ZADD", "myzsetkey"+strconv.Itoa(int(pk)), subkey, member)
	}

	bench("zadd", f)
}

func benchZDel() {
	atomic.StoreInt64(&zsetPKBase, 0)
	subKeyCnt := int64(*number / (*primaryKeyCnt))
	f := func(c *goredis.PoolConn, cindex int, loopi int) error {
		n := atomic.AddInt64(&zsetDelBase, 1)
		pk := n / subKeyCnt
		subkey := n - pk*subKeyCnt
		return waitBench(c, "ZREM", "myzsetkey"+strconv.Itoa(int(pk)), subkey)
	}

	bench("zrem", f)
}

func benchZRangeByScore() {
	atomic.StoreInt64(&zsetPKBase, 0)
	subKeyCnt := int64(*number / (*primaryKeyCnt))
	f := func(c *goredis.PoolConn, cindex int, loopi int) error {
		n := atomic.AddInt64(&zsetPKBase, 1)
		pk := n / subKeyCnt
		return waitBench(c, "ZRANGEBYSCORE", "myzsetkey"+strconv.Itoa(int(pk)), 0, rand.Int(), "limit", rand.Int()%100, 100)
	}

	bench("zrangebyscore", f)
}

func benchZRangeByRank() {
	atomic.StoreInt64(&zsetPKBase, 0)
	subKeyCnt := int64(*number / (*primaryKeyCnt))
	f := func(c *goredis.PoolConn, cindex int, loopi int) error {
		n := atomic.AddInt64(&zsetPKBase, 1)
		pk := n / subKeyCnt
		return waitBench(c, "ZRANGE", "myzsetkey"+strconv.Itoa(int(pk)), 0, rand.Int()%100)
	}

	bench("zrange", f)
}

func benchZRevRangeByScore() {
	atomic.StoreInt64(&zsetPKBase, 0)
	subKeyCnt := int64(*number / (*primaryKeyCnt))
	f := func(c *goredis.PoolConn, cindex int, loopi int) error {
		n := atomic.AddInt64(&zsetPKBase, 1)
		pk := n / subKeyCnt
		return waitBench(c, "ZREVRANGEBYSCORE", "myzsetkey"+strconv.Itoa(int(pk)), 0, rand.Int(), "limit", rand.Int()%100, 100)
	}

	bench("zrevrangebyscore", f)
}

func benchZRevRangeByRank() {
	atomic.StoreInt64(&zsetPKBase, 0)
	subKeyCnt := int64(*number / (*primaryKeyCnt))
	f := func(c *goredis.PoolConn, cindex int, loopi int) error {
		n := atomic.AddInt64(&zsetPKBase, 1)
		pk := n / subKeyCnt
		return waitBench(c, "ZREVRANGE", "myzsetkey"+strconv.Itoa(int(pk)), 0, rand.Int()%100)
	}

	bench("zrevrange", f)
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	flag.Parse()

	if *number <= 0 {
		panic("invalid number")
		return
	}

	if *clients <= 0 || *number < *clients {
		panic("invalid client number")
		return
	}

	loop = *number / *clients

	addr := fmt.Sprintf("%s:%d", *ip, *port)

	client = goredis.NewClient(addr, "")
	client.SetReadBufferSize(10240)
	client.SetWriteBufferSize(10240)
	client.SetMaxIdleConns(16)

	for i := 0; i < *clients; i++ {
		c, _ := client.Get()
		c.Close()
	}

	if *round <= 0 {
		*round = 1
	}

	ts := strings.Split(*tests, ",")

	for i := 0; i < *round; i++ {
		for _, s := range ts {
			switch strings.ToLower(s) {
			case "set":
				benchSet()
			case "get":
				benchGet()
			case "randget":
				benchRandGet()
			case "del":
				benchDel()
			case "lpush":
				benchPushList()
			case "lrange":
				benchRangeList10()
				benchRangeList50()
				benchRangeList100()
			case "lpop":
				benchPopList()
			case "hset":
				benchHset()
			case "hget":
				benchHGet()
				benchHRandGet()
			case "hdel":
				benchHDel()
			case "zadd":
				benchZAdd()
			case "zrange":
				benchZRangeByRank()
				benchZRangeByScore()
			case "zrevrange":
				//rev is too slow in leveldb, rocksdb or other
				//maybe disable for huge data benchmark
				benchZRevRangeByRank()
				benchZRevRangeByScore()
			case "zdel":
				benchZDel()
			}
		}

		println("")
	}
}
