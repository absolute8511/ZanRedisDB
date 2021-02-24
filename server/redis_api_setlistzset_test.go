package server

import (
	"fmt"
	"math/rand"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/siddontang/goredis"
	"github.com/stretchr/testify/assert"
)

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

func TestListPop(t *testing.T) {
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
	} else if n <= 0 {
		t.Fatal(n)
	}

	if n, err := goredis.Int(c.Do("llen", key)); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	// test lpop, rpop empty list
	v, err := goredis.Bytes(c.Do("lpop", key))
	assert.Equal(t, goredis.ErrNil, err)
	assert.Nil(t, v)
	v, err = goredis.Bytes(c.Do("rpop", key))
	assert.Equal(t, goredis.ErrNil, err)
	assert.Nil(t, v)
	sv, err := goredis.String(c.Do("ltrim", key, 0, 1))
	assert.Nil(t, err)
	assert.Equal(t, "OK", sv)
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
	defer func() {
		for _, c := range connPushList {
			c.Close()
		}
		for _, c := range connPopList {
			c.Close()
		}
	}()

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
				assert.True(t, err == nil || err == goredis.ErrNil, "should no value or no error")
				if len(v) > 0 {
					assert.Nil(t, err)
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
	// add again
	if n, err := goredis.Int(c.Do("sadd", key1, 0, 1)); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}
	if n, err := goredis.Int(c.Do("sadd", key1, 1)); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}
	if n, err := goredis.Int(c.Do("scard", key1)); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	if n, err := goredis.Int(c.Do("sadd", key2, 0, 1, 2)); err != nil {
		t.Fatal(err)
	} else if n != 3 {
		t.Fatal(n)
	}
	// part of dup
	if n, err := goredis.Int(c.Do("sadd", key2, 1, 2, 3)); err != nil {
		t.Fatal(err)
	} else if n != 1 {
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

	if n, err := goredis.Values(c.Do("smembers", key2)); err != nil {
		t.Fatal(err)
	} else if len(n) != 4 {
		t.Fatal(n)
	}
	n, err := goredis.Int(c.Do("scard", key2))
	assert.Nil(t, err)
	assert.Equal(t, int(4), n)

	if n, err := goredis.Values(c.Do("srandmember", key2)); err != nil {
		t.Fatal(err)
	} else if len(n) != 1 {
		t.Fatal(n)
	}
	if n, err := goredis.Values(c.Do("srandmember", key2, 2)); err != nil {
		t.Fatal(err)
	} else if len(n) != 2 {
		t.Fatal(n)
	}
	if n, err := goredis.Values(c.Do("srandmember", key2, 10)); err != nil {
		t.Fatal(err)
	} else if len(n) != 4 {
		t.Fatal(n)
	}

	if val, err := goredis.String(c.Do("spop", key2)); err != nil {
		t.Fatal(err)
	} else if val != "0" {
		t.Fatal(val)
	}
	if val, err := goredis.String(c.Do("spop", key2)); err != nil {
		t.Fatal(err)
	} else if val != "1" {
		t.Fatal(val)
	}
	if val, err := goredis.Values(c.Do("spop", key2, 4)); err != nil {
		t.Fatal(err)
	} else if len(val) != 2 {
		t.Fatal(val)
	}
	if n, err := goredis.Values(c.Do("smembers", key2)); err != nil {
		t.Fatal(err)
	} else if len(n) != 0 {
		t.Fatal(n)
	}

	n, err = goredis.Int(c.Do("scard", key2))
	assert.Nil(t, err)
	assert.Equal(t, int(0), n)
	if n, err := goredis.Values(c.Do("srandmember", key2, 10)); err != nil {
		t.Fatal(err)
	} else if len(n) != 0 {
		t.Fatal(n)
	}
	// empty spop single will return nil, but spop multi will return empty array
	if val, err := c.Do("spop", key2); err != nil {
		t.Fatal(err)
	} else if val != nil {
		t.Fatal(val)
	}
	if val, err := goredis.Values(c.Do("spop", key2, 2)); err != nil {
		t.Fatal(err)
	} else if val == nil {
		t.Fatal(val)
	} else if len(val) != 0 {
		t.Fatal(val)
	}
	if n, err := goredis.Int(c.Do("sadd", key2, "member0", "member1", "member2")); err != nil {
		t.Fatal(err)
	} else if n != 3 {
		t.Fatal(n)
	}
	if val, err := goredis.String(c.Do("spop", key2)); err != nil {
		t.Fatal(err)
	} else if val != "member0" {
		t.Fatal(val)
	}
	if val, err := goredis.Values(c.Do("spop", key2, 2)); err != nil {
		t.Fatal(err)
	} else if len(val) != 2 {
		t.Fatal(val)
	} else if string(val[0].([]byte)) != "member1" || string(val[1].([]byte)) != "member2" {
		t.Fatal(val)
	}

	if n, err := goredis.Int(c.Do("sadd", key2, 0, 1, 2, 3)); err != nil {
		t.Fatal(err)
	} else if n != 4 {
		t.Fatal(n)
	}
	if n, err := goredis.Int(c.Do("sclear", key2)); err != nil {
		t.Fatal(err)
	} else if n <= 0 {
		t.Fatal(n)
	}

	n, err = goredis.Int(c.Do("scard", key2))
	assert.Nil(t, err)
	assert.Equal(t, int(0), n)
}

func TestSetSPopCompatile(t *testing.T) {
	c := getTestConn(t)
	defer c.Close()

	key1 := "default:test:testdb_cmd_set_spop"

	binaryStr := make([]byte, 10)
	binaryStr = []byte("binary" + string(binaryStr) + "bb")
	n, err := goredis.Int(c.Do("sadd", key1, binaryStr))
	assert.Nil(t, err)
	assert.Equal(t, int(1), n)
	binaryStr2 := []byte(string(binaryStr) + "2")
	n, err = goredis.Int(c.Do("sadd", key1, binaryStr2))
	assert.Nil(t, err)
	assert.Equal(t, int(1), n)
	n, err = goredis.Int(c.Do("scard", key1))
	assert.Nil(t, err)
	assert.Equal(t, int(2), n)

	val, err := c.Do("spop", key1)
	assert.Nil(t, err)
	assert.Equal(t, binaryStr, val)
	_, ok := val.([]byte)
	assert.Equal(t, true, ok)
	vals, err := goredis.MultiBulk(c.Do("spop", key1, 1))
	assert.Nil(t, err)
	assert.Equal(t, binaryStr2, vals[0])
	_, ok = vals[0].([]byte)
	assert.Equal(t, true, ok)

	// spop empty
	val, err = c.Do("spop", key1)
	assert.Nil(t, err)
	assert.Equal(t, nil, val)
	vals, err = goredis.MultiBulk(c.Do("spop", key1, 1))
	assert.Nil(t, err)
	assert.NotNil(t, vals)
	assert.Equal(t, 0, len(vals))
}

func TestSetSPopSAddConcurrent(t *testing.T) {
	var wg sync.WaitGroup
	for index := 0; index < 4; index++ {
		wg.Add(1)
		client := getTestConn(t)
		defer client.Close()
		go func(c *goredis.PoolConn) {
			defer wg.Done()
			for loop := 0; loop < 400; loop++ {
				key1 := "default:test:testdb_spop_sadd_concurrency" + strconv.Itoa(loop)
				binaryStr := make([]byte, 10)
				binaryStr = []byte("binary" + string(binaryStr) + "bb")
				_, err := goredis.Int(c.Do("sadd", key1, binaryStr))
				assert.Nil(t, err)
				binaryStr2 := []byte(string(binaryStr) + strconv.Itoa(loop%2))
				_, err = goredis.Int(c.Do("sadd", key1, binaryStr2))
				assert.Nil(t, err)
				_, err = c.Do("smembers", key1)
				assert.Nil(t, err)

				_, err = c.Do("spop", key1)
				assert.Nil(t, err)
				_, err = goredis.MultiBulk(c.Do("spop", key1, 2))
				assert.Nil(t, err)
			}
		}(client)
	}
	wg.Wait()
}

func TestSetExpire(t *testing.T) {
	c := getTestConn(t)
	defer c.Close()

	key1 := "default:test:set_expa"
	f1 := "f1"
	f2 := "f2"
	f3 := "f3"
	f4 := "f4"
	f5 := "f5"
	ttl := 2

	n, err := goredis.Int(c.Do("sadd", key1, f1))
	assert.Nil(t, err)
	assert.Equal(t, 1, n)

	c.Do("sexpire", key1, ttl)
	n, err = goredis.Int(c.Do("sismember", key1, f1))
	assert.Nil(t, err)
	assert.Equal(t, 1, n)
	n, err = goredis.Int(c.Do("skeyexist", key1))
	assert.Nil(t, err)
	assert.Equal(t, 1, n)

	realTtl, err := goredis.Int(c.Do("sttl", key1))
	assert.Nil(t, err)
	assertTTLNear(t, ttl, realTtl)

	// check sadd,srem, spop keep ttl
	_, err = goredis.Int(c.Do("sadd", key1, f2, f3))
	assert.Nil(t, err)
	realTtl, err = goredis.Int(c.Do("sttl", key1))
	assert.Nil(t, err)
	assertTTLNear(t, ttl, realTtl)

	vlist, err := goredis.MultiBulk(c.Do("smembers", key1))
	assert.Nil(t, err)
	assert.Equal(t, 3, len(vlist))

	n, err = goredis.Int(c.Do("scard", key1))
	assert.Nil(t, err)
	assert.Equal(t, int(3), n)

	n, err = goredis.Int(c.Do("srem", key1, f1))
	assert.Nil(t, err)
	assert.Equal(t, 1, n)
	realTtl, err = goredis.Int(c.Do("sttl", key1))
	assert.Nil(t, err)
	assertTTLNear(t, ttl, realTtl)

	v, err := goredis.String(c.Do("spop", key1))
	assert.Nil(t, err)
	assert.Equal(t, f2, v)
	realTtl, err = goredis.Int(c.Do("sttl", key1))
	assert.Nil(t, err)
	assertTTLNear(t, ttl, realTtl)
	// wait expire
	time.Sleep(time.Second * time.Duration(ttl+2))

	if v, err := goredis.String(c.Do("spop", key1)); err != goredis.ErrNil {
		t.Fatalf("expired key should be expired: %v, %v", v, err)
	}
	n, err = goredis.Int(c.Do("skeyexist", key1))
	assert.Nil(t, err)
	assert.Equal(t, 0, n)
	n, err = goredis.Int(c.Do("sismember", key1, f3))
	assert.Nil(t, err)
	assert.Equal(t, 0, n)
	n, err = goredis.Int(c.Do("scard", key1))
	assert.Nil(t, err)
	assert.Equal(t, 0, n)
	vlist, err = goredis.MultiBulk(c.Do("smembers", key1))
	assert.Nil(t, err)
	t.Logf("smembers: %v", vlist)
	assert.Equal(t, 0, len(vlist))

	n, err = goredis.Int(c.Do("scard", key1))
	assert.Nil(t, err)
	assert.Equal(t, int(0), n)

	realTtl, err = goredis.Int(c.Do("sttl", key1))
	assert.Nil(t, err)
	assert.Equal(t, -1, realTtl)

	//persist expired data should not success
	_, err = c.Do("spersist", key1)
	assert.Nil(t, err)
	realTtl, err = goredis.Int(c.Do("sttl", key1))
	assert.Nil(t, err)
	assert.Equal(t, -1, realTtl)
	n, err = goredis.Int(c.Do("sismember", key1, f3))
	assert.Nil(t, err)
	assert.Equal(t, 0, n)
	n, err = goredis.Int(c.Do("scard", key1))
	assert.Nil(t, err)
	assert.Equal(t, 0, n)

	// renew hash
	n, err = goredis.Int(c.Do("sadd", key1, f4, f5))
	assert.Nil(t, err)
	assert.Equal(t, 2, n)
	c.Do("sexpire", key1, ttl)

	realTtl, err = goredis.Int(c.Do("sttl", key1))
	assert.Nil(t, err)
	assertTTLNear(t, ttl, realTtl)
	n, err = goredis.Int(c.Do("sismember", key1, f1))
	assert.Nil(t, err)
	assert.Equal(t, 0, n)
	n, err = goredis.Int(c.Do("sismember", key1, f4))
	assert.Nil(t, err)
	assert.Equal(t, 1, n)
	vlist, err = goredis.MultiBulk(c.Do("smembers", key1))
	assert.Nil(t, err)
	t.Logf("smembers: %v", vlist)
	assert.Equal(t, 2, len(vlist))

	n, err = goredis.Int(c.Do("scard", key1))
	assert.Nil(t, err)
	assert.Equal(t, 2, n)
	n, err = goredis.Int(c.Do("skeyexist", key1))
	assert.Nil(t, err)
	assert.Equal(t, 1, n)

	// persist
	_, err = c.Do("spersist", key1)
	assert.Nil(t, err)
	realTtl, err = goredis.Int(c.Do("sttl", key1))
	assert.Nil(t, err)
	assert.Equal(t, -1, realTtl)
	time.Sleep(time.Second * time.Duration(ttl+1))
	// should not expired
	n, err = goredis.Int(c.Do("sismember", key1, f5))
	assert.Nil(t, err)
	assert.Equal(t, 1, n)
	n, err = goredis.Int(c.Do("scard", key1))
	assert.Nil(t, err)
	assert.Equal(t, 2, n)
	n, err = goredis.Int(c.Do("skeyexist", key1))
	assert.Nil(t, err)
	assert.Equal(t, 1, n)

	// change ttl
	_, err = c.Do("sexpire", key1, ttl+4)
	assert.Nil(t, err)
	realTtl, err = goredis.Int(c.Do("sttl", key1))
	assert.Nil(t, err)
	assertTTLNear(t, ttl+4, realTtl)

	time.Sleep(time.Second * time.Duration(ttl+6))
	// check expired kv should not get from any read command
	if v, err := goredis.String(c.Do("spop", key1)); err != goredis.ErrNil {
		t.Fatalf("expired key should be expired: %v, %v", v, err)
	}
	n, err = goredis.Int(c.Do("skeyexist", key1))
	assert.Nil(t, err)
	assert.Equal(t, 0, n)
	n, err = goredis.Int(c.Do("sismember", key1, f5))
	assert.Nil(t, err)
	assert.Equal(t, 0, n)
	n, err = goredis.Int(c.Do("scard", key1))
	assert.Nil(t, err)
	assert.Equal(t, 0, n)
	vlist, err = goredis.MultiBulk(c.Do("smembers", key1))
	assert.Nil(t, err)
	t.Logf("smembers: %v", vlist)
	assert.Equal(t, 0, len(vlist))

	n, err = goredis.Int(c.Do("scard", key1))
	assert.Nil(t, err)
	assert.Equal(t, int(0), n)

	realTtl, err = goredis.Int(c.Do("sttl", key1))
	assert.Nil(t, err)
	assert.Equal(t, -1, realTtl)
}

func TestSetErrorParams(t *testing.T) {
	c := getTestConn(t)
	defer c.Close()

	key := "default:test:set_error_param"
	invalidKey := string(append([]byte("default:test:set_error_param"), make([]byte, 10240)...))
	invalidMember := string(append([]byte("long_param"), make([]byte, 10240)...))
	if _, err := c.Do("sadd", key); err == nil {
		t.Fatalf("invalid err of %v", err)
	}
	_, err := c.Do("sadd", invalidKey, key)
	assert.NotNil(t, err)
	_, err = c.Do("sadd", key, invalidMember)
	assert.NotNil(t, err)
	_, err = c.Do("sadd", key, key, invalidMember)
	assert.NotNil(t, err)
	_, err = c.Do("sadd", invalidKey, invalidMember)
	assert.NotNil(t, err)

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
	if _, err := c.Do("srandmember"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}
	if _, err := c.Do("srandmember", key, 0); err == nil {
		t.Fatalf("invalid err of %v", err)
	}
	if _, err := c.Do("srandmember", key, -1); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("smembers", key, key); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("spop"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}
	if _, err := c.Do("spop", key, "0"); err == nil {
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

func TestZSetEmptyMember(t *testing.T) {
	c := getTestConn(t)
	defer c.Close()

	key := "default:test:myzset_empty"

	if n, err := goredis.Int(c.Do("zadd", key, 4, "0", 5, nil)); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	if s, err := goredis.Int(c.Do("zscore", key, "0")); err != nil {
		t.Fatal(err)
	} else if s != 4 {
		t.Fatal(s)
	}
	if s, err := goredis.Int(c.Do("zscore", key, nil)); err != nil {
		t.Fatal(err)
	} else if s != 5 {
		t.Fatal(s)
	}
	if n, err := goredis.Int(c.Do("zadd", key, 3, "")); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}
	if s, err := goredis.Int(c.Do("zscore", key, "")); err != nil {
		t.Fatal(err)
	} else if s != 3 {
		t.Fatal(s)
	}
}

func TestZSetInfScore(t *testing.T) {
	//TODO: test +inf , -inf score
	// TODO: test negative score
}

func TestZSetFloat64Score(t *testing.T) {
	c := getTestConn(t)
	defer c.Close()

	key := "default:test:myzset_float"

	if n, err := goredis.Int(c.Do("zadd", key, 3, "a", 3, "b")); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	if n, err := goredis.Int(c.Do("zcard", key)); err != nil {
		t.Fatal(n)
	} else if n != 2 {
		t.Fatal(n)
	}

	if n, err := goredis.Int(c.Do("zadd", key, 3.1, "a", 3.2, "b")); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	if n, err := goredis.Int(c.Do("zcard", key)); err != nil {
		t.Fatal(n)
	} else if n != 2 {
		t.Fatal(n)
	}

	if n, err := goredis.Int(c.Do("zadd", key, 3.3, "c", 3.4, "d")); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	if n, err := goredis.Int(c.Do("zcard", key)); err != nil {
		t.Fatal(err)
	} else if n != 4 {
		t.Fatal(n)
	}

	if s, err := goredis.Float64(c.Do("zscore", key, "c")); err != nil {
		t.Fatal(err)
	} else if s != 3.3 {
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

	if n, err := goredis.Float64(c.Do("zincrby", key, 4, "c")); err != nil {
		t.Fatal(err)
	} else if n != 7.3 {
		t.Fatal(n)
	}

	if n, err := goredis.Float64(c.Do("zincrby", key, -4, "c")); err != nil {
		t.Fatal(err)
	} else if n != 3.3 {
		t.Fatal(n)
	}

	if n, err := goredis.Float64(c.Do("zincrby", key, 3.4, "d")); err != nil {
		t.Fatal(err)
	} else if n != 3.4 {
		t.Fatal(n)
	}
	if n, err := goredis.Int(c.Do("zrank", key, "a")); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}
	if n, err := goredis.Int(c.Do("zrevrank", key, "b")); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}
	if n, err := goredis.Int(c.Do("zrank", key, "c")); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}
	if n, err := goredis.Int(c.Do("zrank", key, "d")); err != nil {
		t.Fatal(err)
	} else if n != 3 {
		t.Fatal(n)
	}

	if n, err := goredis.Int(c.Do("zrevrank", key, "a")); err != nil {
		t.Fatal(err)
	} else if n != 3 {
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
	} else if n <= 0 {
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

	if _, err := c.Do("zadd", key, "0.1a", "a"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}
	invalidKey := string(append([]byte("default:test:zset_error_param"), make([]byte, 10240)...))
	invalidMember := string(append([]byte("long_param"), make([]byte, 10240)...))
	_, err := c.Do("zadd", invalidKey, 0, key)
	assert.NotNil(t, err)
	_, err = c.Do("zadd", key, 0, invalidMember)
	assert.NotNil(t, err)
	_, err = c.Do("zadd", key, 0, key, 1, invalidMember)
	assert.NotNil(t, err)
	_, err = c.Do("zadd", invalidKey, 0, invalidMember)
	assert.NotNil(t, err)

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

	//zcount
	if _, err := c.Do("zcount", key); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("zcount", key, "-inf", "=inf"); err == nil {
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
