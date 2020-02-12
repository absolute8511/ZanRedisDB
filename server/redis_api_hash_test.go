package server

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/siddontang/goredis"
	"github.com/stretchr/testify/assert"
)

func TestHashEmptyField(t *testing.T) {
	c := getTestConn(t)
	defer c.Close()

	key := "default:test:hashempty"
	_, err := c.Do("hset", key, "", "v1")
	assert.Nil(t, err)

	v, _ := goredis.String(c.Do("hget", key, ""))
	assert.Equal(t, "v1", v)

	n, err := goredis.Int(c.Do("hexists", key, ""))
	assert.Nil(t, err)
	assert.Equal(t, 1, n)

	_, err = c.Do("hdel", key, "")
	assert.Nil(t, err)

	v, err = goredis.String(c.Do("hget", key, ""))
	assert.Equal(t, goredis.ErrNil, err)
	assert.Equal(t, "", v)

	n, err = goredis.Int(c.Do("hexists", key, ""))
	assert.Nil(t, err)
	assert.Equal(t, 0, n)
}

func TestHash(t *testing.T) {
	c := getTestConn(t)
	defer c.Close()

	key := "default:test:hasha"

	if n, err := goredis.Int(c.Do("hset", key, 1, 0)); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}
	if n, err := goredis.Int(c.Do("hsetnx", key, 1, 0)); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

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

	if v, err := goredis.MultiBulk(c.Do("hkeys", key)); err != nil {
		t.Fatal(err)
	} else {
		if err := testHashArray(v, 1, 2, 3); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := goredis.MultiBulk(c.Do("hvals", key)); err != nil {
		t.Fatal(err)
	} else {
		if err := testHashArray(v, 1, 2, 3); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := goredis.MultiBulk(c.Do("hgetall", key)); err != nil {
		t.Fatal(err)
	} else {
		t.Log(v)
		if err := testHashArray(v, 1, 1, 2, 2, 3, 3); err != nil {
			t.Fatal(err)
		}
	}

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

func assertTTLNear(t *testing.T, ttl int, realTTL int) {
	assert.True(t, realTTL <= ttl, "real ttl should less or equal than set")
	assert.True(t, realTTL >= ttl-2, "real ttl should not diff too large")
}

func TestHashExpire(t *testing.T) {
	c := getTestConn(t)
	defer c.Close()

	key1 := "default:test:hash_expa"
	f1 := "f1"
	f2 := "f2"
	f3 := "f3"
	f4 := "f4"
	fint := "fint"
	ttl := 3

	n, err := goredis.Int(c.Do("hset", key1, f1, "hello"))
	assert.Nil(t, err)
	assert.Equal(t, 1, n)

	c.Do("hexpire", key1, ttl)
	if v, err := goredis.String(c.Do("hget", key1, f1)); err != nil {
		t.Fatal(err)
	} else if v != "hello" {
		t.Fatal(v)
	}
	n, err = goredis.Int(c.Do("hkeyexist", key1))
	assert.Nil(t, err)
	assert.Equal(t, 1, n)
	n, err = goredis.Int(c.Do("hexists", key1, f1))
	assert.Nil(t, err)
	assert.Equal(t, 1, n)

	realTtl, err := goredis.Int(c.Do("httl", key1))
	assert.Nil(t, err)
	assertTTLNear(t, ttl, realTtl)

	// check hset,hmset, hincrby keep ttl
	_, err = goredis.Int(c.Do("hset", key1, f2, " world"))
	assert.Nil(t, err)
	realTtl, err = goredis.Int(c.Do("httl", key1))
	assert.Nil(t, err)
	assertTTLNear(t, ttl, realTtl)

	_, err = goredis.String(c.Do("hmset", key1, f3, f3, f4, f4))
	assert.Nil(t, err)
	realTtl, err = goredis.Int(c.Do("httl", key1))
	assert.Nil(t, err)
	assertTTLNear(t, ttl, realTtl)

	vlist, err := goredis.MultiBulk(c.Do("hmget", key1, f1, f2, f3))
	assert.Nil(t, err)
	assert.Equal(t, 3, len(vlist))

	n, err = goredis.Int(c.Do("hincrby", key1, fint, 1))
	assert.Nil(t, err)
	assert.Equal(t, 1, n)
	realTtl, err = goredis.Int(c.Do("httl", key1))
	assert.Nil(t, err)
	assertTTLNear(t, ttl, realTtl)

	// wait expire
	time.Sleep(time.Second * time.Duration(ttl+2))

	if v, err := goredis.String(c.Do("hget", key1, f1)); err != goredis.ErrNil {
		t.Fatalf("expired hash key should be expired: %v, %v", v, err)
	}
	n, err = goredis.Int(c.Do("hkeyexist", key1))
	assert.Nil(t, err)
	assert.Equal(t, 0, n)
	n, err = goredis.Int(c.Do("hexists", key1, f1))
	assert.Nil(t, err)
	assert.Equal(t, 0, n)
	n, err = goredis.Int(c.Do("hlen", key1))
	assert.Nil(t, err)
	assert.Equal(t, 0, n)
	vlist, err = goredis.MultiBulk(c.Do("hmget", key1, f1, f2, f3))
	assert.Nil(t, err)
	t.Logf("hmget : %v", vlist)
	assert.Equal(t, 3, len(vlist))
	assert.Nil(t, vlist[0])
	assert.Nil(t, vlist[1])
	assert.Nil(t, vlist[2])

	vlist, err = goredis.MultiBulk(c.Do("hgetall", key1))
	assert.Nil(t, err)
	assert.Equal(t, 0, len(vlist))

	realTtl, err = goredis.Int(c.Do("httl", key1))
	assert.Nil(t, err)
	assert.Equal(t, -1, realTtl)

	//persist expired data should not success
	c.Do("hpersist", key1)
	realTtl, err = goredis.Int(c.Do("httl", key1))
	assert.Nil(t, err)
	assert.Equal(t, -1, realTtl)
	n, err = goredis.Int(c.Do("hexists", key1, f1))
	assert.Nil(t, err)
	assert.Equal(t, 0, n)
	n, err = goredis.Int(c.Do("hlen", key1))
	assert.Nil(t, err)
	assert.Equal(t, 0, n)

	// renew hash
	_, err = goredis.String(c.Do("hmset", key1, f3, f3, f4, f4))
	assert.Nil(t, err)
	c.Do("hexpire", key1, ttl)

	realTtl, err = goredis.Int(c.Do("httl", key1))
	assert.Nil(t, err)
	assertTTLNear(t, ttl, realTtl)
	n, err = goredis.Int(c.Do("hexists", key1, f1))
	assert.Nil(t, err)
	assert.Equal(t, 0, n)
	n, err = goredis.Int(c.Do("hexists", key1, f3))
	assert.Nil(t, err)
	assert.Equal(t, 1, n)
	n, err = goredis.Int(c.Do("hlen", key1))
	assert.Nil(t, err)
	assert.Equal(t, 2, n)
	n, err = goredis.Int(c.Do("hkeyexist", key1))
	assert.Nil(t, err)
	assert.Equal(t, 1, n)

	// persist
	c.Do("hpersist", key1)
	realTtl, err = goredis.Int(c.Do("httl", key1))
	assert.Nil(t, err)
	assert.Equal(t, -1, realTtl)
	time.Sleep(time.Second * time.Duration(ttl+1))
	// should not expired
	n, err = goredis.Int(c.Do("hexists", key1, f3))
	assert.Nil(t, err)
	assert.Equal(t, 1, n)
	n, err = goredis.Int(c.Do("hlen", key1))
	assert.Nil(t, err)
	assert.Equal(t, 2, n)
	n, err = goredis.Int(c.Do("hkeyexist", key1))
	assert.Nil(t, err)
	assert.Equal(t, 1, n)

	// change ttl
	_, err = c.Do("hexpire", key1, ttl+4)
	assert.Nil(t, err)
	realTtl, err = goredis.Int(c.Do("httl", key1))
	assert.Nil(t, err)
	assertTTLNear(t, ttl+4, realTtl)

	time.Sleep(time.Second * time.Duration(ttl+6))
	// check expired kv should not get from any read command
	if v, err := goredis.String(c.Do("hget", key1, f1)); err != goredis.ErrNil {
		t.Fatalf("expired hash key should be expired: %v, %v", v, err)
	}
	n, err = goredis.Int(c.Do("hkeyexist", key1))
	assert.Nil(t, err)
	assert.Equal(t, 0, n)
	n, err = goredis.Int(c.Do("hexists", key1, f1))
	assert.Nil(t, err)
	assert.Equal(t, 0, n)
	n, err = goredis.Int(c.Do("hlen", key1))
	assert.Nil(t, err)
	assert.Equal(t, 0, n)
	vlist, err = goredis.MultiBulk(c.Do("hmget", key1, f1, f2, f3))
	assert.Nil(t, err)
	t.Logf("hmget : %v", vlist)
	assert.Equal(t, 3, len(vlist))
	assert.Nil(t, vlist[0])
	assert.Nil(t, vlist[1])
	assert.Nil(t, vlist[2])

	vlist, err = goredis.MultiBulk(c.Do("hgetall", key1))
	assert.Nil(t, err)
	assert.Equal(t, 0, len(vlist))

	realTtl, err = goredis.Int(c.Do("httl", key1))
	assert.Nil(t, err)
	assert.Equal(t, -1, realTtl)
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
