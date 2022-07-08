package server

import (
	"fmt"
	"testing"
	"time"

	"github.com/siddontang/goredis"
	"github.com/stretchr/testify/assert"
)

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
					t.Errorf(fmt.Sprintf("%d %s != %v", i, string(v), values[i]))
				}
			} else if len(v)+8 == len(vv) {
				if string(v) != vv[:len(v)] {
					t.Errorf(fmt.Sprintf("%d %s != %v", i, string(v), values[i]))
				}
			} else {
				t.Errorf(fmt.Sprintf("%d %s != %v", i, string(v), values[i]))
			}
		}
	}
}

func checkAdvanceScan(t *testing.T, c *goredis.PoolConn, tp string) {
	var cursor string
	if ay, err := goredis.Values(c.Do("ADVSCAN", "default:testscan:", tp, "count", 5)); err != nil {
		t.Error(err)
	} else if len(ay) != 2 {
		t.Fatal(len(ay))
		//} else if n := ay[0].([]byte); string(n) != "MDpkR1Z6ZEhOallXNDZOQT09Ow==" {
	} else if n := ay[0].([]byte); string(n) != "MDpOQT09Ow==" {
		t.Fatal(string(n))
	} else {
		cursor = string(n)
		checkScanValues(t, ay[1], "0", "1", "2", "3", "4")
	}

	if ay, err := goredis.Values(c.Do("ADVSCAN", "default:testscan:"+cursor, tp, "count", 6)); err != nil {
		t.Fatal(err)
	} else if len(ay) != 2 {
		t.Fatal(len(ay))
	} else if n := ay[0].([]byte); string(n) != "" {
		t.Fatal(string(n))
	} else {
		checkScanValues(t, ay[1], "5", "6", "7", "8", "9")
	}

	// cursor 9 for base64(0:base64(9);) is MDpPUT09Ow==
	if ay, err := goredis.Values(c.Do("ADVSCAN", "default:testscan:MDpPUT09Ow==", tp, "count", 1)); err != nil {
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

func checkAdvanceRevScan(t *testing.T, c *goredis.PoolConn, tp string) {
	var cursor string
	// use cursor 0::; to search, base64 is base64(0:base64(:);)
	if ay, err := goredis.Values(c.Do("ADVREVSCAN", "default:testscan:MDpPZz09Ow==", tp, "count", 5)); err != nil {
		t.Error(err)
		return
	} else if len(ay) != 2 {
		t.Fatal(len(ay))
		//} else if n := ay[0].([]byte); string(n) != "MDpkR1Z6ZEhOallXNDZOQT09Ow==" {
	} else if n := ay[0].([]byte); string(n) != "MDpOUT09Ow==" {
		t.Fatal(string(n))
	} else {
		cursor = string(n)
		checkScanValues(t, ay[1], "9", "8", "7", "6", "5")
	}

	if ay, err := goredis.Values(c.Do("ADVREVSCAN", "default:testscan:"+cursor, tp, "count", 6)); err != nil {
		t.Error(err)
		return
	} else if len(ay) != 2 {
		t.Fatal(len(ay))
	} else if n := ay[0].([]byte); string(n) != "" {
		t.Fatal(string(n))
	} else {
		checkScanValues(t, ay[1], "4", "3", "2", "1", "0")
	}

	// testscan:9 for base64 is MDpkR1Z6ZEhOallXNDZPUT09Ow=
	if ay, err := goredis.Values(c.Do("ADVREVSCAN", "default:testscan:", tp, "count", 1)); err != nil {
		t.Error(err)
		return
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

	if testing.Verbose() {
		changeLogLevel(t, 4, gredisport+1)
	}
	testKVScan(t, c)
	testHashKeyScan(t, c)
	testListKeyScan(t, c)
	testZSetKeyScan(t, c)
	testSetKeyScan(t, c)
	changeLogLevel(t, 2, gredisport+1)
}

func testKVScan(t *testing.T, c *goredis.PoolConn) {
	for i := 0; i < 10; i++ {
		if _, err := c.Do("set", "default:testscan:"+fmt.Sprintf("%d", i), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}
	checkAdvanceScan(t, c, "KV")
	checkAdvanceRevScan(t, c, "KV")
}

func testHashKeyScan(t *testing.T, c *goredis.PoolConn) {
	for i := 0; i < 10; i++ {
		if _, err := c.Do("hset", "default:testscan:"+fmt.Sprintf("%d", i), fmt.Sprintf("%d", i), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}

	checkAdvanceScan(t, c, "HASH")
	checkAdvanceRevScan(t, c, "HASH")
}

func testListKeyScan(t *testing.T, c *goredis.PoolConn) {
	for i := 0; i < 10; i++ {
		if _, err := c.Do("lpush", "default:testscan:"+fmt.Sprintf("%d", i), fmt.Sprintf("%d", i)); err != nil {
			t.Fatal(err)
		}
	}

	checkAdvanceScan(t, c, "LIST")
	checkAdvanceRevScan(t, c, "HASH")
}

func testZSetKeyScan(t *testing.T, c *goredis.PoolConn) {
	for i := 0; i < 10; i++ {
		if _, err := c.Do("zadd", "default:testscan:"+fmt.Sprintf("%d", i), i, []byte("value")); err != nil {
			t.Fatal(err)
		}
	}

	checkAdvanceScan(t, c, "ZSET")
	checkAdvanceRevScan(t, c, "HASH")
}

func testSetKeyScan(t *testing.T, c *goredis.PoolConn) {
	for i := 0; i < 10; i++ {
		if _, err := c.Do("sadd", "default:testscan:"+fmt.Sprintf("%d", i), fmt.Sprintf("%d", i)); err != nil {
			t.Fatal(err)
		}
	}

	checkAdvanceScan(t, c, "SET")
	checkAdvanceRevScan(t, c, "HASH")
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

	if ay, err := goredis.Values(c.Do("HREVSCAN", key, "c")); err != nil {
		t.Fatal(err)
	} else if len(ay) != 2 {
		t.Fatal(len(ay))
	} else {
		checkScanValues(t, ay[1], "b", 2, "a", 1)
	}
	// test scan expired hash key
	c.Do("hexpire", key, 1)
	time.Sleep(time.Second * 2)
	ay, err := goredis.Values(c.Do("HSCAN", key, ""))
	assert.Nil(t, err)
	assert.Equal(t, 2, len(ay))
	ayv, err := goredis.Strings(ay[1], nil)
	assert.Equal(t, 0, len(ayv))

	ay, err = goredis.Values(c.Do("HREVSCAN", key, "c"))
	assert.Nil(t, err)
	assert.Equal(t, 2, len(ay))
	ayv, err = goredis.Strings(ay[1], nil)
	assert.Equal(t, 0, len(ayv))
	// test scan expired and new created hash key
	c.Do("HMSET", key, "a", 2, "b", 3, "c", 4)
	ay, err = goredis.Values(c.Do("HSCAN", key, ""))
	assert.Nil(t, err)
	assert.Equal(t, 2, len(ay))
	checkScanValues(t, ay[1], "a", 2, "b", 3, "c", 4)

	ay, err = goredis.Values(c.Do("HREVSCAN", key, "d"))
	assert.Nil(t, err)
	assert.Equal(t, 2, len(ay))
	checkScanValues(t, ay[1], "c", 4, "b", 3, "a", 2)
}

func TestSetScan(t *testing.T) {
	c := getTestConn(t)
	defer c.Close()

	if testing.Verbose() {
		changeLogLevel(t, 4, gredisport+1)
	}
	key := "default:test:scan_set"
	c.Do("SADD", key, "a", "b")

	if ay, err := goredis.Values(c.Do("SSCAN", key, "")); err != nil {
		t.Fatal(err)
	} else if len(ay) != 2 {
		t.Fatal(len(ay))
	} else {
		checkScanValues(t, ay[1], "a", "b")
	}

	if ay, err := goredis.Values(c.Do("SREVSCAN", key, "c")); err != nil {
		t.Fatal(err)
	} else if len(ay) != 2 {
		t.Fatal(len(ay))
	} else {
		checkScanValues(t, ay[1], "b", "a")
	}
	// test scan expired key
	c.Do("sexpire", key, 1)
	time.Sleep(time.Second * 2)
	ay, err := goredis.Values(c.Do("SSCAN", key, ""))
	assert.Nil(t, err)
	assert.Equal(t, 2, len(ay))
	ayv, err := goredis.Strings(ay[1], nil)
	assert.Equal(t, 0, len(ayv))

	ay, err = goredis.Values(c.Do("SREVSCAN", key, "c"))
	assert.Nil(t, err)
	assert.Equal(t, 2, len(ay))
	ayv, err = goredis.Strings(ay[1], nil)
	assert.Equal(t, 0, len(ayv))
	// test scan expired and new created key
	c.Do("SADD", key, "c", "d")
	ay, err = goredis.Values(c.Do("SSCAN", key, ""))
	assert.Nil(t, err)
	assert.Equal(t, 2, len(ay))
	checkScanValues(t, ay[1], "c", "d")

	ay, err = goredis.Values(c.Do("SREVSCAN", key, "e"))
	assert.Nil(t, err)
	assert.Equal(t, 2, len(ay))
	checkScanValues(t, ay[1], "d", "c")
	changeLogLevel(t, 2, gredisport+1)
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

	if ay, err := goredis.Values(c.Do("ZREVSCAN", key, "c")); err != nil {
		t.Fatal(err)
	} else if len(ay) != 2 {
		t.Fatal(len(ay))
	} else {
		checkScanValues(t, ay[1], "b", 2, "a", 1)
	}
}
