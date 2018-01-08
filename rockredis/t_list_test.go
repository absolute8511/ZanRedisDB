package rockredis

import (
	"math/rand"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestListCodec(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	key := []byte("test:key")

	ek := lEncodeMetaKey(key)
	if k, err := lDecodeMetaKey(ek); err != nil {
		t.Fatal(err)
	} else if string(k) != "test:key" {
		t.Fatal(string(k))
	}

	ek, _ = convertRedisKeyToDBListKey(key, 1024)
	if tb, k, seq, err := lDecodeListKey(ek); err != nil {
		t.Fatal(err)
	} else if string(k) != "key" {
		t.Fatal(string(k))
	} else if seq != 1024 {
		t.Fatal(seq)
	} else if string(tb) != "test" {
		t.Fatal(string(tb))
	}
}

func TestListTrim(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	key := []byte("test:test_list_trim")

	init := func() {
		db.LClear(key)
		for i := 0; i < 100; i++ {
			n, err := db.RPush(0, key, []byte(strconv.Itoa(i)))
			if err != nil {
				t.Fatal(err)
			}
			if n != int64(i+1) {
				t.Fatal("length wrong")
			}
		}
	}

	init()

	err := db.LTrim(0, key, 0, 99)
	if err != nil {
		t.Fatal(err)
	}
	if l, _ := db.LLen(key); l != int64(100) {
		t.Fatal("wrong len:", l)
	}

	err = db.LTrim(0, key, 0, 50)
	if err != nil {
		t.Fatal(err)
	}
	if l, _ := db.LLen(key); l != int64(51) {
		t.Fatal("wrong len:", l)
	}
	for i := int64(0); i < 51; i++ {
		v, err := db.LIndex(key, i)
		if err != nil {
			t.Fatal(err)
		}
		if string(v) != strconv.Itoa(int(i)) {
			t.Fatal("wrong value")
		}
	}

	err = db.LTrim(0, key, 11, 30)
	if err != nil {
		t.Fatal(err)
	}
	if l, _ := db.LLen(key); l != int64(30-11+1) {
		t.Fatal("wrong len:", l)
	}
	for i := int64(11); i < 31; i++ {
		v, err := db.LIndex(key, i-11)
		if err != nil {
			t.Fatal(err)
		}
		if string(v) != strconv.Itoa(int(i)) {
			t.Fatal("wrong value")
		}
	}

	err = db.LTrim(0, key, 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	if l, _ := db.LLen(key); l != int64(30-11+1) {
		t.Fatal("wrong len:", l)
	}

	t.Logf("reinit list")
	init()
	err = db.LTrim(0, key, -3, -3)
	if err != nil {
		t.Fatal(err)
	}
	if l, _ := db.LLen(key); l != int64(1) {
		t.Fatal("wrong len:", l)
	}
	v, err := db.LIndex(key, 0)
	if err != nil {
		t.Fatal(err)
	}
	if string(v) != "97" {
		t.Fatal("wrong value", string(v))
	}
	// TODO: LTrimFront, LTrimBack
}

func TestDBList(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	key := []byte("test:testdb_list_a")

	if n, err := db.RPush(0, key, []byte("1"), []byte("2"), []byte("3")); err != nil {
		t.Fatal(err)
	} else if n != 3 {
		t.Fatal(n)
	}

	if ay, err := db.LRange(key, 0, -1); err != nil {
		t.Fatal(err)
	} else if len(ay) != 3 {
		t.Fatal(ay)
	} else {
		for i := range ay {
			if ay[i][0] != '1'+byte(i) {
				t.Fatal(string(ay[i]))
			}
		}
	}

	if k, err := db.RPop(0, key); err != nil {
		t.Fatal(err)
	} else if string(k) != "3" {
		t.Fatal(string(k))
	}

	if k, err := db.LPop(0, key); err != nil {
		t.Fatal(err)
	} else if string(k) != "1" {
		t.Fatal(string(k))
	}

	if llen, err := db.LLen(key); err != nil {
		t.Fatal(err)
	} else if llen != 1 {
		t.Fatal(llen)
	}

	if num, err := db.LClear(key); err != nil {
		t.Fatal(err)
	} else if num != 1 {
		t.Error(num)
	}

	if llen, _ := db.LLen(key); llen != 0 {
		t.Fatal(llen)
	}
	// TODO: LSet
}

func TestLKeyExists(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()
	key := []byte("test:lkeyexists_test")
	if n, err := db.LKeyExists(key); err != nil {
		t.Fatal(err.Error())
	} else if n != 0 {
		t.Fatal("invalid value ", n)
	}
	db.LPush(0, key, []byte("hello"), []byte("world"))
	if n, err := db.LKeyExists(key); err != nil {
		t.Fatal(err.Error())
	} else if n != 1 {
		t.Fatal("invalid value ", n)
	}
}

func TestListPop(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	key := []byte("test:lpop_test")

	if v, err := db.LPop(0, key); err != nil {
		t.Fatal(err)
	} else if v != nil {
		t.Fatal(v)
	}

	if s, err := db.LLen(key); err != nil {
		t.Fatal(err)
	} else if s != 0 {
		t.Fatal(s)
	}

	for i := 0; i < 10; i++ {
		if n, err := db.LPush(0, key, []byte("a")); err != nil {
			t.Fatal(err)
		} else if n != 1+int64(i) {
			t.Fatal(n)
		}
	}

	if s, err := db.LLen(key); err != nil {
		t.Fatal(err)
	} else if s != 10 {
		t.Fatal(s)
	}

	for i := 0; i < 10; i++ {
		if _, err := db.LPop(0, key); err != nil {
			t.Fatal(err)
		}
	}

	if s, err := db.LLen(key); err != nil {
		t.Fatal(err)
	} else if s != 0 {
		t.Fatal(s)
	}

	if v, err := db.LPop(0, key); err != nil {
		t.Fatal(err)
	} else if v != nil {
		t.Fatal(v)
	}
}

func TestListLPushEmpty(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	if testing.Verbose() {
		SetLogLevel(int32(4))
	}
	k1 := []byte("test:1")
	_, err := db.LPush(0, k1, nil)
	assert.Nil(t, err)
	_, err = db.LPush(0, k1, nil)
	assert.Nil(t, err)
	v, err := db.RPop(0, k1)
	assert.Nil(t, err)
	t.Log(v)
	assert.Equal(t, []byte(""), v)
	_, err = db.LPush(0, k1, []byte(""))
	assert.Nil(t, err)
	_, err = db.LPush(0, k1, []byte("a"))
	assert.Nil(t, err)
	n, err := db.LLen(k1)
	assert.Nil(t, err)
	assert.Equal(t, int64(3), n)
	ret, err := db.LRange(k1, 0, n)
	assert.Nil(t, err)
	t.Log(ret)
	assert.Equal(t, int(n), len(ret))
	for i := 0; i < len(ret); i++ {
		v, err := db.LIndex(k1, int64(i))
		assert.Nil(t, err)
		assert.Equal(t, ret[i], v)
	}
}

func TestListLPushRPop(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	k1 := []byte("test:1")
	k2 := []byte("test:2")

	s, err := db.LLen(k2)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), s)
	_, err = db.LPush(0, k2, []byte("a"))
	_, err = db.LPush(0, k2, []byte("b"))
	_, err = db.LPush(0, k1, []byte("a"))

	length, err := db.LLen(k1)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), length)
	length, err = db.LLen(k2)
	assert.Nil(t, err)
	assert.Equal(t, int64(2), length)
	_, err = db.RPop(0, k1)
	length, err = db.LLen(k1)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), length)
	_, err = db.RPop(0, k1)
	length, err = db.LLen(k1)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), length)
	db.LPush(0, k1, []byte("a"))
	db.LPush(0, k1, []byte("a"))
	db.LPush(0, k1, []byte("a"))
	db.LPush(0, k1, []byte("a"))
	db.RPop(0, k1)
	db.RPop(0, k1)
	db.LPush(0, k1, []byte("a"))
	db.LPush(0, k1, []byte("a"))
	db.RPop(0, k1)
	db.RPop(0, k1)
	db.LPush(0, k1, []byte("a"))
	db.RPop(0, k1)
	db.RPop(0, k1)
	length, err = db.LLen(k1)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), length)
	db.RPop(0, k1)
	length, err = db.LLen(k1)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), length)

	var pushed int32
	var poped int32

	var wg sync.WaitGroup
	wg.Add(2)
	start := time.Now()
	var mutex sync.Mutex
	go func() {
		defer wg.Done()
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		for {
			mutex.Lock()
			_, err := db.LPush(0, k1, []byte("a"))
			mutex.Unlock()
			assert.Nil(t, err)
			atomic.AddInt32(&pushed, 1)
			time.Sleep(time.Microsecond * time.Duration(r.Int31n(1000)))
			if time.Since(start) > time.Second*10 {
				break
			}
		}
	}()
	go func() {
		defer wg.Done()
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		for {
			mutex.Lock()
			v, err := db.RPop(0, k1)
			mutex.Unlock()
			assert.Nil(t, err)
			if v != nil {
				assert.Equal(t, []byte("a"), v)
				atomic.AddInt32(&poped, 1)
			}
			time.Sleep(time.Microsecond * time.Duration(r.Int31n(1000)))
			if time.Since(start) > time.Second*10 {
				break
			}
		}
	}()
	wg.Wait()

	length, err = db.LLen(k1)
	assert.Nil(t, err)
	t.Logf("pushed %v poped %v", pushed, poped)
	assert.True(t, pushed >= poped)
	assert.Equal(t, int64(pushed-poped), length)
}
