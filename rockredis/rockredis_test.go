package rockredis

import (
	"fmt"
	"github.com/absolute8511/ZanRedisDB/common"
	"io/ioutil"
	"os"
	"testing"
	"time"
)

func getTestDB(t *testing.T) *RockDB {
	cfg := NewRockConfig()
	var err error
	cfg.DataDir, err = ioutil.TempDir("", fmt.Sprintf("rockredis-test-%d", time.Now().UnixNano()))
	if err != nil {
		t.Fatal(err)
	}
	testDB, err := OpenRockDB(cfg)
	if err != nil {
		t.Fatal(err.Error())
	}
	return testDB
}

func TestDB(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()
}

func TestRockDB(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()
	key := []byte("test:test_kv_key")
	value := []byte("value")
	if err := db.KVSet(0, key, value); err != nil {
		t.Fatal(err)
	}

	if v, err := db.KVGet(key); err != nil {
		t.Fatal(err)
	} else if string(v) != string(value) {
		t.Fatal(string(v))
	}

	key = []byte("test:test_list_key")
	if _, err := db.LPush(key, value); err != nil {
		t.Fatal(err)
	}

	if _, err := db.LRange(key, 0, 100); err != nil {
		t.Fatal(err)
	}

	if v, err := db.LPop(key); err != nil {
		t.Fatal(err)
	} else if string(v) != string(value) {
		t.Fatal(string(v))
	}

	key = []byte("test:test_hash_key")
	if _, err := db.HSet(key, []byte("a"), value); err != nil {
		t.Fatal(err)
	}

	if v, err := db.HGet(key, []byte("a")); err != nil {
		t.Fatal(err)
	} else if string(v) != string(value) {
		t.Fatal(string(v))
	}

	key = []byte("test:test_set_key")
	if _, err := db.SAdd(key, []byte("a"), []byte("b")); err != nil {
		t.Fatal(err)
	}

	if n, err := db.SIsMember(key, []byte("a")); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	key = []byte("test:test_zset_key")
	if _, err := db.ZAdd(key, common.ScorePair{Score: 1, Member: []byte("a")},
		common.ScorePair{Score: 2, Member: []byte("b")}); err != nil {
		t.Fatal(err)
	}

	if v, err := db.ZRangeByScore(key, 0, 100, 0, -1); err != nil {
		t.Fatal(err)
	} else if len(v) != 2 {
		t.Fatal(len(v))
	}
}
