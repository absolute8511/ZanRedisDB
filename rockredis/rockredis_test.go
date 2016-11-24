package rockredis

import (
	"fmt"
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
}

func TestRockDB(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	key := []byte("test_kv_key")
	value := []byte("value")
	if err := db.KVSet(key, value); err != nil {
		t.Fatal(err)
	}

	if v, err := db.Get(key); err != nil {
		t.Fatal(err)
	} else if string(v) != string(value) {
		t.Fatal(string(v))
	}

	key = []byte("test_list_key")
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

	key = []byte("test_hash_key")
	if _, err := db.HSet(key, []byte("a"), value); err != nil {
		t.Fatal(err)
	}

	if v, err := db.HGet(key, []byte("a")); err != nil {
		t.Fatal(err)
	} else if string(v) != string(value) {
		t.Fatal(string(v))
	}

	key = []byte("test_set_key")
	if _, err := db.SAdd(key, []byte("a"), []byte("b")); err != nil {
		t.Fatal(err)
	}

	if n, err := db.SIsMember(key, []byte("a")); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	key = []byte("test_zset_key")
	if _, err := db.ZAdd(key, ScorePair{1, []byte("a")}, ScorePair{2, []byte("b")}); err != nil {
		t.Fatal(err)
	}

	if v, err := db.ZRangeByScore(key, 0, 100, 0, -1); err != nil {
		t.Fatal(err)
	} else if len(v) != 2 {
		t.Fatal(len(v))
	}
}
