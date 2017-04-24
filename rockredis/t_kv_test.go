package rockredis

import (
	"bytes"
	"github.com/absolute8511/ZanRedisDB/common"
	"os"
	"testing"
)

func TestKVCodec(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	ek := encodeKVKey([]byte("key"))

	if k, err := decodeKVKey(ek); err != nil {
		t.Fatal(err)
	} else if string(k) != "key" {
		t.Fatal(string(k))
	}
}

func TestDBKV(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	key1 := []byte("test:testdb_kv_a")

	if err := db.KVSet(0, key1, []byte("hello world 1")); err != nil {
		t.Fatal(err)
	}

	key2 := []byte("test:testdb_kv_b")

	if err := db.KVSet(0, key2, []byte("hello world 2")); err != nil {
		t.Fatal(err)
	}
	v1, _ := db.KVGet(key1)
	if string(v1) != "hello world 1" {
		t.Error(v1)
	}
	v2, _ := db.KVGet(key2)
	if string(v2) != "hello world 2" {
		t.Error(v2)
	}
	num, err := db.GetTableKeyCount([]byte("test"))
	if err != nil {
		t.Error(err)
	} else if num != 2 {
		t.Errorf("table count not as expected: %v", num)
	}

	ay, errs := db.MGet(key1, key2)
	if len(ay) != 2 {
		t.Errorf("%v, %v", ay, errs)
	}

	if !bytes.Equal(v1, ay[0]) {
		t.Errorf("%v, %v", ay[0], v1)
	}

	if !bytes.Equal(v2, ay[1]) {
		t.Errorf("%v, %v", ay[1], v2)
	}

	key3 := []byte("test:testdb_kv_range")

	if n, err := db.Append(0, key3, []byte("Hello")); err != nil {
		t.Fatal(err)
	} else if n != 5 {
		t.Fatal(n)
	}

	if n, err := db.Append(0, key3, []byte(" World")); err != nil {
		t.Fatal(err)
	} else if n != 11 {
		t.Fatal(n)
	}

	if n, err := db.StrLen(key3); err != nil {
		t.Fatal(err)
	} else if n != 11 {
		t.Fatal(n)
	}

	if v, err := db.GetRange(key3, 0, 4); err != nil {
		t.Fatal(err)
	} else if string(v) != "Hello" {
		t.Fatal(string(v))
	}

	if v, err := db.GetRange(key3, 0, -1); err != nil {
		t.Fatal(err)
	} else if string(v) != "Hello World" {
		t.Fatal(string(v))
	}

	if v, err := db.GetRange(key3, -5, -1); err != nil {
		t.Fatal(err)
	} else if string(v) != "World" {
		t.Fatal(string(v))
	}

	if n, err := db.SetRange(0, key3, 6, []byte("Redis")); err != nil {
		t.Fatal(err)
	} else if n != 11 {
		t.Fatal(n)
	}

	if v, err := db.KVGet(key3); err != nil {
		t.Fatal(err)
	} else if string(v) != "Hello Redis" {
		t.Fatal(string(v))
	}

	key4 := []byte("test:testdb_kv_range_none")
	if n, err := db.SetRange(0, key4, 6, []byte("Redis")); err != nil {
		t.Fatal(err)
	} else if n != 11 {
		t.Fatal(n)
	}
	r, _ := db.KVExists(key3)
	if r == 0 {
		t.Errorf("key should exist: %v", r)
	}
	r, err = db.SetNX(0, key3, []byte(""))
	if err != nil {
		t.Errorf("setnx failed: %v", err)
	}
	if r != 0 {
		t.Errorf("should set only not exist: %v", r)
	}
	if v, err := db.KVGet(key3); err != nil {
		t.Error(err)
	} else if string(v) != "Hello Redis" {
		t.Error(string(v))
	}
	num, err = db.GetTableKeyCount([]byte("test"))
	if err != nil {
		t.Error(err)
	} else if num != 4 {
		t.Errorf("table count not as expected: %v", num)
	}

	db.KVDel(key3)
	r, _ = db.KVExists(key3)
	if r != 0 {
		t.Errorf("key should not exist: %v", r)
	}
	num, err = db.GetTableKeyCount([]byte("test"))
	if err != nil {
		t.Error(err)
	} else if num != 3 {
		t.Errorf("table count not as expected: %v", num)
	}

	key5 := []byte("test:test_kv_mset_key5")
	key6 := []byte("test:test_kv_mset_key6")
	err = db.MSet(0, common.KVRecord{Key: key3, Value: []byte("key3")},
		common.KVRecord{Key: key5, Value: []byte("key5")}, common.KVRecord{Key: key6, Value: []byte("key6")})
	if err != nil {
		t.Errorf("fail mset: %v", err)
	}
	if v, err := db.KVGet(key3); err != nil {
		t.Error(err)
	} else if string(v) != "key3" {
		t.Error(string(v))
	}
	if v, err := db.KVGet(key5); err != nil {
		t.Error(err)
	} else if string(v) != "key5" {
		t.Error(string(v))
	}
	if v, err := db.KVGet(key6); err != nil {
		t.Error(err)
	} else if string(v) != "key6" {
		t.Error(string(v))
	}
	num, err = db.GetTableKeyCount([]byte("test"))
	if err != nil {
		t.Error(err)
	} else if num != 6 {
		t.Errorf("table count not as expected: %v", num)
	}

}

func TestDBKVWithNoTable(t *testing.T) {
	db := getTestDBNoTableCounter(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	key1 := []byte("testdb_kv_a")

	if err := db.KVSet(0, key1, []byte("hello world 1")); err == nil {
		t.Error("should error without table")
	}

	key2 := []byte("test:testdb_kv_b")

	if err := db.KVSet(0, key2, []byte("hello world 2")); err != nil {
		t.Fatal(err)
	}
	v1, _ := db.KVGet(key1)
	if v1 != nil {
		t.Error(v1)
	}
	v2, err := db.KVGet([]byte("testdb_kv_b"))
	if err == nil {
		t.Error("should be error while get without table")
	}

	v2, _ = db.KVGet(key2)
	if string(v2) != "hello world 2" {
		t.Error(v2)
	}

	key3 := []byte("testdb_kv_range")

	if _, err := db.Append(0, key3, []byte("Hello")); err == nil {
		t.Error("should failed")
	}

	key5 := []byte("test_kv_mset_key5")
	key6 := []byte("test:test_kv_mset_key6")
	err = db.MSet(0, common.KVRecord{Key: key3, Value: []byte("key3")},
		common.KVRecord{Key: key5, Value: []byte("key5")}, common.KVRecord{Key: key6, Value: []byte("key6")})
	if err == nil {
		t.Error("should failed")
	}
	if _, err := db.KVGet(key5); err == nil {
		t.Error("should failed")
	}
	if v, err := db.KVGet(key6); err != nil {
		t.Error("failed to get no value")
	} else if v != nil {
		t.Error("should get no value")
	}
}
