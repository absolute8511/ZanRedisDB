package rockredis

import (
	"bytes"
	"os"
	"testing"
)

func TestKVCodec(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)

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

	key1 := []byte("testdb_kv_a")

	if err := db.KVSet(key1, []byte("hello world 1")); err != nil {
		t.Fatal(err)
	}

	key2 := []byte("testdb_kv_b")

	if err := db.KVSet(key2, []byte("hello world 2")); err != nil {
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

	ay, errs := db.MGet(key1, key2)
	if len(ay) != 2 {
		t.Errorf("%v, %v", ay, errs)
	}

	if !bytes.Equal(v1, ay[0]) {
		t.Errorf("%v, %v", ay[0], errs[0])
	}

	if !bytes.Equal(v2, ay[1]) {
		t.Errorf("%v, %v", ay[1], errs[1])
	}

	key3 := []byte("testdb_kv_range")

	if n, err := db.Append(key3, []byte("Hello")); err != nil {
		t.Fatal(err)
	} else if n != 5 {
		t.Fatal(n)
	}

	if n, err := db.Append(key3, []byte(" World")); err != nil {
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

	if n, err := db.SetRange(key3, 6, []byte("Redis")); err != nil {
		t.Fatal(err)
	} else if n != 11 {
		t.Fatal(n)
	}

	if v, err := db.KVGet(key3); err != nil {
		t.Fatal(err)
	} else if string(v) != "Hello Redis" {
		t.Fatal(string(v))
	}

	key4 := []byte("testdb_kv_range_none")
	if n, err := db.SetRange(key4, 6, []byte("Redis")); err != nil {
		t.Fatal(err)
	} else if n != 11 {
		t.Fatal(n)
	}
	r, _ := db.KVExists(key3)
	if r == 0 {
		t.Errorf("key should exist: %v", r)
	}
	r, err := db.SetNX(key3, []byte(""))
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

	db.KVDel(key3)
	r, _ = db.KVExists(key3)
	if r != 0 {
		t.Errorf("key should not exist: %v", r)
	}
	key5 := []byte("test_kv_mset_key5")
	key6 := []byte("test_kv_mset_key6")
	err = db.MSet(KVPair{key3, []byte("key3")},
		KVPair{key5, []byte("key5")}, KVPair{key6, []byte("key6")})
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

}
