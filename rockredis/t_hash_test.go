package rockredis

import (
	"encoding/binary"
	"fmt"
	"os"
	"testing"

	"github.com/absolute8511/ZanRedisDB/common"
)

func TestHashCodec(t *testing.T) {
	key := []byte("key")
	field := []byte("field")

	ek := hEncodeSizeKey(key)
	if k, err := hDecodeSizeKey(ek); err != nil {
		t.Fatal(err)
	} else if string(k) != "key" {
		t.Fatal(string(k))
	}

	ek = hEncodeHashKey([]byte("test"), key, field)
	if table, k, f, err := hDecodeHashKey(ek); err != nil {
		t.Fatal(err)
	} else if string(k) != "key" {
		t.Fatal(string(k))
	} else if string(f) != "field" {
		t.Fatal(string(f))
	} else if string(table) != "test" {
		t.Fatal(string(table))
	}
}

func TestDBHash(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	key := []byte("test:testdb_hash_a")

	if n, err := db.HSet(key, []byte("a"), []byte("hello world 1")); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if n, err := db.HSet(key, []byte("b"), []byte("hello world 2")); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	v1, _ := db.HGet(key, []byte("a"))
	v2, _ := db.HGet(key, []byte("b"))
	if string(v1) != "hello world 1" {
		t.Error(v1)
	}
	if string(v2) != "hello world 2" {
		t.Error(v2)
	}
	ay, _ := db.HMget(key, []byte("a"), []byte("b"))

	if string(v1) != string(ay[0]) {
		t.Error(ay[0])
	}

	if string(v2) != string(ay[1]) {
		t.Error(ay[1])
	}

	len, err := db.HLen(key)
	if err != nil {
		t.Error(err)
	}
	if len != 2 {
		t.Errorf("length should be 2: %v", len)
	}
	_, ch, _ := db.HGetAll(key)
	results := make([]common.KVRecordRet, 0)
	for r := range ch {
		results = append(results, r)
	}
	if string(results[0].Rec.Key) != "a" {
		t.Error(results)
	}
	if string(results[0].Rec.Value) != "hello world 1" {
		t.Error(results)
	}
	if string(results[1].Rec.Key) != "b" {
		t.Error(results)
	}

	if string(results[1].Rec.Value) != "hello world 2" {
		t.Error(results)
	}

	_, ch, _ = db.HKeys(key)
	results = make([]common.KVRecordRet, 0)
	for r := range ch {
		results = append(results, r)
	}
	if string(results[0].Rec.Key) != "a" {
		t.Error(results)
	}
	if string(results[1].Rec.Key) != "b" {
		t.Error(results)
	}
	_, ch, _ = db.HValues(key)
	results = make([]common.KVRecordRet, 0)
	for r := range ch {
		results = append(results, r)
	}
	if string(results[0].Rec.Value) != "hello world 1" {
		t.Error(results)
	}
	if string(results[1].Rec.Value) != "hello world 2" {
		t.Error(results)
	}
}

func TestHashKeyExists(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()
	key := []byte("test:hkeyexists_test")
	v, err := db.HKeyExists(key)
	if err != nil {
		t.Fatal(err.Error())
	}
	if v != 0 {
		t.Fatal("invalid value ", v)
	}

	if _, err := db.HSet(key, []byte("hello"), []byte("world")); err != nil {
		t.Fatal(err.Error())
	}

	v, err = db.HKeyExists(key)
	if err != nil {
		t.Fatal(err.Error())
	}
	if v != 1 {
		t.Fatal("invalid value ", v)
	}
	if _, err := db.HSet(key, []byte("hello2"), []byte("world2")); err != nil {
		t.Fatal(err.Error())
	}
	db.HDel(key, []byte("hello"))
	v, err = db.HKeyExists(key)
	if err != nil {
		t.Fatal(err.Error())
	}
	if v != 1 {
		t.Fatal("invalid value ", v)
	}
	db.HClear(key)
	v, err = db.HKeyExists(key)
	if err != nil {
		t.Fatal(err.Error())
	}
	if v != 0 {
		t.Fatal("invalid value ", v)
	}
}

func TestHashKeyIncrBy(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()
	key := []byte("test:hkey_incr_test")
	if _, err := db.HSet(key, []byte("hello"), []byte("0")); err != nil {
		t.Fatal(err.Error())
	}

	r, _ := db.HIncrBy(key, []byte("hello"), 3)
	if r != 3 {
		t.Error(r)
	}
	r, _ = db.HIncrBy(key, []byte("hello"), -6)
	if r != -3 {
		t.Error(r)
	}
}

func TestHashScan(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()
	key := []byte("test:hkey_incr_test")
	if _, err := db.HSet(key, []byte("hello"), []byte("0")); err != nil {
		t.Fatal(err.Error())
	}

	prefix := []byte("test")

	minKey := make([]byte, len(prefix)+1+1+2)

	pos := 0
	minKey[pos] = HashType
	pos++

	binary.BigEndian.PutUint16(minKey[pos:], uint16(len(prefix)))
	pos += 2

	copy(minKey[pos:], prefix)
	pos += len(prefix)

	maxKey := make([]byte, 1+2)

	pos = 0
	maxKey[pos] = HashType + 1
	pos++

	binary.BigEndian.PutUint16(maxKey[pos:], uint16(0))
	pos += 2
	fmt.Println(minKey, maxKey)
	it, err := NewDBRangeIterator(db.eng, minKey, maxKey, common.RangeOpen, false)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(it.Valid())
}
