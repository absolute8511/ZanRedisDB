package rockredis

import (
	"bytes"
	"os"
	"strconv"
	"testing"
	"time"
)

func TestTTLCodec(t *testing.T) {
	ts := time.Now().Unix()

	key := []byte("kv-test-key")

	tk := expEncodeTimeKey(KVType, key, ts)

	if dt, k, when, err := expDecodeTimeKey(tk); err != nil {
		t.Fatal(err)
	} else if !(dt == KVType && bytes.Equal(k, key) && when == ts) {
		t.Fatal("expire time key encode and decode do not match")
	}

	mk := expEncodeMetaKey(KVType, key)
	if dt, k, err := expDecodeMetaKey(mk); err != nil {
		t.Fatal(err)
	} else if !(dt == KVType && bytes.Equal(k, key)) {
		t.Fatal("expire meta key encode and decode do not match")
	}
}

func TestTTL(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	ttlChecker := NewTTLChecker(db)

	key1 := []byte("test:testdb_ttl_kv_a")
	var ttl1 int64 = 1

	if err := db.KVSet(0, key1, []byte("hello world 1")); err != nil {
		t.Fatal(err)
	}

	db.KVExpire(key1, ttl1, ttlChecker)

	if v, err := db.KVTtl(key1); err != nil {
		t.Fatal(err)
	} else if v != ttl1 {
		t.Fatal("ttl != expire")
	}

	if err := db.KVPersist(key1); err != nil {
		t.Fatal(err)
	}

	if v, err := db.KVTtl(key1); err != nil {
		t.Fatal(err)
	} else if v != -1 {
		t.Fatal("KVPersist do not clear the ttl")
	}

}

func TestRockDBTTLChecker(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	kvMap := make(map[string]string)
	ttlChecker := NewTTLChecker(db)

	kvExpiredFunc := func(key []byte) error {
		if _, ok := kvMap[string(key)]; !ok {
			t.Fatal("unknown expired key", string(key))
		} else {
			delete(kvMap, string(key))
		}
		return nil
	}

	ttlChecker.RegisterKVExpired(kvExpiredFunc)

	for i := 0; i < expireLimitRange*3; i++ {
		key := "test:ttl_checker:" + strconv.Itoa(i)
		val := "val" + strconv.Itoa(i)
		kvMap[key] = val
		db.KVSet(0, []byte(key), []byte(val))
		db.KVExpire([]byte(key), 0, ttlChecker)
	}

	//time.Sleep(1 * time.Second)

	ttlChecker.check()

	if len(kvMap) != 0 {
		t.Fatalf("%d key do not expired", len(kvMap))
	}
}
