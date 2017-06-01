package rockredis

import (
	"bytes"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/absolute8511/ZanRedisDB/common"
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

func TestKVTTL(t *testing.T) {
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

func TestHashTTL(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	ttlChecker := NewTTLChecker(db)

	hash_key := []byte("test:testdb_ttl_hash")
	var hash_ttl int64 = rand.Int63()

	if v, err := db.HashTtl(hash_key); err != nil {
		t.Fatal(err)
	} else if v != -1 {
		t.Fatal("ttl of not exist hash key is not -1")
	}

	hash_val := []common.KVRecord{
		common.KVRecord{Key: []byte("field0"), Value: []byte("value0")},
		common.KVRecord{Key: []byte("field1"), Value: []byte("value1")},
		common.KVRecord{Key: []byte("field2"), Value: []byte("value2")},
	}

	if err := db.HMset(hash_key, hash_val...); err != nil {
		t.Fatal(err)
	}

	db.HashExpire(hash_key, hash_ttl, ttlChecker)

	if v, err := db.HashTtl(hash_key); err != nil {
		t.Fatal(err)
	} else if v != hash_ttl {
		t.Fatal("ttl != expire")
	}

	if err := db.HashPersist(hash_key); err != nil {
		t.Fatal(err)
	}

	if v, err := db.HashTtl(hash_key); err != nil {
		t.Fatal(err)
	} else if v != -1 {
		t.Fatal("HashPersist do not clear the ttl")
	}
}

func TestListTTL(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	ttlChecker := NewTTLChecker(db)

	list_key := []byte("test:testdb_ttl_list")
	var list_ttl int64 = rand.Int63()

	if v, err := db.ListTtl(list_key); err != nil {
		t.Fatal(err)
	} else if v != -1 {
		t.Fatal("ttl of not exist list key is not -1")
	}

	if _, err := db.LPush(list_key, []byte("this"), []byte("is"), []byte("list"),
		[]byte("ttl"), []byte("test")); err != nil {
		t.Fatal(err)
	}

	db.ListExpire(list_key, list_ttl, ttlChecker)

	if v, err := db.ListTtl(list_key); err != nil {
		t.Fatal(err)
	} else if v != list_ttl {
		t.Fatal("ttl != expire")
	}

	if err := db.ListPersist(list_key); err != nil {
		t.Fatal(err)
	}

	if v, err := db.ListTtl(list_key); err != nil {
		t.Fatal(err)
	} else if v != -1 {
		t.Fatal("ListPersist do not clear the ttl")
	}
}

func TestSetTTL(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	ttlChecker := NewTTLChecker(db)

	set_key := []byte("test:testdb_ttl_set")
	var set_ttl int64 = rand.Int63()

	if v, err := db.SetTtl(set_key); err != nil {
		t.Fatal(err)
	} else if v != -1 {
		t.Fatal("ttl of not exist set key is not -1")
	}

	if _, err := db.SAdd(set_key, []byte("this"), []byte("is"), []byte("set"),
		[]byte("ttl"), []byte("test")); err != nil {
		t.Fatal(err)
	}

	db.SetExpire(set_key, set_ttl, ttlChecker)

	if v, err := db.SetTtl(set_key); err != nil {
		t.Fatal(err)
	} else if v != set_ttl {
		t.Fatal("ttl != expire")
	}

	if err := db.SetPersist(set_key); err != nil {
		t.Fatal(err)
	}

	if v, err := db.SetTtl(set_key); err != nil {
		t.Fatal(err)
	} else if v != -1 {
		t.Fatal("SetPersist do not clear the ttl")
	}
}

func TestZSetTTL(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	ttlChecker := NewTTLChecker(db)

	zset_key := []byte("test:testdb_ttl_zset")
	var zset_ttl int64 = rand.Int63()

	if v, err := db.ZSetTtl(zset_key); err != nil {
		t.Fatal(err)
	} else if v != -1 {
		t.Fatal("ttl of not exist zset key is not -1")
	}

	members := []common.ScorePair{
		common.ScorePair{Member: []byte("member1"), Score: 10},
		common.ScorePair{Member: []byte("member2"), Score: 20},
		common.ScorePair{Member: []byte("member3"), Score: 30},
		common.ScorePair{Member: []byte("member4"), Score: 40},
	}

	if _, err := db.ZAdd(zset_key, members...); err != nil {
		t.Fatal(err)
	}

	db.ZSetExpire(zset_key, zset_ttl, ttlChecker)

	if v, err := db.ZSetTtl(zset_key); err != nil {
		t.Fatal(err)
	} else if v != zset_ttl {
		t.Fatal("ttl != expire")
	}

	if err := db.ZSetPersist(zset_key); err != nil {
		t.Fatal(err)
	}

	if v, err := db.ZSetTtl(zset_key); err != nil {
		t.Fatal(err)
	} else if v != -1 {
		t.Fatal("ZSetPersist do not clear the ttl")
	}
}

func TestRockDBTTLChecker(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	kTypeMap := make(map[string]byte)
	ttlChecker := NewTTLChecker(db)

	dataTypes := []byte{KVType, ListType, HashType, SetType, ZSetType}

	createTestExpiredFunc := func(dataType byte) common.OnExpiredFunc {
		return func(key []byte) error {
			if kt, ok := kTypeMap[string(key)]; !ok {
				t.Fatal("unknown expired key", string(key))
			} else if kt != dataType {
				t.Fatal("mismatched callback called, %d - %d", dataType, t)
			} else {
				delete(kTypeMap, string(key))
			}
			return nil
		}
	}

	ttlChecker.RegisterKVExpired(createTestExpiredFunc(KVType))
	ttlChecker.RegisterHashExpired(createTestExpiredFunc(HashType))
	ttlChecker.RegisterListExpired(createTestExpiredFunc(ListType))
	ttlChecker.RegisterSetExpired(createTestExpiredFunc(SetType))
	ttlChecker.RegisterZSetExpired(createTestExpiredFunc(ZSetType))

	for i := 0; i < expireLimitRange*3+rand.Intn(expireLimitRange); i++ {
		key := "test:ttl_checker:" + strconv.Itoa(i)
		dataType := dataTypes[rand.Int()%len(dataTypes)]
		kTypeMap[key] = dataType
		if err := db.expire(dataType, []byte(key), 2, ttlChecker); err != nil {
			t.Fatal(err)
		}
	}

	time.Sleep(2 * time.Second)
	ttlChecker.check()

	if len(kTypeMap) != 0 {
		t.Fatalf("%d key do not expired", len(kTypeMap))
	}
}
