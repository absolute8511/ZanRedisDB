package rockredis

import (
	"bytes"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/absolute8511/ZanRedisDB/common"
)

//TODO
//the commands need to test

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

	key1 := []byte("test:testdb_ttl_kv_a")
	var ttl1 int64 = rand.Int63()

	if v, err := db.Expire(key1, ttl1); err != nil {
		t.Fatal(err)
	} else if v != 0 {
		t.Fatal("return value from expire of not exist key != 0")
	}

	if v, err := db.Persist(key1); err != nil {
		t.Fatal(err)
	} else if v != 0 {
		t.Fatal("return value from persist of not exist key != 0")
	}

	if err := db.KVSet(0, key1, []byte("hello world 1")); err != nil {
		t.Fatal(err)
	}

	if v, err := db.Expire(key1, ttl1); err != nil {
		t.Fatal(err)
	} else if v != 1 {
		t.Fatal("return value from expire != 1")
	}

	if v, err := db.KVTtl(key1); err != nil {
		t.Fatal(err)
	} else if v != ttl1 {
		t.Fatal("ttl != expire")
	}

	if v, err := db.Persist(key1); err != nil {
		t.Fatal(err)
	} else if v != 1 {
		t.Fatal("return value from persist != 1")
	}

	if v, err := db.KVTtl(key1); err != nil {
		t.Fatal(err)
	} else if v != -1 {
		t.Fatal("KVPersist do not clear the ttl")
	}

	testValue := []byte("test value for SetEx command")
	if err := db.SetEx(0, key1, ttl1, testValue); err != nil {
		t.Fatal(err)
	}

	if v, err := db.KVGet(key1); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(v, testValue) {
		t.Fatal("SetEx: gotten value != set value")
	}

	if v, err := db.KVTtl(key1); err != nil {
		t.Fatal(err)
	} else if v != ttl1 {
		t.Fatal("ttl != setex")
	}
}

func TestHashTTL(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	hash_key := []byte("test:testdb_ttl_hash")
	var hash_ttl int64 = rand.Int63()

	if v, err := db.HashTtl(hash_key); err != nil {
		t.Fatal(err)
	} else if v != -1 {
		t.Fatal("ttl of not exist hash key is not -1")
	}

	if v, err := db.HExpire(hash_key, hash_ttl); err != nil {
		t.Fatal(err)
	} else if v != 0 {
		t.Fatal("return value from expire of not exist hash key != 0")
	}

	if v, err := db.HPersist(hash_key); err != nil {
		t.Fatal(err)
	} else if v != 0 {
		t.Fatal("return value from hpersist of not exist hash key != 0")
	}

	hash_val := []common.KVRecord{
		common.KVRecord{Key: []byte("field0"), Value: []byte("value0")},
		common.KVRecord{Key: []byte("field1"), Value: []byte("value1")},
		common.KVRecord{Key: []byte("field2"), Value: []byte("value2")},
	}

	if err := db.HMset(hash_key, hash_val...); err != nil {
		t.Fatal(err)
	}

	if v, err := db.HExpire(hash_key, hash_ttl); err != nil {
		t.Fatal(err)
	} else if v != 1 {
		t.Fatal("return value from hexpire != 1")
	}

	if v, err := db.HashTtl(hash_key); err != nil {
		t.Fatal(err)
	} else if v != hash_ttl {
		t.Fatal("ttl != expire")
	}

	if v, err := db.HPersist(hash_key); err != nil {
		t.Fatal(err)
	} else if v != 1 {
		t.Fatal("return value from hpersist is != 1")
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

	list_key := []byte("test:testdb_ttl_list")
	var list_ttl int64 = rand.Int63()

	if v, err := db.ListTtl(list_key); err != nil {
		t.Fatal(err)
	} else if v != -1 {
		t.Fatal("ttl of not exist list key is not -1")
	}

	if v, err := db.LExpire(list_key, list_ttl); err != nil {
		t.Fatal(err)
	} else if v != 0 {
		t.Fatal("return value from expire of not exist list key != 0")
	}

	if v, err := db.LPersist(list_key); err != nil {
		t.Fatal(err)
	} else if v != 0 {
		t.Fatal("return value from lpersist of not exist list key != 0")
	}

	if _, err := db.LPush(list_key, []byte("this"), []byte("is"), []byte("list"),
		[]byte("ttl"), []byte("test")); err != nil {
		t.Fatal(err)
	}

	if v, err := db.LExpire(list_key, list_ttl); err != nil {
		t.Fatal(err)
	} else if v != 1 {
		t.Fatal("return value from lexpire != 1")
	}

	if v, err := db.ListTtl(list_key); err != nil {
		t.Fatal(err)
	} else if v != list_ttl {
		t.Fatal("ttl != expire")
	}

	if v, err := db.LPersist(list_key); err != nil {
		t.Fatal(err)
	} else if v != 1 {
		t.Fatal("return value from lpersist != 1")
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

	set_key := []byte("test:testdb_ttl_set")
	var set_ttl int64 = rand.Int63()

	if v, err := db.SetTtl(set_key); err != nil {
		t.Fatal(err)
	} else if v != -1 {
		t.Fatal("ttl of not exist set key is not -1")
	}

	if v, err := db.SExpire(set_key, set_ttl); err != nil {
		t.Fatal(err)
	} else if v != 0 {
		t.Fatal("return value from expire of not exist set key != 0")
	}

	if v, err := db.SPersist(set_key); err != nil {
		t.Fatal(err)
	} else if v != 0 {
		t.Fatal("return value from spersist of not exist set key != 0")
	}

	if _, err := db.SAdd(set_key, []byte("this"), []byte("is"), []byte("set"),
		[]byte("ttl"), []byte("test")); err != nil {
		t.Fatal(err)
	}

	if v, err := db.SExpire(set_key, set_ttl); err != nil {
		t.Fatal(err)
	} else if v != 1 {
		t.Fatal("return value from sexpire != 1")
	}

	if v, err := db.SetTtl(set_key); err != nil {
		t.Fatal(err)
	} else if v != set_ttl {
		t.Fatal("ttl != expire")
	}

	if v, err := db.SPersist(set_key); err != nil {
		t.Fatal(err)
	} else if v != 1 {
		t.Fatal("return value from spersist!= 1")
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

	zset_key := []byte("test:testdb_ttl_zset")
	var zset_ttl int64 = rand.Int63()

	if v, err := db.ZSetTtl(zset_key); err != nil {
		t.Fatal(err)
	} else if v != -1 {
		t.Fatal("ttl of not exist zset key is not -1")
	}

	if v, err := db.ZExpire(zset_key, zset_ttl); err != nil {
		t.Fatal(err)
	} else if v != 0 {
		t.Fatal("return value from expire of not exist zset key != 0")
	}

	if v, err := db.ZPersist(zset_key); err != nil {
		t.Fatal(err)
	} else if v != 0 {
		t.Fatal("return value from zpersist of not exist zset key != 0")
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

	if v, err := db.ZExpire(zset_key, zset_ttl); err != nil {
		t.Fatal(err)
	} else if v != 1 {
		t.Fatal("return value from zexpire != 1")
	}

	if v, err := db.ZSetTtl(zset_key); err != nil {
		t.Fatal(err)
	} else if v != zset_ttl {
		t.Fatal("ttl != expire")
	}

	if v, err := db.ZPersist(zset_key); err != nil {
		t.Fatal(err)
	} else if v != 1 {
		t.Fatal("return value from zpersist != 1")
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

	dataTypes := []byte{KVType, ListType, HashType, SetType, ZSetType}

	var lock sync.Mutex
	createTestExpiredFunc := func(dataType byte) common.OnExpiredFunc {
		return func(key []byte) error {
			lock.Lock()
			defer lock.Unlock()
			if kt, ok := kTypeMap[string(key)]; !ok {
				t.Fatal("unknown expired key", string(key))
			} else if kt != dataType {
				t.Fatal("mismatched callback called, %d - %d", dataType, t)
			} else {
				delete(kTypeMap, string(key))
				db.wb.Clear()
				db.delExpire(dataType, key, db.wb)
				db.eng.Write(db.defaultWriteOpts, db.wb)
			}
			return nil
		}
	}

	db.RegisterKVExpired(createTestExpiredFunc(KVType))
	db.RegisterHashExpired(createTestExpiredFunc(HashType))
	db.RegisterListExpired(createTestExpiredFunc(ListType))
	db.RegisterSetExpired(createTestExpiredFunc(SetType))
	db.RegisterZSetExpired(createTestExpiredFunc(ZSetType))

	db.StartTTLChecker()
	defer db.StopTTLChecker()
	for i := 0; i < 10000*3+rand.Intn(10000); i++ {
		key := "test:ttl_checker:" + strconv.Itoa(i)
		dataType := dataTypes[rand.Int()%len(dataTypes)]
		lock.Lock()
		kTypeMap[key] = dataType
		lock.Unlock()
		if err := db.expire(dataType, []byte(key), 2); err != nil {
			t.Fatal(err)
		}
	}

	time.Sleep(3 * time.Second)

	start := time.Now()
	for {
		lock.Lock()
		length := len(kTypeMap)
		lock.Unlock()
		if length == 0 {
			break
		}
		if time.Since(start) > time.Second*10 {
			t.Fatalf("%d key do not expired", length)
		} else {
			time.Sleep(time.Second)
		}
	}
}
