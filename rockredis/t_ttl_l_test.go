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

func TestKVTTL_L(t *testing.T) {
	db := getTestDBWithExpirationPolicy(t, common.LocalDeletion)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	key1 := []byte("test:testdb_ttl_kv_l")
	var ttl1 int64 = rand.Int63()

	if v, err := db.Expire(key1, ttl1); err != nil {
		t.Fatal(err)
	} else if v != 0 {
		t.Fatal("return value from expire of not exist key != 0")
	}

	if err := db.KVSet(0, key1, []byte("hello world 1")); err != nil {
		t.Fatal(err)
	}

	if v, err := db.Expire(key1, ttl1); err != nil {
		t.Fatal(err)
	} else if v != 1 {
		t.Fatal("return value from expire != 1")
	}

	if v, err := db.Persist(key1); err != nil {
		t.Fatal(err)
	} else if v != 0 {
		t.Fatal("return value from persist of LocalDeletion Policy != 0")
	}

	if v, err := db.KVTtl(key1); err != nil {
		t.Fatal(err)
	} else if v != -1 {
		t.Fatal("return value from KVTtl of LocalDeletion Policy != -1")
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

}

func TestHashTTL_L(t *testing.T) {
	db := getTestDBWithExpirationPolicy(t, common.LocalDeletion)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	hash_key := []byte("test:testdb_ttl_hash_l")
	var hash_ttl int64 = rand.Int63()

	if v, err := db.HExpire(hash_key, hash_ttl); err != nil {
		t.Fatal(err)
	} else if v != 0 {
		t.Fatal("return value from expire of not exist hash key != 0")
	}

	hash_val := []common.KVRecord{
		common.KVRecord{Key: []byte("field0"), Value: []byte("value0")},
		common.KVRecord{Key: []byte("field1"), Value: []byte("value1")},
		common.KVRecord{Key: []byte("field2"), Value: []byte("value2")},
	}

	if err := db.HMset(0, hash_key, hash_val...); err != nil {
		t.Fatal(err)
	}

	if v, err := db.HExpire(hash_key, hash_ttl); err != nil {
		t.Fatal(err)
	} else if v != 1 {
		t.Fatal("return value from hexpire != 1")
	}

	if v, err := db.HashTtl(hash_key); err != nil {
		t.Fatal(err)
	} else if v != -1 {
		t.Fatal("return value from HashTtl of LocalDeletion Policy != -1")
	}

	if v, err := db.HPersist(hash_key); err != nil {
		t.Fatal(err)
	} else if v != 0 {
		t.Fatal("return value from HPersist of  LocalDeletion Policy != 0")
	}
}

func TestListTTL_L(t *testing.T) {
	db := getTestDBWithExpirationPolicy(t, common.LocalDeletion)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	list_key := []byte("test:testdb_ttl_list_l")
	var list_ttl int64 = rand.Int63()

	if v, err := db.LExpire(list_key, list_ttl); err != nil {
		t.Fatal(err)
	} else if v != 0 {
		t.Fatal("return value from expire of not exist list key != 0")
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
	} else if v != -1 {
		t.Fatal("return value from ListTtl of LocalDeletion Policy != -1")
	}

	if v, err := db.LPersist(list_key); err != nil {
		t.Fatal(err)
	} else if v != 0 {
		t.Fatal("return value from LPersist of LocalDeletion Policy != 0")
	}
}

func TestSetTTL_L(t *testing.T) {
	db := getTestDBWithExpirationPolicy(t, common.LocalDeletion)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	set_key := []byte("test:testdb_ttl_set_l")
	var set_ttl int64 = rand.Int63()

	if v, err := db.SExpire(set_key, set_ttl); err != nil {
		t.Fatal(err)
	} else if v != 0 {
		t.Fatal("return value from expire of not exist set key != 0")
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
	} else if v != -1 {
		t.Fatal("return value from SetTtl of LocalDeletion Policy != -1")
	}

	if v, err := db.SPersist(set_key); err != nil {
		t.Fatal(err)
	} else if v != 0 {
		t.Fatal("return value from SPersist of LocalDeletion Policy != 0")
	}
}

func TestZSetTTL_L(t *testing.T) {
	db := getTestDBWithExpirationPolicy(t, common.LocalDeletion)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	zset_key := []byte("test:testdb_ttl_zset_l")
	var zset_ttl int64 = rand.Int63()

	if v, err := db.ZExpire(zset_key, zset_ttl); err != nil {
		t.Fatal(err)
	} else if v != 0 {
		t.Fatal("return value from expire of not exist zset key != 0")
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
	} else if v != -1 {
		t.Fatal("return value from ZSetTtl of LocalDeletion Policy != -1")
	}

	if v, err := db.ZPersist(zset_key); err != nil {
		t.Fatal(err)
	} else if v != 0 {
		t.Fatal("return value from ZPersist of LocalDeletion Policy != 0")
	}

}

func TestLocalDeletionTTLChecker(t *testing.T) {
	db := getTestDBWithExpirationPolicy(t, common.LocalDeletion)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	kTypeMap := make(map[string]byte)

	dataTypes := []byte{KVType, ListType, HashType, SetType, ZSetType}

	for i := 0; i < 10000*3+rand.Intn(10000); i++ {
		key := "test:ttl_checker_local:" + strconv.Itoa(i)
		dataType := dataTypes[rand.Int()%len(dataTypes)]
		kTypeMap[key] = dataType
		switch dataType {
		case KVType:
			db.KVSet(0, []byte("test_checker_local_kv_key"), []byte("test_checker_local_kv_value"))

		case ListType:
			t_list_key := []byte("test_checker_local_list_key")
			db.LPush(t_list_key, []byte("this"), []byte("is"), []byte("list"),
				[]byte("local"), []byte("deletion"), []byte("ttl"), []byte("checker"), []byte("test"))

		case HashType:
			t_hash_key := []byte("test_checker_local_hash_key")
			t_hash_val := []common.KVRecord{
				common.KVRecord{Key: []byte("field0"), Value: []byte("value0")},
				common.KVRecord{Key: []byte("field1"), Value: []byte("value1")},
				common.KVRecord{Key: []byte("field2"), Value: []byte("value2")},
			}
			db.HMset(0, t_hash_key, t_hash_val...)

		case SetType:
			t_set_key := []byte("test_checker_local_set_key")
			db.SAdd(t_set_key, []byte("this"), []byte("is"), []byte("set"),
				[]byte("local"), []byte("deletion"), []byte("ttl"), []byte("checker"), []byte("test"))

		case ZSetType:
			t_zset_key := []byte("test_checker_local_zset_key")
			members := []common.ScorePair{
				common.ScorePair{Member: []byte("member1"), Score: 11},
				common.ScorePair{Member: []byte("member2"), Score: 22},
				common.ScorePair{Member: []byte("member3"), Score: 33},
				common.ScorePair{Member: []byte("member4"), Score: 44},
			}

			db.ZAdd(t_zset_key, members...)
		}

		if err := db.expire(dataType, []byte(key), 8); err != nil {
			t.Fatal(err)
		}
	}

	time.Sleep((localExpCheckInterval + 1) * time.Second)

	for k, v := range kTypeMap {
		switch v {
		case KVType:
			if v, err := db.KVGet([]byte(k)); err != nil {
				t.Fatal(err)
			} else if v != nil {
				t.Fatal("key:%s of KVType do not expired", string(k))
			}
		case HashType:
			if v, err := db.HLen([]byte(k)); err != nil {
				t.Fatal(err)
			} else if v != 0 {
				t.Fatal("key:%s of HashType do not expired", string(k))
			}
		case ListType:
			if v, err := db.LLen([]byte(k)); err != nil {
				t.Fatal(err)
			} else if v != 0 {
				t.Fatal("key:%s of ListType do not expired", string(k))
			}
		case SetType:
			if v, err := db.SCard([]byte(k)); err != nil {
				t.Fatal(err)
			} else if v != 0 {
				t.Fatal("key:%s of SetType do not expired", string(k))
			}
		case ZSetType:
			if v, err := db.ZCard([]byte(k)); err != nil {
				t.Fatal(err)
			} else if v != 0 {
				t.Fatal("key:%s of ZSetType do not expired", string(k))
			}
		}
	}

}
