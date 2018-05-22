package rockredis

import (
	"bytes"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/youzan/ZanRedisDB/common"
)

func TestKVTTL_L(t *testing.T) {
	db := getTestDBWithExpirationPolicy(t, common.LocalDeletion)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	key1 := []byte("test:testdbTTL_kv_l")
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

	hashKey := []byte("test:testdbTTL_hash_l")
	var hashTTL int64 = rand.Int63()

	if v, err := db.HExpire(hashKey, hashTTL); err != nil {
		t.Fatal(err)
	} else if v != 0 {
		t.Fatal("return value from expire of not exist hash key != 0")
	}

	hashVal := []common.KVRecord{
		{Key: []byte("field0"), Value: []byte("value0")},
		{Key: []byte("field1"), Value: []byte("value1")},
		{Key: []byte("field2"), Value: []byte("value2")},
	}

	if err := db.HMset(0, hashKey, hashVal...); err != nil {
		t.Fatal(err)
	}

	if v, err := db.HExpire(hashKey, hashTTL); err != nil {
		t.Fatal(err)
	} else if v != 1 {
		t.Fatal("return value from hexpire != 1")
	}

	if v, err := db.HashTtl(hashKey); err != nil {
		t.Fatal(err)
	} else if v != -1 {
		t.Fatal("return value from HashTtl of LocalDeletion Policy != -1")
	}

	if v, err := db.HPersist(hashKey); err != nil {
		t.Fatal(err)
	} else if v != 0 {
		t.Fatal("return value from HPersist of  LocalDeletion Policy != 0")
	}
}

func TestListTTL_L(t *testing.T) {
	db := getTestDBWithExpirationPolicy(t, common.LocalDeletion)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	listKey := []byte("test:testdbTTL_list_l")
	var listTTL int64 = rand.Int63()

	if v, err := db.LExpire(listKey, listTTL); err != nil {
		t.Fatal(err)
	} else if v != 0 {
		t.Fatal("return value from expire of not exist list key != 0")
	}

	if _, err := db.LPush(0, listKey, []byte("this"), []byte("is"), []byte("list"),
		[]byte("ttl"), []byte("test")); err != nil {
		t.Fatal(err)
	}

	if v, err := db.LExpire(listKey, listTTL); err != nil {
		t.Fatal(err)
	} else if v != 1 {
		t.Fatal("return value from lexpire != 1")
	}

	if v, err := db.ListTtl(listKey); err != nil {
		t.Fatal(err)
	} else if v != -1 {
		t.Fatal("return value from ListTtl of LocalDeletion Policy != -1")
	}

	if v, err := db.LPersist(listKey); err != nil {
		t.Fatal(err)
	} else if v != 0 {
		t.Fatal("return value from LPersist of LocalDeletion Policy != 0")
	}
}

func TestSetTTL_L(t *testing.T) {
	db := getTestDBWithExpirationPolicy(t, common.LocalDeletion)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	setKey := []byte("test:testdbTTL_set_l")
	var setTTL int64 = rand.Int63()

	if v, err := db.SExpire(setKey, setTTL); err != nil {
		t.Fatal(err)
	} else if v != 0 {
		t.Fatal("return value from expire of not exist set key != 0")
	}

	if _, err := db.SAdd(0, setKey, []byte("this"), []byte("is"), []byte("set"),
		[]byte("ttl"), []byte("test")); err != nil {
		t.Fatal(err)
	}

	if v, err := db.SExpire(setKey, setTTL); err != nil {
		t.Fatal(err)
	} else if v != 1 {
		t.Fatal("return value from sexpire != 1")
	}

	if v, err := db.SetTtl(setKey); err != nil {
		t.Fatal(err)
	} else if v != -1 {
		t.Fatal("return value from SetTtl of LocalDeletion Policy != -1")
	}

	if v, err := db.SPersist(setKey); err != nil {
		t.Fatal(err)
	} else if v != 0 {
		t.Fatal("return value from SPersist of LocalDeletion Policy != 0")
	}
}

func TestZSetTTL_L(t *testing.T) {
	db := getTestDBWithExpirationPolicy(t, common.LocalDeletion)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	zsetKey := []byte("test:testdbTTL_zset_l")
	var zsetTTL int64 = rand.Int63()

	if v, err := db.ZExpire(zsetKey, zsetTTL); err != nil {
		t.Fatal(err)
	} else if v != 0 {
		t.Fatal("return value from expire of not exist zset key != 0")
	}

	members := []common.ScorePair{
		{Member: []byte("member1"), Score: 10},
		{Member: []byte("member2"), Score: 20},
		{Member: []byte("member3"), Score: 30},
		{Member: []byte("member4"), Score: 40},
	}

	if _, err := db.ZAdd(0, zsetKey, members...); err != nil {
		t.Fatal(err)
	}

	if v, err := db.ZExpire(zsetKey, zsetTTL); err != nil {
		t.Fatal(err)
	} else if v != 1 {
		t.Fatal("return value from zexpire != 1")
	}

	if v, err := db.ZSetTtl(zsetKey); err != nil {
		t.Fatal(err)
	} else if v != -1 {
		t.Fatal("return value from ZSetTtl of LocalDeletion Policy != -1")
	}

	if v, err := db.ZPersist(zsetKey); err != nil {
		t.Fatal(err)
	} else if v != 0 {
		t.Fatal("return value from ZPersist of LocalDeletion Policy != 0")
	}

}

func TestLocalDeletionTTLChecker(t *testing.T) {
	oldCheckInterval := localExpCheckInterval
	localExpCheckInterval = 5
	defer func() {
		localExpCheckInterval = oldCheckInterval
	}()

	db := getTestDBWithExpirationPolicy(t, common.LocalDeletion)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	kTypeMap := make(map[string]byte)

	dataTypes := []byte{KVType, ListType, HashType, SetType, ZSetType}

	for i := 0; i < 1000*3+rand.Intn(1000); i++ {
		key := "test:ttl_checker_local:" + strconv.Itoa(i)
		dataType := dataTypes[rand.Int()%len(dataTypes)]
		kTypeMap[key] = dataType
		switch dataType {
		case KVType:
			db.KVSet(0, []byte("test_checker_local_kvKey"), []byte("test_checker_local_kvValue"))

		case ListType:
			tListKey := []byte("test_checker_local_listKey")
			db.LPush(0, tListKey, []byte("this"), []byte("is"), []byte("list"),
				[]byte("local"), []byte("deletion"), []byte("ttl"), []byte("checker"), []byte("test"))

		case HashType:
			tHashKey := []byte("test_checker_local_hashKey")
			tHashVal := []common.KVRecord{
				{Key: []byte("field0"), Value: []byte("value0")},
				{Key: []byte("field1"), Value: []byte("value1")},
				{Key: []byte("field2"), Value: []byte("value2")},
			}
			db.HMset(0, tHashKey, tHashVal...)

		case SetType:
			tSetKey := []byte("test_checker_local_setKey")
			db.SAdd(0, tSetKey, []byte("this"), []byte("is"), []byte("set"),
				[]byte("local"), []byte("deletion"), []byte("ttl"), []byte("checker"), []byte("test"))

		case ZSetType:
			tZsetKey := []byte("test_checker_local_zsetKey")
			members := []common.ScorePair{
				{Member: []byte("member1"), Score: 11},
				{Member: []byte("member2"), Score: 22},
				{Member: []byte("member3"), Score: 33},
				{Member: []byte("member4"), Score: 44},
			}

			db.ZAdd(0, tZsetKey, members...)
		}

		if err := db.expire(dataType, []byte(key), 8); err != nil {
			t.Fatal(err)
		}
	}

	time.Sleep(time.Duration(localExpCheckInterval+1) * time.Second)

	for k, v := range kTypeMap {
		switch v {
		case KVType:
			if v, err := db.KVGet([]byte(k)); err != nil {
				t.Fatal(err)
			} else if v != nil {
				t.Fatalf("key:%s of KVType do not expired", string(k))
			}
		case HashType:
			if v, err := db.HLen([]byte(k)); err != nil {
				t.Fatal(err)
			} else if v != 0 {
				t.Fatalf("key:%s of HashType do not expired", string(k))
			}
		case ListType:
			if v, err := db.LLen([]byte(k)); err != nil {
				t.Fatal(err)
			} else if v != 0 {
				t.Fatalf("key:%s of ListType do not expired", string(k))
			}
		case SetType:
			if v, err := db.SCard([]byte(k)); err != nil {
				t.Fatal(err)
			} else if v != 0 {
				t.Fatalf("key:%s of SetType do not expired", string(k))
			}
		case ZSetType:
			if v, err := db.ZCard([]byte(k)); err != nil {
				t.Fatal(err)
			} else if v != 0 {
				t.Fatalf("key:%s of ZSetType do not expired", string(k))
			}
		}
	}

}
