package rockredis

import (
	"bytes"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/youzan/ZanRedisDB/common"
	"github.com/youzan/gorocksdb"
)

func TestKVTTL_C(t *testing.T) {
	db := getTestDBWithExpirationPolicy(t, common.ConsistencyDeletion)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	key1 := []byte("test:testdbTTL_kv_c")
	var ttl1 int64 = rand.Int63() - 10
	ttl2 := ttl1 + 10
	tn := time.Now().UnixNano()

	if v, err := db.Expire(tn, key1, ttl1); err != nil {
		t.Fatal(err)
	} else if v != 0 {
		t.Fatal("return value from expire of not exist key != 0")
	}

	if v, err := db.Persist(tn, key1); err != nil {
		t.Fatal(err)
	} else if v != 0 {
		t.Fatal("return value from persist of not exist key != 0")
	}

	if err := db.KVSet(0, key1, []byte("hello world 1")); err != nil {
		t.Fatal(err)
	}

	if v, err := db.Expire(tn, key1, ttl1); err != nil {
		t.Fatal(err)
	} else if v != 1 {
		t.Fatal("return value from expire != 1")
	}

	if v, err := db.KVTtl(key1); err != nil {
		t.Fatal(err)
	} else if v != ttl1 {
		t.Fatal("ttl != expire")
	}

	if v, err := db.Expire(tn, key1, ttl2); err != nil {
		t.Fatal(err)
	} else if v != 1 {
		t.Fatal("return value from expire != 1")
	}

	if v, err := db.KVTtl(key1); err != nil {
		t.Fatal(err)
	} else if v != ttl2 {
		t.Fatal("ttl != expire")
	}

	if v, err := db.Persist(tn, key1); err != nil {
		t.Fatal(err)
	} else if v != 1 {
		t.Fatal("return value from persist != 1")
	}

	if v, err := db.KVTtl(key1); err != nil {
		t.Fatal(err)
	} else if v != -1 {
		t.Fatal("KVPersist do not clear the ttl")
	}

	tn = time.Now().UnixNano()
	testValue := []byte("test value for SetEx command")
	if err := db.SetEx(tn, key1, ttl1, testValue); err != nil {
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
		t.Fatalf("ttl != setex: %v vs %v", v, ttl1)
	}

	if err := db.SetEx(tn, key1, ttl2, testValue); err != nil {
		t.Fatal(err)
	}
	if v, err := db.KVTtl(key1); err != nil {
		t.Fatal(err)
	} else if v != ttl2 {
		t.Fatalf("ttl != setex: %v vs %v", v, ttl2)
	}

	err := db.SetEx(tn, key1, ttl2, testValue)
	assert.Nil(t, err)

	v, err := db.KVTtl(key1)
	assert.Nil(t, err)
	assert.Equal(t, ttl2, v)
	// set, setnx, mset should clean ttl
	err = db.KVSet(0, key1, testValue)
	assert.Nil(t, err)
	v, err = db.KVTtl(key1)
	assert.Nil(t, err)
	assert.Equal(t, int64(-1), v)

	err = db.SetEx(tn, key1, ttl2, testValue)
	assert.Nil(t, err)
	v, err = db.KVTtl(key1)
	assert.Nil(t, err)
	assert.Equal(t, ttl2, v)

	// set not success should keep ttl
	changed, err := db.SetNX(0, key1, testValue)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), changed)
	v, err = db.KVTtl(key1)
	assert.Nil(t, err)
	assert.Equal(t, ttl2, v)

	db.MSet(0, common.KVRecord{Key: key1, Value: testValue})
	v, err = db.KVTtl(key1)
	assert.Nil(t, err)
	assert.Equal(t, int64(-1), v)

	err = db.SetEx(tn, key1, 1, testValue)
	assert.Nil(t, err)
	v, err = db.KVTtl(key1)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), v)

	time.Sleep(time.Second * 2)
	// set success should clean ttl
	changed, err = db.SetNX(0, key1, testValue)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), changed)
	// set success should clean ttl
	tn = time.Now().UnixNano()
	changed, err = db.SetNX(tn, key1, testValue)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), changed)
	v, err = db.KVTtl(key1)
	assert.Nil(t, err)
	assert.Equal(t, int64(-1), v)
}

func TestKVTTL_C_KeepTTL(t *testing.T) {
	db := getTestDBWithExpirationPolicy(t, common.ConsistencyDeletion)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	key1 := []byte("test:testdbTTL_kv_c_keepttl")
	var ttl1 int64 = int64(rand.Int31())
	tn := time.Now().UnixNano()
	if c, err := db.Incr(0, key1); err != nil {
		t.Fatal(err)
	} else {
		assert.Equal(t, int64(1), c)
	}

	if v, err := db.Expire(tn, key1, ttl1); err != nil {
		t.Fatal(err)
	} else if v != 1 {
		t.Fatal("return value from expire != 1")
	}
	if v, err := db.KVTtl(key1); err != nil {
		t.Fatal(err)
	} else if v != ttl1 {
		t.Fatal("ttl != expire")
	}
	// incr should keep ttl
	if c, err := db.Incr(0, key1); err != nil {
		t.Fatal(err)
	} else {
		assert.Equal(t, int64(2), c)
	}
	if v, err := db.KVTtl(key1); err != nil {
		t.Fatal(err)
	} else if v != ttl1 {
		t.Fatal("ttl != expire")
	}

	// append
	_, err := db.Append(0, key1, []byte("append"))
	assert.Nil(t, err)
	if v, err := db.KVTtl(key1); err != nil {
		t.Fatal(err)
	} else if v != ttl1 {
		t.Fatal("ttl != expire")
	}
	v, err := db.KVGet(key1)
	assert.Nil(t, err)
	assert.Equal(t, "2append", string(v))
	// setrange
	_, err = db.SetRange(0, key1, 1, []byte("range"))
	assert.Nil(t, err)
	if v, err := db.KVTtl(key1); err != nil {
		t.Fatal(err)
	} else if v != ttl1 {
		t.Fatal("ttl != expire")
	}
	v, err = db.KVGet(key1)
	assert.Nil(t, err)
	assert.Equal(t, "2ranged", string(v))
	db.KVDel(key1)
	// bitset
	db.BitSetOld(0, key1, 1, 1)
	if v, err := db.Expire(tn, key1, ttl1); err != nil {
		t.Fatal(err)
	} else if v != 1 {
		t.Fatal("return value from expire != 1")
	}
	db.BitSetOld(0, key1, 2, 1)
	db.BitSetOld(0, key1, 1, 0)
	if v, err := db.KVTtl(key1); err != nil {
		t.Fatal(err)
	} else if v != ttl1 {
		t.Fatal("ttl != expire")
	}
	bitV, err := db.BitGetV2(key1, 1)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), bitV)
	bitV, err = db.BitGetV2(key1, 2)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), bitV)
}

func TestKVTTL_C_TTLExpired(t *testing.T) {
	db := getTestDBWithExpirationPolicy(t, common.ConsistencyDeletion)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	key1 := []byte("test:testdbTTL_kv_c_ttlexpired")
	var ttl1 int64 = int64(2)
	tn := time.Now().UnixNano()
	if c, err := db.Incr(0, key1); err != nil {
		t.Fatal(err)
	} else {
		assert.Equal(t, int64(1), c)
	}

	if v, err := db.Expire(tn, key1, ttl1); err != nil {
		t.Fatal(err)
	} else if v != 1 {
		t.Fatal("return value from expire != 1")
	}
	if v, err := db.KVTtl(key1); err != nil {
		t.Fatal(err)
	} else if v != ttl1 {
		t.Fatal("ttl != expire")
	}
	// incr should keep ttl
	if c, err := db.Incr(0, key1); err != nil {
		t.Fatal(err)
	} else {
		assert.Equal(t, int64(2), c)
	}
	if v, err := db.KVTtl(key1); err != nil {
		t.Fatal(err)
	} else if v != ttl1 {
		t.Fatal("ttl != expire")
	}

	time.Sleep(time.Second * time.Duration(ttl1+1))
	dbLog.Infof("wait expired done")
	if v, err := db.KVTtl(key1); err != nil {
		t.Fatal(err)
	} else if v != -1 {
		t.Fatalf("should expired: %v", v)
	}
	tn = time.Now().UnixNano()
	exist, err := db.KVExists(key1)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), exist)
	v, err := db.KVGet(key1)
	assert.Nil(t, err)
	assert.Nil(t, v)
	vlist, errs := db.MGet(key1)
	assert.Nil(t, errs[0])
	assert.Nil(t, vlist[0])
	v, err = db.KVGetSet(tn, key1, []byte("new"))
	assert.Nil(t, err)
	assert.Nil(t, v)
	if v, err := db.KVTtl(key1); err != nil {
		t.Fatal(err)
	} else if v != -1 {
		t.Fatalf("should has no expired: %v", v)
	}
	v, err = db.KVGet(key1)
	assert.Nil(t, err)
	assert.Equal(t, []byte("new"), v)
}

func TestHashTTL_C(t *testing.T) {
	db := getTestDBWithExpirationPolicy(t, common.ConsistencyDeletion)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	hashKey := []byte("test:testdbTTL_hash_c")
	var hashTTL int64 = int64(rand.Int31() - 10)

	if v, err := db.HashTtl(hashKey); err != nil {
		t.Fatal(err)
	} else if v != -1 {
		t.Fatal("ttl of not exist hash key is not -1")
	}

	tn := time.Now().UnixNano()
	if v, err := db.HExpire(tn, hashKey, hashTTL); err != nil {
		t.Fatal(err)
	} else if v != 0 {
		t.Fatal("return value from expire of not exist hash key != 0")
	}

	if v, err := db.HPersist(tn, hashKey); err != nil {
		t.Fatal(err)
	} else if v != 0 {
		t.Fatal("return value from hpersist of not exist hash key != 0")
	}

	hash_val := []common.KVRecord{
		{Key: []byte("field0"), Value: []byte("value0")},
		{Key: []byte("field1"), Value: []byte("value1")},
		{Key: []byte("field2"), Value: []byte("value2")},
	}

	if err := db.HMset(0, hashKey, hash_val...); err != nil {
		t.Fatal(err)
	}

	if v, err := db.HExpire(tn, hashKey, hashTTL); err != nil {
		t.Fatal(err)
	} else if v != 1 {
		t.Fatal("return value from hexpire != 1")
	}

	v, err := db.HashTtl(hashKey)
	assert.Nil(t, err)
	assert.Equal(t, hashTTL, v)

	if v, err := db.HPersist(tn, hashKey); err != nil {
		t.Fatal(err)
	} else if v != 1 {
		t.Fatal("return value from hpersist is != 1")
	}

	v, err = db.HashTtl(hashKey)
	assert.Nil(t, err)
	assert.Equal(t, int64(-1), v)
}

func TestHashTTL_C_KeepTTL(t *testing.T) {
	db := getTestDBWithExpirationPolicy(t, common.ConsistencyDeletion)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	hashKey := []byte("test:testdbTTL_hash_c_keepttl")
	var hashTTL int64 = int64(rand.Int31() - 10)
	tn := time.Now().UnixNano()
	hash_val := []common.KVRecord{
		{Key: []byte("field0"), Value: []byte("0")},
		{Key: []byte("field1"), Value: []byte("value1")},
		{Key: []byte("field2"), Value: []byte("value2")},
	}

	err := db.HMset(tn, hashKey, hash_val...)
	assert.Nil(t, err)
	n, err := db.HExpire(tn, hashKey, hashTTL)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)

	// should keep ttl
	n, err = db.HSet(tn, false, hashKey, hash_val[0].Key, hash_val[1].Value)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)
	n, err = db.HashTtl(hashKey)
	assert.Nil(t, err)
	assert.Equal(t, hashTTL, n)

	// hmset
	err = db.HMset(tn, hashKey, hash_val...)
	assert.Nil(t, err)
	n, err = db.HashTtl(hashKey)
	assert.Nil(t, err)
	assert.Equal(t, hashTTL, n)
	// hdel
	n, err = db.HDel(hashKey, hash_val[0].Key)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)
	n, err = db.HashTtl(hashKey)
	assert.Nil(t, err)
	assert.Equal(t, hashTTL, n)
	// hincrby
	n, err = db.HIncrBy(tn, hashKey, hash_val[0].Key, 1)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)
	n, err = db.HashTtl(hashKey)
	assert.Nil(t, err)
	assert.Equal(t, hashTTL, n)
}

func TestHashTTL_C_TTLExpired(t *testing.T) {
	db := getTestDBWithExpirationPolicy(t, common.ConsistencyDeletion)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	hashKey := []byte("test:testdbTTL_hash_c_expired")
	var hashTTL int64 = int64(3)
	tn := time.Now().UnixNano()
	hash_val := []common.KVRecord{
		{Key: []byte("field0"), Value: []byte("0")},
		{Key: []byte("field1"), Value: []byte("value1")},
		{Key: []byte("field2"), Value: []byte("value2")},
	}

	err := db.HMset(tn, hashKey, hash_val...)
	assert.Nil(t, err)
	n, err := db.HExpire(tn, hashKey, hashTTL)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)

	n, err = db.HashTtl(hashKey)
	assert.Nil(t, err)
	assert.Equal(t, hashTTL, n)

	time.Sleep(time.Second * time.Duration(hashTTL+1))
	dbLog.Infof("wait expired done")
	n, err = db.HashTtl(hashKey)
	assert.Nil(t, err)
	assert.Equal(t, int64(-1), n)

	tn = time.Now().UnixNano()
	v, err := db.HGet(hashKey, hash_val[0].Key)
	assert.Nil(t, err)
	assert.Nil(t, v)
	n, err = db.HKeyExists(hashKey)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)
	vlist, err := db.HMget(hashKey, hash_val[0].Key, hash_val[1].Key)
	assert.Nil(t, err)
	assert.Nil(t, vlist[0])
	assert.Nil(t, vlist[1])
	n, recs, err := db.HGetAll(hashKey)
	assert.Equal(t, int64(0), n)
	assert.Equal(t, 0, len(recs))
	n, recs, err = db.HKeys(hashKey)
	assert.Equal(t, int64(0), n)
	assert.Equal(t, 0, len(recs))
	n, recs, err = db.HValues(hashKey)
	assert.Equal(t, int64(0), n)
	assert.Equal(t, 0, len(recs))
	n, err = db.HLen(hashKey)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)

	// renew hash
	hash_val2 := []common.KVRecord{
		{Key: []byte("field0"), Value: []byte("new")},
		{Key: []byte("field1"), Value: []byte("value1_new")},
	}
	time.Sleep(time.Second)
	tn = time.Now().UnixNano()
	// consistence expire can not ensure we delete all while rewrite the expired data
	// even we write new data, it will keep expired until the check processed expired data
	err = db.HMset(tn, hashKey, hash_val2...)
	assert.Nil(t, err)

	n, err = db.HashTtl(hashKey)
	assert.Nil(t, err)
	assert.Equal(t, int64(-1), n)

	n, err = db.HLen(hashKey)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)

	n, recs, err = db.HGetAll(hashKey)
	assert.Equal(t, int64(0), n)
	assert.Equal(t, 0, len(recs))
	// clean and rewrite
	db.HClear(hashKey)

	err = db.HMset(tn, hashKey, hash_val2...)
	assert.Nil(t, err)

	n, err = db.HashTtl(hashKey)
	assert.Nil(t, err)
	assert.Equal(t, int64(-1), n)

	n, err = db.HLen(hashKey)
	assert.Nil(t, err)
	assert.Equal(t, int64(len(hash_val2)), n)

	n, recs, err = db.HGetAll(hashKey)
	assert.Equal(t, int64(len(hash_val2)), n)
	assert.Equal(t, len(hash_val2), len(recs))
}

func TestListTTL_C(t *testing.T) {
	db := getTestDBWithExpirationPolicy(t, common.ConsistencyDeletion)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	listKey := []byte("test:testdbTTL_list_c")
	var listTTL int64 = rand.Int63()
	tn := time.Now().UnixNano()

	if v, err := db.ListTtl(listKey); err != nil {
		t.Fatal(err)
	} else if v != -1 {
		t.Fatal("ttl of not exist list key is not -1")
	}

	if v, err := db.LExpire(tn, listKey, listTTL); err != nil {
		t.Fatal(err)
	} else if v != 0 {
		t.Fatal("return value from expire of not exist list key != 0")
	}

	if v, err := db.LPersist(tn, listKey); err != nil {
		t.Fatal(err)
	} else if v != 0 {
		t.Fatal("return value from lpersist of not exist list key != 0")
	}

	if _, err := db.LPush(0, listKey, []byte("this"), []byte("is"), []byte("list"),
		[]byte("ttl"), []byte("test")); err != nil {
		t.Fatal(err)
	}

	if v, err := db.LExpire(tn, listKey, listTTL); err != nil {
		t.Fatal(err)
	} else if v != 1 {
		t.Fatal("return value from lexpire != 1")
	}

	if v, err := db.ListTtl(listKey); err != nil {
		t.Fatal(err)
	} else if v != listTTL {
		t.Fatal("ttl != expire")
	}

	if v, err := db.LPersist(tn, listKey); err != nil {
		t.Fatal(err)
	} else if v != 1 {
		t.Fatal("return value from lpersist != 1")
	}

	if v, err := db.ListTtl(listKey); err != nil {
		t.Fatal(err)
	} else if v != -1 {
		t.Fatal("ListPersist do not clear the ttl")
	}
}

func TestSetTTL_C(t *testing.T) {
	db := getTestDBWithExpirationPolicy(t, common.ConsistencyDeletion)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	setKey := []byte("test:testdbTTL_set_c")
	var setTTL int64 = rand.Int63()

	if v, err := db.SetTtl(setKey); err != nil {
		t.Fatal(err)
	} else if v != -1 {
		t.Fatal("ttl of not exist set key is not -1")
	}
	tn := time.Now().UnixNano()

	if v, err := db.SExpire(tn, setKey, setTTL); err != nil {
		t.Fatal(err)
	} else if v != 0 {
		t.Fatal("return value from expire of not exist set key != 0")
	}

	if v, err := db.SPersist(tn, setKey); err != nil {
		t.Fatal(err)
	} else if v != 0 {
		t.Fatal("return value from spersist of not exist set key != 0")
	}

	if _, err := db.SAdd(0, setKey, []byte("this"), []byte("is"), []byte("set"),
		[]byte("ttl"), []byte("test")); err != nil {
		t.Fatal(err)
	}

	if v, err := db.SExpire(tn, setKey, setTTL); err != nil {
		t.Fatal(err)
	} else if v != 1 {
		t.Fatal("return value from sexpire != 1")
	}

	if v, err := db.SetTtl(setKey); err != nil {
		t.Fatal(err)
	} else if v != setTTL {
		t.Fatal("ttl != expire")
	}

	if v, err := db.SPersist(tn, setKey); err != nil {
		t.Fatal(err)
	} else if v != 1 {
		t.Fatal("return value from spersist!= 1")
	}

	if v, err := db.SetTtl(setKey); err != nil {
		t.Fatal(err)
	} else if v != -1 {
		t.Fatal("SetPersist do not clear the ttl")
	}
}

func TestSetTTL_C_KeepTTL(t *testing.T) {
	db := getTestDBWithExpirationPolicy(t, common.ConsistencyDeletion)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	setKey := []byte("test:testdbTTL_set_c_keepttl")
	var ttl int64 = int64(rand.Int31() - 10)
	tn := time.Now().UnixNano()
	mems := [][]byte{
		[]byte("m1"), []byte("m2"), []byte("m3"),
	}

	n, err := db.SAdd(tn, setKey, mems...)
	assert.Nil(t, err)
	assert.Equal(t, int64(len(mems)), n)
	n, err = db.SExpire(tn, setKey, ttl)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)
	n, err = db.SetTtl(setKey)
	assert.Nil(t, err)
	assert.Equal(t, ttl, n)

	// should keep ttl
	n, err = db.SAdd(tn, setKey, []byte("newm1"))
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)
	n, err = db.SetTtl(setKey)
	assert.Nil(t, err)
	assert.Equal(t, ttl, n)

	n, err = db.SRem(tn, setKey, mems[0])
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)
	n, err = db.SetTtl(setKey)
	assert.Nil(t, err)
	assert.Equal(t, ttl, n)
	_, err = db.SPop(tn, setKey, 1)
	assert.Nil(t, err)
	n, err = db.SetTtl(setKey)
	assert.Nil(t, err)
	assert.Equal(t, ttl, n)
}

func TestSetTTL_C_TTLExpired(t *testing.T) {
	db := getTestDBWithExpirationPolicy(t, common.ConsistencyDeletion)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	setKey := []byte("test:testdbTTL_set_c_expired")
	var ttl int64 = int64(3)
	tn := time.Now().UnixNano()
	mems := [][]byte{
		[]byte("mem1"), []byte("mem2"), []byte("mem3"),
	}

	n, err := db.SAdd(tn, setKey, mems...)
	assert.Nil(t, err)
	assert.Equal(t, int64(len(mems)), n)
	n, err = db.SExpire(tn, setKey, ttl)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)

	n, err = db.SetTtl(setKey)
	assert.Nil(t, err)
	assert.Equal(t, ttl, n)

	n, err = db.SIsMember(setKey, mems[0])
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)
	vlist, err := db.SMembers(setKey)
	assert.Nil(t, err)
	assert.Equal(t, len(mems), len(vlist))
	n, err = db.SCard(setKey)
	assert.Nil(t, err)
	assert.Equal(t, int64(len(mems)), n)

	time.Sleep(time.Second * time.Duration(ttl+1))
	dbLog.Infof("wait expired done")
	n, err = db.SetTtl(setKey)
	assert.Nil(t, err)
	assert.Equal(t, int64(-1), n)

	tn = time.Now().UnixNano()
	n, err = db.SIsMember(setKey, mems[0])
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)
	n, err = db.SKeyExists(setKey)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)
	vlist, err = db.SMembers(setKey)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(vlist))
	vlist, err = db.SScan(setKey, []byte(""), -1, "", false)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(vlist))
	n, err = db.SCard(setKey)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)
	vlist, err = db.SPop(tn, setKey, 1)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(vlist))

	// renew
	mems2 := [][]byte{
		[]byte("newmem1"), []byte("newmem2"),
	}
	time.Sleep(time.Second)
	tn = time.Now().UnixNano()
	// consistence expire can not ensure we delete all while rewrite the expired data
	// even we write new data, it will keep expired until the check processed expired data
	n, err = db.SAdd(tn, setKey, mems2...)
	assert.Nil(t, err)

	n, err = db.SetTtl(setKey)
	assert.Nil(t, err)
	assert.Equal(t, int64(-1), n)

	n, err = db.SCard(setKey)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)

	vlist, err = db.SMembers(setKey)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(vlist))
	// clean and rewrite
	db.SClear(setKey)

	n, err = db.SAdd(tn, setKey, mems2...)
	assert.Nil(t, err)
	assert.Equal(t, int64(len(mems2)), n)

	n, err = db.SetTtl(setKey)
	assert.Nil(t, err)
	assert.Equal(t, int64(-1), n)

	n, err = db.SCard(setKey)
	assert.Nil(t, err)
	assert.Equal(t, int64(len(mems2)), n)

	vlist, err = db.SMembers(setKey)
	assert.Nil(t, err)
	assert.Equal(t, len(mems2), len(vlist))
}

func TestZSetTTL_C(t *testing.T) {
	db := getTestDBWithExpirationPolicy(t, common.ConsistencyDeletion)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	zsetKey := []byte("test:testdbTTL_zset_c")
	var zsetTTL int64 = rand.Int63()

	tn := time.Now().UnixNano()
	if v, err := db.ZSetTtl(zsetKey); err != nil {
		t.Fatal(err)
	} else if v != -1 {
		t.Fatal("ttl of not exist zset key is not -1")
	}

	if v, err := db.ZExpire(tn, zsetKey, zsetTTL); err != nil {
		t.Fatal(err)
	} else if v != 0 {
		t.Fatal("return value from expire of not exist zset key != 0")
	}

	if v, err := db.ZPersist(tn, zsetKey); err != nil {
		t.Fatal(err)
	} else if v != 0 {
		t.Fatal("return value from zpersist of not exist zset key != 0")
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

	if v, err := db.ZExpire(tn, zsetKey, zsetTTL); err != nil {
		t.Fatal(err)
	} else if v != 1 {
		t.Fatal("return value from zexpire != 1")
	}

	if v, err := db.ZSetTtl(zsetKey); err != nil {
		t.Fatal(err)
	} else if v != zsetTTL {
		t.Fatal("ttl != expire")
	}

	if v, err := db.ZPersist(tn, zsetKey); err != nil {
		t.Fatal(err)
	} else if v != 1 {
		t.Fatal("return value from zpersist != 1")
	}

	if v, err := db.ZSetTtl(zsetKey); err != nil {
		t.Fatal(err)
	} else if v != -1 {
		t.Fatal("ZSetPersist do not clear the ttl")
	}
}

type TExpiredDataBuffer struct {
	db           *RockDB
	wb           *gorocksdb.WriteBatch
	kTypeMap     map[string]byte
	expiredCount int
	t            *testing.T
}

func (buff *TExpiredDataBuffer) Write(dt common.DataType, key []byte) error {
	buff.expiredCount += 1
	if kt, ok := buff.kTypeMap[string(key)]; !ok {
		buff.t.Fatalf("unknown expired key: %v", string(key))
	} else if dataType2CommonType(kt) != dt {
		buff.t.Fatalf("mismatched key-type, %s - %d, should be [%s - %d]", string(key), dt, string(key), dataType2CommonType(kt))
	} else {
		buff.wb.Clear()
		buff.db.delExpire(kt, key, nil, false, buff.wb)
		buff.db.eng.Write(buff.db.defaultWriteOpts, buff.wb)
		delete(buff.kTypeMap, string(key))
	}
	return nil
}

func (buff *TExpiredDataBuffer) Full() bool {
	return false
}

func TestConsistencyTTLChecker(t *testing.T) {
	db := getTestDBWithExpirationPolicy(t, common.ConsistencyDeletion)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	kTypeMap := make(map[string]byte)
	dataTypes := []byte{KVType, ListType, HashType, SetType, ZSetType}

	for i := 0; i < 10000*3+rand.Intn(10000); i++ {
		key := "test:ttl_checker_consistency:" + strconv.Itoa(i)
		dataType := dataTypes[rand.Int()%len(dataTypes)]
		kTypeMap[key] = dataType
		tn := time.Now().UnixNano()
		if _, err := db.expire(tn, dataType, []byte(key), nil, 2); err != nil {
			t.Fatal(err)
		}
	}

	time.Sleep(3 * time.Second)
	buffer := &TExpiredDataBuffer{
		t:        t,
		db:       db,
		wb:       gorocksdb.NewWriteBatch(),
		kTypeMap: kTypeMap,
	}

	if err := db.CheckExpiredData(buffer, make(chan struct{})); err != nil {
		t.Fatal(err)
	}

	if len(kTypeMap) != 0 {
		t.Fatal("not all keys has expired")
	}

	buffer.expiredCount = 0

	if err := db.CheckExpiredData(buffer, make(chan struct{})); err != nil {
		t.Fatal(err)
	}

	if buffer.expiredCount != 0 {
		t.Fatal("find some keys expired after all the keys stored has expired and deleted")
	}
}
