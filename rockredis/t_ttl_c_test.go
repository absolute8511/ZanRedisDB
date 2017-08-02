package rockredis

import (
	"bytes"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/absolute8511/gorocksdb"
)

func TestKVTTL_C(t *testing.T) {
	db := getTestDBWithExpirationPolicy(t, common.ConsistencyDeletion)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	key1 := []byte("test:testdb_ttl_kv_c")
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

func TestHashTTL_C(t *testing.T) {
	db := getTestDBWithExpirationPolicy(t, common.ConsistencyDeletion)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	hash_key := []byte("test:testdb_ttl_hash_c")
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

func TestListTTL_C(t *testing.T) {
	db := getTestDBWithExpirationPolicy(t, common.ConsistencyDeletion)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	list_key := []byte("test:testdb_ttl_list_c")
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

func TestSetTTL_C(t *testing.T) {
	db := getTestDBWithExpirationPolicy(t, common.ConsistencyDeletion)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	set_key := []byte("test:testdb_ttl_set_c")
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

func TestZSetTTL_C(t *testing.T) {
	db := getTestDBWithExpirationPolicy(t, common.ConsistencyDeletion)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	zset_key := []byte("test:testdb_ttl_zset_c")
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
		buff.t.Fatal("unknown expired key", string(key))
	} else if dataType2CommonType(kt) != dt {
		buff.t.Fatal("mismatched key-type, %s - %d, should be [%s - %d]", string(key), dt, string(key), dataType2CommonType(kt))
	} else {
		buff.wb.Clear()
		buff.db.delExpire(kt, key, buff.wb)
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
		if err := db.expire(dataType, []byte(key), 2); err != nil {
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
