package rockredis

import (
	"bytes"
	math "math"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/youzan/ZanRedisDB/common"
)

func TestCompactionFilterInWaitCompact(t *testing.T) {
	// test case need for
	// 1. expired in ttl
	// 2. meta not exist (coll delete)
	// 3. version changed (coll update)
	// 4. lazy check should be ensured
	db := getTestDBWithCompactTTL(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()
	key1 := []byte("test:testdbTTL_compactfilter1")
	key2 := []byte("test:testdbTTL_compactfilter2")
	tn := time.Now().UnixNano()
	err := db.KVSet(tn, key1, []byte("hello world 1"))
	assert.Nil(t, err)
	err = db.KVSet(tn, key2, []byte("hello world 2"))
	assert.Nil(t, err)

	hkeyKeep := []byte("test:testdbTTL_compactfilter_hashkeep")
	hkeyDel := []byte("test:testdbTTL_compactfilter_hashdel")
	hkeyUpdate := []byte("test:testdbTTL_compactfilter_hashudpate")
	hkeyExpire := []byte("test:testdbTTL_compactfilter_hashexpire")
	db.HSet(tn, false, hkeyKeep, []byte("test_hash_keep"), []byte("value_hash_keep"))
	db.HSet(tn, false, hkeyDel, []byte("test_hash_del"), []byte("value_hash_del"))
	db.HSet(tn, false, hkeyDel, []byte("test_hash_del2"), []byte("value_hash_del2"))
	db.HSet(tn, false, hkeyUpdate, []byte("test_hash_update"), []byte("value_hash_update"))
	db.HSet(tn, false, hkeyExpire, []byte("test_hash_expired"), []byte("value_hash_expire"))
	db.HSet(tn, false, hkeyExpire, []byte("test_hash_expired2"), []byte("value_hash_expire2"))

	db.Expire(tn, key1, 2)
	db.Expire(tn, key2, 3)
	db.HExpire(tn, hkeyExpire, 4)
	// compact filter should hit all keys
	db.CompactAllRange()
	stats := db.GetCompactFilterStats()
	assert.Equal(t, int64(0), stats.ExpiredCleanCnt)
	assert.Equal(t, int64(0), stats.DelCleanCnt)
	assert.Equal(t, int64(0), stats.VersionCleanCnt)

	time.Sleep(time.Millisecond)
	tn = time.Now().UnixNano()
	db.DelKeys(key1)
	db.HClear(tn, hkeyDel)
	db.HClear(tn, hkeyUpdate)
	db.HSet(tn, false, hkeyUpdate, []byte("test_hash_update2"), []byte("value_hash_update2"))

	// compact filter should hit all undeleted keys
	db.CompactAllRange()
	db.CompactAllRange()
	stats = db.GetCompactFilterStats()
	// should clean deleted until lazy period
	assert.Equal(t, int64(0), stats.ExpiredCleanCnt)
	assert.Equal(t, int64(0), stats.DelCleanCnt)
	assert.Equal(t, int64(0), stats.VersionCleanCnt)
	// wait ttl
	time.Sleep(time.Second * 5)
	db.CompactAllRange()
	db.CompactAllRange()
	// should clean expired until lazy period
	stats = db.GetCompactFilterStats()
	assert.Equal(t, int64(0), stats.ExpiredCleanCnt)
	// should clean deleted after lazy period (in test is 3s)
	assert.Equal(t, int64(2), stats.DelCleanCnt)
	assert.Equal(t, int64(1), stats.VersionCleanCnt)

	// wait lazy check
	time.Sleep(lazyCleanExpired * 2)
	db.CompactAllRange()
	stats = db.GetCompactFilterStats()
	assert.Equal(t, int64(4), stats.ExpiredCleanCnt)
	assert.Equal(t, int64(2), stats.DelCleanCnt)
	assert.Equal(t, int64(1), stats.VersionCleanCnt)
	hv, err := db.HGet(hkeyUpdate, []byte("test_hash_update2"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("value_hash_update2"), hv)
	hv, err = db.HGet(hkeyUpdate, []byte("test_hash_update"))
	assert.Nil(t, err)
	assert.Nil(t, hv)
	db.CompactAllRange()
	stats = db.GetCompactFilterStats()
	assert.Equal(t, int64(4), stats.ExpiredCleanCnt)
	assert.Equal(t, int64(2), stats.DelCleanCnt)
	assert.Equal(t, int64(1), stats.VersionCleanCnt)
}

func TestCompactionFilterInWaitCompactForZSet(t *testing.T) {
	// test case need for
	// 1. expired in ttl
	// 2. meta not exist (coll delete)
	// 3. version changed (coll update)
	// 4. lazy check should be ensured
	db := getTestDBWithCompactTTL(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	tn := time.Now().UnixNano()
	keyKeep := []byte("test:testdbTTL_compactfilter_keep")
	keyDel := []byte("test:testdbTTL_compactfilter_del")
	keyUpdate := []byte("test:testdbTTL_compactfilter_udpate")
	keyExpire := []byte("test:testdbTTL_compactfilter_expire")
	db.ZAdd(tn, keyKeep, common.ScorePair{Member: []byte("test_keep"), Score: 1})
	db.ZAdd(tn, keyDel, common.ScorePair{Member: []byte("test_del"), Score: 1})
	db.ZAdd(tn, keyDel, common.ScorePair{Member: []byte("test_del2"), Score: 2})
	db.ZAdd(tn, keyUpdate, common.ScorePair{Member: []byte("test_update"), Score: 1})
	db.ZAdd(tn, keyExpire, common.ScorePair{Member: []byte("test_expired"), Score: 1})
	db.ZAdd(tn, keyExpire, common.ScorePair{Member: []byte("test_expired2"), Score: 2})

	db.ZExpire(tn, keyExpire, 4)
	// compact filter should hit all keys
	db.CompactAllRange()
	stats := db.GetCompactFilterStats()
	assert.Equal(t, int64(0), stats.ExpiredCleanCnt)
	assert.Equal(t, int64(0), stats.DelCleanCnt)
	assert.Equal(t, int64(0), stats.VersionCleanCnt)

	time.Sleep(time.Millisecond)
	tn = time.Now().UnixNano()
	db.ZClear(tn, keyDel)
	db.ZClear(tn, keyUpdate)
	db.ZAdd(tn, keyUpdate, common.ScorePair{Member: []byte("test_update2"), Score: 2})

	// compact filter should hit all undeleted keys
	db.CompactAllRange()
	db.CompactAllRange()
	stats = db.GetCompactFilterStats()
	// should clean deleted until lazy period
	assert.Equal(t, int64(0), stats.ExpiredCleanCnt)
	assert.Equal(t, int64(0), stats.DelCleanCnt)
	assert.Equal(t, int64(0), stats.VersionCleanCnt)
	// wait ttl
	time.Sleep(time.Second * 5)
	db.CompactAllRange()
	db.CompactAllRange()
	// should clean expired until lazy period
	stats = db.GetCompactFilterStats()
	assert.Equal(t, int64(0), stats.ExpiredCleanCnt)
	// should clean deleted after lazy period (in test is 3s)
	assert.Equal(t, int64(4), stats.DelCleanCnt)
	assert.Equal(t, int64(2), stats.VersionCleanCnt)

	// wait lazy check
	time.Sleep(lazyCleanExpired * 2)
	db.CompactAllRange()
	stats = db.GetCompactFilterStats()
	assert.Equal(t, int64(5), stats.ExpiredCleanCnt)
	assert.Equal(t, int64(4), stats.DelCleanCnt)
	assert.Equal(t, int64(2), stats.VersionCleanCnt)
	hv, err := db.ZScore(keyUpdate, []byte("test_update2"))
	assert.Nil(t, err)
	assert.Equal(t, float64(2), hv)
	hv, err = db.ZScore(keyUpdate, []byte("test_update"))
	assert.Equal(t, errScoreMiss, err)
	db.CompactAllRange()
	stats = db.GetCompactFilterStats()
	assert.Equal(t, int64(5), stats.ExpiredCleanCnt)
	assert.Equal(t, int64(4), stats.DelCleanCnt)
	assert.Equal(t, int64(2), stats.VersionCleanCnt)
}

func TestCompactionFilterInWaitCompactForList(t *testing.T) {
	// test case need for
	// 1. expired in ttl
	// 2. meta not exist (coll delete)
	// 3. version changed (coll update)
	// 4. lazy check should be ensured
	db := getTestDBWithCompactTTL(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()
	tn := time.Now().UnixNano()

	keyKeep := []byte("test:testdbTTL_compactfilter_keep")
	keyDel := []byte("test:testdbTTL_compactfilter_del")
	keyUpdate := []byte("test:testdbTTL_compactfilter_update")
	keyExpire := []byte("test:testdbTTL_compactfilter_expire")
	db.LPush(tn, keyKeep, []byte("test_keep"))
	db.LPush(tn, keyDel, []byte("test_del"))
	db.LPush(tn, keyDel, []byte("test_del2"))
	db.LPush(tn, keyUpdate, []byte("test_update"))
	db.LPush(tn, keyExpire, []byte("test_expired"))
	db.LPush(tn, keyExpire, []byte("test_expired2"))

	db.LExpire(tn, keyExpire, 4)
	// compact filter should hit all keys
	db.CompactAllRange()
	stats := db.GetCompactFilterStats()
	assert.Equal(t, int64(0), stats.ExpiredCleanCnt)
	assert.Equal(t, int64(0), stats.DelCleanCnt)
	assert.Equal(t, int64(0), stats.VersionCleanCnt)

	time.Sleep(time.Millisecond)
	tn = time.Now().UnixNano()
	db.LClear(tn, keyDel)
	db.LClear(tn, keyUpdate)
	db.LPush(tn, keyUpdate, []byte("test_update2"))

	// compact filter should hit all undeleted keys
	db.CompactAllRange()
	db.CompactAllRange()
	stats = db.GetCompactFilterStats()
	// should clean deleted until lazy period
	assert.Equal(t, int64(0), stats.ExpiredCleanCnt)
	assert.Equal(t, int64(0), stats.DelCleanCnt)
	assert.Equal(t, int64(0), stats.VersionCleanCnt)
	// wait ttl
	time.Sleep(time.Second * 5)
	db.CompactAllRange()
	db.CompactAllRange()
	// should clean expired until lazy period
	stats = db.GetCompactFilterStats()
	assert.Equal(t, int64(0), stats.ExpiredCleanCnt)
	// should clean deleted after lazy period (in test is 3s)
	assert.Equal(t, int64(2), stats.DelCleanCnt)
	assert.Equal(t, int64(1), stats.VersionCleanCnt)

	// wait lazy check
	time.Sleep(lazyCleanExpired * 2)
	db.CompactAllRange()
	stats = db.GetCompactFilterStats()
	assert.Equal(t, int64(3), stats.ExpiredCleanCnt)
	assert.Equal(t, int64(2), stats.DelCleanCnt)
	assert.Equal(t, int64(1), stats.VersionCleanCnt)
	hv, err := db.LIndex(keyUpdate, 0)
	assert.Nil(t, err)
	assert.Equal(t, []byte("test_update2"), hv)
	db.CompactAllRange()
	stats = db.GetCompactFilterStats()
	assert.Equal(t, int64(3), stats.ExpiredCleanCnt)
	assert.Equal(t, int64(2), stats.DelCleanCnt)
	assert.Equal(t, int64(1), stats.VersionCleanCnt)
}

func TestCompactionFilterInWaitCompactForBitmap(t *testing.T) {
	// test case need for
	// 1. expired in ttl
	// 2. meta not exist (coll delete)
	// 3. version changed (coll update)
	// 4. lazy check should be ensured
	db := getTestDBWithCompactTTL(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()
	tn := time.Now().UnixNano()

	keyKeep := []byte("test:testdbTTL_compactfilter_keep")
	keyDel := []byte("test:testdbTTL_compactfilter_del")
	keyUpdate := []byte("test:testdbTTL_compactfilter_udpate")
	keyExpire := []byte("test:testdbTTL_compactfilter_expire")
	db.BitSetV2(tn, keyKeep, 1, 1)
	db.BitSetV2(tn, keyDel, 1, 1)
	db.BitSetV2(tn, keyDel, 1024000, 1)
	db.BitSetV2(tn, keyUpdate, 1, 1)
	db.BitSetV2(tn, keyExpire, 1, 1)
	db.BitSetV2(tn, keyExpire, 1024000, 1)

	db.BitExpire(tn, keyExpire, 4)
	// compact filter should hit all keys
	db.CompactAllRange()
	stats := db.GetCompactFilterStats()
	assert.Equal(t, int64(0), stats.ExpiredCleanCnt)
	assert.Equal(t, int64(0), stats.DelCleanCnt)
	assert.Equal(t, int64(0), stats.VersionCleanCnt)

	time.Sleep(time.Millisecond)
	tn = time.Now().UnixNano()
	db.BitClear(tn, keyDel)
	db.BitClear(tn, keyUpdate)
	db.BitSetV2(tn, keyUpdate, 2, 1)

	// compact filter should hit all undeleted keys
	db.CompactAllRange()
	db.CompactAllRange()
	stats = db.GetCompactFilterStats()
	// should clean deleted until lazy period
	assert.Equal(t, int64(0), stats.ExpiredCleanCnt)
	assert.Equal(t, int64(0), stats.DelCleanCnt)
	assert.Equal(t, int64(0), stats.VersionCleanCnt)
	// wait ttl
	time.Sleep(time.Second * 5)
	db.CompactAllRange()
	db.CompactAllRange()
	// should clean expired until lazy period
	stats = db.GetCompactFilterStats()
	assert.Equal(t, int64(0), stats.ExpiredCleanCnt)
	// should clean deleted after lazy period (in test is 3s)
	assert.Equal(t, int64(2), stats.DelCleanCnt)
	assert.Equal(t, int64(1), stats.VersionCleanCnt)

	// wait lazy check
	time.Sleep(lazyCleanExpired * 2)
	db.CompactAllRange()
	stats = db.GetCompactFilterStats()
	assert.Equal(t, int64(3), stats.ExpiredCleanCnt)
	assert.Equal(t, int64(2), stats.DelCleanCnt)
	assert.Equal(t, int64(1), stats.VersionCleanCnt)
	v, err := db.BitGetV2(keyUpdate, 2)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), v)
	v, err = db.BitGetV2(keyUpdate, 1)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), v)
	db.CompactAllRange()
	stats = db.GetCompactFilterStats()
	assert.Equal(t, int64(3), stats.ExpiredCleanCnt)
	assert.Equal(t, int64(2), stats.DelCleanCnt)
	assert.Equal(t, int64(1), stats.VersionCleanCnt)
}

func TestKVTTL_Compact(t *testing.T) {
	db := getTestDBWithCompactTTL(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	key1 := []byte("test:testdbTTL_kv_compact")
	var ttl1 int64 = int64(rand.Int31() - 10)
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

	// test change ttl
	n, err := db.Expire(tn, key1, ttl2)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)

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
	n, err = db.KVGetVer(key1)
	assert.Nil(t, err)
	assert.Equal(t, tn, n)
	v, err := db.KVGetExpired(key1)
	assert.Nil(t, err)
	assert.Equal(t, testValue, v)

	err = db.SetEx(tn, key1, ttl2, testValue)
	assert.Nil(t, err)

	n, err = db.KVTtl(key1)
	assert.Nil(t, err)
	assert.Equal(t, ttl2, n)
	// set, setnx, mset should clean ttl
	err = db.KVSet(0, key1, testValue)
	assert.Nil(t, err)
	n, err = db.KVTtl(key1)
	assert.Nil(t, err)
	assert.Equal(t, int64(-1), n)

	err = db.SetEx(tn, key1, ttl2, testValue)
	assert.Nil(t, err)
	n, err = db.KVTtl(key1)
	assert.Nil(t, err)
	assert.Equal(t, ttl2, n)

	// set not success should keep ttl
	changed, err := db.SetNX(0, key1, testValue)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), changed)
	n, err = db.KVTtl(key1)
	assert.Nil(t, err)
	assert.Equal(t, ttl2, n)

	db.MSet(0, common.KVRecord{Key: key1, Value: testValue})
	n, err = db.KVTtl(key1)
	assert.Nil(t, err)
	assert.Equal(t, int64(-1), n)

	err = db.SetEx(tn, key1, 1, testValue)
	assert.Nil(t, err)
	n, err = db.KVTtl(key1)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)

	time.Sleep(time.Second * 2)
	// set success should clean ttl
	tn = time.Now().UnixNano()
	changed, err = db.SetNX(tn, key1, testValue)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), changed)
	n, err = db.KVTtl(key1)
	assert.Nil(t, err)
	assert.Equal(t, int64(-1), n)
	// compact should clean all expired data
	db.CompactAllRange()
}

func TestKVTTL_CompactKeepTTL(t *testing.T) {
	db := getTestDBWithCompactTTL(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	key1 := []byte("test:testdbTTL_kv_compact_keepttl")
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
	n, err := db.KVTtl(key1)
	assert.Nil(t, err)
	assert.Equal(t, int64(ttl1), n)
	// incr should keep ttl
	if c, err := db.Incr(0, key1); err != nil {
		t.Fatal(err)
	} else {
		assert.Equal(t, int64(2), c)
	}
	n, err = db.KVTtl(key1)
	assert.Nil(t, err)
	assert.Equal(t, int64(ttl1), n)

	// append
	_, err = db.Append(0, key1, []byte("append"))
	assert.Nil(t, err)
	if v, err := db.KVTtl(key1); err != nil {
		t.Fatal(err)
	} else if v != ttl1 {
		t.Fatal("ttl != expire")
	}
	// setrange
	_, err = db.SetRange(0, key1, 1, []byte("range"))
	assert.Nil(t, err)
	if v, err := db.KVTtl(key1); err != nil {
		t.Fatal(err)
	} else if v != ttl1 {
		t.Fatal("ttl != expire")
	}
	v, err := db.KVGet(key1)
	assert.Nil(t, err)
	assert.Equal(t, "2ranged", string(v))
	db.DelKeys(key1)
	// bitset
	db.BitSetOld(0, key1, 1, 1)
	if v, err := db.Expire(tn, key1, ttl1); err != nil {
		t.Fatal(err)
	} else if v != 1 {
		t.Fatalf("return value from expire %v != 1", v)
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

func TestKVTTL_Compact_TTLExpired(t *testing.T) {
	db := getTestDBWithCompactTTL(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	key1 := []byte("test:testdbTTL_kv_compact_ttlexpire")
	var ttl1 int64 = int64(2)
	tn := time.Now().UnixNano()
	if c, err := db.Incr(tn, key1); err != nil {
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
	if c, err := db.Incr(tn, key1); err != nil {
		t.Fatal(err)
	} else {
		assert.Equal(t, int64(2), c)
	}
	if v, err := db.KVTtl(key1); err != nil {
		t.Fatal(err)
	} else if v != ttl1 {
		t.Fatal("ttl != expire")
	}
	// setnx no ok should keep ttl
	n, err := db.SetNX(tn, key1, []byte("new"))
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)
	n, err = db.KVTtl(key1)
	assert.Nil(t, err)
	assert.Equal(t, int64(ttl1), n)

	n, err = db.KVGetVer(key1)
	assert.Nil(t, err)
	assert.Equal(t, tn, n)
	v, err := db.KVGetExpired(key1)
	assert.Nil(t, err)
	assert.Equal(t, []byte("2"), v)

	time.Sleep(time.Second * time.Duration(ttl1+1))
	if v, err := db.KVTtl(key1); err != nil {
		t.Fatal(err)
	} else if v != -1 {
		t.Fatalf("should expired: %v", v)
	}
	exist, err := db.KVExists(key1)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), exist)
	v, err = db.KVGet(key1)
	assert.Nil(t, err)
	assert.Nil(t, v)

	n, err = db.KVGetVer(key1)
	assert.Nil(t, err)
	assert.Equal(t, tn, n)
	v, err = db.KVGetExpired(key1)
	assert.Nil(t, err)
	assert.Equal(t, []byte("2"), v)

	vlist, errs := db.MGet(key1)
	assert.Nil(t, errs[0])
	assert.Nil(t, vlist[0])

	// success setnx should renew ttl
	tn = time.Now().UnixNano()
	n, err = db.SetNX(tn, key1, []byte("new1"))
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)
	n, err = db.KVTtl(key1)
	assert.Nil(t, err)
	assert.Equal(t, int64(-1), n)
	v, err = db.KVGet(key1)
	assert.Nil(t, err)
	assert.Equal(t, []byte("new1"), v)

	n, err = db.KVGetVer(key1)
	assert.Nil(t, err)
	assert.Equal(t, tn, n)
	v, err = db.KVGetExpired(key1)
	assert.Nil(t, err)
	assert.Equal(t, []byte("new1"), v)

	n, err = db.Expire(tn, key1, ttl1)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)
	n, err = db.KVTtl(key1)
	assert.Nil(t, err)
	assert.Equal(t, int64(ttl1), n)

	time.Sleep(time.Second * time.Duration(ttl1+1))
	exist, err = db.KVExists(key1)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), exist)
	v, err = db.KVGet(key1)
	assert.Nil(t, err)
	assert.Nil(t, v)
	vlist, errs = db.MGet(key1)
	assert.Nil(t, errs[0])
	assert.Nil(t, vlist[0])
	n, err = db.KVTtl(key1)
	assert.Nil(t, err)
	assert.Equal(t, int64(-1), n)

	tn = time.Now().UnixNano()
	v, err = db.KVGetSet(tn, key1, []byte("new2"))
	assert.Nil(t, err)
	assert.Nil(t, v)
	n, err = db.KVTtl(key1)
	assert.Nil(t, err)
	assert.Equal(t, int64(-1), n)

	v, err = db.KVGet(key1)
	assert.Nil(t, err)
	assert.Equal(t, []byte("new2"), v)
}

func TestKVTTL_CompactOverflow(t *testing.T) {
	db := getTestDBWithCompactTTL(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	key1 := []byte("test:testdbTTL_kv_compact")
	var ttl1 int64 = math.MaxUint32
	tn := time.Now().UnixNano()
	err := db.KVSet(0, key1, []byte("hello world 1"))
	assert.Nil(t, err)

	_, err = db.Expire(tn, key1, ttl1)
	assert.NotNil(t, err)

	if v, err := db.KVTtl(key1); err != nil {
		t.Fatal(err)
	} else if v != -1 {
		t.Fatal("ttl != -1")
	}

	tn = time.Now().UnixNano()
	testValue := []byte("test value for SetEx command")
	err = db.SetEx(tn, key1, ttl1, testValue)
	assert.NotNil(t, err)

	if v, err := db.KVTtl(key1); err != nil {
		t.Fatal(err)
	} else if v != -1 {
		t.Fatalf("ttl != setex: %v vs %v", v, ttl1)
	}
}

func TestHashTTL_Compact(t *testing.T) {
	db := getTestDBWithCompactTTL(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	hashKey := []byte("test:testdbTTL_hash_compact")
	var hashTTL int64 = int64(rand.Int31()) - 10

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

	if err := db.HMset(tn, hashKey, hash_val...); err != nil {
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

	if v, err := db.HashTtl(hashKey); err != nil {
		t.Fatal(err)
	} else if v != -1 {
		t.Fatal("HashPersist do not clear the ttl")
	}

	// compact should clean all expired data
	db.CompactAllRange()
}

func TestHashTTL_Compact_KeepTTL(t *testing.T) {
	db := getTestDBWithCompactTTL(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	hashKey := []byte("test:testdbTTL_hash_compact_keepttl")
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
	n, err = db.HDel(0, hashKey, hash_val[0].Key)
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

func TestHashTTL_Compact_TTLExpired(t *testing.T) {
	db := getTestDBWithCompactTTL(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	hashKey := []byte("test:testdbTTL_hash_compact_expired")
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

	// test hash get exipred
	n, err = db.HGetVer(hashKey, hash_val[0].Key)
	assert.Nil(t, err)
	assert.Equal(t, int64(tn), n)
	tn1 := tn

	n, vals, err := db.HGetAll(hashKey)
	assert.Nil(t, err)
	assert.Equal(t, int64(3), n)
	assert.Equal(t, 3, len(vals))
	n, vals, err = db.HGetAllExpired(hashKey)
	assert.Nil(t, err)
	assert.Equal(t, int64(3), n)
	assert.Equal(t, 3, len(vals))

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
	vlist, err = db.HMgetExpired(hashKey, hash_val[0].Key, hash_val[1].Key)
	assert.Nil(t, err)
	assert.Equal(t, hash_val[0].Value, vlist[0])
	assert.Equal(t, hash_val[1].Value, vlist[1])

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

	// test hash get exipred
	n, err = db.HGetVer(hashKey, hash_val[0].Key)
	assert.Nil(t, err)
	assert.Equal(t, int64(tn1), n)

	n, vals2, err := db.HGetAllExpired(hashKey)
	assert.Nil(t, err)
	assert.Equal(t, int64(3), n)
	assert.Equal(t, 3, len(vals2))
	assert.Equal(t, vals, vals2)

	// renew hash
	hash_val2 := []common.KVRecord{
		{Key: []byte("field0"), Value: []byte("new")},
		{Key: []byte("field1"), Value: []byte("value1_new")},
	}
	time.Sleep(time.Second)
	tn = time.Now().UnixNano()
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

	v, err = db.HGet(hashKey, hash_val2[0].Key)
	assert.Nil(t, err)
	assert.Equal(t, hash_val2[0].Value, v)
	n, err = db.HKeyExists(hashKey)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)

	n, err = db.HGetVer(hashKey, hash_val2[0].Key)
	assert.Nil(t, err)
	assert.Equal(t, int64(tn), n)

	n, vals2, err = db.HGetAllExpired(hashKey)
	assert.Nil(t, err)
	assert.Equal(t, int64(2), n)
	assert.Equal(t, 2, len(vals2))
	assert.NotEqual(t, vals, vals2)

	vlist, err = db.HMget(hashKey, hash_val2[0].Key, hash_val2[1].Key)
	assert.Nil(t, err)
	assert.Equal(t, hash_val2[0].Value, vlist[0])
	assert.Equal(t, hash_val2[1].Value, vlist[1])

	n, recs, err = db.HKeys(hashKey)
	assert.Equal(t, int64(len(hash_val2)), n)
	assert.Equal(t, len(hash_val2), len(recs))
	n, recs, err = db.HValues(hashKey)
	assert.Equal(t, int64(len(hash_val2)), n)
	assert.Equal(t, len(hash_val2), len(recs))
}

func TestBitmapTTL_Compact(t *testing.T) {
	db := getTestDBWithCompactTTL(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	setKey := []byte("test:testdbTTL_bitmap_compact")
	var setTTL int64 = int64(rand.Int31() - 10)

	if v, err := db.BitTtl(setKey); err != nil {
		t.Fatal(err)
	} else if v != -1 {
		t.Fatal("ttl of not exist key is not -1")
	}
	tn := time.Now().UnixNano()

	if v, err := db.BitExpire(tn, setKey, setTTL); err != nil {
		t.Fatal(err)
	} else if v != 0 {
		t.Fatal("return value from expire of not exist key != 0")
	}

	if v, err := db.BitPersist(tn, setKey); err != nil {
		t.Fatal(err)
	} else if v != 0 {
		t.Fatal("return value from persist of not exist set key != 0")
	}

	if _, err := db.BitSetV2(tn, setKey, 1, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := db.BitSetV2(tn, setKey, 2, 1); err != nil {
		t.Fatal(err)
	}

	if v, err := db.BitExpire(tn, setKey, setTTL); err != nil {
		t.Fatal(err)
	} else if v != 1 {
		t.Fatal("return value from expire != 1")
	}

	if v, err := db.BitTtl(setKey); err != nil {
		t.Fatal(err)
	} else if v != setTTL {
		t.Errorf("ttl != expire, %v, %v", v, setTTL)
	}

	if v, err := db.BitPersist(tn, setKey); err != nil {
		t.Fatal(err)
	} else if v != 1 {
		t.Fatal("return value from persist!= 1")
	}

	if v, err := db.BitTtl(setKey); err != nil {
		t.Fatal(err)
	} else if v != -1 {
		t.Fatal("Persist do not clear the ttl")
	}
}

func TestBitmapTTL_Compact_KeepTTL(t *testing.T) {
	db := getTestDBWithCompactTTL(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	setKey := []byte("test:testdbTTL_bitmap_compact_keepttl")
	var ttl int64 = int64(rand.Int31() - 10)
	tn := time.Now().UnixNano()

	n, err := db.BitSetV2(tn, setKey, 1, 1)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)
	n, err = db.BitSetV2(tn, setKey, 2, 1)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)
	n, err = db.BitExpire(tn, setKey, ttl)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)
	n, err = db.BitTtl(setKey)
	assert.Nil(t, err)
	assert.Equal(t, ttl, n)

	// should keep ttl
	n, err = db.BitSetV2(tn, setKey, 3, 1)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)
	n, err = db.BitTtl(setKey)
	assert.Nil(t, err)
	assert.Equal(t, ttl, n)

	n, err = db.BitSetV2(tn, setKey, 1, 0)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)
	n, err = db.BitTtl(setKey)
	assert.Nil(t, err)
	assert.Equal(t, ttl, n)
	_, err = db.BitSetV2(tn, setKey, 2, 0)
	assert.Nil(t, err)
	n, err = db.BitTtl(setKey)
	assert.Nil(t, err)
	assert.Equal(t, ttl, n)
}

func TestBitmapTTL_Compact_TTLExpired(t *testing.T) {
	db := getTestDBWithCompactTTL(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	setKey := []byte("test:testdbTTL_bitmap_compact_expired")
	var ttl int64 = int64(3)
	tn := time.Now().UnixNano()

	n, err := db.BitSetV2(tn, setKey, 1, 1)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)
	n, err = db.BitSetV2(tn, setKey, 2, 1)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)
	n, err = db.BitExpire(tn, setKey, ttl)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)

	n, err = db.BitTtl(setKey)
	assert.Nil(t, err)
	assert.Equal(t, ttl, n)

	n, err = db.BitGetV2(setKey, 1)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)
	n, err = db.BitCountV2(setKey, 0, -1)
	assert.Nil(t, err)
	assert.Equal(t, int64(2), n)
	n, err = db.BitKeyExist(setKey)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)

	time.Sleep(time.Second * time.Duration(ttl+1))
	dbLog.Infof("wait expired done")
	n, err = db.BitTtl(setKey)
	assert.Nil(t, err)
	assert.Equal(t, int64(-1), n)

	tn = time.Now().UnixNano()
	n, err = db.BitGetV2(setKey, 1)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)
	n, err = db.BitKeyExist(setKey)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)
	n, err = db.BitCountV2(setKey, 0, -1)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)

	// renew
	time.Sleep(time.Second)
	tn = time.Now().UnixNano()
	n, err = db.BitSetV2(tn, setKey, 3, 1)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)
	n, err = db.BitSetV2(tn, setKey, 4, 1)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)

	n, err = db.BitTtl(setKey)
	assert.Nil(t, err)
	assert.Equal(t, int64(-1), n)

	n, err = db.BitCountV2(setKey, 0, -1)
	assert.Nil(t, err)
	assert.Equal(t, int64(2), n)

	n, err = db.BitGetV2(setKey, 1)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)
	n, err = db.BitGetV2(setKey, 3)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)

	n, err = db.BitKeyExist(setKey)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)
}

func TestListTTL_Compact(t *testing.T) {
	db := getTestDBWithCompactTTL(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	listKey := []byte("test:testdbTTL_list_c")
	var listTTL int64 = 1000
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
func TestListTTL_Compact_KeepTTL(t *testing.T) {
	db := getTestDBWithCompactTTL(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	setKey := []byte("test:testdbTTL_list_compact_keepttl")
	var ttl int64 = int64(rand.Int31() - 10)
	tn := time.Now().UnixNano()
	mems := [][]byte{
		[]byte("m1"), []byte("m2"), []byte("m3"),
	}

	n, err := db.RPush(tn, setKey, mems...)
	assert.Nil(t, err)
	assert.Equal(t, int64(len(mems)), n)
	n, err = db.LExpire(tn, setKey, ttl)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)
	n, err = db.ListTtl(setKey)
	assert.Nil(t, err)
	assert.InDelta(t, ttl, n, 1)

	// should keep ttl
	n, err = db.RPush(tn, setKey, []byte("newm1"))
	assert.Nil(t, err)
	assert.Equal(t, int64(4), n)
	n, err = db.ListTtl(setKey)
	assert.Nil(t, err)
	assert.InDelta(t, ttl, n, 1)

	elem, err := db.LPop(tn, setKey)
	assert.Nil(t, err)
	assert.Equal(t, mems[0], elem)
	n, err = db.ListTtl(setKey)
	assert.Nil(t, err)
	assert.InDelta(t, ttl, n, 1)
}

func TestListTTL_Compact_TTLExpired(t *testing.T) {
	db := getTestDBWithCompactTTL(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	setKey := []byte("test:testdbTTL_list_compact_expired")
	var ttl int64 = int64(3)
	tn := time.Now().UnixNano()
	mems := [][]byte{
		[]byte("mem1"), []byte("mem2"), []byte("mem3"),
	}

	n, err := db.RPush(tn, setKey, mems...)
	assert.Nil(t, err)
	assert.Equal(t, int64(len(mems)), n)
	n, err = db.LExpire(tn, setKey, ttl)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)

	n, err = db.ListTtl(setKey)
	assert.Nil(t, err)
	assert.InDelta(t, ttl, n, 1)

	elem, err := db.LIndex(setKey, 0)
	assert.Nil(t, err)
	assert.Equal(t, mems[0], elem)
	vlist, err := db.LRange(setKey, 0, 100)
	assert.Nil(t, err)
	assert.Equal(t, len(mems), len(vlist))
	n, err = db.LLen(setKey)
	assert.Nil(t, err)
	assert.Equal(t, int64(len(mems)), n)

	err = db.LSet(tn, setKey, 0, []byte("newv"))
	assert.Nil(t, err)
	elem, err = db.LIndex(setKey, 0)
	assert.Nil(t, err)
	assert.Equal(t, []byte("newv"), elem)

	time.Sleep(time.Second * time.Duration(ttl+1))
	dbLog.Infof("wait expired done")
	n, err = db.ListTtl(setKey)
	assert.Nil(t, err)
	assert.Equal(t, int64(-1), n)

	tn = time.Now().UnixNano()
	elem, err = db.LIndex(setKey, 0)
	assert.Nil(t, err)
	assert.Nil(t, elem)
	err = db.LSet(tn, setKey, 0, []byte("newv2"))
	assert.NotNil(t, err)
	n, err = db.LKeyExists(setKey)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)
	vlist, err = db.LRange(setKey, 0, 100)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(vlist))
	n, err = db.LLen(setKey)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)

	elem, err = db.LPop(tn, setKey)
	assert.Nil(t, err)
	assert.Nil(t, elem)

	// renew
	mems2 := [][]byte{
		[]byte("newmem1"), []byte("newmem2"),
	}
	time.Sleep(time.Second)
	tn = time.Now().UnixNano()
	n, err = db.RPush(tn, setKey, mems2...)
	assert.Nil(t, err)
	assert.Equal(t, int64(len(mems2)), n)

	n, err = db.ListTtl(setKey)
	assert.Nil(t, err)
	assert.Equal(t, int64(-1), n)

	n, err = db.LLen(setKey)
	assert.Nil(t, err)
	assert.Equal(t, int64(len(mems2)), n)

	vlist, err = db.LRange(setKey, 0, 100)
	assert.Nil(t, err)
	assert.Equal(t, len(mems2), len(vlist))
	assert.Equal(t, mems2[0], vlist[0])

	elem, err = db.LIndex(setKey, 0)
	assert.Nil(t, err)
	assert.Equal(t, mems2[0], elem)
	err = db.LSet(tn, setKey, 0, []byte("newv3"))
	assert.Nil(t, err)
	elem, err = db.LIndex(setKey, 0)
	assert.Nil(t, err)
	assert.Equal(t, []byte("newv3"), elem)

	n, err = db.LKeyExists(setKey)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)
}

func TestSetTTL_Compact(t *testing.T) {
	db := getTestDBWithCompactTTL(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	setKey := []byte("test:testdbTTL_set_compact")
	var setTTL int64 = int64(rand.Int31() - 10)

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

func TestSetTTL_Compact_KeepTTL(t *testing.T) {
	db := getTestDBWithCompactTTL(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	setKey := []byte("test:testdbTTL_set_compact_keepttl")
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
	assert.InDelta(t, ttl, n, 1)

	// should keep ttl
	n, err = db.SAdd(tn, setKey, []byte("newm1"))
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)
	n, err = db.SetTtl(setKey)
	assert.Nil(t, err)
	assert.InDelta(t, ttl, n, 1)

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
	assert.InDelta(t, ttl, n, 1)
}

func TestSetTTL_Compact_TTLExpired(t *testing.T) {
	db := getTestDBWithCompactTTL(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	setKey := []byte("test:testdbTTL_set_compact_expired")
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
	assert.Equal(t, mems2[0], vlist[0])

	n, err = db.SIsMember(setKey, mems[0])
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)
	n, err = db.SIsMember(setKey, mems2[0])
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)

	n, err = db.SKeyExists(setKey)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)

	vlist, err = db.SScan(setKey, []byte(""), -1, "", false)
	assert.Nil(t, err)
	assert.Equal(t, len(mems2), len(vlist))
	assert.Equal(t, mems2[0], vlist[0])

	vlist, err = db.SPop(tn, setKey, 1)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(vlist))
	assert.Equal(t, mems2[0], vlist[0])
}

func TestZSetTTL_Compact(t *testing.T) {
	db := getTestDBWithCompactTTL(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	zsetKey := []byte("test:testdbTTL_zset_c")
	var zsetTTL int64 = 1000

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

func TestZSetTTL_Compact_KeepTTL(t *testing.T) {
	db := getTestDBWithCompactTTL(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	setKey := []byte("test:testdbTTL_zset_compact_keepttl")
	var ttl int64 = int64(rand.Int31() - 10)
	tn := time.Now().UnixNano()
	mems := []common.ScorePair{
		common.ScorePair{1, []byte("m1")},
		common.ScorePair{2, []byte("m2")},
		common.ScorePair{3, []byte("m3")},
	}

	n, err := db.ZAdd(tn, setKey, mems...)
	assert.Nil(t, err)
	assert.Equal(t, int64(len(mems)), n)
	n, err = db.ZExpire(tn, setKey, ttl)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)
	n, err = db.ZSetTtl(setKey)
	assert.Nil(t, err)
	assert.Equal(t, ttl, n)

	mems2 := []common.ScorePair{
		common.ScorePair{4, []byte("newm4")},
		common.ScorePair{5, []byte("newm5")},
	}
	// should keep ttl
	n, err = db.ZAdd(tn, setKey, mems2[0])
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)
	n, err = db.ZSetTtl(setKey)
	assert.Nil(t, err)
	assert.Equal(t, ttl, n)

	n, err = db.ZRem(tn, setKey, mems[0].Member)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)
	n, err = db.ZSetTtl(setKey)
	assert.Nil(t, err)
	assert.Equal(t, ttl, n)
}

func TestZSetTTL_Compact_TTLExpired(t *testing.T) {
	db := getTestDBWithCompactTTL(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	setKey := []byte("test:testdbTTL_zset_compact_expired")
	var ttl int64 = int64(3)
	tn := time.Now().UnixNano()

	mems := []common.ScorePair{
		common.ScorePair{1, []byte("m1")},
		common.ScorePair{2, []byte("m2")},
		common.ScorePair{3, []byte("m3")},
	}

	n, err := db.ZAdd(tn, setKey, mems...)
	assert.Nil(t, err)
	assert.Equal(t, int64(len(mems)), n)
	n, err = db.ZExpire(tn, setKey, ttl)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)

	n, err = db.ZSetTtl(setKey)
	assert.Nil(t, err)
	assert.Equal(t, ttl, n)

	score, err := db.ZScore(setKey, mems[0].Member)
	assert.Nil(t, err)
	assert.Equal(t, mems[0].Score, score)
	n, err = db.ZRank(setKey, mems[0].Member)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)
	n, err = db.ZRevRank(setKey, mems[0].Member)
	assert.Nil(t, err)
	assert.Equal(t, int64(2), n)

	vlist, err := db.ZRange(setKey, 0, 100)
	assert.Nil(t, err)
	assert.Equal(t, len(mems), len(vlist))
	mlist, err := db.ZRangeByLex(setKey, nil, nil, common.RangeClose, 0, -1)
	assert.Nil(t, err)
	assert.Equal(t, len(mems), len(mlist))
	vlist, err = db.ZRangeByScore(setKey, 0, 100, 0, -1)
	assert.Nil(t, err)
	assert.Equal(t, len(mems), len(vlist))

	n, err = db.ZCard(setKey)
	assert.Nil(t, err)
	assert.Equal(t, int64(len(mems)), n)
	n, err = db.ZCount(setKey, 0, 100)
	assert.Nil(t, err)
	assert.Equal(t, int64(len(mems)), n)
	n, err = db.ZLexCount(setKey, nil, nil, common.RangeClose)
	assert.Nil(t, err)
	assert.Equal(t, int64(len(mems)), n)

	time.Sleep(time.Second * time.Duration(ttl+1))
	dbLog.Infof("wait expired done")
	n, err = db.ZSetTtl(setKey)
	assert.Nil(t, err)
	assert.Equal(t, int64(-1), n)

	tn = time.Now().UnixNano()
	score, err = db.ZScore(setKey, mems[0].Member)
	assert.Equal(t, errScoreMiss, err)
	assert.Equal(t, float64(0), score)
	n, err = db.ZRank(setKey, mems[0].Member)
	assert.Nil(t, err)
	assert.Equal(t, int64(-1), n)
	n, err = db.ZRevRank(setKey, mems[0].Member)
	assert.Nil(t, err)
	assert.Equal(t, int64(-1), n)

	n, err = db.ZKeyExists(setKey)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)
	vlist, err = db.ZRange(setKey, 0, 100)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(vlist))
	mlist, err = db.ZRangeByLex(setKey, nil, nil, common.RangeClose, 0, -1)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(mlist))
	vlist, err = db.ZRangeByScore(setKey, 0, 100, 0, -1)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(vlist))
	vlist, err = db.ZRevRange(setKey, 0, 100)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(vlist))
	vlist, err = db.ZScan(setKey, []byte(""), -1, "", false)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(vlist))

	n, err = db.ZCard(setKey)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)
	n, err = db.ZCount(setKey, 0, 100)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)
	n, err = db.ZLexCount(setKey, nil, nil, common.RangeClose)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)

	// renew
	mems2 := []common.ScorePair{
		common.ScorePair{4, []byte("newm4")},
		common.ScorePair{5, []byte("newm5")},
	}
	time.Sleep(time.Second)
	tn = time.Now().UnixNano()
	n, err = db.ZAdd(tn, setKey, mems2...)
	assert.Nil(t, err)
	assert.Equal(t, int64(len(mems2)), n)

	n, err = db.ZSetTtl(setKey)
	assert.Nil(t, err)
	assert.Equal(t, int64(-1), n)

	n, err = db.ZCard(setKey)
	assert.Nil(t, err)
	assert.Equal(t, int64(len(mems2)), n)

	vlist, err = db.ZRange(setKey, 0, 100)
	assert.Nil(t, err)
	assert.Equal(t, len(mems2), len(vlist))
	assert.Equal(t, mems2[0], vlist[0])

	score, err = db.ZScore(setKey, mems[0].Member)
	assert.Equal(t, errScoreMiss, err)
	assert.Equal(t, float64(0), score)
	n, err = db.ZRank(setKey, mems[0].Member)
	assert.Nil(t, err)
	assert.Equal(t, int64(-1), n)
	n, err = db.ZRevRank(setKey, mems[0].Member)
	assert.Nil(t, err)
	assert.Equal(t, int64(-1), n)

	score, err = db.ZScore(setKey, mems2[0].Member)
	assert.Nil(t, err)
	assert.Equal(t, float64(mems2[0].Score), score)
	n, err = db.ZRank(setKey, mems2[0].Member)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)
	n, err = db.ZRevRank(setKey, mems2[0].Member)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)

	n, err = db.ZKeyExists(setKey)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)

	vlist, err = db.ZScan(setKey, []byte(""), -1, "", false)
	assert.Nil(t, err)
	assert.Equal(t, len(mems2), len(vlist))
	assert.Equal(t, mems2[0], vlist[0])
}

func TestDBCompactTTL(t *testing.T) {
	db := getTestDBWithCompactTTL(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	kTypeMap := make(map[string]byte)
	dataTypes := []byte{KVType, ListType, HashType, SetType, ZSetType, BitmapType}

	for i := 0; i < 10000*3+rand.Intn(10000); i++ {
		key := "test:ttl_checker_compact:" + strconv.Itoa(i)
		dataType := dataTypes[rand.Int()%len(dataTypes)]
		kTypeMap[key] = dataType

		switch dataType {
		case KVType:
			db.KVSet(0, []byte(key), []byte("test_checker_local_kvValue"))

		case ListType:
			tListKey := []byte(key)
			db.LPush(0, tListKey, []byte("this"), []byte("is"), []byte("list"),
				[]byte("local"), []byte("deletion"), []byte("ttl"), []byte("checker"), []byte("test"))

		case HashType:
			tHashKey := []byte(key)
			tHashVal := []common.KVRecord{
				{Key: []byte("field0"), Value: []byte("value0")},
				{Key: []byte("field1"), Value: []byte("value1")},
				{Key: []byte("field2"), Value: []byte("value2")},
			}
			db.HMset(0, tHashKey, tHashVal...)

		case SetType:
			tSetKey := []byte(key)
			db.SAdd(0, tSetKey, []byte("this"), []byte("is"), []byte("set"),
				[]byte("local"), []byte("deletion"), []byte("ttl"), []byte("checker"), []byte("test"))

		case ZSetType:
			tZsetKey := []byte(key)
			members := []common.ScorePair{
				{Member: []byte("member1"), Score: 11},
				{Member: []byte("member2"), Score: 22},
				{Member: []byte("member3"), Score: 33},
				{Member: []byte("member4"), Score: 44},
			}

			db.ZAdd(0, tZsetKey, members...)
		case BitmapType:
			tBitKey := []byte(key)
			db.BitSetV2(0, tBitKey, 0, 1)
			db.BitSetV2(0, tBitKey, 1, 1)
			db.BitSetV2(0, tBitKey, bitmapSegBits, 1)
		}

		tn := time.Now().UnixNano()
		if _, err := db.expire(tn, dataType, []byte(key), nil, 2); err != nil {
			t.Fatalf("expire key %v , type %v, err: %v", key, dataType, err)
		}
	}

	time.Sleep(3 * time.Second)

	for k, v := range kTypeMap {
		switch v {
		case KVType:
			if v, err := db.KVGet([]byte(k)); err != nil {
				t.Fatal(err)
			} else if v != nil {
				t.Errorf("key:%s of KVType do not expired", string(k))
			}
		case HashType:
			if v, err := db.HLen([]byte(k)); err != nil {
				t.Fatal(err)
			} else if v != 0 {
				t.Errorf("key:%s of HashType do not expired", string(k))
			}
		case ListType:
			if v, err := db.LLen([]byte(k)); err != nil {
				t.Fatal(err)
			} else if v != 0 {
				t.Errorf("key:%s of ListType do not expired", string(k))
			}
		case SetType:
			if v, err := db.SCard([]byte(k)); err != nil {
				t.Fatal(err)
			} else if v != 0 {
				t.Errorf("key:%s of SetType do not expired", string(k))
			}
		case ZSetType:
			if v, err := db.ZCard([]byte(k)); err != nil {
				t.Fatal(err)
			} else if v != 0 {
				t.Errorf("key:%s of ZSetType do not expired", string(k))
			}
		case BitmapType:
			if n, err := db.BitCountV2([]byte(k), 0, -1); err != nil {
				t.Fatal(err)
			} else if n != 0 {
				t.Errorf("key:%s of BitmapType do not expired", string(k))
			}
		}
	}
}

func TestDBTableCounterWithExpired(t *testing.T) {
	db := getTestDBWithCompactTTL(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()
	key := []byte("test-set:compact_ttl_tablecounter_test")
	key2 := []byte("test-set:compact_ttl_tablecounter_test2")
	ttl := 2

	tn := time.Now().UnixNano()
	db.SAdd(tn, key, []byte("hello"), []byte("world"))
	db.SAdd(tn, key2, []byte("hello"), []byte("world"))

	n, err := db.GetTableKeyCount([]byte("test-set"))
	assert.Nil(t, err)
	assert.Equal(t, int64(2), n)

	db.SExpire(tn, key, int64(ttl))
	// wait expired and renew should keep counter
	time.Sleep(time.Second * time.Duration(ttl+1))

	tn = time.Now().UnixNano()
	db.SAdd(tn, key, []byte("hello2"), []byte("world2"))

	n, err = db.GetTableKeyCount([]byte("test-set"))
	assert.Nil(t, err)
	assert.Equal(t, int64(2), n)

	key = []byte("test-list:compact_ttl_tablecounter_test")
	key2 = []byte("test-list:compact_ttl_tablecounter_test2")

	tn = time.Now().UnixNano()
	db.RPush(tn, key, []byte("hello"), []byte("world"))
	db.RPush(tn, key2, []byte("hello"), []byte("world"))

	n, err = db.GetTableKeyCount([]byte("test-list"))
	assert.Nil(t, err)
	assert.Equal(t, int64(2), n)

	n, err = db.LExpire(tn, key, int64(ttl))
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)
	// wait expired and renew should keep counter
	time.Sleep(time.Second * time.Duration(ttl+1))

	n, err = db.ListTtl(key)
	assert.Nil(t, err)
	assert.Equal(t, int64(-1), n)
	n, err = db.LLen(key)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)

	tn = time.Now().UnixNano()
	n, err = db.RPush(tn, key, []byte("hello2"), []byte("world2"))
	assert.Nil(t, err)
	assert.Equal(t, int64(2), n)

	n, err = db.GetTableKeyCount([]byte("test-list"))
	assert.Nil(t, err)
	assert.Equal(t, int64(2), n)

	key = []byte("test-hash:compact_ttl_tablecounter_test")
	key2 = []byte("test-hash:compact_ttl_tablecounter_test2")

	tn = time.Now().UnixNano()
	db.HSet(tn, false, key, []byte("hello"), []byte("world"))
	db.HSet(tn, false, key2, []byte("hello"), []byte("world"))

	n, err = db.GetTableKeyCount([]byte("test-hash"))
	assert.Nil(t, err)
	assert.Equal(t, int64(2), n)

	n, err = db.HExpire(tn, key, int64(ttl))
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)
	// wait expired and renew should keep counter
	time.Sleep(time.Second * time.Duration(ttl+1))

	n, err = db.HashTtl(key)
	assert.Nil(t, err)
	assert.Equal(t, int64(-1), n)
	n, err = db.HLen(key)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)

	tn = time.Now().UnixNano()
	n, err = db.HSet(tn, false, key, []byte("hello2"), []byte("world2"))
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)

	n, err = db.GetTableKeyCount([]byte("test-hash"))
	assert.Nil(t, err)
	assert.Equal(t, int64(2), n)

	key = []byte("test-zset:compact_ttl_tablecounter_test")
	key2 = []byte("test-zset:compact_ttl_tablecounter_test2")

	tn = time.Now().UnixNano()
	db.ZAdd(tn, key, common.ScorePair{1, []byte("hello")})
	db.ZAdd(tn, key2, common.ScorePair{2, []byte("hello")})

	n, err = db.GetTableKeyCount([]byte("test-zset"))
	assert.Nil(t, err)
	assert.Equal(t, int64(2), n)

	n, err = db.ZExpire(tn, key, int64(ttl))
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)
	// wait expired and renew should keep counter
	time.Sleep(time.Second * time.Duration(ttl+1))

	n, err = db.ZSetTtl(key)
	assert.Nil(t, err)
	assert.Equal(t, int64(-1), n)
	n, err = db.ZCard(key)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)

	tn = time.Now().UnixNano()
	n, err = db.ZAdd(tn, key, common.ScorePair{3, []byte("hello")})
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)

	n, err = db.GetTableKeyCount([]byte("test-zset"))
	assert.Nil(t, err)
	assert.Equal(t, int64(2), n)
}

func TestDBTableCounterWithKVExpired(t *testing.T) {
	db := getTestDBWithCompactTTL(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()
	key := []byte("test-kv:compact_ttl_tablecounter_test")
	key2 := []byte("test-kv:compact_ttl_tablecounter_test2")
	ttl := 2

	tn := time.Now().UnixNano()
	db.KVSet(tn, key, []byte("hello"))
	db.KVSet(tn, key2, []byte("hello"))

	n, err := db.GetTableKeyCount([]byte("test-kv"))
	assert.Nil(t, err)
	assert.Equal(t, int64(2), n)

	n, err = db.Expire(tn, key, int64(ttl))
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)
	// wait expired and renew should keep counter
	time.Sleep(time.Second * time.Duration(ttl+1))

	n, err = db.KVTtl(key)
	assert.Nil(t, err)
	assert.Equal(t, int64(-1), n)
	n, err = db.KVExists(key)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)

	tn = time.Now().UnixNano()
	err = db.KVSet(tn, key, []byte("hello2"))
	assert.Nil(t, err)

	n, err = db.GetTableKeyCount([]byte("test-kv"))
	assert.Nil(t, err)
	assert.Equal(t, int64(2), n)

	n, err = db.Expire(tn, key, int64(ttl))
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)
	time.Sleep(time.Second * time.Duration(ttl+1))
	n, err = db.KVExists(key)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)

	tn = time.Now().UnixNano()
	n, err = db.SetNX(tn, key, []byte("hello2"))
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)

	n, err = db.GetTableKeyCount([]byte("test-kv"))
	assert.Nil(t, err)
	assert.Equal(t, int64(2), n)

	n, err = db.Expire(tn, key, int64(ttl))
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)
	time.Sleep(time.Second * time.Duration(ttl+1))
	n, err = db.KVExists(key)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)

	tn = time.Now().UnixNano()
	err = db.SetEx(tn, key, int64(ttl), []byte("hello2"))
	assert.Nil(t, err)

	n, err = db.GetTableKeyCount([]byte("test-kv"))
	assert.Nil(t, err)
	assert.Equal(t, int64(2), n)

	time.Sleep(time.Second * time.Duration(ttl+1))
	n, err = db.KVExists(key)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)

	tn = time.Now().UnixNano()
	n, err = db.SetRange(tn, key, 10, []byte("hello2"))
	assert.Nil(t, err)
	assert.Equal(t, int64(16), n)

	n, err = db.GetTableKeyCount([]byte("test-kv"))
	assert.Nil(t, err)
	assert.Equal(t, int64(2), n)
}
