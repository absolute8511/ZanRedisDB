package rockredis

import (
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/stretchr/testify/assert"
)

func getTestDBNoTableCounter(t *testing.T) *RockDB {
	cfg := NewRockConfig()
	cfg.EnableTableCounter = false
	var err error
	cfg.DataDir, err = ioutil.TempDir("", fmt.Sprintf("rockredis-test-%d", time.Now().UnixNano()))
	assert.Nil(t, err)
	testDB, err := OpenRockDB(cfg)
	assert.Nil(t, err)
	return testDB
}

func getTestDB(t *testing.T) *RockDB {
	cfg := NewRockConfig()
	cfg.EnableTableCounter = true
	var err error
	cfg.DataDir, err = ioutil.TempDir("", fmt.Sprintf("rockredis-test-%d", time.Now().UnixNano()))
	assert.Nil(t, err)
	testDB, err := OpenRockDB(cfg)
	assert.Nil(t, err)
	if testing.Verbose() {
		SetLogLevel(int32(4))
	}
	return testDB
}

func TestDB(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()
}

func TestRockDB(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()
	key := []byte("test:test_kv_key")
	value := []byte("value")
	err := db.KVSet(0, key, value)
	assert.Nil(t, err)

	v, err := db.KVGet(key)
	assert.Nil(t, err)
	assert.Equal(t, string(value), string(v))

	key = []byte("test:test_list_key")
	_, err = db.LPush(key, value)
	assert.Nil(t, err)

	_, err = db.LRange(key, 0, 100)
	assert.Nil(t, err)

	v, err = db.LPop(key)
	assert.Nil(t, err)
	assert.Equal(t, string(value), string(v))

	key = []byte("test:test_hash_key")
	_, err = db.HSet(0, false, key, []byte("a"), value)
	assert.Nil(t, err)

	v, err = db.HGet(key, []byte("a"))
	assert.Nil(t, err)
	assert.Equal(t, string(value), string(v))

	key = []byte("test:test_set_key")
	_, err = db.SAdd(key, []byte("a"), []byte("b"))
	assert.Nil(t, err)

	n, err := db.SIsMember(key, []byte("a"))
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)

	key = []byte("test:test_zset_key")
	_, err = db.ZAdd(key, common.ScorePair{Score: 1, Member: []byte("a")},
		common.ScorePair{Score: 2, Member: []byte("b")})
	assert.Nil(t, err)

	vlist, err := db.ZRangeByScore(key, 0, 100, 0, -1)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(vlist))
}

func TestRockDBScanTableForHash(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	keyList1 := make([][]byte, 0)
	keyList2 := make([][]byte, 0)
	for i := 0; i < 5; i++ {
		keyList1 = append(keyList1, []byte("test:test_hash_scan_key"+strconv.Itoa(i)))
		keyList2 = append(keyList2, []byte("test2:test2_hash_scan_key"+strconv.Itoa(i)))
	}
	for i := 0; i < 5; i++ {
		keyList1 = append(keyList1, []byte("test:test_hash_scan_key_longlonglonglonglonglong"+strconv.Itoa(i)))
		keyList2 = append(keyList2, []byte("test2:test2_hash_scan_key_longlonglonglonglonglong"+strconv.Itoa(i)))
	}
	for _, key := range keyList1 {
		_, err := db.HSet(0, false, key, []byte("a"), key)
		assert.Nil(t, err)
		_, err = db.HSet(0, false, key, []byte("b"), key)
		assert.Nil(t, err)
	}
	for _, key := range keyList2 {
		_, err := db.HSet(0, false, key, []byte("a"), key)
		assert.Nil(t, err)
		_, err = db.HSet(0, false, key, []byte("b"), key)
		assert.Nil(t, err)
	}

	minKey := encodeDataTableStart(HashType, []byte("test"))
	maxKey := encodeDataTableEnd(HashType, []byte("test"))
	it, err := db.buildScanIterator(minKey, maxKey)
	assert.Nil(t, err)
	it.NoTimestamp(HashType)
	func() {
		defer it.Close()
		cnt := 0
		for ; it.Valid(); it.Next() {
			table, k, f, err := hDecodeHashKey(it.Key())
			assert.Nil(t, err)
			assert.Equal(t, "test", string(table))
			if string(f) != "a" && string(f) != "b" {
				t.Fatal("scan field mismatch: " + string(f))
			}
			assert.Equal(t, string(table)+":"+string(k), string(it.Value()))
			cnt++
		}
		assert.Equal(t, len(keyList1)*2, cnt)
	}()

	minKey = encodeDataTableStart(HashType, []byte("test2"))
	maxKey = encodeDataTableEnd(HashType, []byte("test2"))
	it, err = db.buildScanIterator(minKey, maxKey)
	assert.Nil(t, err)
	it.NoTimestamp(HashType)
	func() {
		defer it.Close()
		cnt := 0
		for ; it.Valid(); it.Next() {
			table, k, f, err := hDecodeHashKey(it.Key())
			assert.Nil(t, err)
			assert.Equal(t, "test2", string(table))
			if string(f) != "a" && string(f) != "b" {
				t.Fatal("scan field mismatch: " + string(f))
			}
			assert.Equal(t, string(table)+":"+string(k), string(it.Value()))
			cnt++
		}
		assert.Equal(t, len(keyList2)*2, cnt)
	}()
}

func TestRockDBScanTableForList(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	keyList1 := make([][]byte, 0)
	keyList2 := make([][]byte, 0)
	for i := 0; i < 5; i++ {
		keyList1 = append(keyList1, []byte("test:test_list_scan_key"+strconv.Itoa(i)))
		keyList2 = append(keyList2, []byte("test2:test2_list_scan_key"+strconv.Itoa(i)))
	}
	for i := 0; i < 5; i++ {
		keyList1 = append(keyList1, []byte("test:test_list_scan_key_longlonglonglonglonglong"+strconv.Itoa(i)))
		keyList2 = append(keyList2, []byte("test2:test2_list_scan_key_longlonglonglonglonglong"+strconv.Itoa(i)))
	}
	for _, key := range keyList1 {
		_, err := db.LPush(key, key, key)
		assert.Nil(t, err)
	}
	for _, key := range keyList2 {
		_, err := db.LPush(key, key, key)
		assert.Nil(t, err)
	}

	minKey := encodeDataTableStart(ListType, []byte("test"))
	maxKey := encodeDataTableEnd(ListType, []byte("test"))
	it, err := db.buildScanIterator(minKey, maxKey)
	assert.Nil(t, err)
	func() {
		defer it.Close()
		cnt := 0
		for ; it.Valid(); it.Next() {
			tb, k, _, err := lDecodeListKey(it.Key())
			assert.Nil(t, err)
			assert.Equal(t, "test", string(tb))
			if !strings.HasPrefix(string(k), "test") {
				t.Fatal("should has table prefix for key")
			}
			assert.Equal(t, "test:"+string(k), string(it.Value()))
			cnt++
		}
		assert.Equal(t, len(keyList1)*2, cnt)
	}()

	minKey = encodeDataTableStart(ListType, []byte("test2"))
	maxKey = encodeDataTableEnd(ListType, []byte("test2"))
	it, err = db.buildScanIterator(minKey, maxKey)
	assert.Nil(t, err)
	func() {
		defer it.Close()
		cnt := 0
		for ; it.Valid(); it.Next() {
			tb, k, _, err := lDecodeListKey(it.Key())
			assert.Nil(t, err)
			assert.Equal(t, "test2", string(tb))

			if !strings.HasPrefix(string(k), "test2") {
				t.Fatal("should has table prefix for key")
			}
			assert.Equal(t, "test2:"+string(k), string(it.Value()))
			cnt++
		}
		assert.Equal(t, len(keyList2)*2, cnt)
	}()
}

func TestRockDBScanTableForSet(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	keyList1 := make([][]byte, 0)
	keyList2 := make([][]byte, 0)
	for i := 0; i < 5; i++ {
		keyList1 = append(keyList1, []byte("test:test_set_scan_key"+strconv.Itoa(i)))
		keyList2 = append(keyList2, []byte("test2:test2_set_scan_key"+strconv.Itoa(i)))
	}
	for i := 0; i < 5; i++ {
		keyList1 = append(keyList1, []byte("test:test_set_scan_key_longlonglonglonglonglong"+strconv.Itoa(i)))
		keyList2 = append(keyList2, []byte("test2:test2_set_scan_key_longlonglonglonglonglong"+strconv.Itoa(i)))
	}
	for _, key := range keyList1 {
		_, err := db.SAdd(key, []byte("test:a"), []byte("test:b"))
		assert.Nil(t, err)
	}
	for _, key := range keyList2 {
		_, err := db.SAdd(key, []byte("test2:a"), []byte("test2:b"))
		assert.Nil(t, err)
	}

	minKey := encodeDataTableStart(SetType, []byte("test"))
	maxKey := encodeDataTableEnd(SetType, []byte("test"))
	it, err := db.buildScanIterator(minKey, maxKey)
	assert.Nil(t, err)
	func() {
		defer it.Close()
		cnt := 0
		for ; it.Valid(); it.Next() {
			tb, k, m, err := sDecodeSetKey(it.Key())
			assert.Nil(t, err)
			assert.Equal(t, "test", string(tb))
			if !strings.HasPrefix(string(k), "test") {
				t.Fatal("should has table prefix for key")
			}
			if string(m) != "test:a" && string(m) != "test:b" {
				t.Fatal("scan set member mismatch: " + string(m))
			}
			cnt++
		}
		assert.Equal(t, len(keyList1)*2, cnt)
	}()

	minKey = encodeDataTableStart(SetType, []byte("test2"))
	maxKey = encodeDataTableEnd(SetType, []byte("test2"))
	it, err = db.buildScanIterator(minKey, maxKey)
	assert.Nil(t, err)
	func() {
		defer it.Close()
		cnt := 0
		for ; it.Valid(); it.Next() {
			tb, k, m, err := sDecodeSetKey(it.Key())
			assert.Nil(t, err)
			assert.Equal(t, "test2", string(tb))
			if !strings.HasPrefix(string(k), "test2") {
				t.Fatal("should has table prefix for key")
			}
			if string(m) != "test2:a" && string(m) != "test2:b" {
				t.Fatal("scan member mismatch: " + string(m))
			}
			cnt++
		}
		assert.Equal(t, len(keyList2)*2, cnt)
	}()
}

func TestRockDBScanTableForZSet(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	keyList1 := make([][]byte, 0)
	keyList2 := make([][]byte, 0)
	for i := 0; i < 5; i++ {
		keyList1 = append(keyList1, []byte("test:test_zset_scan_key"+strconv.Itoa(i)))
		keyList2 = append(keyList2, []byte("test2:test2_zset_scan_key"+strconv.Itoa(i)))
	}
	for i := 0; i < 5; i++ {
		keyList1 = append(keyList1, []byte("test:test_zset_scan_key_longlonglonglonglonglong"+strconv.Itoa(i)))
		keyList2 = append(keyList2, []byte("test2:test2_zset_scan_key_longlonglonglonglonglong"+strconv.Itoa(i)))
	}
	for _, key := range keyList1 {
		_, err := db.ZAdd(key, common.ScorePair{1, []byte("test:a")},
			common.ScorePair{2, []byte("test:b")})
		assert.Nil(t, err)
	}
	for _, key := range keyList2 {
		_, err := db.ZAdd(key, common.ScorePair{1, []byte("test2:a")},
			common.ScorePair{2, []byte("test2:b")})
		assert.Nil(t, err)
	}

	minKey := encodeDataTableStart(ZScoreType, []byte("test"))
	maxKey := encodeDataTableEnd(ZScoreType, []byte("test"))
	it, err := db.buildScanIterator(minKey, maxKey)
	assert.Nil(t, err)
	func() {
		defer it.Close()
		cnt := 0
		for ; it.Valid(); it.Next() {
			tb, k, m, s, err := zDecodeScoreKey(it.Key())
			assert.Nil(t, err)
			assert.Equal(t, "test", string(tb))
			if !strings.HasPrefix(string(k), "test") {
				t.Fatal("key should has table prefix " + string(k))
			}
			if string(m) == "test:a" {
				assert.Equal(t, float64(1), s)
			} else if string(m) == "test:b" {
				assert.Equal(t, float64(2), s)
			} else {
				t.Fatal("scan field mismatch: " + string(m))
			}
			cnt++
		}
		assert.Equal(t, len(keyList1)*2, cnt)
	}()

	minKey = encodeDataTableStart(ZScoreType, []byte("test2"))
	maxKey = encodeDataTableEnd(ZScoreType, []byte("test2"))
	it, err = db.buildScanIterator(minKey, maxKey)
	assert.Nil(t, err)
	func() {
		defer it.Close()
		cnt := 0
		for ; it.Valid(); it.Next() {
			tb, k, m, s, err := zDecodeScoreKey(it.Key())
			assert.Nil(t, err)
			assert.Equal(t, "test2", string(tb))
			if !strings.HasPrefix(string(k), "test2") {
				t.Fatal("key should has table prefix " + string(k))
			}
			if string(m) == "test2:a" {
				assert.Equal(t, float64(1), s)
			} else if string(m) == "test2:b" {
				assert.Equal(t, float64(2), s)
			} else {
				t.Fatal("scan field mismatch: " + string(m))
			}
			cnt++
		}
		assert.Equal(t, len(keyList2)*2, cnt)
	}()
}
