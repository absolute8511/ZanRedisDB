package rockredis

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/youzan/ZanRedisDB/common"
	"github.com/youzan/ZanRedisDB/engine"
	"github.com/youzan/ZanRedisDB/slow"
)

const (
	testEngineType = "rocksdb"
)

type testLogger struct {
	t *testing.T
}

func newTestLogger(t *testing.T) *testLogger {
	return &testLogger{t: t}
}

func (l *testLogger) Output(maxdepth int, s string) error {
	l.t.Logf("%v:%v", time.Now().UnixNano(), s)
	return nil
}

func (l *testLogger) OutputErr(maxdepth int, s string) error {
	l.t.Logf("%v:%v", time.Now().UnixNano(), s)
	return nil
}

func (l *testLogger) OutputWarning(maxdepth int, s string) error {
	l.t.Logf("%v:%v", time.Now().UnixNano(), s)
	return nil
}

func getTestDBNoTableCounter(t *testing.T) *RockDB {
	cfg := NewRockRedisDBConfig()
	cfg.EnableTableCounter = false
	cfg.EnablePartitionedIndexFilter = true
	cfg.EngineType = testEngineType
	var err error
	cfg.DataDir, err = ioutil.TempDir("", fmt.Sprintf("rockredis-test-%d", time.Now().UnixNano()))
	assert.Nil(t, err)
	testDB, err := OpenRockDB(cfg)
	assert.Nil(t, err)
	return testDB
}

func getTestDBForBench() *RockDB {
	cfg := NewRockRedisDBConfig()
	cfg.EnableTableCounter = true
	cfg.EnablePartitionedIndexFilter = true
	cfg.EngineType = testEngineType
	var err error
	cfg.DataDir, err = ioutil.TempDir("", fmt.Sprintf("rockredis-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	testDB, err := OpenRockDB(cfg)
	if err != nil {
		panic(err)
	}
	return testDB
}

func getTestDBWithDirType(t *testing.T, dataDir string, tp string) *RockDB {
	cfg := NewRockRedisDBConfig()
	cfg.EnableTableCounter = true
	cfg.WriteBufferSize = 1000
	cfg.MaxWriteBufferNumber = 1
	cfg.EnablePartitionedIndexFilter = true
	cfg.EngineType = tp
	cfg.DataDir = dataDir
	testDB, err := OpenRockDB(cfg)
	assert.Nil(t, err)
	if testing.Verbose() {
		SetLogLevel(int32(3))
	}
	return testDB
}

func getTestDBWithDir(t *testing.T, dataDir string) *RockDB {
	return getTestDBWithDirType(t, dataDir, testEngineType)
}

func getTestDB(t *testing.T) *RockDB {
	dataDir, err := ioutil.TempDir("", fmt.Sprintf("rockredis-test-%d", time.Now().UnixNano()))
	assert.Nil(t, err)
	return getTestDBWithDirType(t, dataDir, testEngineType)
}

func TestMain(m *testing.M) {
	SetLogger(int32(common.LOG_INFO), nil)
	engine.SetLogger(int32(common.LOG_INFO), nil)
	slow.SetLogger(int32(common.LOG_INFO), nil)
	flag.Parse()
	if testing.Verbose() {
		SetLogger(int32(common.LOG_DEBUG), common.NewLogger())
		SetLogLevel(int32(common.LOG_DETAIL))
		engine.SetLogLevel(int32(common.LOG_DETAIL))
	}
	lazyCleanExpired = time.Second * 3
	timeUpdateFreq = 1
	ret := m.Run()
	os.Exit(ret)
}

func TestDB(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()
}

func TestDBCompact(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	key := []byte("test:test_kv_key")
	value := []byte("value")
	err := db.KVSet(0, key, value)
	assert.Nil(t, err)
	for i := 0; i < 100; i++ {
		err := db.KVSet(0, []byte(string(key)+strconv.Itoa(i)), value)
		assert.Nil(t, err)
	}

	v, err := db.KVGet(key)
	assert.Nil(t, err)
	assert.Equal(t, string(value), string(v))

	db.CompactAllRange()

	for i := 0; i < 50; i++ {
		db.DelKeys([]byte(string(key) + strconv.Itoa(i)))
	}

	db.CompactAllRange()

	v, err = db.KVGet(key)
	assert.Nil(t, err)
	assert.Equal(t, string(value), string(v))
	err = db.SetMaxBackgroundOptions(10, 0)
	assert.Nil(t, err)
	err = db.SetMaxBackgroundOptions(0, 10)
	assert.Nil(t, err)
	err = db.SetMaxBackgroundOptions(10, 10)
	assert.Nil(t, err)
}

func TestIsSameSST(t *testing.T) {
	d1, err := ioutil.TempDir("", fmt.Sprintf("rockredis-test-%d", time.Now().UnixNano()))
	assert.Nil(t, err)
	defer os.RemoveAll(d1)
	// small file, large file
	f1small := path.Join(d1, "f1small")
	f2small := path.Join(d1, "f2small")
	f3small := path.Join(d1, "f3small")
	err = ioutil.WriteFile(f1small, []byte("aaa"), 0655)
	assert.Nil(t, err)
	err = ioutil.WriteFile(f2small, []byte("aaa"), 0655)
	err = ioutil.WriteFile(f3small, []byte("aab"), 0655)
	assert.Nil(t, isSameSSTFile(f1small, f2small))
	assert.NotNil(t, isSameSSTFile(f2small, f3small))
	assert.NotNil(t, isSameSSTFile(f1small, f3small))
	fileData := make([]byte, 1024*256*2)
	for i := 0; i < len(fileData); i++ {
		fileData[i] = 'a'
	}

	f1large := path.Join(d1, "f1large")
	f2large := path.Join(d1, "f2large")
	f3large := path.Join(d1, "f3large")
	f4large := path.Join(d1, "f4large")
	err = ioutil.WriteFile(f1large, fileData, 0655)
	assert.Nil(t, err)
	err = ioutil.WriteFile(f2large, fileData, 0655)
	assert.Nil(t, err)
	fileData[0] = 'b'
	err = ioutil.WriteFile(f3large, fileData, 0655)
	assert.Nil(t, err)
	fileData[1024*256+1] = 'b'
	err = ioutil.WriteFile(f4large, fileData, 0655)
	assert.Nil(t, err)
	assert.Nil(t, isSameSSTFile(f1large, f2large))
	assert.Nil(t, isSameSSTFile(f1large, f3large))
	assert.NotNil(t, isSameSSTFile(f1large, f4large))
	assert.Nil(t, isSameSSTFile(f2large, f3large))
	assert.NotNil(t, isSameSSTFile(f2large, f4large))
	assert.NotNil(t, isSameSSTFile(f3large, f4large))
	assert.NotNil(t, isSameSSTFile(f1small, f1large))
	assert.NotNil(t, isSameSSTFile(f3small, f3large))
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
	_, err = db.LPush(0, key, value)
	assert.Nil(t, err)

	_, err = db.LRange(key, 0, 100)
	assert.Nil(t, err)

	v, err = db.LPop(0, key)
	assert.Nil(t, err)
	assert.Equal(t, string(value), string(v))

	key = []byte("test:test_hash_key")
	_, err = db.HSet(0, false, key, []byte("a"), value)
	assert.Nil(t, err)

	v, err = db.HGet(key, []byte("a"))
	assert.Nil(t, err)
	assert.Equal(t, string(value), string(v))

	key = []byte("test:test_set_key")
	_, err = db.SAdd(0, key, []byte("a"), []byte("b"))
	assert.Nil(t, err)

	n, err := db.SIsMember(key, []byte("a"))
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)

	key = []byte("test:test_zset_key")
	_, err = db.ZAdd(0, key, common.ScorePair{Score: 1, Member: []byte("a")},
		common.ScorePair{Score: 2, Member: []byte("b")})
	assert.Nil(t, err)

	vlist, err := db.ZRangeByScore(key, 0, 100, 0, -1)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(vlist))
}

func TestRockDBRevScanTableForHash(t *testing.T) {
	testRockDBScanTableForHash(t, true)
}

func TestRockDBScanTableForHash(t *testing.T) {
	testRockDBScanTableForHash(t, false)
}

func testRockDBScanTableForHash(t *testing.T, reverse bool) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	total := 500
	keyList1, keyList2 := fillScanKeysForType(t, "hash", total, func(key []byte, prefix string) {
		_, err := db.HSet(0, false, key, []byte(prefix+":a"), key)
		assert.Nil(t, err)
		_, err = db.HSet(0, false, key, []byte(prefix+":b"), key)
		assert.Nil(t, err)
	})

	minKey := encodeDataTableStart(HashType, []byte("test"))
	maxKey := encodeDataTableEnd(HashType, []byte("test"))
	it, err := db.buildScanIterator(minKey, maxKey, reverse)
	assert.Nil(t, err)
	it.NoTimestamp(HashType)
	func() {
		defer it.Close()
		cnt := 0
		for ; it.Valid(); it.Next() {
			table, k, f, err := hDecodeHashKey(it.Key())
			assert.Nil(t, err)
			assert.Equal(t, "test", string(table))
			if string(f) != "test:a" && string(f) != "test:b" {
				t.Fatal("scan field mismatch: " + string(f))
			}
			assert.Equal(t, string(table)+":"+string(k), string(it.Value()))
			cnt++
		}
		assert.Equal(t, len(keyList1)*2, cnt)
	}()

	minKey = encodeDataTableStart(HashType, []byte("test2"))
	maxKey = encodeDataTableEnd(HashType, []byte("test2"))
	it, err = db.buildScanIterator(minKey, maxKey, reverse)
	assert.Nil(t, err)
	it.NoTimestamp(HashType)
	func() {
		defer it.Close()
		cnt := 0
		for ; it.Valid(); it.Next() {
			table, k, f, err := hDecodeHashKey(it.Key())
			assert.Nil(t, err)
			assert.Equal(t, "test2", string(table))
			if string(f) != "test2:a" && string(f) != "test2:b" {
				t.Fatal("scan field mismatch: " + string(f))
			}
			assert.Equal(t, string(table)+":"+string(k), string(it.Value()))
			cnt++
		}
		assert.Equal(t, len(keyList2)*2, cnt)
	}()

	keyNum := db.GetTableApproximateNumInRange("test", nil, nil)
	diskUsage := db.GetTableSizeInRange("test", nil, nil)
	t.Logf("test key number: %v, usage: %v", keyNum, diskUsage)
	keyNum = db.GetTableApproximateNumInRange("test2", nil, nil)
	diskUsage = db.GetTableSizeInRange("test2", nil, nil)
	t.Logf("test2 key number: %v, usage: %v", keyNum, diskUsage)

	err = db.DeleteTableRange(false, "test", nil, nil)
	assert.Nil(t, err)

	minKey = encodeDataTableStart(HashType, []byte("test"))
	maxKey = encodeDataTableEnd(HashType, []byte("test"))
	it, err = db.buildScanIterator(minKey, maxKey, reverse)
	assert.Nil(t, err)
	it.NoTimestamp(HashType)
	func() {
		defer it.Close()
		cnt := 0
		for ; it.Valid(); it.Next() {
			table, k, f, err := hDecodeHashKey(it.Key())
			assert.Nil(t, err)
			assert.Equal(t, "test", string(table))
			if string(f) != "test:a" && string(f) != "test:b" {
				t.Fatal("scan field mismatch: " + string(f))
			}
			assert.Equal(t, string(table)+":"+string(k), string(it.Value()))
			cnt++
		}
		assert.Equal(t, 0, cnt)
	}()

	minKey = encodeDataTableStart(HashType, []byte("test2"))
	maxKey = encodeDataTableEnd(HashType, []byte("test2"))
	it, err = db.buildScanIterator(minKey, maxKey, reverse)
	assert.Nil(t, err)
	it.NoTimestamp(HashType)
	func() {
		defer it.Close()
		cnt := 0
		for ; it.Valid(); it.Next() {
			table, k, f, err := hDecodeHashKey(it.Key())
			assert.Nil(t, err)
			assert.Equal(t, "test2", string(table))
			if string(f) != "test2:a" && string(f) != "test2:b" {
				t.Fatal("scan field mismatch: " + string(f))
			}
			assert.Equal(t, string(table)+":"+string(k), string(it.Value()))
			cnt++
		}
		assert.Equal(t, len(keyList2)*2, cnt)
	}()

	keyNum = db.GetTableApproximateNumInRange("test", nil, nil)
	diskUsage = db.GetTableSizeInRange("test", nil, nil)
	t.Logf("test key number: %v, usage: %v", keyNum, diskUsage)
	keyNum = db.GetTableApproximateNumInRange("test2", nil, nil)
	diskUsage = db.GetTableSizeInRange("test2", nil, nil)
	t.Logf("test2 key number: %v, usage: %v", keyNum, diskUsage)
}

func TestRockDBRevScanTableForList(t *testing.T) {
	testRockDBScanTableForList(t, true)
}

func TestRockDBScanTableForList(t *testing.T) {
	testRockDBScanTableForList(t, false)
}

func testRockDBScanTableForList(t *testing.T, reverse bool) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	totalCnt := 50
	keyList1, keyList2 := fillScanKeysForType(t, "list", totalCnt, func(key []byte, prefix string) {
		_, err := db.LPush(0, key, key, key)
		assert.Nil(t, err)
	})

	minKey := encodeDataTableStart(ListType, []byte("test"))
	maxKey := encodeDataTableEnd(ListType, []byte("test"))
	it, err := db.buildScanIterator(minKey, maxKey, reverse)
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
	it, err = db.buildScanIterator(minKey, maxKey, reverse)
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

	keyNum := db.GetTableApproximateNumInRange("test", nil, nil)
	diskUsage := db.GetTableSizeInRange("test", nil, nil)
	t.Logf("test key number: %v, usage: %v", keyNum, diskUsage)
	keyNum = db.GetTableApproximateNumInRange("test2", nil, nil)
	diskUsage = db.GetTableSizeInRange("test2", nil, nil)
	t.Logf("test2 key number: %v, usage: %v", keyNum, diskUsage)

	err = db.DeleteTableRange(false, "test", nil, nil)
	assert.Nil(t, err)

	minKey = encodeDataTableStart(ListType, []byte("test"))
	maxKey = encodeDataTableEnd(ListType, []byte("test"))
	it, err = db.buildScanIterator(minKey, maxKey, reverse)
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
		assert.Equal(t, 0, cnt)
	}()

	minKey = encodeDataTableStart(ListType, []byte("test2"))
	maxKey = encodeDataTableEnd(ListType, []byte("test2"))
	it, err = db.buildScanIterator(minKey, maxKey, reverse)
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

func TestRockDBRevScanTableForSet(t *testing.T) {
	testRockDBScanTableForSet(t, true)
}

func TestRockDBScanTableForSet(t *testing.T) {
	testRockDBScanTableForSet(t, false)
}

func testRockDBScanTableForSet(t *testing.T, reverse bool) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	totalCnt := 50
	keyList1, keyList2 := fillScanKeysForType(t, "set", totalCnt, func(key []byte, prefix string) {
		_, err := db.SAdd(0, key, []byte(prefix+":a"), []byte(prefix+":b"))
		assert.Nil(t, err)
	})

	minKey := encodeDataTableStart(SetType, []byte("test"))
	maxKey := encodeDataTableEnd(SetType, []byte("test"))
	it, err := db.buildScanIterator(minKey, maxKey, reverse)
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
	it, err = db.buildScanIterator(minKey, maxKey, reverse)
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

	keyNum := db.GetTableApproximateNumInRange("test", nil, nil)
	diskUsage := db.GetTableSizeInRange("test", nil, nil)
	t.Logf("test key number: %v, usage: %v", keyNum, diskUsage)
	keyNum = db.GetTableApproximateNumInRange("test2", nil, nil)
	diskUsage = db.GetTableSizeInRange("test2", nil, nil)
	t.Logf("test2 key number: %v, usage: %v", keyNum, diskUsage)

	err = db.DeleteTableRange(false, "test", nil, nil)
	assert.Nil(t, err)

	minKey = encodeDataTableStart(SetType, []byte("test"))
	maxKey = encodeDataTableEnd(SetType, []byte("test"))
	it, err = db.buildScanIterator(minKey, maxKey, reverse)
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
		assert.Equal(t, 0, cnt)
	}()

	minKey = encodeDataTableStart(SetType, []byte("test2"))
	maxKey = encodeDataTableEnd(SetType, []byte("test2"))
	it, err = db.buildScanIterator(minKey, maxKey, reverse)
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

	keyNum = db.GetTableApproximateNumInRange("test", nil, nil)
	diskUsage = db.GetTableSizeInRange("test", nil, nil)
	t.Logf("test key number: %v, usage: %v", keyNum, diskUsage)
	keyNum = db.GetTableApproximateNumInRange("test2", nil, nil)
	diskUsage = db.GetTableSizeInRange("test2", nil, nil)
	t.Logf("test2 key number: %v, usage: %v", keyNum, diskUsage)
}

func TestRockDBRevScanTableForZSet(t *testing.T) {
	testRockDBScanTableForZSet(t, true)
}

func TestRockDBScanTableForZSet(t *testing.T) {
	testRockDBScanTableForZSet(t, false)
}

func testRockDBScanTableForZSet(t *testing.T, reverse bool) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	totalCnt := 50

	keyList1, keyList2 := fillScanKeysForType(t, "zset", totalCnt, func(key []byte, prefix string) {
		_, err := db.ZAdd(0, key, common.ScorePair{1, []byte(prefix + ":a")},
			common.ScorePair{2, []byte(prefix + ":b")})
		assert.Nil(t, err)
	})

	minKey := encodeDataTableStart(ZScoreType, []byte("test"))
	maxKey := encodeDataTableEnd(ZScoreType, []byte("test"))
	t.Logf("scan test : %v, %v", minKey, maxKey)
	it, err := db.buildScanIterator(minKey, maxKey, reverse)
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
	it, err = db.buildScanIterator(minKey, maxKey, reverse)
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

	keyNum := db.GetTableApproximateNumInRange("test", nil, nil)
	diskUsage := db.GetTableSizeInRange("test", nil, nil)
	t.Logf("test key number: %v, usage: %v", keyNum, diskUsage)
	keyNum = db.GetTableApproximateNumInRange("test2", nil, nil)
	diskUsage = db.GetTableSizeInRange("test2", nil, nil)
	t.Logf("test2 key number: %v, usage: %v", keyNum, diskUsage)

	err = db.DeleteTableRange(false, "test", nil, nil)
	assert.Nil(t, err)

	minKey = encodeDataTableStart(ZScoreType, []byte("test"))
	maxKey = encodeDataTableEnd(ZScoreType, []byte("test"))
	it, err = db.buildScanIterator(minKey, maxKey, reverse)
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
		assert.Equal(t, 0, cnt)
	}()

	minKey = encodeDataTableStart(ZScoreType, []byte("test2"))
	maxKey = encodeDataTableEnd(ZScoreType, []byte("test2"))
	it, err = db.buildScanIterator(minKey, maxKey, reverse)
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
	keyNum = db.GetTableApproximateNumInRange("test", nil, nil)
	diskUsage = db.GetTableSizeInRange("test", nil, nil)
	t.Logf("test key number: %v, usage: %v", keyNum, diskUsage)
	keyNum = db.GetTableApproximateNumInRange("test2", nil, nil)
	diskUsage = db.GetTableSizeInRange("test2", nil, nil)
	t.Logf("test2 key number: %v, usage: %v", keyNum, diskUsage)
}

func Test_purgeOldCheckpoint(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("sm-test-%d", time.Now().UnixNano()))
	assert.Nil(t, err)
	defer os.RemoveAll(tmpDir)
	t.Logf("dir:%v\n", tmpDir)
	term := uint64(0x011a)
	index := uint64(0x0c000)
	cntIdx := 25

	type args struct {
		keepNum         int
		checkpointDir   string
		latestSnapIndex uint64
	}
	tests := []struct {
		name string
		args args
	}{
		{"keep0_1", args{0, "keep0_1dir", index + 1}},
		{"keep0_2", args{0, "keep0_2dir", index + 2}},
		{"keep0_10", args{0, "keep0_10dir", index + 10}},
		{"keep0_max", args{0, "keep0_maxdir", index + uint64(cntIdx)}},
		{"keep1_1", args{1, "keep1_1dir", index + 1}},
		{"keep1_2", args{1, "keep1_2dir", index + 2}},
		{"keep1_10", args{1, "keep1_10dir", index + 10}},
		{"keep1_max", args{1, "keep1_maxdir", index + uint64(cntIdx)}},
		{"keep10_1", args{10, "keep10_1dir", index + 1}},
		{"keep10_2", args{10, "keep10_2dir", index + 2}},
		{"keep10_10", args{10, "keep10_10dir", index + 10}},
		{"keep10_max", args{10, "keep10_maxdir", index + uint64(cntIdx)}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			checkDir := path.Join(tmpDir, tt.args.checkpointDir)
			fns := make([]string, 0, cntIdx)
			fnIndexes := make([]uint64, 0, cntIdx)
			for j := 0; j < cntIdx; j++ {
				idx := index + uint64(j)
				p := path.Join(checkDir, fmt.Sprintf("%04x-%05x", term, idx))
				err := os.MkdirAll(p, 0755)
				assert.Nil(t, err)
				fns = append(fns, p)
				fnIndexes = append(fnIndexes, idx)
			}
			purgeOldCheckpoint(tt.args.keepNum, checkDir, tt.args.latestSnapIndex)
			for i, fn := range fns {
				_, err := os.Stat(fn)
				t.Logf("checking file: %v, %v", fn, err)
				if int64(fnIndexes[i]) >= int64(tt.args.latestSnapIndex)-int64(tt.args.keepNum) {
					assert.Nil(t, err)
					continue
				}
				assert.True(t, os.IsNotExist(err), "should not keep")
			}
		})
	}
}

func TestRockDBRecovery(t *testing.T) {
	// test restore should overwrite new local data and have the restored data
	dataDir, err := ioutil.TempDir("", fmt.Sprintf("rockredis-test-%d", time.Now().UnixNano()))
	assert.Nil(t, err)
	db := getTestDBWithDirType(t, dataDir, "rocksdb")
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	t.Log(db.cfg.DataDir)
	key := []byte("test:test_kv_recovery")
	value := []byte("value")
	err = db.KVSet(0, key, value)
	assert.Nil(t, err)
	wcnt := 50000
	for i := 0; i < wcnt; i++ {
		expectedV := []byte(string(key) + strconv.Itoa(i))
		err := db.KVSet(0, expectedV, expectedV)
		assert.Nil(t, err)
	}
	db.CompactAllRange()

	v, err := db.KVGet(key)
	assert.Nil(t, err)
	assert.Equal(t, string(value), string(v))
	bi := db.Backup(1, 1)
	_, err = bi.GetResult()
	assert.Nil(t, err)
	checkpointDir := GetCheckpointDir(1, 1)
	fullBackupPath := path.Join(db.GetBackupDir(), checkpointDir)
	// copy file to make the backup unlink from local to test recover
	backupfiles, _ := filepath.Glob(path.Join(fullBackupPath, "*"))
	var backupSSTfiles []string
	for _, f := range backupfiles {
		fi, _ := os.Stat(f)
		ofi, _ := os.Stat(path.Join(db.GetDataDir(), path.Base(f)))
		if strings.HasSuffix(f, ".sst") {
			assert.True(t, os.SameFile(fi, ofi))
			dst := f + ".tmp"
			common.CopyFile(f, dst, true)
			os.Remove(f)
			os.Rename(dst, f)
			fi, _ = os.Stat(f)
			assert.False(t, os.SameFile(fi, ofi))
			backupSSTfiles = append(backupSSTfiles, f)
		} else {
			assert.False(t, os.SameFile(fi, ofi))
			assert.Nil(t, isSameSSTFile(f, path.Join(db.GetDataDir(), path.Base(f))))
		}
	}

	ok, err := db.IsLocalBackupOK(1, 1)
	assert.Nil(t, err)
	assert.True(t, ok)

	// write new data to local to test overwrite for restore
	err = db.KVSet(0, key, []byte("changed"))
	assert.Nil(t, err)

	v, err = db.KVGet(key)
	assert.Nil(t, err)
	assert.Equal(t, "changed", string(v))

	err = db.restoreFromPath(db.GetBackupDir(), 1, 1)
	assert.Nil(t, err)
	localfiles, _ := filepath.Glob(path.Join(db.GetDataDir(), "*.sst"))
	assert.Equal(t, len(localfiles), len(backupSSTfiles))
	assert.True(t, len(localfiles) >= 1, localfiles)
	for _, f := range backupfiles {
		if strings.HasPrefix(f, "LOG") {
			continue
		}
		fi, err := os.Stat(f)
		assert.Nil(t, err)
		lfi, err := os.Stat(path.Join(db.GetDataDir(), path.Base(f)))
		assert.Nil(t, err)
		if strings.HasSuffix(f, "sst") {
			assert.True(t, os.SameFile(fi, lfi))
		} else {
			assert.False(t, os.SameFile(fi, lfi))
			assert.Nil(t, isSameSSTFile(f, path.Join(db.GetDataDir(), path.Base(f))))
		}
	}

	v, err = db.KVGet(key)
	assert.Nil(t, err)
	assert.Equal(t, string(value), string(v))
	for i := 0; i < wcnt; i++ {
		expectedV := []byte(string(key) + strconv.Itoa(i))
		v, err := db.KVGet(expectedV)
		assert.Nil(t, err)
		assert.Equal(t, string(expectedV), string(v))
	}

	err = db.KVSet(0, key, []byte("changed"))
	assert.Nil(t, err)

	err = db.restoreFromPath(db.GetBackupDir(), 1, 1)
	assert.Nil(t, err)

	assert.Equal(t, len(localfiles), len(backupSSTfiles))
	assert.True(t, len(localfiles) >= 1, localfiles)
	for _, f := range backupfiles {
		if strings.HasPrefix(f, "LOG") {
			continue
		}
		fi, err := os.Stat(f)
		assert.Nil(t, err)
		lfi, err := os.Stat(path.Join(db.GetDataDir(), path.Base(f)))
		assert.Nil(t, err)
		if strings.HasSuffix(f, "sst") {
			assert.True(t, os.SameFile(fi, lfi))
		} else {
			assert.False(t, os.SameFile(fi, lfi))
			assert.Nil(t, isSameSSTFile(f, path.Join(db.GetDataDir(), path.Base(f))))
		}
	}

	v, err = db.KVGet(key)
	assert.Nil(t, err)
	assert.Equal(t, string(value), string(v))
	for i := 0; i < wcnt; i++ {
		expectedV := []byte(string(key) + strconv.Itoa(i))
		v, err := db.KVGet(expectedV)
		assert.Nil(t, err)
		assert.Equal(t, string(expectedV), string(v))
	}
}
