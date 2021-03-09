package rockredis

import (
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/youzan/ZanRedisDB/common"
)

func TestKVCodec(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	ek := encodeKVKey([]byte("key"))

	k, err := decodeKVKey(ek)
	assert.Nil(t, err)
	assert.Equal(t, "key", string(k))
}

func TestDBKV(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	key1 := []byte("test:testdb_kv_a")
	key2 := []byte("test:testdb_kv_b")
	n, err := db.KVExists(key1, key2)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)

	err = db.KVSet(0, key1, []byte("hello world 1"))
	assert.Nil(t, err)
	n, err = db.KVExists(key1, key2)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)

	err = db.KVSet(0, key2, []byte("hello world 2"))
	assert.Nil(t, err)

	n, err = db.KVExists(key1, key2)
	assert.Nil(t, err)
	assert.Equal(t, int64(2), n)

	v1, _ := db.KVGet(key1)
	assert.Equal(t, "hello world 1", string(v1))
	v2, _ := db.KVGet(key2)
	assert.Equal(t, "hello world 2", string(v2))
	num, err := db.GetTableKeyCount([]byte("test"))
	assert.Nil(t, err)
	assert.Equal(t, int64(2), num)

	ay, errs := db.MGet(key1, key2)
	assert.Equal(t, 2, len(errs))
	assert.Nil(t, errs[0])
	assert.Nil(t, errs[1])
	assert.Equal(t, 2, len(ay))
	assert.Equal(t, v1, ay[0])
	assert.Equal(t, v2, ay[1])

	key3 := []byte("test:testdb_kv_range")

	n, err = db.Append(0, key3, []byte("Hello"))
	assert.Nil(t, err)
	assert.Equal(t, int64(5), n)

	n, err = db.Append(0, key3, []byte(" World"))
	assert.Nil(t, err)
	assert.Equal(t, int64(11), n)

	n, err = db.StrLen(key3)
	assert.Nil(t, err)
	assert.Equal(t, int64(11), n)

	v, err := db.GetRange(key3, 0, 4)
	assert.Nil(t, err)
	assert.Equal(t, "Hello", string(v))

	v, err = db.GetRange(key3, 0, -1)
	assert.Nil(t, err)
	assert.Equal(t, "Hello World", string(v))

	v, err = db.GetRange(key3, -5, -1)
	assert.Nil(t, err)
	assert.Equal(t, "World", string(v))

	n, err = db.SetRange(0, key3, 6, []byte("Redis"))
	assert.Nil(t, err)
	assert.Equal(t, int64(11), n)

	v, err = db.KVGet(key3)
	assert.Nil(t, err)
	assert.Equal(t, "Hello Redis", string(v))

	v, err = db.KVGetExpired(key3)
	assert.Nil(t, err)
	assert.Equal(t, "Hello Redis", string(v))

	n, err = db.KVGetVer(key3)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)

	key4 := []byte("test:testdb_kv_range_none")
	n, err = db.SetRange(0, key4, 6, []byte("Redis"))
	assert.Nil(t, err)
	assert.Equal(t, int64(11), n)

	r, err := db.KVExists(key3)
	assert.Nil(t, err)
	assert.NotEqual(t, int64(0), r)
	r, err = db.SetNX(0, key3, []byte(""))
	assert.Nil(t, err)
	assert.Equal(t, int64(0), r)

	v, err = db.KVGet(key3)
	assert.Nil(t, err)
	assert.Equal(t, "Hello Redis", string(v))

	num, err = db.GetTableKeyCount([]byte("test"))
	assert.Nil(t, err)
	assert.Equal(t, int64(4), num)

	db.DelKeys(key3)
	r, _ = db.KVExists(key3)
	assert.Equal(t, int64(0), r)
	num, err = db.GetTableKeyCount([]byte("test"))
	assert.Nil(t, err)
	assert.Equal(t, int64(3), num)

	key5 := []byte("test:test_kv_mset_key5")
	key6 := []byte("test:test_kv_mset_key6")
	err = db.MSet(0, common.KVRecord{Key: key3, Value: []byte("key3")},
		common.KVRecord{Key: key5, Value: []byte("key5")}, common.KVRecord{Key: key6, Value: []byte("key6")})
	if err != nil {
		t.Errorf("fail mset: %v", err)
	}
	if v, err := db.KVGet(key3); err != nil {
		t.Error(err)
	} else if string(v) != "key3" {
		t.Error(string(v))
	}
	if v, err := db.KVGet(key5); err != nil {
		t.Error(err)
	} else if string(v) != "key5" {
		t.Error(string(v))
	}
	if v, err := db.KVGet(key6); err != nil {
		t.Error(err)
	} else if string(v) != "key6" {
		t.Error(string(v))
	}
	num, err = db.GetTableKeyCount([]byte("test"))
	if err != nil {
		t.Error(err)
	} else if num != 6 {
		t.Errorf("table count not as expected: %v", num)
	}
}

func TestDBKVSetIF(t *testing.T) {
	db := getTestDBWithCompactTTL(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	key1 := []byte("test:testdb_kv_setif_a")
	ts := time.Now().UnixNano()

	n, err := db.SetIfEQ(ts, key1, nil, []byte("hello world 1"), 30)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)
	n, err = db.GetTableKeyCount([]byte("test"))
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)

	n, err = db.KVTtl(key1)
	assert.Nil(t, err)
	assert.Equal(t, int64(30), n)

	n, err = db.SetIfEQ(ts, key1, []byte("not equal"), []byte("hello world 1"), 30)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)

	v1, _ := db.KVGet(key1)
	assert.Equal(t, "hello world 1", string(v1))

	n, err = db.SetIfEQ(ts, key1, v1, []byte("hello world 2"), 30)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)

	n, err = db.GetTableKeyCount([]byte("test"))
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)

	v1, _ = db.KVGet(key1)
	assert.Equal(t, "hello world 2", string(v1))

	n, err = db.DelIfEQ(ts, key1, []byte("not equal"))
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)

	n, err = db.GetTableKeyCount([]byte("test"))
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)

	n, err = db.DelIfEQ(ts, key1, v1)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)

	n, err = db.GetTableKeyCount([]byte("test"))
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)
}

func TestDBKVBit(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	key := []byte("test:testdb_kv_bit")
	n, err := db.BitSetOld(0, key, 5, 1)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)

	n, err = db.bitGetOld(key, 0)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)
	n, err = db.bitGetOld(key, 5)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)

	n, err = db.bitGetOld(key, 100)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)

	n, err = db.bitCountOld(key, 0, -1)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)

	n, err = db.BitSetOld(0, key, 5, 0)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)

	_, err = db.BitSetOld(0, key, -5, 0)
	assert.NotNil(t, err)

	for i := 0; i < bitmapSegBits*3; i++ {
		n, err = db.bitGetOld(key, int64(i))
		assert.Nil(t, err)
		assert.Equal(t, int64(0), n)

		n, err = db.bitCountOld(key, 0, -1)
		assert.Nil(t, err)
		assert.Equal(t, int64(0), n)
	}
	bitsForOne := make(map[int]bool)
	bitsForOne[0] = true
	bitsForOne[bitmapSegBytes-1] = true
	bitsForOne[bitmapSegBytes] = true
	bitsForOne[bitmapSegBytes+1] = true
	bitsForOne[bitmapSegBytes*2-1] = true
	bitsForOne[bitmapSegBytes*2] = true
	bitsForOne[bitmapSegBits-bitmapSegBytes-1] = true
	bitsForOne[bitmapSegBits-bitmapSegBytes] = true
	bitsForOne[bitmapSegBits-bitmapSegBytes+1] = true

	bitsForOne[bitmapSegBits-1] = true
	bitsForOne[bitmapSegBits] = true
	bitsForOne[bitmapSegBits+1] = true
	bitsForOne[bitmapSegBits+bitmapSegBytes-1] = true
	bitsForOne[bitmapSegBits+bitmapSegBytes] = true
	bitsForOne[bitmapSegBits+bitmapSegBytes+1] = true
	bitsForOne[bitmapSegBits*2-bitmapSegBytes-1] = true
	bitsForOne[bitmapSegBits*2-bitmapSegBytes] = true
	bitsForOne[bitmapSegBits*2-1] = true
	bitsForOne[bitmapSegBits*2] = true
	bitsForOne[bitmapSegBits*2+1] = true
	bitsForOne[bitmapSegBits*2+bitmapSegBytes-1] = true
	bitsForOne[bitmapSegBits*2+bitmapSegBytes] = true
	bitsForOne[bitmapSegBits*2+bitmapSegBytes+1] = true

	for bpos := range bitsForOne {
		n, err = db.BitSetOld(0, key, int64(bpos), 1)
		assert.Nil(t, err)
		assert.Equal(t, int64(0), n)
		n, err = db.bitGetOld(key, int64(bpos))
		assert.Nil(t, err)
		assert.Equal(t, int64(1), n)
		// new v2 should read old
		n, err = db.BitGetV2(key, int64(bpos))
		assert.Nil(t, err)
		assert.Equal(t, int64(1), n)
	}

	for i := 0; i < bitmapSegBits*3; i++ {
		n, err = db.bitGetOld(key, int64(i))
		assert.Nil(t, err)
		if _, ok := bitsForOne[i]; ok {
			assert.Equal(t, int64(1), n)
		} else {
			assert.Equal(t, int64(0), n)
		}
	}
	n, err = db.bitCountOld(key, 0, -1)
	assert.Nil(t, err)
	assert.Equal(t, int64(len(bitsForOne)), n)
	// new v2 should read old
	n, err = db.BitCountV2(key, 0, -1)
	assert.Nil(t, err)
	assert.Equal(t, int64(len(bitsForOne)), n)
	err = db.KVSet(0, key, []byte{0x00, 0x00, 0x00})
	assert.Nil(t, err)

	err = db.KVSet(0, key, []byte("foobar"))
	assert.Nil(t, err)

	n, err = db.bitCountOld(key, 0, -1)
	assert.Nil(t, err)
	assert.Equal(t, int64(26), n)

	n, err = db.bitCountOld(key, 0, 0)
	assert.Nil(t, err)
	assert.Equal(t, int64(4), n)

	n, err = db.bitCountOld(key, 1, 1)
	assert.Nil(t, err)
	assert.Equal(t, int64(6), n)

}

func TestDBKVWithNoTable(t *testing.T) {
	db := getTestDBNoTableCounter(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	key1 := []byte("testdb_kv_a")

	if err := db.KVSet(0, key1, []byte("hello world 1")); err == nil {
		t.Error("should error without table")
	}

	key2 := []byte("test:testdb_kv_b")

	if err := db.KVSet(0, key2, []byte("hello world 2")); err != nil {
		t.Fatal(err)
	}
	v1, _ := db.KVGet(key1)
	if v1 != nil {
		t.Error(v1)
	}
	v2, err := db.KVGet([]byte("testdb_kv_b"))
	if err == nil {
		t.Error("should be error while get without table")
	}

	v2, _ = db.KVGet(key2)
	if string(v2) != "hello world 2" {
		t.Error(v2)
	}

	key3 := []byte("testdb_kv_range")

	if _, err := db.Append(0, key3, []byte("Hello")); err == nil {
		t.Error("should failed")
	}

	key5 := []byte("test_kv_mset_key5")
	key6 := []byte("test:test_kv_mset_key6")
	err = db.MSet(0, common.KVRecord{Key: key3, Value: []byte("key3")},
		common.KVRecord{Key: key5, Value: []byte("key5")}, common.KVRecord{Key: key6, Value: []byte("key6")})
	if err == nil {
		t.Error("should failed")
	}
	if _, err := db.KVGet(key5); err == nil {
		t.Error("should failed")
	}
	if v, err := db.KVGet(key6); err != nil {
		t.Error("failed to get no value")
	} else if v != nil {
		t.Error("should get no value")
	}
}

func BenchmarkKVSetSingleKey(b *testing.B) {
	db := getTestDBForBench()
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	b.StopTimer()
	key := []byte("test:testdb_kv_bench")

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		db.KVSet(0, key, key)
	}
	b.StopTimer()
}

func BenchmarkKVSetManyKeys(b *testing.B) {
	db := getTestDBForBench()
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	b.StopTimer()
	key := "test:testdb_kv_bench"

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		db.KVSet(0, []byte(key+(strconv.Itoa(i))), []byte("1"))
	}
	b.StopTimer()
}
