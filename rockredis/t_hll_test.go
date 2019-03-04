package rockredis

import (
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/golang/snappy"
	"github.com/spaolacci/murmur3"

	hll2 "github.com/absolute8511/go-hll"
	hll "github.com/absolute8511/hyperloglog"
	hll3 "github.com/absolute8511/hyperloglog2"
	"github.com/stretchr/testify/assert"
)

func TestHLLPlusSpace(t *testing.T) {
	hllp, _ := hll.NewPlus(hllPrecision)
	hasher64 := murmur3.New64()
	for i := 0; i < 5000; i++ {
		hasher64.Write([]byte(strconv.Itoa(rand.Int())))
		hllp.Add(hasher64)
		hasher64.Reset()
		if i%100 == 0 {
			b, _ := hllp.GobEncode()
			t.Log(len(b))
		}
	}
}
func TestHLLPlusPerf(t *testing.T) {
	hllp, _ := hll.NewPlus(hllPrecision)
	hasher64 := murmur3.New64()
	for i := 0; i < 500; i++ {
		hasher64.Write([]byte(strconv.Itoa(rand.Int())))
		hllp.Add(hasher64)
		hasher64.Reset()
	}
	b, err := hllp.GobEncode()
	s := time.Now()
	for i := 0; i < 100000; i++ {
		hllp, _ := hll.NewPlus(14)
		hllp.GobDecode(b)
	}
	t.Logf("small hll unmarshal cost: %v", time.Since(s))
	for i := 0; i < 100000; i++ {
		hllp.GobEncode()
	}
	t.Logf("small hll marshal cost: %v", time.Since(s))
	for i := 0; i < 10000; i++ {
		hllp.Count()
	}
	t.Logf("small hll count cost: %v", time.Since(s))
	for i := 0; i < 100000; i++ {
		hasher64.Write([]byte(strconv.Itoa(rand.Int())))
		hllp.Add(hasher64)
		hasher64.Reset()
	}
	//t.Log(hllp.Count())
	t.Log(time.Since(s))
	for i := 0; i < 10000; i++ {
		hllp.Count()
	}
	b, err = hllp.GobEncode()
	assert.Nil(t, err)
	t.Log(time.Since(s))
	for i := 0; i < 100000; i++ {
		hllp, _ := hll.NewPlus(14)
		hllp.GobDecode(b)
	}
	t.Logf("large hll unmarshal cost: %v", time.Since(s))
	for i := 0; i < 100000; i++ {
		hllp.GobEncode()
	}
	t.Logf("large hll marshal cost: %v", time.Since(s))
}
func TestHLL3Space(t *testing.T) {
	hllp, _ := hll3.New(hllPrecision, true)
	hasher64 := murmur3.New64()
	for i := 0; i < 5000; i++ {
		hasher64.Write([]byte(strconv.Itoa(rand.Int())))
		hllp.InsertHash(hasher64.Sum64())
		hasher64.Reset()
		if i%100 == 0 {
			b, _ := hllp.MarshalBinary()
			t.Log(len(b))
		}
	}
}

func TestHLL3Perf(t *testing.T) {
	hllp, _ := hll3.New(hllPrecision, true)
	hasher64 := murmur3.New64()
	for i := 0; i < 500; i++ {
		hasher64.Write([]byte(strconv.Itoa(rand.Int())))
		hllp.InsertHash(hasher64.Sum64())
		hasher64.Reset()
	}
	b, err := hllp.MarshalBinary()
	s := time.Now()
	for i := 0; i < 100000; i++ {
		hllp, _ := hll3.New(hllPrecision, true)
		hllp.UnmarshalBinary(b)
	}
	t.Logf("small hll unmarshal cost: %v", time.Since(s))
	for i := 0; i < 100000; i++ {
		hllp.MarshalBinary()
	}
	t.Logf("small hll marshal cost: %v", time.Since(s))
	for i := 0; i < 10000; i++ {
		hllp.Estimate()
	}
	t.Logf("small hll count cost: %v", time.Since(s))
	for i := 0; i < 100000; i++ {
		hasher64.Write([]byte(strconv.Itoa(rand.Int())))
		hllp.InsertHash(hasher64.Sum64())
		hasher64.Reset()
	}
	//t.Log(hllp.Estimate())
	t.Log(time.Since(s))
	for i := 0; i < 10000; i++ {
		hllp.Estimate()
	}
	t.Logf("large hll count cost: %v", time.Since(s))
	b, err = hllp.MarshalBinary()
	assert.Nil(t, err)
	for i := 0; i < 100000; i++ {
		hllp, _ := hll3.New(hllPrecision, true)
		hllp.UnmarshalBinary(b)
	}
	t.Logf("large hll unmarshal cost: %v", time.Since(s))
	for i := 0; i < 100000; i++ {
		hllp.MarshalBinary()
	}
	t.Logf("large hll marshal cost: %v", time.Since(s))
}

func TestHLL2Space(t *testing.T) {
	sz, err := hll2.SizeByP(int(hllPrecision))
	assert.Nil(t, err)
	hllp := make(hll2.HLL, sz)
	hasher64 := murmur3.New64()
	for i := 0; i < 5000; i++ {
		hasher64.Write([]byte(strconv.Itoa(rand.Int())))
		hllp.Add(hasher64.Sum64())
		hasher64.Reset()
		if i%100 == 0 {
			b := snappy.Encode(nil, hllp)
			t.Log(len(b))
		}
	}
}
func TestHLL2Perf(t *testing.T) {
	sz, err := hll2.SizeByP(int(hllPrecision))
	assert.Nil(t, err)
	hllp := make(hll2.HLL, sz)

	hasher64 := murmur3.New64()
	for i := 0; i < 500; i++ {
		hasher64.Write([]byte(strconv.Itoa(rand.Int())))
		hllp.Add(hasher64.Sum64())
		hasher64.Reset()
	}

	b := snappy.Encode(nil, []byte(hllp))
	s := time.Now()
	for i := 0; i < 100000; i++ {
		snappy.Decode(nil, b)
	}
	t.Logf("small hll unmarshal cost: %v", time.Since(s))

	for i := 0; i < 100000; i++ {
		snappy.Encode(nil, hllp)
	}
	t.Logf("small hll marshal cost: %v", time.Since(s))

	for i := 0; i < 100000; i++ {
		hasher64.Write([]byte(strconv.Itoa(rand.Int())))
		hllp.Add(hasher64.Sum64())
		hasher64.Reset()
	}
	t.Log(time.Since(s))
	//t.Log(hllp.EstimateCardinality())
	for i := 0; i < 1000; i++ {
		hllp.EstimateCardinality()
	}
	t.Log(hllp.IsSparse())
	b = snappy.Encode(nil, []byte(hllp))
	t.Log(time.Since(s))
	for i := 0; i < 100000; i++ {
		snappy.Decode(nil, b)
	}
	t.Logf("large hll unmarshal cost: %v", time.Since(s))
	for i := 0; i < 100000; i++ {
		snappy.Encode(nil, hllp)
	}
	t.Logf("large hll marshal cost: %v", time.Since(s))
}

func TestDBHLLOp(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	key1 := []byte("test:testdb_hll_a")
	v1, err := db.PFCount(0, key1)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), v1)

	var ret int64
	ret, err = db.PFAdd(0, key1, []byte("hello world 1"))
	assert.Nil(t, err)
	assert.Equal(t, int64(1), ret)

	key2 := []byte("test:testdb_hll_b")

	ret, err = db.PFAdd(0, key2, []byte("hello world 2"))
	assert.Nil(t, err)
	assert.Equal(t, int64(1), ret)

	v1, err = db.PFCount(0, key1)
	assert.Nil(t, err)
	t.Log(v1)
	assert.True(t, v1 > 0, "should have pf count")
	v11, _ := db.PFCount(0, key1)
	assert.Equal(t, v1, v11)
	// add same value
	ret, err = db.PFAdd(0, key1, []byte("hello world 1"))
	assert.Nil(t, err)
	v11, _ = db.PFCount(0, key1)
	assert.Equal(t, v1, v11)

	v2, _ := db.PFCount(0, key2)
	t.Log(v2)
	assert.True(t, v2 > 0, "should have pf count")
	v22, _ := db.PFCount(0, key2)
	assert.Equal(t, v2, v22)

	for i := 0; i < 200; i++ {
		db.PFAdd(0, key1, []byte(strconv.Itoa(i)))
		db.PFAdd(0, key2, []byte(strconv.Itoa(i)))
	}
	v11, _ = db.PFCount(0, key1)
	v22, _ = db.PFCount(0, key2)
	t.Log(v11)
	t.Log(v22)
	assert.NotEqual(t, v1, v11)
	assert.NotEqual(t, v2, v22)

	db.hllCache.Flush()
	num, err := db.GetTableKeyCount([]byte("test"))
	assert.Nil(t, err)
	assert.Equal(t, int64(2), num)

	v3, err := db.PFCount(0, key1, key2)
	assert.Nil(t, err)
	t.Log(v3)
	assert.True(t, v3 <= v11+v22, "merged count should not great than add")
	db.Close()
	db = getTestDBWithDir(t, db.cfg.DataDir)
	defer db.Close()
	v1Reopen, err := db.PFCount(0, key1)
	assert.Nil(t, err)
	v2Reopen, err := db.PFCount(0, key2)
	assert.Nil(t, err)
	t.Log(v1Reopen)
	t.Log(v2Reopen)
	assert.Equal(t, v11, v1Reopen)
	assert.Equal(t, v22, v2Reopen)
	v3Reopen, err := db.PFCount(0, key1, key2)
	assert.Nil(t, err)
	t.Log(v3Reopen)
	assert.Equal(t, v3, v3Reopen)
	stopC := make(chan bool, 0)
	go func() {
		var lastC1 int64
		var lastC2 int64
		var cnt int64
		var largeDiff int64
		loop := true
		var total int64
		for loop {
			c1, err := db.PFCount(0, key1)
			assert.Nil(t, err)
			c2, err := db.PFCount(0, key2)
			assert.Nil(t, err)
			if c1 < lastC1 {
				t.Logf("pfcount not increased: %v, %v", c1, lastC1)
				cnt++
				if c1 < lastC1-lastC1/100 {
					largeDiff++
				}
			}
			if c2 < lastC2 {
				t.Logf("pfcount not increased: %v, %v", c2, lastC2)
				cnt++
				if c2 < lastC2-lastC2/100 {
					largeDiff++
				}
			}
			total += 2
			lastC1 = c1
			lastC2 = c2
			select {
			case <-stopC:
				loop = false
				break
			default:
				time.Sleep(time.Microsecond)
			}
		}
		t.Logf("pfcount not increased: %v, %v, %v", largeDiff, cnt, total)
		assert.True(t, largeDiff < 10, "not increased count has large diff: %v, %v, %v", cnt, largeDiff, total)
		assert.True(t, cnt < total/10, "not increased count: %v, %v, %v", cnt, largeDiff, total)
	}()
	totalCnt := MAX_BATCH_NUM * 10
	elems := make([][]byte, totalCnt)
	for i := 0; i < totalCnt; i++ {
		elems[i] = []byte(strconv.Itoa(i))
	}
	ret, err = db.PFAdd(0, key1, elems...)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), ret)
	totalCnt = totalCnt * 2
	for i := 0; i < totalCnt; i++ {
		db.PFAdd(0, key1, []byte(strconv.Itoa(i)))
		db.PFAdd(0, key2, []byte(strconv.Itoa(i+totalCnt)))
	}
	ret, err = db.PFAdd(0, key1, []byte("1"))
	assert.Nil(t, err)
	assert.Equal(t, int64(0), ret)
	oldkey1, _ := db.KVGet(key1)
	t.Logf("pf key len: %v\n", len(oldkey1))
	//oldkey2, _ := db.KVGet(key2)
	// lazy compute should modify value
	v1, _ = db.PFCount(0, key1)
	v2, _ = db.PFCount(0, key2)
	t.Log(v1)
	t.Log(v2)
	//newkey2, _ := db.KVGet(key2)
	//assert.NotEqual(t, oldkey1, newkey1)
	//assert.NotEqual(t, oldkey2, newkey2)
	assert.NotEqual(t, v1, v11)
	assert.NotEqual(t, v2, v22)
	assert.True(t, int64(totalCnt-totalCnt/40) < v1, "error should be less than 2%")
	assert.True(t, int64(totalCnt+totalCnt/40) > v1, "error should be less than 2%")
	assert.True(t, int64(totalCnt-totalCnt/40) < v2, "error should be less than 2%")
	assert.True(t, int64(totalCnt+totalCnt/40) > v2, "error should be less than 2%")
	v33, err := db.PFCount(0, key1, key2)
	assert.Nil(t, err)
	t.Log(v33)
	assert.NotEqual(t, v3, v33)
	assert.True(t, v33 <= v1+v2+int64(totalCnt/20), "merged count should not diff too much")
	assert.True(t, v33 >= v1+v2-int64(totalCnt/20), "merged count should not diff too much")
	db.hllCache.Flush()
	close(stopC)
	// refill cache with key1, key2 to remove read cache
	db.PFAdd(0, key1, []byte(strconv.Itoa(0)))
	db.PFAdd(0, key2, []byte(strconv.Itoa(0)))
	// test cache evict to remove write cache
	for i := 0; i < HLLReadCacheSize*2; i++ {
		db.PFAdd(0, []byte("test:"+strconv.Itoa(i)), []byte(strconv.Itoa(i)))
	}
	// refill cache with key1, key2
	for i := 0; i < totalCnt; i++ {
		db.PFAdd(0, key1, []byte(strconv.Itoa(i)))
		db.PFAdd(0, key2, []byte(strconv.Itoa(i+totalCnt)))
	}
	// cache evict, remove read cache
	for i := 0; i < HLLReadCacheSize*2; i++ {
		db.PFAdd(0, []byte("test:"+strconv.Itoa(i)), []byte(strconv.Itoa(i)))
	}
	v3, err = db.PFCount(0, key1, key2)
	assert.Nil(t, err)
	assert.Equal(t, v3, v33)
	v11, _ = db.PFCount(0, key1)
	v22, _ = db.PFCount(0, key2)
	assert.Equal(t, v1, v11)
	assert.Equal(t, v2, v22)
	//assert.True(t, false, "failed")
}

func TestDBHLLDifferentType(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	// add type1, read type1
	// change default type to type2 read type1
	// add type2, read type2
	// change default type to type1, read type1, read type2
	key1 := []byte("test:testdb_hll_a_t1")
	v1, err := db.PFCount(0, key1)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), v1)

	var ret int64
	ret, err = db.PFAdd(0, key1, []byte("hello world 1"))
	assert.Nil(t, err)
	assert.Equal(t, int64(1), ret)
	v1, err = db.PFCount(0, key1)
	assert.Nil(t, err)
	t.Log(v1)
	assert.True(t, v1 > 0, "should have pf count")

	initHLLType = hllType2

	key2 := []byte("test:testdb_hll_b_t2")
	ret, err = db.PFAdd(0, key2, []byte("hello world 2"))
	assert.Nil(t, err)
	assert.Equal(t, int64(1), ret)
	v2, _ := db.PFCount(0, key2)
	t.Log(v2)
	assert.True(t, v2 > 0, "should have pf count")

	v11, _ := db.PFCount(0, key1)
	assert.Equal(t, v1, v11)
	v22, _ := db.PFCount(0, key2)
	assert.Equal(t, v2, v22)

	for i := 0; i < 200; i++ {
		db.PFAdd(0, key1, []byte(strconv.Itoa(i)))
		db.PFAdd(0, key2, []byte(strconv.Itoa(i)))
	}
	v11, _ = db.PFCount(0, key1)
	v22, _ = db.PFCount(0, key2)
	t.Log(v11)
	t.Log(v22)
	assert.NotEqual(t, v1, v11)
	assert.NotEqual(t, v2, v22)

	db.hllCache.Flush()
	num, err := db.GetTableKeyCount([]byte("test"))
	assert.Nil(t, err)
	assert.Equal(t, int64(2), num)

	initHLLType = hllPlusDefault

	key3 := []byte("test:testdb_hll_b_t3")
	ret, err = db.PFAdd(0, key3, []byte("hello world 2"))
	assert.Nil(t, err)
	assert.Equal(t, int64(1), ret)
	v3, _ := db.PFCount(0, key2)
	t.Log(v3)
	assert.True(t, v3 > 0, "should have pf count")

	db.Close()
	db = getTestDBWithDir(t, db.cfg.DataDir)
	defer db.Close()
	v1Reopen, err := db.PFCount(0, key1)
	assert.Nil(t, err)
	v2Reopen, err := db.PFCount(0, key2)
	assert.Nil(t, err)
	t.Log(v1Reopen)
	t.Log(v2Reopen)
	assert.Equal(t, v11, v1Reopen)
	assert.Equal(t, v22, v2Reopen)
	_, err = db.PFCount(0, key1, key2, key3)
	assert.NotNil(t, err)

	totalCnt := MAX_BATCH_NUM * 10
	elems := make([][]byte, totalCnt)
	for i := 0; i < totalCnt; i++ {
		elems[i] = []byte(strconv.Itoa(i))
	}
	ret, err = db.PFAdd(0, key1, elems...)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), ret)
	totalCnt = totalCnt * 2
	for i := 0; i < totalCnt; i++ {
		db.PFAdd(0, key1, []byte(strconv.Itoa(i)))
		db.PFAdd(0, key2, []byte(strconv.Itoa(i+totalCnt)))
	}
	ret, err = db.PFAdd(0, key1, []byte("1"))
	assert.Nil(t, err)
	assert.Equal(t, int64(0), ret)
	// lazy compute should modify value
	v1, _ = db.PFCount(0, key1)
	v2, _ = db.PFCount(0, key2)
	t.Log(v1)
	t.Log(v2)

	db.hllCache.Flush()
	// refill cache with key1, key2 to remove read cache
	db.PFAdd(0, key1, []byte(strconv.Itoa(0)))
	db.PFAdd(0, key2, []byte(strconv.Itoa(0)))
	// test cache evict to remove write cache
	for i := 0; i < HLLReadCacheSize*2; i++ {
		db.PFAdd(0, []byte("test:"+strconv.Itoa(i)), []byte(strconv.Itoa(i)))
	}
	// refill cache with key1, key2
	for i := 0; i < totalCnt; i++ {
		db.PFAdd(0, key1, []byte(strconv.Itoa(i)))
		db.PFAdd(0, key2, []byte(strconv.Itoa(i+totalCnt)))
	}
	// cache evict, remove read cache
	for i := 0; i < HLLReadCacheSize*2; i++ {
		db.PFAdd(0, []byte("test:"+strconv.Itoa(i)), []byte(strconv.Itoa(i)))
	}
	v11, _ = db.PFCount(0, key1)
	v22, _ = db.PFCount(0, key2)
	assert.Equal(t, v1, v11)
	assert.Equal(t, v2, v22)
	_, err = db.PFCount(0, key1, key2, key3)
	assert.NotNil(t, err)
}

func TestDBHLLOpPerf1(t *testing.T) {
	initHLLType = hllPlusDefault
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	key1 := []byte("test:testdb_hll_perf1_a")
	v1, err := db.PFCount(0, key1)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), v1)

	var ret int64
	ret, err = db.PFAdd(0, key1, []byte("hello world 1"))
	assert.Nil(t, err)
	assert.Equal(t, int64(1), ret)

	key2 := []byte("test:testdb_hll_perf1_b")

	stopC := make(chan bool, 0)
	for i := 0; i < 5; i++ {
		go func() {
			var lastC1 int64
			var lastC2 int64
			var cnt int64
			loop := true
			for loop {
				c1, err := db.PFCount(0, key1)
				assert.Nil(t, err)
				c2, err := db.PFCount(0, key2)
				assert.Nil(t, err)
				if c1 < lastC1 {
					cnt++
				}
				if c2 < lastC2 {
					cnt++
				}
				lastC1 = c1
				lastC2 = c2
				select {
				case <-stopC:
					loop = false
					break
				default:
					time.Sleep(time.Microsecond)
				}
			}
			assert.True(t, cnt < 10, "not increased count: %v", cnt)
		}()
	}

	for i := 0; i < 100; i++ {
		db.PFAdd(0, key1, []byte(strconv.Itoa(i)))
		db.PFAdd(0, key2, []byte(strconv.Itoa(i)))
	}
	db.hllCache.Flush()
	kv1, _ := db.KVGet(key1)
	kv2, _ := db.KVGet(key2)
	t.Log(len(kv1))
	t.Log(len(kv2))
	t.Log(db.PFCount(0, key1))
	t.Log(db.PFCount(0, key2))

	for i := 0; i < 10; i++ {
		// test cache evict to remove write cache
		for j := 0; j < HLLReadCacheSize*2; j++ {
			db.PFAdd(0, []byte("test:"+strconv.Itoa(j)), []byte(strconv.Itoa(i)))
		}
		// refill cache with key1, key2
		for j := 0; j < MAX_BATCH_NUM*2; j++ {
			db.PFAdd(0, key1, []byte(strconv.Itoa(j)))
			db.PFAdd(0, key2, []byte(strconv.Itoa(j+MAX_BATCH_NUM*10)))
		}
		// cache evict, remove read cache
		for j := 0; j < HLLReadCacheSize*2; j++ {
			db.PFAdd(0, []byte("test:"+strconv.Itoa(j)), []byte(strconv.Itoa(i+1000)))
		}
		time.Sleep(time.Microsecond)
	}
}

func TestDBHLLOpPerf3(t *testing.T) {
	initHLLType = hllType3
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	key1 := []byte("test:testdb_hll_perf3_a")
	v1, err := db.PFCount(0, key1)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), v1)

	var ret int64
	ret, err = db.PFAdd(0, key1, []byte("hello world 1"))
	assert.Nil(t, err)
	assert.Equal(t, int64(1), ret)

	key2 := []byte("test:testdb_hll_perf3_b")

	stopC := make(chan bool, 0)
	for i := 0; i < 5; i++ {
		go func() {
			var lastC1 int64
			var lastC2 int64
			var cnt int64
			loop := true
			for loop {
				c1, err := db.PFCount(0, key1)
				assert.Nil(t, err)
				c2, err := db.PFCount(0, key2)
				assert.Nil(t, err)
				if c1 < lastC1 {
					cnt++
				}
				if c2 < lastC2 {
					cnt++
				}
				lastC1 = c1
				lastC2 = c2
				select {
				case <-stopC:
					loop = false
					break
				default:
					time.Sleep(time.Microsecond)
				}
			}
			assert.True(t, cnt < 10, "not increased count: %v", cnt)
		}()
	}

	for i := 0; i < 100; i++ {
		db.PFAdd(0, key1, []byte(strconv.Itoa(i)))
		db.PFAdd(0, key2, []byte(strconv.Itoa(i)))
	}
	db.hllCache.Flush()
	kv1, _ := db.KVGet(key1)
	kv2, _ := db.KVGet(key2)
	t.Log(len(kv1))
	t.Log(len(kv2))
	t.Log(db.PFCount(0, key1))
	t.Log(db.PFCount(0, key2))

	for i := 0; i < 10; i++ {
		// test cache evict to remove write cache
		for j := 0; j < HLLReadCacheSize*2; j++ {
			db.PFAdd(0, []byte("test:"+strconv.Itoa(j)), []byte(strconv.Itoa(i)))
		}
		// refill cache with key1, key2
		for j := 0; j < MAX_BATCH_NUM*2; j++ {
			db.PFAdd(0, key1, []byte(strconv.Itoa(j)))
			db.PFAdd(0, key2, []byte(strconv.Itoa(j+MAX_BATCH_NUM*10)))
		}
		// cache evict, remove read cache
		for j := 0; j < HLLReadCacheSize*2; j++ {
			db.PFAdd(0, []byte("test:"+strconv.Itoa(j)), []byte(strconv.Itoa(i+1000)))
		}
		time.Sleep(time.Microsecond)
	}
}
