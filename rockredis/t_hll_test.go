package rockredis

import (
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/spaolacci/murmur3"

	hll2 "github.com/absolute8511/hyperloglog"
	//hll "github.com/axiomhq/hyperloglog"
	"github.com/stretchr/testify/assert"
)

// func TestHLLPerf(t *testing.T) {
// 	hllp := hll.New14()
// 	hasher64 := murmur3.New64()
// 	for i := 0; i < 100000; i++ {
// 		hasher64.Write([]byte(strconv.Itoa(i)))
// 		hllp.InsertHash(hasher64.Sum64())
// 		hasher64.Reset()
// 	}
// 	b, err := hllp.MarshalBinary()
// 	t.Log(len(b))
// 	//t.Log(hllp.Estimate())
// 	//assert.True(t, false, "")
// 	assert.Nil(t, err)
// 	for i := 0; i < 100000; i++ {
// 		hllp := hll.New14()
// 		err = hllp.UnmarshalBinary(b)
// 		hasher64.Write([]byte(strconv.Itoa(i)))
// 		hllp.InsertHash(hasher64.Sum64())
// 		hasher64.Reset()
// 		//hllp.Estimate()
// 		hllp.MarshalBinary()
// 	}
// }

func TestHLLPlusPerf(t *testing.T) {
	hllp, _ := hll2.NewPlus(14)
	hasher64 := murmur3.New64()
	for i := 0; i < 100000; i++ {
		hasher64.Write([]byte(strconv.Itoa(i)))
		hllp.Add(hasher64)
		hasher64.Reset()
	}
	b, err := hllp.GobEncode()
	t.Log(len(b))
	//t.Log(hllp.Count())
	//assert.True(t, false, "")
	assert.Nil(t, err)
	for i := 0; i < 100000; i++ {
		hllp, _ := hll2.NewPlus(14)
		hllp.GobDecode(b)
		hasher64.Write([]byte(strconv.Itoa(i)))
		hllp.Add(hasher64)
		hasher64.Reset()
		//hllp.Count()
		hllp.GobEncode()
	}
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
	v1Reopen, err := db.PFCount(0, key1)
	assert.Nil(t, err)
	rawV, _ := db.KVGet(key1)
	t.Logf("pf key : %v\n", rawV)
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
		loop := true
		for loop {
			c1, err := db.PFCount(0, key1)
			assert.Nil(t, err)
			c2, err := db.PFCount(0, key2)
			assert.Nil(t, err)
			if c1 < lastC1 {
				t.Logf("pfcount not increased: %v, %v", c1, lastC1)
				cnt++
			}
			if c2 < lastC2 {
				t.Logf("pfcount not increased: %v, %v", c2, lastC2)
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
	newkey1, _ := db.KVGet(key1)
	t.Log(newkey1)
	//newkey2, _ := db.KVGet(key2)
	//assert.NotEqual(t, oldkey1, newkey1)
	//assert.NotEqual(t, oldkey2, newkey2)
	assert.NotEqual(t, v1, v11)
	assert.NotEqual(t, v2, v22)
	assert.True(t, int64(totalCnt-totalCnt/100) < v1, "error should be less than 1%")
	assert.True(t, int64(totalCnt+totalCnt/100) > v1, "error should be less than 1%")
	assert.True(t, int64(totalCnt-totalCnt/100) < v2, "error should be less than 1%")
	assert.True(t, int64(totalCnt+totalCnt/100) > v2, "error should be less than 1%")
	v33, err := db.PFCount(0, key1, key2)
	assert.Nil(t, err)
	t.Log(v33)
	assert.NotEqual(t, v3, v33)
	assert.True(t, v33 <= v1+v2+int64(totalCnt/100), "merged count should not diff too much")
	assert.True(t, v33 >= v1+v2-int64(totalCnt/100), "merged count should not diff too much")
	db.hllCache.Flush()
	close(stopC)
	// refill cache with key1, key2 to remove read cache
	db.PFAdd(0, key1, []byte(strconv.Itoa(0)))
	db.PFAdd(0, key2, []byte(strconv.Itoa(0+totalCnt)))
	// test cache evict to remove write cache
	for i := 0; i < HLLCacheSize*2; i++ {
		db.PFAdd(0, []byte(strconv.Itoa(i)), []byte(strconv.Itoa(i)))
	}
	// refill cache with key1, key2
	for i := 0; i < totalCnt; i++ {
		db.PFAdd(0, key1, []byte(strconv.Itoa(i)))
		db.PFAdd(0, key2, []byte(strconv.Itoa(i+totalCnt)))
	}
	// cache evict, remove read cache
	for i := 0; i < HLLCacheSize*2; i++ {
		db.PFAdd(0, []byte(strconv.Itoa(i)), []byte(strconv.Itoa(i)))
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
