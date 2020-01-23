package rockredis

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBitmapV2(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	bitsForOne := make(map[int]bool)
	key := []byte("test:testdb_kv_bitv2")
	n, err := db.BitSetV2(0, key, 5, 1)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)
	bitsForOne[5] = true

	n, err = db.BitGetV2(key, 0)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)
	n, err = db.BitGetV2(key, 5)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)

	n, err = db.BitGetV2(key, 100)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)

	n, err = db.BitCountV2(key, 0, -1)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)

	n, err = db.BitKeyExist(key)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)

	n, err = db.BitSetV2(0, key, 5, 0)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)
	delete(bitsForOne, 5)

	_, err = db.BitSetV2(0, key, -5, 0)
	assert.NotNil(t, err)

	for i := 0; i < bitmapSegBits*3; i++ {
		n, err = db.BitGetV2(key, int64(i))
		assert.Nil(t, err)
		assert.Equal(t, int64(0), n)
		n, err = db.BitCountV2(key, 0, -1)
		assert.Nil(t, err)
		assert.Equal(t, int64(0), n)
	}

	// insert one bit at start and end of each segment
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

	for bpos, _ := range bitsForOne {
		n, err = db.BitSetV2(0, key, int64(bpos), 1)
		assert.Nil(t, err)
		assert.Equal(t, int64(0), n)
		n, err = db.BitGetV2(key, int64(bpos))
		assert.Nil(t, err)
		assert.Equal(t, int64(1), n)
	}
	n, err = db.BitCountV2(key, 0, -1)
	assert.Nil(t, err)
	assert.Equal(t, int64(len(bitsForOne)), n)

	for i := 0; i < bitmapSegBits*3; i++ {
		n, err = db.BitGetV2(key, int64(i))
		assert.Nil(t, err)
		if _, ok := bitsForOne[i]; ok {
			assert.Equal(t, int64(1), n)
		} else {
			assert.Equal(t, int64(0), n)
		}
	}

	_, err = db.BitSetV2(0, key, MaxBitOffsetV2+1, 0)
	assert.NotNil(t, err)
	n, err = db.BitSetV2(0, key, MaxBitOffsetV2, 1)
	assert.Nil(t, err)
	n, err = db.BitGetV2(key, MaxBitOffsetV2)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)
	n, err = db.BitCountV2(key, 0, -1)
	assert.Nil(t, err)
	assert.Equal(t, int64(len(bitsForOne)+1), n)
	n, err = db.BitKeyExist(key)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)
}

func TestBitmapV2Clear(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	bitsForOne := make(map[int]bool)
	key := []byte("test:testdb_kv_bitv2_clear")
	n, err := db.BitSetV2(0, key, 5, 1)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)
	bitsForOne[5] = true

	n, err = db.BitKeyExist(key)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)
	// insert one bit at start and end of each segment
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

	for bpos, _ := range bitsForOne {
		n, err = db.BitSetV2(0, key, int64(bpos), 1)
		assert.Nil(t, err)
		if bpos == 5 {
			assert.Equal(t, int64(1), n)
		} else {
			assert.Equal(t, int64(0), n)
		}
		n, err = db.BitGetV2(key, int64(bpos))
		assert.Nil(t, err)
		assert.Equal(t, int64(1), n)
	}
	n, err = db.BitCountV2(key, 0, -1)
	assert.Nil(t, err)
	assert.Equal(t, int64(len(bitsForOne)), n)

	db.BitClear(key)
	n, err = db.BitCountV2(key, 0, -1)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)

	n, err = db.BitKeyExist(key)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)

	n, err = db.BitSetV2(0, key, 5, 1)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)

	for i := 6; i < bitmapSegBits*3; i++ {
		n, err = db.BitGetV2(key, int64(i))
		assert.Nil(t, err)
		assert.Equal(t, int64(0), n)
	}

	n, err = db.BitGetV2(key, 5)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)
	n, err = db.BitCountV2(key, 0, -1)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)

	n, err = db.BitKeyExist(key)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)
}

func TestBitmapV2FromOld(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	key := []byte("test:testdb_kv_bit_convert")
	tn := time.Now().UnixNano()
	n, err := db.BitSetOld(tn, key, 5, 1)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)
	n, err = db.BitGetV2(key, int64(5))
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)
	n, err = db.bitCountOld(key, 0, -1)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)

	n, err = db.BitKeyExist(key)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)

	n, err = db.BitSetV2(tn, key, 6, 1)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)

	n, err = db.BitCountV2(key, 0, -1)
	assert.Nil(t, err)
	assert.Equal(t, int64(2), n)
	n, err = db.BitGetV2(key, int64(6))
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)
	n, err = db.BitGetV2(key, int64(5))
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)
	n, err = db.BitCountV2(key, 0, -1)
	assert.Nil(t, err)
	assert.Equal(t, int64(2), n)
	// old data should be deleted
	n, err = db.bitGetOld(key, int64(5))
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)

	n, err = db.BitKeyExist(key)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)

	key = []byte("test:testdb_kv_bit_convert2")

	n, err = db.BitKeyExist(key)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)

	bitsForOne := make(map[int]bool)
	// insert one bit at start and end of each segment
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
		n, err = db.BitSetOld(tn, key, int64(bpos), 1)
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

	for i := 10; i < bitmapSegBits*3; i++ {
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

	n, err = db.BitKeyExist(key)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)

	n, err = db.BitSetV2(tn, key, bitmapSegBits, 1)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)
	n, err = db.BitCountV2(key, 0, -1)
	assert.Nil(t, err)
	assert.Equal(t, int64(len(bitsForOne)), n)

	n, err = db.BitKeyExist(key)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)

	// test bitmap convert across two segments
	n, err = db.BitCountV2(key, bitmapSegBytes-1, bitmapSegBytes*2+1)
	assert.Nil(t, err)
	assert.Equal(t, int64(11), n)

	for i := 10; i < bitmapSegBits*3; i++ {
		n, err = db.BitGetV2(key, int64(i))
		assert.Nil(t, err)
		if _, ok := bitsForOne[i]; ok {
			assert.Equal(t, int64(1), n)
		} else {
			assert.Equal(t, int64(0), n)
		}
	}
	key = []byte("test:testdb_kv_bit_convert3")
	err = db.KVSet(tn, key, []byte("foobar"))
	assert.Nil(t, err)
	n, err = db.BitKeyExist(key)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)

	n, err = db.BitCountV2(key, 0, -1)
	assert.Nil(t, err)
	assert.Equal(t, int64(26), n)

	n, err = db.BitCountV2(key, 0, 0)
	assert.Nil(t, err)
	assert.Equal(t, int64(4), n)

	n, err = db.BitCountV2(key, 1, 1)
	assert.Nil(t, err)
	assert.Equal(t, int64(6), n)

	n, err = db.BitGetV2(key, 0)
	assert.Nil(t, err)
	// convert to new
	n, err = db.BitSetV2(tn, key, 0, int(n))
	assert.Nil(t, err)

	n, err = db.BitCountV2(key, 0, -1)
	assert.Nil(t, err)
	assert.Equal(t, int64(26), n)

	n, err = db.BitCountV2(key, 0, 0)
	assert.Nil(t, err)
	assert.Equal(t, int64(4), n)

	n, err = db.BitCountV2(key, 1, 1)
	assert.Nil(t, err)
	assert.Equal(t, int64(6), n)

	n, err = db.BitKeyExist(key)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)
}

func BenchmarkBitCountV2(b *testing.B) {
	db := getTestDBForBench()
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	key := []byte("test:testdb_kv_bit_benchcount")
	db.BitSetV2(0, key, MaxBitOffsetV2, 1)
	for i := 0; i < b.N; i++ {
		n, _ := db.BitCountV2(key, 0, -1)
		if n != int64(1) {
			panic("count error")
		}
	}
}

func BenchmarkBitSetV2(b *testing.B) {
	db := getTestDBForBench()
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	key := []byte("test:testdb_kv_bit_benchset")
	bitsForOne := make(map[int]bool)
	// insert one bit at start and end of each segment
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

	for i := 0; i < b.N; i++ {
		// test set 0 to 1
		for bpos := range bitsForOne {
			db.BitSetV2(0, key, int64(bpos), 1)
		}
		// test set 1 to 1
		for bpos := range bitsForOne {
			db.BitSetV2(0, key, int64(bpos), 1)
		}
		// test set 1 to 0
		for bpos := range bitsForOne {
			db.BitSetV2(0, key, int64(bpos), 0)
		}
		// test set 0 to 0
		for bpos := range bitsForOne {
			db.BitSetV2(0, key, int64(bpos), 0)
		}
	}
}

func BenchmarkBitGetV2(b *testing.B) {
	db := getTestDBForBench()
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	key := []byte("test:testdb_kv_bit_benchget")
	bitsForOne := make(map[int]bool)
	// insert one bit at start and end of each segment
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
		db.BitSetV2(0, key, int64(bpos), 1)
	}

	for i := 0; i < b.N; i++ {
		// test get 1
		for bpos := range bitsForOne {
			db.BitGetV2(key, int64(bpos))
		}
		// test get non 1
		for i := 0; i < 10; i++ {
			db.BitGetV2(key, int64(i))
		}
		for i := MaxBitOffsetV2 - bitmapSegBits; i < MaxBitOffsetV2-bitmapSegBits+10; i++ {
			db.BitGetV2(key, int64(i))
		}
	}
}
