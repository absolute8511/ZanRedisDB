package rockredis

import (
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/youzan/ZanRedisDB/common"
)

const (
	endPos int = -1
)

func bin(sz string) []byte {
	return []byte(sz)
}

func pair(memb string, score int) common.ScorePair {
	return common.ScorePair{Score: float64(score), Member: bin(memb)}
}

func TestZSetCodec(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	key := []byte("test:key")
	member := []byte("member")

	ek := zEncodeSizeKey(key)
	if k, err := zDecodeSizeKey(ek); err != nil {
		t.Fatal(err)
	} else if string(k) != "test:key" {
		t.Fatal(string(k))
	}

	ek, _ = convertRedisKeyToDBZSetKey(key, member)
	if tb, k, m, err := zDecodeSetKey(ek); err != nil {
		t.Fatal(err)
	} else if string(k) != "key" {
		t.Fatal(string(k))
	} else if string(m) != "member" {
		t.Fatal(string(m))
	} else if string(tb) != "test" {
		t.Fatal(string(tb))
	}

	ek, _ = convertRedisKeyToDBZScoreKey(key, member, 100)
	if tb, k, m, s, err := zDecodeScoreKey(ek); err != nil {
		t.Fatal(err)
	} else if string(k) != "key" {
		t.Fatal(string(k))
	} else if string(m) != "member" {
		t.Fatal(string(m))
	} else if s != 100 {
		t.Fatal(s)
	} else if string(tb) != "test" {
		t.Fatal(string(tb))
	}

}

func TestDBZSetWithEmptyMember(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	key := bin("test:testdb_zset_empty")
	if n, err := db.ZAdd(0, key, pair("a", 0), pair("b", 1),
		pair("c", 2), pair("", 3)); err != nil {
		t.Fatal(err)
	} else if n != 4 {
		t.Fatal(n)
	}

	if n, err := db.ZCount(key, 0, 0XFF); err != nil {
		t.Fatal(err)
	} else if n != 4 {
		t.Fatal(n)
	}

	if s, err := db.ZScore(key, bin("")); err != nil {
		t.Fatal(err)
	} else if s != 3 {
		t.Fatal(s)
	}

	if n, err := db.ZRem(0, key, bin("a"), bin("b")); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	if n, err := db.ZRem(0, key, bin("a"), bin("b")); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	if n, err := db.ZCard(key); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	if n, err := db.ZClear(key); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	if n, err := db.ZCount(key, 0, 0XFF); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}
}

func TestDBZSet(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	key := bin("test:testdb_zset_a")

	// {'a':0, 'b':1, 'c':2, 'd':3}
	if n, err := db.ZAdd(0, key, pair("a", 0), pair("b", 1),
		pair("c", 2), pair("d", 3)); err != nil {
		t.Fatal(err)
	} else if n != 4 {
		t.Fatal(n)
	}

	if n, err := db.ZCount(key, 0, 0XFF); err != nil {
		t.Fatal(err)
	} else if n != 4 {
		t.Fatal(n)
	}

	if s, err := db.ZScore(key, bin("d")); err != nil {
		t.Fatal(err)
	} else if s != 3 {
		t.Fatal(s)
	}

	if s, err := db.ZScore(key, bin("zzz")); err != errScoreMiss {
		t.Fatal(fmt.Sprintf("s=[%v] err=[%s]", s, err))
	}

	// {c':2, 'd':3}
	if n, err := db.ZRem(0, key, bin("a"), bin("b")); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	if n, err := db.ZRem(0, key, bin("a"), bin("b")); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	if n, err := db.ZCard(key); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	// {}
	if n, err := db.ZClear(key); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	if n, err := db.ZCount(key, 0, 0XFF); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}
}

func TestZSetOrder(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	key := bin("test:testdb_zset_order")

	// {'a':0, 'b':1, 'c':2, 'd':3, 'e':4, 'f':5}
	membs := [...]string{"a", "b", "c", "d", "e", "f"}
	membCnt := len(membs)

	for i := 0; i < membCnt; i++ {
		db.ZAdd(0, key, pair(membs[i], i))
	}

	if n, _ := db.ZCount(key, 0, 0XFFFF); int(n) != membCnt {
		t.Fatal(n)
	}

	for i := 0; i < membCnt; i++ {
		if pos, err := db.ZRank(key, bin(membs[i])); err != nil {
			t.Fatal(err)
		} else if int(pos) != i {
			t.Fatal(pos)
		}

		if pos, err := db.ZRevRank(key, bin(membs[i])); err != nil {
			t.Fatal(err)
		} else if int(pos) != membCnt-i-1 {
			t.Fatal(pos)
		}
	}

	if qMembs, err := db.ZRange(key, 0, endPos); err != nil {
		t.Fatal(err)
	} else if len(qMembs) != membCnt {
		t.Fatal(fmt.Sprintf("%d vs %d", len(qMembs), membCnt))
	} else {
		for i := 0; i < membCnt; i++ {
			if string(qMembs[i].Member) != membs[i] {
				t.Fatal(fmt.Sprintf("[%v] vs [%s], %v", qMembs[i], membs[i], qMembs))
			}
		}
	}

	// {'a':0, 'b':1, 'c':2, 'd':999, 'e':4, 'f':5}
	if n, err := db.ZAdd(0, key, pair("d", 999)); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	if pos, _ := db.ZRank(key, bin("d")); int(pos) != membCnt-1 {
		t.Fatal(pos)
	}

	if pos, _ := db.ZRevRank(key, bin("d")); int(pos) != 0 {
		t.Fatal(pos)
	}

	if pos, _ := db.ZRank(key, bin("e")); int(pos) != 3 {
		t.Fatal(pos)
	}

	if pos, _ := db.ZRank(key, bin("f")); int(pos) != 4 {
		t.Fatal(pos)
	}

	if qMembs, err := db.ZRangeByScore(key, 999, 0XFFFF, 0, membCnt); err != nil {
		t.Fatal(err)
	} else if len(qMembs) != 1 {
		t.Fatal(len(qMembs))
	}

	// {'a':0, 'b':1, 'c':2, 'd':999, 'e':6, 'f':5}
	if s, err := db.ZIncrBy(0, key, 2, bin("e")); err != nil {
		t.Fatal(err)
	} else if s != 6 {
		t.Fatal(s)
	}

	if pos, _ := db.ZRank(key, bin("e")); int(pos) != 4 {
		t.Fatal(pos)
	}

	if pos, _ := db.ZRevRank(key, bin("e")); int(pos) != 1 {
		t.Fatal(pos)
	}

	if datas, _ := db.ZRange(key, 0, endPos); len(datas) != 6 {
		t.Fatal(len(datas))
	} else {
		scores := []float64{0, 1, 2, 5, 6, 999}
		for i := 0; i < len(datas); i++ {
			if datas[i].Score != scores[i] {
				t.Fatal(fmt.Sprintf("[%d]=%v", i, datas[i]))
			}
		}
	}

	return
}

func TestZRemRange(t *testing.T) {
	// all range remove test
}

func TestZRangeLimit(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	key := []byte("test:myzset_range")
	for i := 0; i < MAX_BATCH_NUM-1; i++ {
		m := fmt.Sprintf("%8d", i)
		_, err := db.ZAdd(0, key, common.ScorePair{Score: float64(i), Member: []byte(m)})
		assert.Nil(t, err)
	}

	maxMem := fmt.Sprintf("%8d", MAX_BATCH_NUM+1)
	ay, err := db.ZRangeByLex(key, nil, []byte(maxMem), common.RangeClose, 0, -1)
	assert.Nil(t, err)
	for _, v := range ay {
		t.Logf("zrange key: %v", v)
	}
	assert.Equal(t, MAX_BATCH_NUM-1, len(ay))

	elems, err := db.ZRange(key, 0, -1)
	assert.Nil(t, err)
	assert.Equal(t, MAX_BATCH_NUM-1, len(elems))

	for i := MAX_BATCH_NUM; i < MAX_BATCH_NUM+10; i++ {
		m := fmt.Sprintf("%8d", i)
		_, err := db.ZAdd(0, key, common.ScorePair{Score: float64(i), Member: []byte(m)})
		assert.Nil(t, err)
	}
	_, err = db.ZRange(key, 0, -1)
	assert.Equal(t, errTooMuchBatchSize, err)
	_, err = db.ZRangeByLex(key, nil, []byte(maxMem), common.RangeClose, 0, -1)
	assert.Equal(t, errTooMuchBatchSize, err)

	elems, err = db.ZRange(key, 0, MAX_BATCH_NUM-2)
	assert.Nil(t, err)
	assert.Equal(t, MAX_BATCH_NUM-1, len(elems))

	ay, err = db.ZRangeByLex(key, nil, []byte(maxMem), common.RangeClose, 0, MAX_BATCH_NUM-1)
	assert.Nil(t, err)
	assert.Equal(t, MAX_BATCH_NUM-1, len(ay))
}

func TestZRangeLimitPreCheck(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	key := []byte("test:myzset_range_precheck")
	for i := 0; i < MAX_BATCH_NUM-1; i++ {
		m := fmt.Sprintf("%8d", i)
		_, err := db.ZAdd(0, key, common.ScorePair{Score: float64(i), Member: []byte(m)})
		assert.Nil(t, err)
	}

	maxMem := fmt.Sprintf("%8d", MAX_BATCH_NUM+1)
	ay, err := db.ZRangeByLex(key, nil, nil, common.RangeClose, 0, -1)
	assert.Nil(t, err)
	for _, v := range ay {
		t.Logf("zrange key: %v", v)
	}
	assert.Equal(t, MAX_BATCH_NUM-1, len(ay))
	mems, err := db.ZRangeByScore(key, 0, float64(MAX_BATCH_NUM*2), 0, -1)
	assert.Nil(t, err)
	assert.Equal(t, MAX_BATCH_NUM-1, len(mems))

	elems, err := db.ZRange(key, 0, -1)
	assert.Nil(t, err)
	assert.Equal(t, MAX_BATCH_NUM-1, len(elems))

	total := MAX_BATCH_NUM + 10
	for i := MAX_BATCH_NUM; i < MAX_BATCH_NUM+10; i++ {
		m := fmt.Sprintf("%8d", i)
		_, err := db.ZAdd(0, key, common.ScorePair{Score: float64(i), Member: []byte(m)})
		assert.Nil(t, err)
	}
	_, err = db.ZRange(key, 0, -1)
	assert.Equal(t, errTooMuchBatchSize, err)
	_, err = db.ZRange(key, total-10, -1)
	assert.Nil(t, err)

	_, err = db.ZRangeByLex(key, nil, []byte(maxMem), common.RangeClose, 0, -1)
	assert.Equal(t, errTooMuchBatchSize, err)
	_, err = db.ZRangeByLex(key, nil, nil, common.RangeClose, 0, -1)
	assert.Equal(t, errTooMuchBatchSize, err)
	ay, err = db.ZRangeByLex(key, nil, nil, common.RangeClose, total-10, -1)
	assert.Nil(t, err)
	assert.Equal(t, 9, len(ay))

	r1 := fmt.Sprintf("%8d", 1)
	r2 := fmt.Sprintf("%8d", 2)
	// count = -1 , but have a small range should return the results
	ay, err = db.ZRangeByLex(key, []byte(r1), []byte(r2), common.RangeClose, 0, -1)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(ay))

	_, err = db.ZRangeByScore(key, common.MinScore, common.MaxScore, 0, -1)
	assert.Equal(t, errTooMuchBatchSize, err)
	elems, err = db.ZRangeByScore(key, common.MinScore, common.MaxScore, total-10, -1)
	assert.Nil(t, err)
	assert.Equal(t, 9, len(elems))
	elems, err = db.ZRangeByScore(key, 0, 1, 0, -1)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(elems))

	ay, err = db.ZRangeByLex(key, nil, []byte(maxMem), common.RangeClose, 0, MAX_BATCH_NUM-1)
	assert.Nil(t, err)
	assert.Equal(t, MAX_BATCH_NUM-1, len(ay))
}

func TestZLex(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	key := []byte("test:myzset")
	if _, err := db.ZAdd(0, key, common.ScorePair{Score: 0, Member: []byte("a")},
		common.ScorePair{Score: 0, Member: []byte("b")},
		common.ScorePair{Score: 0, Member: []byte("c")},
		common.ScorePair{Score: 0, Member: []byte("d")},
		common.ScorePair{Score: 0, Member: []byte("e")},
		common.ScorePair{Score: 0, Member: []byte("f")},
		common.ScorePair{Score: 0, Member: []byte("g")}); err != nil {
		t.Fatal(err)
	}

	if ay, err := db.ZRangeByLex(key, nil, []byte("c"), common.RangeClose, 0, -1); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(ay, [][]byte{[]byte("a"), []byte("b"), []byte("c")}) {
		t.Errorf("must equal a, b, c: %v", ay)
	}

	if ay, err := db.ZRangeByLex(key, nil, []byte("c"), common.RangeROpen, 0, -1); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(ay, [][]byte{[]byte("a"), []byte("b")}) {
		t.Errorf("must equal a, b: %v", ay)
	}

	if ay, err := db.ZRangeByLex(key, []byte("aaa"), []byte("g"), common.RangeROpen, 0, -1); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(ay, [][]byte{[]byte("b"),
		[]byte("c"), []byte("d"), []byte("e"), []byte("f")}) {
		t.Fatal("must equal b, c, d, e, f", fmt.Sprintf("%q", ay))
	}

	if n, err := db.ZLexCount(key, nil, nil, common.RangeClose); err != nil {
		t.Fatal(err)
	} else if n != 7 {
		t.Fatal(n)
	}

	if n, err := db.ZRemRangeByLex(0, key, []byte("aaa"), []byte("g"), common.RangeROpen); err != nil {
		t.Fatal(err)
	} else if n != 5 {
		t.Fatal(n)
	}

	if n, err := db.ZLexCount(key, nil, nil, common.RangeClose); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

}

func TestZKeyExists(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()
	key := []byte("test:zkeyexists_test")
	if n, err := db.ZKeyExists(key); err != nil {
		t.Fatal(err.Error())
	} else if n != 0 {
		t.Fatal("invalid value ", n)
	}

	db.ZAdd(0, key, common.ScorePair{Score: 0, Member: []byte("a")}, common.ScorePair{Score: 0, Member: []byte("b")})

	if n, err := db.ZKeyExists(key); err != nil {
		t.Fatal(err.Error())
	} else if n != 1 {
		t.Fatal("invalid value ", n)
	}
}
