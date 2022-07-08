package rockredis

import (
	"fmt"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"

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

	ek = zEncodeSetKey([]byte("test"), []byte("key"), member)
	if tb, k, m, err := zDecodeSetKey(ek); err != nil {
		t.Fatal(err)
	} else if string(k) != "key" {
		t.Fatal(string(k))
	} else if string(m) != "member" {
		t.Fatal(string(m))
	} else if string(tb) != "test" {
		t.Fatal(string(tb))
	}

	ek = zEncodeScoreKey(false, false, []byte("test"), []byte("key"), member, 100)
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

	tn := time.Now().UnixNano()
	key := bin("test:testdb_zset_empty")
	n, err := db.ZAdd(tn, key, pair("a", 0), pair("b", 1),
		pair("c", 2), pair("", 3))
	assert.Nil(t, err)
	assert.Equal(t, int64(4), n)

	s, err := db.ZScore(key, bin("b"))
	assert.Nil(t, err)
	assert.Equal(t, float64(1), s)
	s, err = db.ZScore(key, bin(""))
	assert.Nil(t, err)
	assert.Equal(t, float64(3), s)

	n, err = db.ZCount(key, 0, 0xFF)
	assert.Nil(t, err)
	assert.Equal(t, int64(4), n)

	n, err = db.ZRem(tn, key, bin("a"), bin("b"))
	assert.Nil(t, err)
	assert.Equal(t, int64(2), n)

	n, err = db.ZCount(key, 0, 0xFF)
	assert.Nil(t, err)
	assert.Equal(t, int64(2), n)

	n, err = db.ZRem(tn, key, bin("a"), bin("b"))
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)

	n, err = db.ZCard(key)
	assert.Nil(t, err)
	assert.Equal(t, int64(2), n)

	n, err = db.ZClear(0, key)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)

	n, err = db.ZCount(key, 0, 0xFF)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)
	// test zrange, zrank, zscore for empty
	vals, err := db.ZRange(key, 0, 0xFF)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(vals))
	n, err = db.ZRank(key, []byte("a"))
	assert.Nil(t, err)
	assert.Equal(t, true, n < 0)
	_, err = db.ZScore(key, []byte("a"))
	assert.NotNil(t, err)
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

	s, err := db.ZScore(key, bin("d"))
	assert.Nil(t, err)
	assert.Equal(t, float64(3), s)
	_, err = db.ZScore(key, bin("zzz"))
	assert.Equal(t, errScoreMiss, err)

	n, err := db.ZCount(key, 0, 0xFF)
	assert.Nil(t, err)
	assert.Equal(t, int64(4), n)

	// test zadd mixed with the same score
	n, err = db.ZAdd(0, key, pair("a", 0), pair("b", 1),
		pair("c", 2), pair("d", 4))
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)

	s, err = db.ZScore(key, bin("d"))
	assert.Nil(t, err)
	assert.Equal(t, float64(4), s)

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
	if n, err := db.ZClear(0, key); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if n, err := db.ZCount(key, 0, 0XFF); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}
}

func TestDBZSetIncrby(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	key := bin("test:testdb_zset_incr")

	// {'a':0, 'b':1, 'c':2, 'd':3}
	if n, err := db.ZAdd(0, key, pair("a", 0), pair("b", 1)); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	s, err := db.ZScore(key, bin("b"))
	assert.Nil(t, err)
	assert.Equal(t, float64(1), s)
	_, err = db.ZScore(key, bin("c"))
	assert.Equal(t, errScoreMiss, err)

	s, err = db.ZIncrBy(0, key, 3, bin("b"))
	assert.Nil(t, err)
	assert.Equal(t, float64(4), s)
	s, err = db.ZIncrBy(0, key, 1, bin("c"))
	assert.Nil(t, err)
	assert.Equal(t, float64(1), s)
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

func TestZSetRange(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	// all range remove test
	key := []byte("test:zkey_zrange_test")
	mems := []common.ScorePair{
		common.ScorePair{1, []byte("a")},
		common.ScorePair{2, []byte("b")},
		common.ScorePair{3, []byte("c")},
		common.ScorePair{4, []byte("d")},
	}
	n, err := db.ZAdd(0, key, mems...)
	assert.Nil(t, err)
	assert.Equal(t, int64(len(mems)), n)

	vlist, err := db.ZRange(key, 0, 3)
	assert.Nil(t, err)
	assert.Equal(t, mems, vlist)

	vlist, err = db.ZRange(key, 1, 4)
	assert.Nil(t, err)
	assert.Equal(t, mems[1:], vlist)

	vlist, err = db.ZRange(key, -2, -1)
	assert.Nil(t, err)
	assert.Equal(t, mems[2:], vlist)

	vlist, err = db.ZRange(key, 0, -1)
	assert.Nil(t, err)
	assert.Equal(t, mems, vlist)

	vlist, err = db.ZRange(key, -1, -2)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(vlist))

	vlist, err = db.ZRevRange(key, 0, 4)
	assert.Nil(t, err)
	assert.Equal(t, len(mems), len(vlist))
	assert.Equal(t, mems[0], vlist[len(vlist)-1])

	vlist, err = db.ZRevRange(key, 0, -1)
	assert.Nil(t, err)
	assert.Equal(t, len(mems), len(vlist))
	assert.Equal(t, mems[len(mems)-1], vlist[0])

	vlist, err = db.ZRevRange(key, 2, 3)
	assert.Nil(t, err)
	assert.Equal(t, len(mems)-2, len(vlist))
	assert.Equal(t, mems[len(mems)-1-2], vlist[0])

	vlist, err = db.ZRevRange(key, -2, -1)
	assert.Nil(t, err)
	assert.Equal(t, len(mems)-2, len(vlist))
	assert.Equal(t, mems[1], vlist[0])
}

func TestZRemRange(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	// all range remove test
	key := []byte("test:zkey_range_rm_test")
	mems := []common.ScorePair{
		common.ScorePair{1, []byte("a")},
		common.ScorePair{2, []byte("b")},
		common.ScorePair{3, []byte("c")},
		common.ScorePair{4, []byte("d")},
		common.ScorePair{5, []byte("e")},
		common.ScorePair{6, []byte("f")},
	}
	n, err := db.ZAdd(0, key, mems...)
	assert.Nil(t, err)
	assert.Equal(t, int64(len(mems)), n)

	vlist, err := db.ZRange(key, 0, len(mems))
	assert.Nil(t, err)
	assert.Equal(t, mems, vlist)

	total := len(mems)
	n, err = db.ZRemRangeByRank(0, key, 2, 3)
	assert.Nil(t, err)
	assert.Equal(t, int64(2), n)
	total -= 2
	n, err = db.ZCard(key)
	assert.Nil(t, err)
	assert.Equal(t, int64(total), n)
	vlist, err = db.ZRange(key, 0, len(mems))
	assert.Nil(t, err)
	assert.Equal(t, mems[:2], vlist[:2])
	assert.Equal(t, mems[4:], vlist[2:])

	n, err = db.ZRem(0, key, mems[0].Member)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)
	total--
	n, err = db.ZCard(key)
	assert.Nil(t, err)
	assert.Equal(t, int64(total), n)
	vlist, err = db.ZRange(key, 0, len(mems))
	assert.Nil(t, err)
	assert.Equal(t, total, len(vlist))
	assert.Equal(t, mems[1], vlist[0])
	assert.Equal(t, mems[4:], vlist[1:])

	n, err = db.ZRemRangeByScore(0, key, 0, 3)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)
	total--

	n, err = db.ZRemRangeByLex(0, key, []byte("e"), []byte("e"), common.RangeClose)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)
	total--
	n, err = db.ZCard(key)
	assert.Nil(t, err)
	assert.Equal(t, int64(total), n)
	vlist, err = db.ZRange(key, 0, len(mems))
	assert.Nil(t, err)
	assert.Equal(t, total, len(vlist))
	assert.Equal(t, mems[5:], vlist)

	n, err = db.ZClear(0, key)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)

	n, err = db.ZCard(key)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)
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
	_, err = db.ZRangeByScore(key, 0, MAX_BATCH_NUM+1, 0, -1)
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

func TestDBZClearInCompactTTL(t *testing.T) {
	db := getTestDBWithCompactTTL(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	key := []byte("test:testdb_zset_clear_compact_a")
	member := []byte("member")
	memberNew := []byte("memberNew")

	ts := time.Now().UnixNano()
	db.ZAdd(ts, key, common.ScorePair{
		Score:  1,
		Member: member,
	})

	n, err := db.ZCard(key)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)

	ts = time.Now().UnixNano()
	n, err = db.ZClear(ts, key)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)

	n, err = db.ZCard(key)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)

	vlist, err := db.ZRange(key, 0, 100)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(vlist))
	vlist, err = db.ZRevRange(key, 0, 100)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(vlist))
	mlist, err := db.ZRangeByLex(key, nil, nil, common.RangeClose, 0, -1)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(mlist))
	vlist, err = db.ZRangeByScore(key, 0, 100, 0, -1)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(vlist))
	score, err := db.ZScore(key, member)
	assert.Equal(t, errScoreMiss, err)
	assert.Equal(t, float64(0), score)

	n, err = db.ZRank(key, member)
	assert.Nil(t, err)
	assert.Equal(t, int64(-1), n)
	n, err = db.ZRevRank(key, member)
	assert.Nil(t, err)
	assert.Equal(t, int64(-1), n)

	n, err = db.ZKeyExists(key)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)

	vlist, err = db.ZScan(key, []byte(""), -1, "", false)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(vlist))

	// renew
	ts = time.Now().UnixNano()
	db.ZAdd(ts, key, common.ScorePair{
		Score:  2,
		Member: memberNew,
	})

	n, err = db.ZCard(key)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)

	vlist, err = db.ZRange(key, 0, 100)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(vlist))
	assert.Equal(t, float64(2), vlist[0].Score)
	assert.Equal(t, memberNew, vlist[0].Member)
	vlist, err = db.ZRevRange(key, 0, 100)
	assert.Equal(t, 1, len(vlist))
	assert.Equal(t, float64(2), vlist[0].Score)
	assert.Equal(t, memberNew, vlist[0].Member)
	mlist, err = db.ZRangeByLex(key, nil, nil, common.RangeClose, 0, -1)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(mlist))
	vlist, err = db.ZRangeByScore(key, 0, 100, 0, -1)
	assert.Equal(t, 1, len(vlist))
	assert.Equal(t, float64(2), vlist[0].Score)
	assert.Equal(t, memberNew, vlist[0].Member)
	score, err = db.ZScore(key, member)
	assert.Equal(t, errScoreMiss, err)
	assert.Equal(t, float64(0), score)
	score, err = db.ZScore(key, memberNew)
	assert.Nil(t, err)
	assert.Equal(t, float64(2), score)

	n, err = db.ZRank(key, member)
	assert.Nil(t, err)
	assert.Equal(t, int64(-1), n)
	n, err = db.ZRevRank(key, member)
	assert.Nil(t, err)
	assert.Equal(t, int64(-1), n)
	n, err = db.ZRank(key, memberNew)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)
	n, err = db.ZRevRank(key, memberNew)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)

	n, err = db.ZKeyExists(key)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)

	vlist, err = db.ZScan(key, []byte(""), -1, "", false)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(vlist))
}

func BenchmarkZaddAndZRembyrange(b *testing.B) {
	db := getTestDBForBench()
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	// all range remove test
	key := []byte("test:zkey_range_rm_bench")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		member := common.ScorePair{
			Score:  float64(i),
			Member: []byte(strconv.Itoa(i)),
		}
		db.ZAdd(0, key, member)
	}

	for i := 0; i < b.N; i++ {
		db.ZRemRangeByRank(0, key, 1, 100)
		db.ZRemRangeByScore(0, key, 101, 201)
		db.ZRemRangeByLex(0, key, []byte("0"), []byte("100"), common.RangeClose)
	}
	b.StopTimer()
}
