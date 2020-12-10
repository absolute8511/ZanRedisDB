package rockredis

import (
	"os"
	"path"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/youzan/ZanRedisDB/common"
)

func convertRedisKeyToDBSKey(key []byte, member []byte) ([]byte, error) {
	table, rk, err := extractTableFromRedisKey(key)
	if err != nil {
		return nil, err
	}
	if err := checkCollKFSize(rk, member); err != nil {
		return nil, err
	}
	dbKey := sEncodeSetKey(table, rk, member)
	return dbKey, nil
}

func TestSetCodec(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	key := []byte("test:key")
	member := []byte("member")

	ek := sEncodeSizeKey(key)
	if k, err := sDecodeSizeKey(ek); err != nil {
		t.Fatal(err)
	} else if string(k) != "test:key" {
		t.Fatal(string(k))
	}

	ek, _ = convertRedisKeyToDBSKey(key, member)
	if tb, k, m, err := sDecodeSetKey(ek); err != nil {
		t.Fatal(err)
	} else if string(k) != "key" {
		t.Fatal(string(k))
	} else if string(m) != "member" {
		t.Fatal(string(m))
	} else if string(tb) != "test" {
		t.Fatal(string(tb))
	}
}

func TestDBSetWithEmptyMember(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	key := []byte("test:testdb_set_empty")
	member := []byte("")

	if n, err := db.SAdd(0, key, member); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if cnt, err := db.SCard(key); err != nil {
		t.Fatal(err)
	} else if cnt != 1 {
		t.Fatal(cnt)
	}

	if n, err := db.SIsMember(key, member); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if v, err := db.SMembers(key); err != nil {
		t.Fatal(err)
	} else if string(v[0]) != string(member) {
		t.Fatal(string(v[0]))
	}

	if n, err := db.SRem(0, key, member); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if n, err := db.SIsMember(key, member); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	if v, err := db.SMembers(key); err != nil {
		t.Fatal(err)
	} else if len(v) != 0 {
		t.Fatal(string(v[0]))
	}
	n, err := db.SCard(key)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)
}

func TestDBSet(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	key := []byte("test:testdb_set_a")
	member := []byte("member")
	key1 := []byte("test:testdb_set_a1")
	key2 := []byte("test:testdb_set_a2")
	member1 := []byte("testdb_set_m1")
	member2 := []byte("testdb_set_m2")

	if n, err := db.SAdd(0, key, member); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}
	tbcnt, err := db.GetTableKeyCount([]byte("test"))
	assert.Nil(t, err)
	assert.Equal(t, int64(1), tbcnt)
	n, err := db.SAdd(0, key, member)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)

	if cnt, err := db.SCard(key); err != nil {
		t.Fatal(err)
	} else if cnt != 1 {
		t.Fatal(cnt)
	}

	if n, err := db.SIsMember(key, member); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if v, err := db.SMembers(key); err != nil {
		t.Fatal(err)
	} else if string(v[0]) != "member" {
		t.Fatal(string(v[0]))
	}

	if n, err := db.SRem(0, key, member); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	db.SAdd(0, key1, member1, member2)

	tbcnt, err = db.GetTableKeyCount([]byte("test"))
	assert.Nil(t, err)
	assert.Equal(t, int64(1), tbcnt)
	if n, err := db.SClear(0, key1); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	tbcnt, err = db.GetTableKeyCount([]byte("test"))
	assert.Nil(t, err)
	assert.Equal(t, int64(0), tbcnt)
	db.SAdd(0, key1, member1, member2)
	db.SAdd(0, key2, member1, member2, []byte("xxx"))

	if n, _ := db.SCard(key2); n != 3 {
		t.Fatal(n)
	}

	tbcnt, err = db.GetTableKeyCount([]byte("test"))
	assert.Nil(t, err)
	assert.Equal(t, int64(2), tbcnt)
	if n, err := db.SMclear(key1, key2); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	n, err = db.SCard(key1)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)
	n, err = db.SCard(key2)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)

	tbcnt, err = db.GetTableKeyCount([]byte("test"))
	assert.Nil(t, err)
	assert.Equal(t, int64(0), tbcnt)
	db.SAdd(0, key2, member1, member2)

	tbcnt, err = db.GetTableKeyCount([]byte("test"))
	assert.Nil(t, err)
	assert.Equal(t, int64(1), tbcnt)

	n, err = db.SCard(key1)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)
	n, err = db.SCard(key2)
	assert.Nil(t, err)
	assert.Equal(t, int64(2), n)
}

func TestDBSetClearInCompactTTL(t *testing.T) {
	db := getTestDBWithCompactTTL(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	key := []byte("test:testdb_set_clear_compact_a")
	member := []byte("member")
	memberNew := []byte("memberNew")
	key1 := []byte("test:testdb_set_clear_compact_a1")
	member1 := []byte("testdb_set_m1")
	member2 := []byte("testdb_set_m2")

	ts := time.Now().UnixNano()
	db.SAdd(ts, key, member)
	db.SAdd(ts, key, member)

	n, err := db.SCard(key)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)

	n, err = db.SIsMember(key, member)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)

	v, err := db.SMembers(key)
	assert.Nil(t, err)
	assert.Equal(t, member, v[0])

	ts = time.Now().UnixNano()
	n, err = db.SClear(ts, key)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)

	n, err = db.SCard(key)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)

	n, err = db.SIsMember(key, member)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)

	v, err = db.SMembers(key)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(v))

	// renew
	ts = time.Now().UnixNano()
	db.SAdd(ts, key, memberNew)
	n, err = db.SCard(key)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)

	n, err = db.SIsMember(key, member)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)
	n, err = db.SIsMember(key, memberNew)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)

	v, err = db.SMembers(key)
	assert.Nil(t, err)
	assert.Equal(t, memberNew, v[0])

	ts = time.Now().UnixNano()
	db.SAdd(ts, key1, member1, member2)

	ts = time.Now().UnixNano()
	n, err = db.SClear(ts, key1)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)

	n, err = db.SCard(key1)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)

	n, err = db.SIsMember(key1, member1)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)

	v, err = db.SMembers(key1)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(v))

	n, err = db.SCard(key1)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)

	ts = time.Now().UnixNano()
	db.SAdd(ts, key1, member, memberNew)

	n, err = db.SCard(key1)
	assert.Nil(t, err)
	assert.Equal(t, int64(2), n)

	n, err = db.SIsMember(key1, member1)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)
	n, err = db.SIsMember(key1, member2)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)
	n, err = db.SIsMember(key1, member)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)
	n, err = db.SIsMember(key1, memberNew)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)

	v, err = db.SMembers(key1)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(v))
}

func TestSKeyExists(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()
	key := []byte("test:skeyexists_test")
	if n, err := db.SKeyExists(key); err != nil {
		t.Fatal(err.Error())
	} else if n != 0 {
		t.Fatal("invalid value ", n)
	}

	db.SAdd(0, key, []byte("hello"), []byte("world"))

	if n, err := db.SKeyExists(key); err != nil {
		t.Fatal(err.Error())
	} else if n != 1 {
		t.Fatal("invalid value ", n)
	}
}

func TestDBSPop(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()
	key := []byte("test:spop_test")
	if vals, err := db.SPop(0, key, 1); err != nil {
		t.Fatal(err.Error())
	} else if len(vals) != 0 {
		t.Fatal("invalid value ", vals)
	}

	db.SAdd(0, key, []byte("hello"), []byte("world"), []byte("hello2"))

	if vals, err := db.SPop(0, key, 1); err != nil {
		t.Fatal(err.Error())
	} else if len(vals) != 1 {
		t.Fatal("invalid value ", vals)
	}

	if vals, err := db.SPop(0, key, 3); err != nil {
		t.Fatal(err.Error())
	} else if len(vals) != 2 {
		t.Fatal("invalid value ", vals)
	}

	if vals, err := db.SPop(0, key, 1); err != nil {
		t.Fatal(err.Error())
	} else if len(vals) != 0 {
		t.Fatal("invalid value ", vals)
	}
	if vals, _ := db.SMembers(key); len(vals) != 0 {
		t.Errorf("should empty set")
	}

	n, err := db.SCard(key)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)
}

func BenchmarkSIsMember(b *testing.B) {
	db := getTestDBForBench()
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()
	key := []byte("test:sismember_bench")

	for i := 0; i < b.N+1000; i++ {
		if i%2 == 0 {
			continue
		}
		db.SAdd(0, key, []byte("hello"+strconv.Itoa(i)))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.SIsMember(key, []byte("hello"+strconv.Itoa(i)))
	}
	b.StopTimer()
}

func BenchmarkSAddAndSPop(b *testing.B) {
	db := getTestDBForBench()
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()
	key := []byte("test:spop_bench")

	b.ResetTimer()

	stopC := make(chan bool)
	go func() {
		tmp := path.Join(db.cfg.DataDir, "snapshot")
		os.MkdirAll(tmp, common.DIR_PERM)
		for i := 0; ; i++ {
			db.SIsMember(key, []byte("hello"+strconv.Itoa(i%b.N)))
			db.SMembers(key)
			ck, _ := db.rockEng.NewCheckpoint()
			ck.Save(tmp, nil)
			select {
			case <-stopC:
				return
			default:
			}
		}
	}()
	for i := 0; i < b.N+1000; i++ {
		db.SAdd(0, key, []byte("hello"+strconv.Itoa(i)))
		if i%121 == 0 {
			db.SPop(0, key, 100)
		}
	}

	for i := 0; i < b.N; i++ {
		db.SPop(0, key, 100)
	}
	close(stopC)
	b.StopTimer()
}
