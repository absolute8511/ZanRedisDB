package rockredis

import (
	"os"
	"testing"
)

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

	if n, err := db.SClear(key1); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	db.SAdd(0, key1, member1, member2)
	db.SAdd(0, key2, member1, member2, []byte("xxx"))

	if n, _ := db.SCard(key2); n != 3 {
		t.Fatal(n)
	}
	if n, err := db.SMclear(key1, key2); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	db.SAdd(0, key2, member1, member2)
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
}
