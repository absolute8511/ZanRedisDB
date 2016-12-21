package rockredis

import (
	"os"
	"testing"
)

func TestSetCodec(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)

	key := []byte("test:key")
	member := []byte("member")

	ek := sEncodeSizeKey(key)
	if k, err := sDecodeSizeKey(ek); err != nil {
		t.Fatal(err)
	} else if string(k) != "test:key" {
		t.Fatal(string(k))
	}

	ek = sEncodeSetKey(key, member)
	if k, m, err := sDecodeSetKey(ek); err != nil {
		t.Fatal(err)
	} else if string(k) != "test:key" {
		t.Fatal(string(k))
	} else if string(m) != "member" {
		t.Fatal(string(m))
	}
}

func TestDBSet(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)

	key := []byte("test:testdb_set_a")
	member := []byte("member")
	key1 := []byte("test:testdb_set_a1")
	key2 := []byte("test:testdb_set_a2")
	member1 := []byte("testdb_set_m1")
	member2 := []byte("testdb_set_m2")

	if n, err := db.SAdd(key, member); err != nil {
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

	if n, err := db.SRem(key, member); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	db.SAdd(key1, member1, member2)

	if n, err := db.SClear(key1); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	db.SAdd(key1, member1, member2)
	db.SAdd(key2, member1, member2, []byte("xxx"))

	if n, _ := db.SCard(key2); n != 3 {
		t.Fatal(n)
	}
	if n, err := db.SMclear(key1, key2); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	db.SAdd(key2, member1, member2)
}

func TestSKeyExists(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	key := []byte("test:skeyexists_test")
	if n, err := db.SKeyExists(key); err != nil {
		t.Fatal(err.Error())
	} else if n != 0 {
		t.Fatal("invalid value ", n)
	}

	db.SAdd(key, []byte("hello"), []byte("world"))

	if n, err := db.SKeyExists(key); err != nil {
		t.Fatal(err.Error())
	} else if n != 1 {
		t.Fatal("invalid value ", n)
	}

}
