package rockredis

import (
	"os"
	"strconv"
	"testing"
)

func TestListCodec(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)

	key := []byte("test:key")

	ek := lEncodeMetaKey(key)
	if k, err := lDecodeMetaKey(ek); err != nil {
		t.Fatal(err)
	} else if string(k) != "test:key" {
		t.Fatal(string(k))
	}

	ek = lEncodeListKey(key, 1024)
	if k, seq, err := lDecodeListKey(ek); err != nil {
		t.Fatal(err)
	} else if string(k) != "test:key" {
		t.Fatal(string(k))
	} else if seq != 1024 {
		t.Fatal(seq)
	}
}

func TestListTrim(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)

	key := []byte("test:test_list_trim")

	init := func() {
		db.LClear(key)
		for i := 0; i < 100; i++ {
			n, err := db.RPush(key, []byte(strconv.Itoa(i)))
			if err != nil {
				t.Fatal(err)
			}
			if n != int64(i+1) {
				t.Fatal("length wrong")
			}
		}
	}

	init()

	err := db.LTrim(key, 0, 99)
	if err != nil {
		t.Fatal(err)
	}
	if l, _ := db.LLen(key); l != int64(100) {
		t.Fatal("wrong len:", l)
	}

	err = db.LTrim(key, 0, 50)
	if err != nil {
		t.Fatal(err)
	}
	if l, _ := db.LLen(key); l != int64(51) {
		t.Fatal("wrong len:", l)
	}
	for i := int64(0); i < 51; i++ {
		v, err := db.LIndex(key, i)
		if err != nil {
			t.Fatal(err)
		}
		if string(v) != strconv.Itoa(int(i)) {
			t.Fatal("wrong value")
		}
	}

	err = db.LTrim(key, 11, 30)
	if err != nil {
		t.Fatal(err)
	}
	if l, _ := db.LLen(key); l != int64(30-11+1) {
		t.Fatal("wrong len:", l)
	}
	for i := int64(11); i < 31; i++ {
		v, err := db.LIndex(key, i-11)
		if err != nil {
			t.Fatal(err)
		}
		if string(v) != strconv.Itoa(int(i)) {
			t.Fatal("wrong value")
		}
	}

	err = db.LTrim(key, 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	if l, _ := db.LLen(key); l != int64(30-11+1) {
		t.Fatal("wrong len:", l)
	}

	init()
	err = db.LTrim(key, -3, -3)
	if err != nil {
		t.Fatal(err)
	}
	if l, _ := db.LLen(key); l != int64(1) {
		t.Fatal("wrong len:", l)
	}
	v, err := db.LIndex(key, 0)
	if err != nil {
		t.Fatal(err)
	}
	if string(v) != "97" {
		t.Fatal("wrong value", string(v))
	}
	// TODO: LTrimFront, LTrimBack
}

func TestDBList(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)

	key := []byte("test:testdb_list_a")

	if n, err := db.RPush(key, []byte("1"), []byte("2"), []byte("3")); err != nil {
		t.Fatal(err)
	} else if n != 3 {
		t.Fatal(n)
	}

	if ay, err := db.LRange(key, 0, -1); err != nil {
		t.Fatal(err)
	} else if len(ay) != 3 {
		t.Fatal(ay)
	} else {
		for i := range ay {
			if ay[i][0] != '1'+byte(i) {
				t.Fatal(string(ay[i]))
			}
		}
	}

	if k, err := db.RPop(key); err != nil {
		t.Fatal(err)
	} else if string(k) != "3" {
		t.Fatal(string(k))
	}

	if k, err := db.LPop(key); err != nil {
		t.Fatal(err)
	} else if string(k) != "1" {
		t.Fatal(string(k))
	}

	if llen, err := db.LLen(key); err != nil {
		t.Fatal(err)
	} else if llen != 1 {
		t.Fatal(llen)
	}

	if num, err := db.LClear(key); err != nil {
		t.Fatal(err)
	} else if num != 1 {
		t.Error(num)
	}

	if llen, _ := db.LLen(key); llen != 0 {
		t.Fatal(llen)
	}
	// TODO: LSet
}

func TestLKeyExists(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	key := []byte("test:lkeyexists_test")
	if n, err := db.LKeyExists(key); err != nil {
		t.Fatal(err.Error())
	} else if n != 0 {
		t.Fatal("invalid value ", n)
	}
	db.LPush(key, []byte("hello"), []byte("world"))
	if n, err := db.LKeyExists(key); err != nil {
		t.Fatal(err.Error())
	} else if n != 1 {
		t.Fatal("invalid value ", n)
	}
}

func TestListPop(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)

	key := []byte("test:lpop_test")

	if v, err := db.LPop(key); err != nil {
		t.Fatal(err)
	} else if v != nil {
		t.Fatal(v)
	}

	if s, err := db.LLen(key); err != nil {
		t.Fatal(err)
	} else if s != 0 {
		t.Fatal(s)
	}

	for i := 0; i < 10; i++ {
		if n, err := db.LPush(key, []byte("a")); err != nil {
			t.Fatal(err)
		} else if n != 1+int64(i) {
			t.Fatal(n)
		}
	}

	if s, err := db.LLen(key); err != nil {
		t.Fatal(err)
	} else if s != 10 {
		t.Fatal(s)
	}

	for i := 0; i < 10; i++ {
		if _, err := db.LPop(key); err != nil {
			t.Fatal(err)
		}
	}

	if s, err := db.LLen(key); err != nil {
		t.Fatal(err)
	} else if s != 0 {
		t.Fatal(s)
	}

	if v, err := db.LPop(key); err != nil {
		t.Fatal(err)
	} else if v != nil {
		t.Fatal(v)
	}

}
