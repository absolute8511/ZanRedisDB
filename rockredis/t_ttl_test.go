package rockredis

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"testing"
	"time"

	"github.com/youzan/ZanRedisDB/common"
)

func getTestDBWithExpirationPolicy(t *testing.T, ePolicy common.ExpirationPolicy) *RockDB {
	cfg := NewRockRedisDBConfig()
	cfg.ExpirationPolicy = ePolicy
	cfg.EnableTableCounter = true

	var err error
	cfg.DataDir, err = ioutil.TempDir("", fmt.Sprintf("rockredis-test-%d", time.Now().UnixNano()))
	if err != nil {
		t.Fatal(err)
	}

	testDB, err := OpenRockDB(cfg)
	if err != nil {
		t.Fatal(err.Error())
	}
	return testDB
}

func TestTTLCodec(t *testing.T) {
	ts := time.Now().Unix()

	key := []byte("kv-test-key")

	tk := expEncodeTimeKey(KVType, key, ts)

	if dt, k, when, err := expDecodeTimeKey(tk); err != nil {
		t.Fatal(err)
	} else if !(dt == KVType && bytes.Equal(k, key) && when == ts) {
		t.Fatal("expire time key encode and decode do not match")
	}

	mk := expEncodeMetaKey(KVType, key)
	if dt, k, err := expDecodeMetaKey(mk); err != nil {
		t.Fatal(err)
	} else if !(dt == KVType && bytes.Equal(k, key)) {
		t.Fatal("expire meta key encode and decode do not match")
	}
}
