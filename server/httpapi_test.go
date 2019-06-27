package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/siddontang/goredis"
	"github.com/stretchr/testify/assert"
	"github.com/youzan/ZanRedisDB/common"
	"github.com/youzan/ZanRedisDB/node"
)

func insertData(t *testing.T, c *goredis.PoolConn, cnt int, cmd, prefixkey string, args ...interface{}) {
	for i := 0; i < cnt; i++ {
		nargs := make([]interface{}, 0, len(args)+1)
		nargs = append(nargs, prefixkey+fmt.Sprintf("%04d", i))
		nargs = append(nargs, args...)
		if _, err := c.Do(cmd, nargs...); err != nil {
			t.Fatal(err)
		}
	}
}

func checkScanKeys(t *testing.T, c *goredis.PoolConn, prefix string, dt string, expectCnt int) {
	if ay, err := goredis.Values(c.Do("ADVSCAN", prefix, dt, "COUNT", 1000)); err != nil {
		t.Fatal(err)
	} else if len(ay) != 2 {
		t.Fatal(len(ay))
	} else {
		a, err := goredis.Strings(ay[1], nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(a) != expectCnt {
			t.Errorf("data %v want %v get %v", dt, expectCnt, len(a))
		}
	}
}

func checkFullScan(t *testing.T, c *goredis.PoolConn, prefix string, dt string, expectCnt int) {
	if ay, err := goredis.Values(c.Do("FULLSCAN", prefix, dt, "COUNT", 1000)); err != nil {
		t.Fatal(err)
	} else if len(ay) != 2 {
		t.Fatal(len(ay))
	} else {
		a, err := goredis.MultiBulk(ay[1], nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(a) != expectCnt {
			t.Errorf("data %v want %v get %v", dt, expectCnt, len(a))
		}
	}
}

func deleteTableRange(t *testing.T, table string, start []byte, end []byte) {
	var dr node.DeleteTableRange
	dr.Table = table
	dr.StartFrom = start
	dr.EndTo = end
	dr.DeleteAll = true
	buf, _ := json.Marshal(dr)
	url := fmt.Sprintf("http://127.0.0.1:%v/kv/delrange/default/%s", redisportMerge+1, table)
	rsp, err := http.Post(url, "json", bytes.NewBuffer(buf))
	if err != nil {
		t.Error(err)
	}
	if rsp.StatusCode != http.StatusOK {
		t.Error(rsp.Status)
	}
}

func TestDeleteRangeCrossTable(t *testing.T) {
	c := getMergeTestConn(t)
	defer c.Close()

	prefixkv1 := "default:testdeleterange_kv:"
	prefixkv2 := "default:testdeleterange_kv1:"
	prefixhash1 := "default:testdeleterange_hash:"
	prefixhash2 := "default:testdeleterange_hash1:"
	prefixlist1 := "default:testdeleterange_list:"
	prefixlist2 := "default:testdeleterange_list1:"
	prefixset1 := "default:testdeleterange_set:"
	prefixset2 := "default:testdeleterange_set1:"
	prefixzset1 := "default:testdeleterange_zset:"
	prefixzset2 := "default:testdeleterange_zset1:"
	tableCnt := 20
	// insert kv
	insertData(t, c, tableCnt, "set", prefixkv1, []byte("value"))
	insertData(t, c, tableCnt, "set", prefixkv2, []byte("value"))

	insertData(t, c, tableCnt, "hset", prefixhash1, "0", []byte("value"))
	insertData(t, c, tableCnt, "hset", prefixhash2, "0", []byte("value"))

	insertData(t, c, tableCnt, "sadd", prefixset1, []byte("value"))
	insertData(t, c, tableCnt, "sadd", prefixset2, []byte("value"))

	insertData(t, c, tableCnt, "lpush", prefixlist1, []byte("value"))
	insertData(t, c, tableCnt, "lpush", prefixlist2, []byte("value"))

	insertData(t, c, tableCnt, "zadd", prefixzset1, "0", []byte("value"))
	insertData(t, c, tableCnt, "zadd", prefixzset2, "0", []byte("value"))

	// scan deleted range should be zero
	// scan non-deleted range (other types, other range in same type) should be the same before delete
	midKey := fmt.Sprintf("%04d", tableCnt/2)
	checkScanKeys(t, c, prefixkv1, "KV", tableCnt)
	checkFullScan(t, c, prefixkv1, "KV", tableCnt)
	deleteTableRange(t, "testdeleterange_kv", nil, nil)
	checkScanKeys(t, c, prefixkv1, "KV", 0)
	checkFullScan(t, c, prefixkv1, "KV", 0)

	checkScanKeys(t, c, prefixkv2, "KV", tableCnt)
	checkFullScan(t, c, prefixkv2, "KV", tableCnt)
	deleteTableRange(t, "testdeleterange_kv1", nil, []byte(midKey))
	checkScanKeys(t, c, prefixkv2, "KV", tableCnt/2)
	checkFullScan(t, c, prefixkv2, "KV", tableCnt/2)

	checkScanKeys(t, c, prefixhash1, "HASH", tableCnt)
	checkFullScan(t, c, prefixhash1, "HASH", tableCnt)
	deleteTableRange(t, "testdeleterange_hash", nil, nil)
	checkScanKeys(t, c, prefixhash1, "hash", 0)
	checkFullScan(t, c, prefixhash1, "hash", 0)

	checkScanKeys(t, c, prefixhash2, "HASH", tableCnt)
	checkFullScan(t, c, prefixhash2, "HASH", tableCnt)
	deleteTableRange(t, "testdeleterange_hash1", nil, []byte(midKey))
	checkScanKeys(t, c, prefixhash2, "HASH", tableCnt/2)
	checkFullScan(t, c, prefixhash2, "HASH", tableCnt/2)

	checkScanKeys(t, c, prefixlist1, "list", tableCnt)
	checkFullScan(t, c, prefixlist1, "list", tableCnt)
	deleteTableRange(t, "testdeleterange_list", nil, nil)
	checkScanKeys(t, c, prefixlist1, "list", 0)
	checkFullScan(t, c, prefixlist1, "list", 0)

	checkScanKeys(t, c, prefixlist2, "list", tableCnt)
	checkFullScan(t, c, prefixlist2, "list", tableCnt)
	deleteTableRange(t, "testdeleterange_list1", nil, []byte(midKey))
	checkScanKeys(t, c, prefixlist2, "list", tableCnt/2)
	checkFullScan(t, c, prefixlist2, "list", tableCnt/2)

	checkScanKeys(t, c, prefixset1, "set", tableCnt)
	checkFullScan(t, c, prefixset1, "set", tableCnt)
	deleteTableRange(t, "testdeleterange_set", nil, nil)
	checkScanKeys(t, c, prefixset1, "set", 0)
	checkFullScan(t, c, prefixset1, "set", 0)

	checkScanKeys(t, c, prefixset2, "set", tableCnt)
	checkFullScan(t, c, prefixset2, "set", tableCnt)
	deleteTableRange(t, "testdeleterange_set1", nil, []byte(midKey))
	checkScanKeys(t, c, prefixset2, "set", tableCnt/2)
	checkFullScan(t, c, prefixset2, "set", tableCnt/2)

	checkScanKeys(t, c, prefixzset1, "zset", tableCnt)
	checkFullScan(t, c, prefixzset1, "zset", tableCnt)
	deleteTableRange(t, "testdeleterange_zset", nil, nil)
	checkScanKeys(t, c, prefixzset1, "zset", 0)
	checkFullScan(t, c, prefixzset1, "zset", 0)

	checkScanKeys(t, c, prefixzset2, "zset", tableCnt)
	checkFullScan(t, c, prefixzset2, "zset", tableCnt)
	deleteTableRange(t, "testdeleterange_zset1", nil, []byte(midKey))
	checkScanKeys(t, c, prefixzset2, "zset", tableCnt/2)
	checkFullScan(t, c, prefixzset2, "zset", tableCnt/2)
}

func TestMarshalRaftStats(t *testing.T) {
	c := getMergeTestConn(t)
	defer c.Close()
	uri := fmt.Sprintf("http://127.0.0.1:%v/raft/stats?leader_only=true",
		redisportMerge+1)
	rstat := make([]*RaftStatus, 0)
	sc, err := common.APIRequest("GET", uri, nil, time.Second*3, &rstat)
	if err != nil {
		t.Errorf("request %v error: %v", uri, err)
		return
	}
	if sc != http.StatusOK {
		t.Errorf("request %v error: %v", uri, sc)
		return
	}
	if len(rstat) == 0 {
		t.Errorf("get raft stats %v empty !!!", rstat)
		return
	}
	d, _ := json.Marshal(rstat)
	t.Logf("%v =======", string(d))
}

func TestSetGetDynamicConf(t *testing.T) {
	c := getMergeTestConn(t)
	defer c.Close()
	// empty str conf
	uriGet := fmt.Sprintf("http://127.0.0.1:%v/conf/get?type=str&key=test_str",
		redisportMerge+1)

	type strConf struct {
		Key   string
		Value string
	}
	resp := strConf{}
	sc, err := common.APIRequest("GET", uriGet, nil, time.Second*3, &resp)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusOK, sc)
	assert.Equal(t, "test_str", resp.Key)
	assert.Equal(t, "test_str", resp.Value)

	uriSet := fmt.Sprintf("http://127.0.0.1:%v/conf/set?type=str&key=test_str&value=",
		redisportMerge+1)

	sc, err = common.APIRequest("POST", uriSet, nil, time.Second*3, nil)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusOK, sc)

	sc, err = common.APIRequest("GET", uriGet, nil, time.Second*3, &resp)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusOK, sc)
	assert.Equal(t, "test_str", resp.Key)
	assert.Equal(t, "", resp.Value)

	type intConf struct {
		Key   string `json:"key,omitempty"`
		Value int    `json:"value,omitempty"`
	}
	// change int conf
	uriGet = fmt.Sprintf("http://127.0.0.1:%v/conf/get?type=int&key=empty_int",
		redisportMerge+1)
	respInt := intConf{}
	sc, err = common.APIRequest("GET", uriGet, nil, time.Second*3, &respInt)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusOK, sc)
	assert.Equal(t, "empty_int", respInt.Key)
	assert.Equal(t, 0, respInt.Value)
	uriSet = fmt.Sprintf("http://127.0.0.1:%v/conf/set?type=int&key=empty_int&value=10",
		redisportMerge+1)
	sc, err = common.APIRequest("POST", uriSet, nil, time.Second*3, nil)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusOK, sc)
	sc, err = common.APIRequest("GET", uriGet, nil, time.Second*3, &respInt)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusOK, sc)
	assert.Equal(t, "empty_int", respInt.Key)
	assert.Equal(t, 10, respInt.Value)

	// set not exist int/str conf
	uriSet = fmt.Sprintf("http://127.0.0.1:%v/conf/set?type=int&key=noexist_int&value=10",
		redisportMerge+1)
	sc, err = common.APIRequest("POST", uriSet, nil, time.Second*3, nil)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusOK, sc)
	uriGet = fmt.Sprintf("http://127.0.0.1:%v/conf/get?type=int&key=noexist_int",
		redisportMerge+1)
	sc, err = common.APIRequest("GET", uriGet, nil, time.Second*3, &respInt)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusOK, sc)
	assert.Equal(t, "noexist_int", respInt.Key)
	assert.Equal(t, 0, respInt.Value)

	uriSet = fmt.Sprintf("http://127.0.0.1:%v/conf/set?type=str&key=noexist_str&value=nostr",
		redisportMerge+1)
	sc, err = common.APIRequest("POST", uriSet, nil, time.Second*3, nil)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusOK, sc)
	uriGet = fmt.Sprintf("http://127.0.0.1:%v/conf/get?type=str&key=noexist_str",
		redisportMerge+1)
	sc, err = common.APIRequest("GET", uriGet, nil, time.Second*3, &resp)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusOK, sc)
	assert.Equal(t, "noexist_str", resp.Key)
	assert.Equal(t, "nostr", resp.Value)
}
