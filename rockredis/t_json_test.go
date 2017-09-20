package rockredis

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func TestJSONGetSet(t *testing.T) {
	r, err := sjson.Set("{}", "1", "1v")
	assert.Nil(t, err)
	t.Log(r)
	ret := gjson.Get(r, "1")
	t.Log(ret)
	assert.Equal(t, "1v", ret.String())
	r, err = sjson.Set(r, "1", 11)
	assert.Nil(t, err)
	t.Log(r)
	assert.Equal(t, `{"1":11}`, r)
	ret = gjson.Get(r, "1")
	t.Log(ret)
	assert.Equal(t, int64(11), ret.Int())
	r, err = sjson.Set(r, "2.-1", "arr1")
	assert.Nil(t, err)
	t.Log(r)
	ret = gjson.Get(r, "2")
	t.Log(ret)
	assert.Equal(t, 1, len(ret.Array()))
	assert.Equal(t, "arr1", ret.Array()[0].String())
	r, err = sjson.Set(r, "2.0", "arr0")
	assert.Nil(t, err)
	t.Log(r)
	ret = gjson.Get(r, "2")
	t.Log(ret)
	assert.Equal(t, 1, len(ret.Array()))
	assert.Equal(t, "arr0", ret.Array()[0].String())

	r, err = sjson.SetRaw(r, "3.-1", `{"31":"31v"}`)
	assert.Nil(t, err)
	t.Log(r)
	ret = gjson.Get(r, "3.0.31")
	t.Log(ret)
	assert.Equal(t, "31v", ret.String())
	r, err = sjson.Set(`{"1":[1]}`, "1.-1", 3)
	t.Log(r)
	assert.Equal(t, 2, len(gjson.Get(r, "1").Array()))
	r, err = sjson.SetRaw(`{"1":"1v"}`, "1", `["changed"]`)
	t.Log(r)
	assert.Nil(t, err)
	ret = gjson.Get(r, "1")
	t.Log(ret)
	assert.Equal(t, 1, len(ret.Array()))
	r, err = sjson.SetRaw(r, "age", "18")
	t.Log(r)
	assert.Nil(t, err)
	assert.Equal(t, int64(18), gjson.Get(r, "age").Int())
	// append array
	t.Log(r)
	r, err = sjson.Set(r, "1.2", "20")
	t.Log(r)
	assert.Nil(t, err)
	arrs := gjson.Get(r, "1").Array()
	assert.Equal(t, 3, len(arrs))
	assert.Equal(t, "changed", arrs[0].String())
	assert.Equal(t, nil, arrs[1].Value())
	assert.Equal(t, "20", arrs[2].String())
	r, err = sjson.Set(r, "3", "20")
	assert.Nil(t, err)
	t.Log(r)
	assert.Equal(t, "20", gjson.Get(r, "3").String())

	ret = gjson.Get(`{"1":"1v"}`, "1")
	assert.Equal(t, "1v", ret.String())
	json := `{"1":{"11":"11v"}, "2":[21, 22]}`
	rets := gjson.GetMany(json, "1.11", "2.1", "2")
	assert.Equal(t, 3, len(rets))

	t.Log(rets)
	assert.Equal(t, "11v", rets[0].String())
	assert.Equal(t, int64(22), rets[1].Int())
	assert.Equal(t, 2, len(rets[2].Array()))
}

func TestValidJSON(t *testing.T) {
	assert.True(t, gjson.Valid(`{
        "address": {
            "street": "2 Avenue",
            "zipcode": "10075",
            "building": "1480",
            "coord": [-73.9557413, 40.7720266]
        },
        "borough": "Manhattan",
        "cuisine": "Italian",
        "grades": [
            {
                "date": "2014-10-01",
                "grade": "A",
                "score": 11
            },
            {
                "date": "2014-01-16",
                "grade": "B",
                "score": 17
            }
        ],
        "name": "Vella",
        "restaurant_id": "41704620"
    }`))
}
func TestJSONArray(t *testing.T) {
	r := `[1, 2]`
	r, err := sjson.SetRaw(r, "-1", `{"3":[]}`)
	assert.Nil(t, err)
	t.Log(r)
	r, err = sjson.SetRaw(r, "-1", "4")
	assert.Nil(t, err)
	t.Log(r)
	ret := gjson.Get(r, "#")
	assert.Equal(t, int64(4), ret.Int())
	r, err = sjson.SetRaw(r, "2.3.-1", "31")
	assert.Nil(t, err)
	t.Log(r)
	ret = gjson.Get(r, "2.3.0")
	assert.Equal(t, int64(31), ret.Int())
}

func TestJSONCodec(t *testing.T) {
	table := []byte("test")
	key := []byte("key")
	ek, err := encodeJSONKey(table, key)
	assert.Nil(t, err)
	tt, k, err := decodeJSONKey(ek)
	assert.Nil(t, err)
	assert.Equal(t, table, tt)
	assert.Equal(t, key, k)
}

func TestDBJSON(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	key := []byte("test:testdb_json_a")

	n, err := db.JSet(0, key, []byte("a"), []byte("hello world 1"))
	assert.NotNil(t, err)

	n, err = db.JSet(0, key, []byte("a"), []byte(`"hello world 1"`))
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)

	n, err = db.JSet(0, key, []byte("b"), []byte(`"hello world 2"`))
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)

	v1, err := db.JGet(key, []byte("a"))
	assert.Nil(t, err)
	assert.Equal(t, 1, len(v1))
	typeStr, err := db.JType(key, []byte("a"))
	assert.Nil(t, err)
	assert.Equal(t, "string", typeStr)

	v2, err := db.JGet(key, []byte("b"))
	assert.Nil(t, err)
	assert.Equal(t, 1, len(v2))
	assert.Equal(t, "hello world 1", v1[0])
	assert.Equal(t, "hello world 2", v2[0])
	typeStr, err = db.JType(key, []byte("b"))
	assert.Nil(t, err)
	assert.Equal(t, "string", typeStr)
	typeStr, err = db.JType(key, []byte("c"))
	assert.Nil(t, err)
	assert.Equal(t, "null", typeStr)

	ay, err := db.JGet(key, []byte("a"), []byte("b"))
	assert.Nil(t, err)
	assert.Equal(t, 2, len(ay))

	assert.Equal(t, "hello world 1", ay[0])
	assert.Equal(t, "hello world 2", ay[1])
	ay, err = db.JGet(key, []byte(""), []byte("a"))
	assert.Nil(t, err)
	assert.Equal(t, 2, len(ay))
	assert.Equal(t, "{\"b\":\"hello world 2\",\"a\":\"hello world 1\"}", ay[0])
	assert.Equal(t, "hello world 1", ay[1])
	typeStr, err = db.JType(key, []byte(""))
	assert.Nil(t, err)
	assert.Equal(t, "object", typeStr)

	n, err = db.JSet(0, key, []byte("c"), []byte(ay[0]))
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)

	ay, err = db.JGet(key, []byte("c.b"), []byte("c.a"))
	assert.Nil(t, err)
	assert.Equal(t, 2, len(ay))
	assert.Equal(t, "hello world 2", ay[0])
	assert.Equal(t, "hello world 1", ay[1])

	l, err := db.JObjLen(key, []byte(""))
	assert.Nil(t, err)
	assert.Equal(t, int64(3), l)

	keys, err := db.JObjKeys(key, []byte(""))
	assert.Nil(t, err)
	assert.Equal(t, 3, len(keys))

	t.Log(keys)
	keysMap := make(map[string]bool)
	for _, k := range keys {
		keysMap[k] = true
	}
	_, ok := keysMap["a"]
	assert.True(t, ok)
	_, ok = keysMap["b"]
	assert.True(t, ok)
	_, ok = keysMap["c"]
	assert.True(t, ok)
	keys, err = db.JObjKeys(key, []byte("c"))
	assert.Nil(t, err)
	assert.Equal(t, 2, len(keys))
	keysMap = make(map[string]bool)
	for _, k := range keys {
		keysMap[k] = true
	}
	_, ok = keysMap["a"]
	assert.True(t, ok)
	_, ok = keysMap["b"]
	assert.True(t, ok)

	typeStr, err = db.JType(key, []byte("c"))
	assert.Nil(t, err)
	assert.Equal(t, "object", typeStr)
}

func TestJSONKeyExists(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()
	key := []byte("test:jsonkeyexists_test")
	v, err := db.JKeyExists(key)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), v)

	_, err = db.JSet(0, key, []byte("hello"), []byte(`"world"`))
	assert.Nil(t, err)

	v, err = db.JKeyExists(key)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), v)

	_, err = db.JSet(0, key, []byte("hello2"), []byte(`"world2"`))
	assert.Nil(t, err)
	db.JDel(0, key, []byte("hello"))
	v, err = db.JKeyExists(key)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), v)
	db.JDel(0, key, []byte(""))
	v, err = db.JKeyExists(key)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), v)
}

func TestDBJSONKeyDelete(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()
	key := []byte("test:jsonkey_del_test")

	n, err := db.JDel(0, key, []byte("b"))
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)

	n, err = db.JSet(0, key, []byte("a"), []byte(`"hello world 1"`))
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)

	n, err = db.JSet(0, key, []byte("b"), []byte(`"hello world 2"`))
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)
	n, err = db.JDel(0, key, []byte("b"))
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)
	n, err = db.JDel(0, key, []byte("b"))
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)
}

func TestDBJSONKeyArrayOp(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()
	key := []byte("test:jsonkey_array_test")

	n, err := db.JSet(0, key, []byte("a"), []byte(`"hello world 1"`))
	assert.Nil(t, err)
	assert.Equal(t, int64(1), n)

	_, err = db.JArrayAppend(0, key, []byte("a"), []byte("1"))
	assert.NotNil(t, err)
	assert.Equal(t, errJSONPathNotArray.Error(), err.Error())

	n, err = db.JArrayLen(key, []byte(""))
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)
	n, err = db.JArrayLen(key, []byte("a"))
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)

	typeStr, err := db.JType(key, []byte(""))
	assert.Nil(t, err)
	assert.Equal(t, "object", typeStr)

	n, err = db.JSet(0, key, []byte("b"), []byte(`[1, 2]`))
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)

	n, err = db.JArrayLen(key, []byte("b"))
	assert.Nil(t, err)
	assert.Equal(t, int64(2), n)

	typeStr, err = db.JType(key, []byte("b"))
	assert.Nil(t, err)
	assert.Equal(t, "array", typeStr)
	// append, del, pop
	n, err = db.JArrayAppend(0, key, []byte("b"), []byte("4"))
	assert.Nil(t, err)
	assert.Equal(t, int64(3), n)
	n, err = db.JArrayLen(key, []byte("b"))
	assert.Nil(t, err)
	assert.Equal(t, int64(3), n)

	poped, err := db.JArrayPop(0, key, []byte("b"))
	assert.Nil(t, err)
	assert.Equal(t, "4", poped)
	poped, err = db.JArrayPop(0, key, []byte("a"))
	assert.NotNil(t, err)
	assert.Equal(t, errJSONPathNotArray.Error(), err.Error())

	n, err = db.JArrayAppend(0, key, []byte("b"), []byte("true"), []byte(`"str"`), []byte(`"10"`))
	assert.Nil(t, err)
	assert.Equal(t, int64(5), n)
	n, err = db.JArrayLen(key, []byte("b"))
	assert.Nil(t, err)
	assert.Equal(t, int64(5), n)

	poped, err = db.JArrayPop(0, key, []byte("b"))
	assert.Nil(t, err)
	assert.Equal(t, "10", poped)
	poped, err = db.JArrayPop(0, key, []byte("b"))
	assert.Nil(t, err)
	assert.Equal(t, "str", poped)
	poped, err = db.JArrayPop(0, key, []byte("b"))
	assert.Nil(t, err)
	assert.Equal(t, "true", poped)
	poped, err = db.JArrayPop(0, key, []byte("b"))
	assert.Nil(t, err)
	assert.Equal(t, "2", poped)
	poped, err = db.JArrayPop(0, key, []byte("b"))
	assert.Nil(t, err)
	assert.Equal(t, `1`, poped)
	poped, err = db.JArrayPop(0, key, []byte("b"))
	assert.Nil(t, err)
	assert.Equal(t, "", poped)
	n, err = db.JArrayLen(key, []byte("b"))
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n)
}
