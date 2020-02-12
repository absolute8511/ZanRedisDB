package server

import (
	"testing"

	"github.com/siddontang/goredis"
	"github.com/stretchr/testify/assert"
)

func TestJSON(t *testing.T) {
	c := getTestConn(t)
	defer c.Close()

	key := "default:test:jsonapi_a"
	n, err := goredis.Int(c.Do("json.keyexists", key))
	assert.Nil(t, err)
	assert.Equal(t, int(0), n)

	strRet, err := goredis.String(c.Do("json.set", key, ".a", `"str"`))
	assert.Nil(t, err)
	assert.Equal(t, "OK", strRet)

	n, err = goredis.Int(c.Do("json.keyexists", key))
	assert.Nil(t, err)
	assert.Equal(t, int(1), n)

	strRets, err := goredis.Strings(c.Do("json.get", key, ".a"))
	assert.Nil(t, err)
	assert.Equal(t, 1, len(strRets))
	assert.Equal(t, "str", strRets[0])

	typeStr, err := goredis.String(c.Do("json.type", key, ".a"))
	assert.Nil(t, err)
	assert.Equal(t, "string", typeStr)

	typeStr, err = goredis.String(c.Do("json.type", key, ""))
	assert.Nil(t, err)
	assert.Equal(t, "object", typeStr)

	strRet, err = goredis.String(c.Do("json.set", key, "1", "3"))
	assert.Nil(t, err)
	assert.Equal(t, "OK", strRet)

	strRets, err = goredis.Strings(c.Do("json.get", key, ""))
	assert.Nil(t, err)
	t.Log(strRets)
	assert.Equal(t, 1, len(strRets))
	assert.True(t, strRets[0] != "")
	t.Log(strRets[0])
	assert.True(t, strRets[0] == `{"a":"str","1":3}` || (strRets[0] == `{"1":3,"a":"str"}`))

	strRets, err = goredis.Strings(c.Do("json.get", key, "a"))
	assert.Nil(t, err)
	assert.Equal(t, 1, len(strRets))
	assert.Equal(t, "str", strRets[0])

	strRets, err = goredis.Strings(c.Do("json.get", key, "1"))
	assert.Nil(t, err)
	assert.Equal(t, 1, len(strRets))
	t.Log(strRets)
	assert.Equal(t, "3", strRets[0])

	strRets, err = goredis.Strings(c.Do("json.get", key, "1", "a"))
	assert.Nil(t, err)
	assert.Equal(t, 2, len(strRets))
	t.Log(strRets)
	assert.Equal(t, "3", strRets[0])
	assert.Equal(t, "str", strRets[1])

	n, err = goredis.Int(c.Do("json.objlen", key))
	assert.Nil(t, err)
	assert.Equal(t, 2, n)
	strRets, err = goredis.Strings(c.Do("json.objkeys", key))
	assert.Nil(t, err)
	assert.Equal(t, 2, len(strRets))
	for _, s := range strRets {
		assert.True(t, s == "a" || s == "1")
	}
	c.Do("json.del", key, "1")
	strRets, err = goredis.Strings(c.Do("json.get", key, "1"))
	assert.Nil(t, err)
	assert.Equal(t, 1, len(strRets))
	assert.Equal(t, "", strRets[0])

	typeStr, err = goredis.String(c.Do("json.type", key, "1"))
	assert.Nil(t, err)
	assert.Equal(t, "null", typeStr)

	n, err = goredis.Int(c.Do("json.objlen", key))
	assert.Nil(t, err)
	assert.Equal(t, 1, n)
	strRets, err = goredis.Strings(c.Do("json.objkeys", key))
	assert.Nil(t, err)
	assert.Equal(t, 1, len(strRets))
	for _, s := range strRets {
		assert.True(t, s == "a")
	}

	c.Do("json.del", key, "a")
	strRets, err = goredis.Strings(c.Do("json.get", key, ".a"))
	assert.Nil(t, err)
	assert.Equal(t, 1, len(strRets))
	assert.Equal(t, "", strRets[0])

	n, err = goredis.Int(c.Do("json.objlen", key))
	assert.Nil(t, err)
	assert.Equal(t, 0, n)
	strRets, err = goredis.Strings(c.Do("json.objkeys", key))
	assert.Nil(t, err)
	assert.Equal(t, 0, len(strRets))
}

func TestJSONInvalidJSON(t *testing.T) {
	c := getTestConn(t)
	defer c.Close()

	key := "default:test:jsonapi_invalid"

	strRet, err := goredis.String(c.Do("json.set", key, ".a", `"str"`))
	assert.Nil(t, err)
	assert.Equal(t, "OK", strRet)

	strRet, err = goredis.String(c.Do("json.set", key, "1", "3"))
	assert.Nil(t, err)
	assert.Equal(t, "OK", strRet)

	_, err = c.Do("json.set", key, "2", "invalid_str")
	assert.NotNil(t, err)
}

func TestJSONSetComplexJSON(t *testing.T) {
	c := getTestConn(t)
	defer c.Close()

	key := "default:test:jsonapi_complex"

	strRet, err := goredis.String(c.Do("json.set", key, "", `{
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

	assert.Nil(t, err)
	assert.Equal(t, "OK", strRet)
	strRets, err := goredis.Strings(c.Do("json.get", key, "borough"))
	assert.Nil(t, err)
	assert.Equal(t, 1, len(strRets))
	assert.Equal(t, "Manhattan", strRets[0])
	strRets, err = goredis.Strings(c.Do("json.get", key, "address.zipcode"))
	assert.Nil(t, err)
	assert.Equal(t, 1, len(strRets))
	assert.Equal(t, "10075", strRets[0])
	strRets, err = goredis.Strings(c.Do("json.get", key, "grades.0.score"))
	assert.Nil(t, err)
	assert.Equal(t, 1, len(strRets))
	assert.Equal(t, "11", strRets[0])
	c.Do("json.set", key, "cuisine", `"American"`)
	c.Do("json.set", key, "address.street", `"East 31st Street"`)
	strRets, err = goredis.Strings(c.Do("json.get", key, "cuisine"))
	assert.Nil(t, err)
	assert.Equal(t, 1, len(strRets))
	assert.Equal(t, "American", strRets[0])
	strRets, err = goredis.Strings(c.Do("json.get", key, "address.street"))
	assert.Nil(t, err)
	assert.Equal(t, 1, len(strRets))
	assert.Equal(t, "East 31st Street", strRets[0])
}

func TestJSONArrayOp(t *testing.T) {
	c := getTestConn(t)
	defer c.Close()

	key := "default:test:json_arrayop_d"
	_, err := c.Do("json.set", key, "", `[1, 2]`)
	assert.Nil(t, err)
	n, err := goredis.Int(c.Do("json.arrappend", key, ".", `{"3":[]}`))
	assert.Nil(t, err)
	assert.Equal(t, 3, n)

	n, err = goredis.Int(c.Do("json.arrappend", key, ".", "4", "5"))
	assert.Nil(t, err)
	assert.Equal(t, 5, n)

	n, err = goredis.Int(c.Do("json.arrlen", key))
	assert.Nil(t, err)
	assert.Equal(t, 5, n)

	n, err = goredis.Int(c.Do("json.arrappend", key, "2.3", "33", "34"))
	assert.Nil(t, err)
	assert.Equal(t, 2, n)

	n, err = goredis.Int(c.Do("json.arrlen", key, "2.3"))
	assert.Nil(t, err)
	assert.Equal(t, 2, n)

	typeStr, err := goredis.String(c.Do("json.type", key, "2.3"))
	assert.Nil(t, err)
	assert.Equal(t, "array", typeStr)

	typeStr, err = goredis.String(c.Do("json.type", key))
	assert.Nil(t, err)
	assert.Equal(t, "array", typeStr)

	poped, err := goredis.String(c.Do("json.arrpop", key))
	assert.Nil(t, err)
	assert.Equal(t, "5", poped)

	poped, err = goredis.String(c.Do("json.arrpop", key))
	assert.Nil(t, err)
	assert.Equal(t, "4", poped)

	n, err = goredis.Int(c.Do("json.arrlen", key))
	assert.Nil(t, err)
	assert.Equal(t, 3, n)

	poped, err = goredis.String(c.Do("json.arrpop", key, "2.3"))
	assert.Nil(t, err)
	assert.Equal(t, "34", poped)

	n, err = goredis.Int(c.Do("json.arrlen", key, "2.3"))
	assert.Nil(t, err)
	assert.Equal(t, 1, n)

	poped, err = goredis.String(c.Do("json.arrpop", key))
	assert.Nil(t, err)
	assert.Equal(t, `{"3":[33]}`, poped)

	n, err = goredis.Int(c.Do("json.arrlen", key))
	assert.Nil(t, err)
	assert.Equal(t, 2, n)

	poped, err = goredis.String(c.Do("json.arrpop", key))
	assert.Nil(t, err)
	assert.Equal(t, "2", poped)
	poped, err = goredis.String(c.Do("json.arrpop", key))
	assert.Nil(t, err)
	assert.Equal(t, "1", poped)

	n, err = goredis.Int(c.Do("json.arrlen", key))
	assert.Nil(t, err)
	assert.Equal(t, 0, n)

	poped, err = goredis.String(c.Do("json.arrpop", key))
	assert.Nil(t, err)
	assert.Equal(t, "", poped)
}

func TestJSONErrorParams(t *testing.T) {
	c := getTestConn(t)
	defer c.Close()

	key := "default:test:json_err_param"
	if _, err := c.Do("json.set", key); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("json.get", key); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("json.del", key); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("json.arrylen"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("json.arrappend", key, "a"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("json.arrpop"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("json.objkeys"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("json.objlen"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}
}
