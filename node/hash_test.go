package node

import (
	"os"
	"testing"

	"github.com/absolute8511/redcon"
	"github.com/stretchr/testify/assert"
)

func TestKVNode_hashCommand(t *testing.T) {
	nd, dataDir, stopC := getTestKVNode(t)
	testKey := []byte("default:test:1")
	testKeyValue := []byte("1")
	testKey2Value := []byte("2")
	testField := []byte("f1")
	testField2 := []byte("f2")
	tests := []struct {
		name string
		args redcon.Command
	}{
		{"hget", buildCommand([][]byte{[]byte("hget"), testKey, testField})},
		{"hmget", buildCommand([][]byte{[]byte("hmget"), testKey, testField, testField2})},
		{"hgetall", buildCommand([][]byte{[]byte("hgetall"), testKey})},
		{"hkeys", buildCommand([][]byte{[]byte("hkeys"), testKey})},
		{"hexists", buildCommand([][]byte{[]byte("hexists"), testKey, testField})},
		{"hlen", buildCommand([][]byte{[]byte("hlen"), testKey})},
		{"hset", buildCommand([][]byte{[]byte("hset"), testKey, testField, testKeyValue})},
		{"hsetnx", buildCommand([][]byte{[]byte("hsetnx"), testKey, testField, testKeyValue})},
		{"hmset", buildCommand([][]byte{[]byte("hmset"), testKey, testField, testKeyValue, testField2, testKey2Value})},
		{"hdel", buildCommand([][]byte{[]byte("hdel"), testKey, testField})},
		{"hincrby", buildCommand([][]byte{[]byte("hincrby"), testKey, testField2, []byte("1")})},
		{"hget", buildCommand([][]byte{[]byte("hget"), testKey, testField})},
		{"hmget", buildCommand([][]byte{[]byte("hmget"), testKey, testField, testField2})},
		{"hgetall", buildCommand([][]byte{[]byte("hgetall"), testKey})},
		{"hkeys", buildCommand([][]byte{[]byte("hkeys"), testKey})},
		{"hexists", buildCommand([][]byte{[]byte("hexists"), testKey, testField})},
		{"hlen", buildCommand([][]byte{[]byte("hlen"), testKey})},
		{"hclear", buildCommand([][]byte{[]byte("hclear"), testKey})},
	}
	defer os.RemoveAll(dataDir)
	defer nd.Stop()
	defer close(stopC)
	c := &fakeRedisConn{}
	for _, cmd := range tests {
		c.Reset()
		handler, _, _ := nd.router.GetCmdHandler(cmd.name)
		handler(c, cmd.args)
		assert.Nil(t, c.GetError())
	}
}
