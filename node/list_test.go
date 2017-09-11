package node

import (
	"os"
	"testing"

	"github.com/absolute8511/redcon"
	"github.com/stretchr/testify/assert"
)

func TestKVNode_listCommand(t *testing.T) {
	nd, dataDir, stopC := getTestKVNode(t)
	testKey := []byte("default:test:1")
	testKeyValue := []byte("1")
	tests := []struct {
		name string
		args redcon.Command
	}{
		{"lindex", buildCommand([][]byte{[]byte("lindex"), testKey, []byte("0")})},
		{"llen", buildCommand([][]byte{[]byte("llen"), testKey})},
		{"lrange", buildCommand([][]byte{[]byte("lrange"), testKey, []byte("0"), []byte("10")})},
		{"lpop", buildCommand([][]byte{[]byte("lpop"), testKey})},
		{"rpop", buildCommand([][]byte{[]byte("rpop"), testKey})},
		{"lpush", buildCommand([][]byte{[]byte("lpush"), testKey, testKeyValue})},
		{"lindex", buildCommand([][]byte{[]byte("lindex"), testKey, []byte("0")})},
		{"lpop", buildCommand([][]byte{[]byte("lpop"), testKey})},
		//{"lset", buildCommand([][]byte{[]byte("lset"), testKey, []byte("0"), testKeyValue})},
		{"lpush", buildCommand([][]byte{[]byte("lpush"), testKey, testKeyValue})},
		{"lset", buildCommand([][]byte{[]byte("lset"), testKey, []byte("0"), testKeyValue})},
		{"lpush", buildCommand([][]byte{[]byte("lpush"), testKey, testKeyValue})},
		{"lpush", buildCommand([][]byte{[]byte("lpush"), testKey, testKeyValue})},
		{"ltrim", buildCommand([][]byte{[]byte("ltrim"), testKey, []byte("0"), []byte("2")})},
		{"lfixkey", buildCommand([][]byte{[]byte("lfixkey"), testKey})},
		{"rpop", buildCommand([][]byte{[]byte("rpop"), testKey})},
		{"rpush", buildCommand([][]byte{[]byte("rpush"), testKey, testKeyValue})},
		{"lclear", buildCommand([][]byte{[]byte("lclear"), testKey})},
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
