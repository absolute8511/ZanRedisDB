package node

import (
	"os"
	"testing"

	"github.com/absolute8511/redcon"
	"github.com/stretchr/testify/assert"
)

func TestKVNode_setCommand(t *testing.T) {
	nd, dataDir, stopC := getTestKVNode(t)
	testKey := []byte("default:test:1")
	testMember := []byte("1")

	tests := []struct {
		name string
		args redcon.Command
	}{
		{"scard", buildCommand([][]byte{[]byte("scard"), testKey})},
		{"sismember", buildCommand([][]byte{[]byte("sismember"), testKey, testMember})},
		{"smembers", buildCommand([][]byte{[]byte("smembers"), testKey})},
		{"srandmember", buildCommand([][]byte{[]byte("srandmember"), testKey})},
		{"srandmember", buildCommand([][]byte{[]byte("srandmember"), testKey, []byte("2")})},
		{"sadd", buildCommand([][]byte{[]byte("sadd"), testKey, testMember})},
		{"sismember", buildCommand([][]byte{[]byte("sismember"), testKey, testMember})},
		{"smembers", buildCommand([][]byte{[]byte("smembers"), testKey})},
		{"scard", buildCommand([][]byte{[]byte("scard"), testKey})},
		{"spop", buildCommand([][]byte{[]byte("spop"), testKey})},
		{"srem", buildCommand([][]byte{[]byte("srem"), testKey, testMember})},
		{"sttl", buildCommand([][]byte{[]byte("sttl"), testKey})},
		{"skeyexist", buildCommand([][]byte{[]byte("skeyexist"), testKey})},
		{"sexpire", buildCommand([][]byte{[]byte("sexpire"), testKey, []byte("10")})},
		{"spersist", buildCommand([][]byte{[]byte("spersist"), testKey})},
		{"sclear", buildCommand([][]byte{[]byte("sclear"), testKey})},
	}
	defer os.RemoveAll(dataDir)
	defer nd.Stop()
	defer close(stopC)
	c := &fakeRedisConn{}
	for _, cmd := range tests {
		c.Reset()
		handler, ok := nd.router.GetCmdHandler(cmd.name)
		if ok {
			handler(c, cmd.args)
			assert.Nil(t, c.GetError())
		} else {
			whandler, _ := nd.router.GetWCmdHandler(cmd.name)
			rsp, err := whandler(cmd.args)
			assert.Nil(t, err)
			_, ok := rsp.(error)
			assert.True(t, !ok)
		}
	}
}
