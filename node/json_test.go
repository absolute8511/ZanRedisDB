package node

import (
	"os"
	"testing"

	"github.com/absolute8511/redcon"
	"github.com/stretchr/testify/assert"
)

func TestKVNode_jsonCommand(t *testing.T) {
	nd, dataDir, stopC := getTestKVNode(t)
	testKey := []byte("default:test:1")
	testJsonField := []byte("1")
	testJsonFieldValue := []byte("1")

	tests := []struct {
		name string
		args redcon.Command
	}{
		{"json.get", buildCommand([][]byte{[]byte("json.get"), testKey, testJsonField})},
		{"json.keyexists", buildCommand([][]byte{[]byte("json.keyexists"), testKey})},
		{"json.mkget", buildCommand([][]byte{[]byte("json.mkget"), testKey, testJsonField})},
		{"json.type", buildCommand([][]byte{[]byte("json.type"), testKey})},
		{"json.type", buildCommand([][]byte{[]byte("json.type"), testKey, testJsonField})},
		{"json.arrlen", buildCommand([][]byte{[]byte("json.arrlen"), testKey, testJsonField})},
		{"json.objkeys", buildCommand([][]byte{[]byte("json.objkeys"), testKey})},
		{"json.objlen", buildCommand([][]byte{[]byte("json.objlen"), testKey})},
		{"json.set", buildCommand([][]byte{[]byte("json.set"), testKey, testJsonField, testJsonFieldValue})},
		{"json.del", buildCommand([][]byte{[]byte("json.del"), testKey, testJsonField})},
		{"json.arrappend", buildCommand([][]byte{[]byte("json.arrappend"), testKey, testJsonField, testJsonFieldValue})},
		{"json.arrpop", buildCommand([][]byte{[]byte("json.arrpop"), testKey, testJsonField})},
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
