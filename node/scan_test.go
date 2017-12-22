package node

import (
	"os"
	"testing"

	"github.com/absolute8511/redcon"
	"github.com/stretchr/testify/assert"
)

func TestKVNode_scanCommand(t *testing.T) {
	nd, dataDir, stopC := getTestKVNode(t)
	testKey := []byte("default:test:1")
	tests := []struct {
		name string
		args redcon.Command
	}{
		{"hscan", buildCommand([][]byte{[]byte("hscan"), testKey, []byte("")})},
		{"sscan", buildCommand([][]byte{[]byte("sscan"), testKey, []byte("")})},
		{"zscan", buildCommand([][]byte{[]byte("zscan"), testKey, []byte("")})},
		{"scan", buildCommand([][]byte{[]byte("scan"), testKey})},
		{"scan", buildCommand([][]byte{[]byte("scan"), testKey, []byte("match"), []byte("test"), []byte("count"), []byte("1")})},
		{"advscan", buildCommand([][]byte{[]byte("advscan"), testKey, []byte("kv")})},
		{"advscan", buildCommand([][]byte{[]byte("advscan"), testKey, []byte("hash")})},
		{"advscan", buildCommand([][]byte{[]byte("advscan"), testKey, []byte("list")})},
		{"advscan", buildCommand([][]byte{[]byte("advscan"), testKey, []byte("set")})},
		{"advscan", buildCommand([][]byte{[]byte("advscan"), testKey, []byte("zset")})},
		{"fullscan", buildCommand([][]byte{[]byte("fullscan"), testKey, []byte("kv")})},
		{"fullscan", buildCommand([][]byte{[]byte("fullscan"), testKey, []byte("hash")})},
		{"fullscan", buildCommand([][]byte{[]byte("fullscan"), testKey, []byte("list")})},
		{"fullscan", buildCommand([][]byte{[]byte("fullscan"), testKey, []byte("set")})},
		{"fullscan", buildCommand([][]byte{[]byte("fullscan"), testKey, []byte("zset")})},
	}
	defer os.RemoveAll(dataDir)
	defer nd.Stop()
	defer close(stopC)
	c := &fakeRedisConn{}
	for _, cmd := range tests {
		c.Reset()
		handler, _, _ := nd.router.GetCmdHandler(cmd.name)
		if handler != nil {
			handler(c, cmd.args)
			assert.Nil(t, c.GetError())
		} else {
			mhandler, _, _ := nd.router.GetMergeCmdHandler(cmd.name)
			_, err := mhandler(cmd.args)
			assert.Nil(t, err)
		}
	}
}
