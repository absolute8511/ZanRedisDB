package node

import (
	"os"
	"testing"

	"github.com/absolute8511/redcon"
	"github.com/stretchr/testify/assert"
)

func TestKVNode_zsetCommand(t *testing.T) {
	nd, dataDir, stopC := getTestKVNode(t)
	testKey := []byte("default:test:1")
	testMember := []byte("1")
	testScore := []byte("1")
	testLrange := []byte("(1")
	testRrange := []byte("(2")
	testLexLrange := []byte("[1")
	testLexRrange := []byte("(2")

	tests := []struct {
		name string
		args redcon.Command
	}{
		{"zscore", buildCommand([][]byte{[]byte("zscore"), testKey, testMember})},
		{"zcount", buildCommand([][]byte{[]byte("zcount"), testKey, testLrange, testRrange})},
		{"zcard", buildCommand([][]byte{[]byte("zcard"), testKey})},
		{"zlexcount", buildCommand([][]byte{[]byte("zlexcount"), testKey, testLexLrange, testLexRrange})},
		{"zrange", buildCommand([][]byte{[]byte("zrange"), testKey, testScore, testScore})},
		{"zrevrange", buildCommand([][]byte{[]byte("zrevrange"), testKey, testScore, testScore})},
		{"zrangebylex", buildCommand([][]byte{[]byte("zrangebylex"), testKey, testLexLrange, testLexRrange})},
		{"zrangebyscore", buildCommand([][]byte{[]byte("zrangebyscore"), testKey, testLrange, testRrange})},
		{"zrevrangebyscore", buildCommand([][]byte{[]byte("zrevrangebyscore"), testKey, testLrange, testRrange})},
		{"zrank", buildCommand([][]byte{[]byte("zrank"), testKey, testMember})},
		{"zrevrank", buildCommand([][]byte{[]byte("zrevrank"), testKey, testMember})},
		{"zadd", buildCommand([][]byte{[]byte("zadd"), testKey, testScore, testMember})},
		{"zadd", buildCommand([][]byte{[]byte("zadd"), testKey, testScore, testMember, testScore, testMember})},
		{"zincrby", buildCommand([][]byte{[]byte("zincrby"), testKey, testScore, testMember})},
		{"zscore", buildCommand([][]byte{[]byte("zscore"), testKey, testMember})},
		{"zcount", buildCommand([][]byte{[]byte("zcount"), testKey, testLrange, testRrange})},
		{"zcard", buildCommand([][]byte{[]byte("zcard"), testKey})},
		{"zlexcount", buildCommand([][]byte{[]byte("zlexcount"), testKey, testLexLrange, testLexRrange})},
		{"zrange", buildCommand([][]byte{[]byte("zrange"), testKey, testScore, testScore})},
		{"zrevrange", buildCommand([][]byte{[]byte("zrevrange"), testKey, testScore, testScore})},
		{"zrangebylex", buildCommand([][]byte{[]byte("zrangebylex"), testKey, testLexLrange, testLexRrange})},
		{"zrangebylex", buildCommand([][]byte{[]byte("zrangebylex"), testKey, testLexLrange, testLexRrange, []byte("limit"), []byte("0"), []byte("1")})},
		{"zrangebyscore", buildCommand([][]byte{[]byte("zrangebyscore"), testKey, testLrange, testRrange, []byte("withscores"), []byte("limit"), []byte("0"), []byte("1")})},
		{"zrevrangebyscore", buildCommand([][]byte{[]byte("zrevrangebyscore"), testKey, testLrange, testRrange})},
		{"zrank", buildCommand([][]byte{[]byte("zrank"), testKey, testMember})},
		{"zrevrank", buildCommand([][]byte{[]byte("zrevrank"), testKey, testMember})},
		{"zrem", buildCommand([][]byte{[]byte("zrem"), testKey, testMember})},
		{"zremrangebyrank", buildCommand([][]byte{[]byte("zremrangebyrank"), testKey, []byte("0"), []byte("1")})},
		{"zremrangebyscore", buildCommand([][]byte{[]byte("zremrangebyscore"), testKey, testLrange, testRrange})},
		{"zremrangebylex", buildCommand([][]byte{[]byte("zremrangebylex"), testKey, testLexLrange, testLexRrange})},
		{"zclear", buildCommand([][]byte{[]byte("zclear"), testKey})},
	}
	defer os.RemoveAll(dataDir)
	defer nd.Stop()
	defer close(stopC)
	c := &fakeRedisConn{}
	for _, cmd := range tests {
		c.Reset()
		handler, _, _ := nd.router.GetCmdHandler(cmd.name)
		handler(c, cmd.args)
		assert.Nil(t, c.GetError(), cmd.name)
	}
}
