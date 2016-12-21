package node

import (
	"errors"
	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/tidwall/redcon"
)

// all pipeline command handle here
func (self *KVNode) plgetCommand(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) < 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	vals, _ := self.store.MGet(cmd.Args[1:]...)
	for _, v := range vals {
		if v == nil {
			conn.WriteNull()
		} else {
			conn.WriteBulk(v)
		}
	}
}

func (self *KVNode) plsetCommand(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) < 3 || (len(cmd.Args)-1)%2 != 0 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	_, err := self.Propose(cmd.Raw)
	if err != nil {
		for i := 1; i < len(cmd.Args); i += 2 {
			conn.WriteError("ERR :" + err.Error())
		}
		return
	}
	for i := 1; i < len(cmd.Args); i += 2 {
		conn.WriteString("OK")
	}
}

func (self *KVNode) localPlsetCommand(cmd redcon.Command) (interface{}, error) {
	if len(cmd.Args) < 3 || (len(cmd.Args)-1)%2 != 0 {
		return nil, errors.New("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
	}

	var kvpairs []common.KVRecord
	for i := 1; i < len(cmd.Args); i += 2 {
		kvpairs = append(kvpairs, common.KVRecord{Key: cmd.Args[i], Value: cmd.Args[i+1]})
	}
	err := self.store.MSet(kvpairs...)
	return nil, err
}
