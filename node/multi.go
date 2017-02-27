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
	for i := 1; i < len(cmd.Args); i++ {
		_, key, err := common.ExtractNamesapce(cmd.Args[i])
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
		cmd.Args[i] = key
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
	args := cmd.Args[1:]
	for i, v := range args {
		if i%2 != 0 {
			continue
		}
		_, key, err := common.ExtractNamesapce(v)
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
		args[i] = key
	}
	ncmd := buildCommand(cmd.Args)
	copy(cmd.Raw[0:], ncmd.Raw[:])
	cmd.Raw = cmd.Raw[:len(ncmd.Raw)]

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

func (self *KVNode) localPlsetCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	if len(cmd.Args) < 3 || (len(cmd.Args)-1)%2 != 0 {
		return nil, errors.New("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
	}

	var kvpairs []common.KVRecord
	for i := 1; i < len(cmd.Args); i += 2 {
		kvpairs = append(kvpairs, common.KVRecord{Key: cmd.Args[i], Value: cmd.Args[i+1]})
	}
	err := self.store.MSet(ts, kvpairs...)
	return nil, err
}
