package node

import (
	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/tidwall/redcon"
)

func (self *KVNode) Lookup(key []byte) ([]byte, error) {
	_, key, err := common.ExtractNamesapce(key)
	if err != nil {
		return nil, err
	}

	v, err := self.store.LocalLookup(key)
	return v, err
}

func (self *KVNode) getCommand(conn redcon.Conn, cmd redcon.Command) {
	val, err := self.store.LocalLookup(cmd.Args[1])
	if err != nil {
		conn.WriteNull()
	} else {
		conn.WriteBulk(val)
	}
}

func (self *KVNode) existsCommand(conn redcon.Conn, cmd redcon.Command) {
	val, _ := self.store.KVExists(cmd.Args[1])
	if val != 1 {
		conn.WriteInt(0)
	} else {
		conn.WriteInt(1)
	}
}

func (self *KVNode) mgetCommand(conn redcon.Conn, cmd redcon.Command) {
	vals, _ := self.store.MGet(cmd.Args[1:]...)
	conn.WriteArray(len(vals))
	for _, v := range vals {
		if v == nil {
			conn.WriteNull()
		} else {
			conn.WriteBulk(v)
		}
	}
}

func (self *KVNode) setCommand(conn redcon.Conn, cmd redcon.Command, v interface{}) {
	conn.WriteString("OK")
}

func (self *KVNode) setnxCommand(conn redcon.Conn, cmd redcon.Command, v interface{}) {
	if rsp, ok := v.(int64); ok {
		conn.WriteInt64(rsp)
	} else {
		conn.WriteError(errInvalidResponse.Error())
	}
}

func (self *KVNode) msetCommand(conn redcon.Conn, cmd redcon.Command, v interface{}) {
	conn.WriteString("OK")
}

func (self *KVNode) incrCommand(conn redcon.Conn, cmd redcon.Command, v interface{}) {
	if rsp, ok := v.(int64); ok {
		conn.WriteInt64(rsp)
	} else {
		conn.WriteError(errInvalidResponse.Error())
	}
}

func (self *KVNode) delCommand(conn redcon.Conn, cmd redcon.Command, v interface{}) {
	if rsp, ok := v.(int64); ok {
		conn.WriteInt64(rsp)
	} else {
		conn.WriteError(errInvalidResponse.Error())
	}
}

// local write command execute only on follower or on the local commit of leader
// the return value of follower is ignored, return value of local leader will be
// return to the future response.
func (self *KVNode) localSetCommand(cmd redcon.Command) (interface{}, error) {
	err := self.store.LocalPut(cmd.Args[1], cmd.Args[2])
	return nil, err
}

func (self *KVNode) localSetnxCommand(cmd redcon.Command) (interface{}, error) {
	v, err := self.store.SetNX(cmd.Args[1], cmd.Args[2])
	return v, err
}

func (self *KVNode) localMSetCommand(cmd redcon.Command) (interface{}, error) {
	args := cmd.Args[1:]
	kvlist := make([]common.KVRecord, 0, len(args)/2)
	for i := 0; i < len(args); i += 2 {
		kvlist = append(kvlist, common.KVRecord{Key: args[i], Value: args[i+1]})
	}
	err := self.store.MSet(kvlist...)
	return nil, err
}

func (self *KVNode) localIncrCommand(cmd redcon.Command) (interface{}, error) {
	v, err := self.store.Incr(cmd.Args[1])
	return v, err
}

func (self *KVNode) localDelCommand(cmd redcon.Command) (interface{}, error) {
	self.store.DelKeys(cmd.Args[1:]...)
	return int64(len(cmd.Args[1:])), nil
}
