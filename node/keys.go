package node

import (
	"bytes"
	"encoding/gob"
	"github.com/absolute8511/ZanRedisDB/rockredis"
	"github.com/tidwall/redcon"
	"log"
)

type kv struct {
	Key   string
	Val   string
	Err   error
	ReqID int64
	// the write operation describe
	Op string
}

func (self *KVNode) Put(k string, v string) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(kv{Key: k, Val: v}); err != nil {
		log.Println(err)
		return err
	}
	_, err := self.HTTPPropose(buf.Bytes())
	return err
}

func (self *KVNode) Lookup(key string) (string, error) {
	v, err := self.store.LocalLookup([]byte(key))
	return string(v), err
}

func (self *KVNode) getCommand(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	val, err := self.store.LocalLookup(cmd.Args[1])
	if err != nil {
		conn.WriteNull()
	} else {
		conn.WriteBulk(val)
	}
}

func (self *KVNode) existsCommand(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	val, _ := self.store.KVExists(cmd.Args[1])
	if val != 1 {
		conn.WriteInt(0)
	} else {
		conn.WriteInt(1)
	}
}

func (self *KVNode) mgetCommand(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) < 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	if len(cmd.Args[1:]) >= rockredis.MAX_BATCH_NUM {
		conn.WriteError("batch size too much")
		return
	}

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

func (self *KVNode) setCommand(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 3 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	_, err := self.Propose(cmd.Raw)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteString("OK")
}

func (self *KVNode) setnxCommand(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 3 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	v, err := self.Propose(cmd.Raw)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	if rsp, ok := v.(int64); ok {
		conn.WriteInt64(rsp)
	} else {
		conn.WriteError(errInvalidResponse.Error())
	}
}

func (self *KVNode) msetCommand(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) < 3 || len(cmd.Args[1:])%2 != 0 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	if len(cmd.Args[1:]) >= rockredis.MAX_BATCH_NUM {
		conn.WriteError("too much batch size")
		return
	}
	_, err := self.Propose(cmd.Raw)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteString("OK")
}

func (self *KVNode) incrCommand(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	v, err := self.Propose(cmd.Raw)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	if rsp, ok := v.(int64); ok {
		conn.WriteInt64(rsp)
	} else {
		conn.WriteError(errInvalidResponse.Error())
	}
}

func (self *KVNode) delCommand(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) < 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	v, err := self.Propose(cmd.Raw)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
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
	kvlist := make([]rockredis.KVPair, 0, len(args)/2)
	for i := 0; i < len(args); i += 2 {
		kvlist = append(kvlist, rockredis.KVPair{args[i], args[i+1]})
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
