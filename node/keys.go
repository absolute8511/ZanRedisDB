package node

import (
	"bytes"
	"encoding/gob"
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
	self.propose(buf)
	// TODO: wait write done here
	return nil
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

	vals, _ := self.store.MGet(cmd.Args[1:]...)
	conn.WriteArray(len(vals))
	for _, v := range vals {
		conn.WriteBulk(v)
	}
}

func (self *KVNode) setCommand(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 3 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	// insert future to wait response
	_, err := self.Propose(cmd.Raw)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteString("OK")
}

func (self *KVNode) delCommand(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	_, err := self.Propose(cmd.Raw)
	if err != nil {
		conn.WriteInt(0)
	} else {
		conn.WriteInt(1)
	}
}

// local write command execute only on follower or on the local commit of leader
// the return value of follower is ignored, return value of local leader will be
// return to the future response.
func (self *KVNode) localSetCommand(cmd redcon.Command) (interface{}, error) {
	err := self.store.LocalPut(cmd.Args[1], cmd.Args[2])
	return nil, err
}

func (self *KVNode) localDelCommand(cmd redcon.Command) (interface{}, error) {
	err := self.store.LocalDelete(cmd.Args[1])
	if err != nil {
		// leader write need response
		return int(0), err
	} else {
		return int(1), nil
	}
}
