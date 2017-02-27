package node

import (
	"github.com/tidwall/redcon"
)

func (self *KVNode) scardCommand(conn redcon.Conn, cmd redcon.Command) {
	n, err := self.store.SCard(cmd.Args[1])
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteInt64(n)
}

func (self *KVNode) sismemberCommand(conn redcon.Conn, cmd redcon.Command) {
	n, err := self.store.SIsMember(cmd.Args[1], cmd.Args[2])
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	conn.WriteInt64(n)
}

func (self *KVNode) smembersCommand(conn redcon.Conn, cmd redcon.Command) {
	v, err := self.store.SMembers(cmd.Args[1])
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	conn.WriteArray(len(v))
	for _, vv := range v {
		conn.WriteBulk(vv)
	}
}

func (self *KVNode) saddCommand(conn redcon.Conn, cmd redcon.Command, v interface{}) {
	if rsp, ok := v.(int64); ok {
		conn.WriteInt64(rsp)
	} else {
		conn.WriteError(errInvalidResponse.Error())
	}
}

func (self *KVNode) sremCommand(conn redcon.Conn, cmd redcon.Command, v interface{}) {
	if rsp, ok := v.(int64); ok {
		conn.WriteInt64(rsp)
	} else {
		conn.WriteError(errInvalidResponse.Error())
	}
}

func (self *KVNode) sclearCommand(conn redcon.Conn, cmd redcon.Command, v interface{}) {
	if rsp, ok := v.(int64); ok {
		conn.WriteInt64(rsp)
	} else {
		conn.WriteError(errInvalidResponse.Error())
	}
}

func (self *KVNode) smclearCommand(conn redcon.Conn, cmd redcon.Command, v interface{}) {
	if rsp, ok := v.(int64); ok {
		conn.WriteInt64(rsp)
	} else {
		conn.WriteError(errInvalidResponse.Error())
	}
}

func (self *KVNode) localSadd(cmd redcon.Command, ts int64) (interface{}, error) {
	return self.store.SAdd(cmd.Args[1], cmd.Args[2:]...)
}

func (self *KVNode) localSrem(cmd redcon.Command, ts int64) (interface{}, error) {
	return self.store.SRem(cmd.Args[1], cmd.Args[2:]...)
}

func (self *KVNode) localSclear(cmd redcon.Command, ts int64) (interface{}, error) {
	return self.store.SClear(cmd.Args[1])
}
func (self *KVNode) localSmclear(cmd redcon.Command, ts int64) (interface{}, error) {
	return self.store.SMclear(cmd.Args[1:]...)
}
