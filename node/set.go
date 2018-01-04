package node

import (
	"github.com/absolute8511/redcon"
)

func (nd *KVNode) scardCommand(conn redcon.Conn, cmd redcon.Command) {
	n, err := nd.store.SCard(cmd.Args[1])
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteInt64(n)
}

func (nd *KVNode) sismemberCommand(conn redcon.Conn, cmd redcon.Command) {
	n, err := nd.store.SIsMember(cmd.Args[1], cmd.Args[2])
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	conn.WriteInt64(n)
}

func (nd *KVNode) smembersCommand(conn redcon.Conn, cmd redcon.Command) {
	v, err := nd.store.SMembers(cmd.Args[1])
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	conn.WriteArray(len(v))
	for _, vv := range v {
		conn.WriteBulk(vv)
	}
}

func (nd *KVNode) saddCommand(conn redcon.Conn, cmd redcon.Command, v interface{}) {
	if rsp, ok := v.(int64); ok {
		conn.WriteInt64(rsp)
	} else {
		conn.WriteError(errInvalidResponse.Error())
	}
}

func (nd *KVNode) sremCommand(conn redcon.Conn, cmd redcon.Command, v interface{}) {
	if rsp, ok := v.(int64); ok {
		conn.WriteInt64(rsp)
	} else {
		conn.WriteError(errInvalidResponse.Error())
	}
}

func (nd *KVNode) sclearCommand(conn redcon.Conn, cmd redcon.Command, v interface{}) {
	if rsp, ok := v.(int64); ok {
		conn.WriteInt64(rsp)
	} else {
		conn.WriteError(errInvalidResponse.Error())
	}
}

func (nd *KVNode) smclearCommand(conn redcon.Conn, cmd redcon.Command, v interface{}) {
	if rsp, ok := v.(int64); ok {
		conn.WriteInt64(rsp)
	} else {
		conn.WriteError(errInvalidResponse.Error())
	}
}

func (kvsm *kvStoreSM) localSadd(cmd redcon.Command, ts int64) (interface{}, error) {
	return kvsm.store.SAdd(cmd.Args[1], cmd.Args[2:]...)
}

func (kvsm *kvStoreSM) localSrem(cmd redcon.Command, ts int64) (interface{}, error) {
	return kvsm.store.SRem(cmd.Args[1], cmd.Args[2:]...)
}

func (kvsm *kvStoreSM) localSclear(cmd redcon.Command, ts int64) (interface{}, error) {
	return kvsm.store.SClear(cmd.Args[1])
}
func (kvsm *kvStoreSM) localSmclear(cmd redcon.Command, ts int64) (interface{}, error) {
	return kvsm.store.SMclear(cmd.Args[1:]...)
}
