package node

import (
	"strconv"

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

func (nd *KVNode) spopCommand(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 2 && len(cmd.Args) != 3 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	hasCount := len(cmd.Args) == 3
	if hasCount {
		cnt, err := strconv.Atoi(string(cmd.Args[2]))
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
		if cnt < 1 {
			conn.WriteError("Invalid count")
			return
		}
	}
	_, v, ok := rebuildFirstKeyAndPropose(nd, conn, cmd)
	if !ok {
		return
	}

	// without the count argument, it is bulk string
	if !hasCount {
		if v == nil {
			conn.WriteNull()
			return
		}
		if rsp, ok := v.(string); ok {
			conn.WriteBulkString(rsp)
			return
		}
		if !ok {
			conn.WriteError("Invalid response type")
			return
		}
	} else {
		rsp, ok := v.([][]byte)
		if !ok {
			conn.WriteError("Invalid response type")
			return
		}
		conn.WriteArray(len(rsp))
		for _, d := range rsp {
			conn.WriteBulk(d)
		}
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
	return kvsm.store.SAdd(ts, cmd.Args[1], cmd.Args[2:]...)
}

func (kvsm *kvStoreSM) localSrem(cmd redcon.Command, ts int64) (interface{}, error) {
	return kvsm.store.SRem(ts, cmd.Args[1], cmd.Args[2:]...)
}

func (kvsm *kvStoreSM) localSpop(cmd redcon.Command, ts int64) (interface{}, error) {
	cnt := 1
	if len(cmd.Args) == 3 {
		cnt, _ = strconv.Atoi(string(cmd.Args[2]))
	}
	vals, err := kvsm.store.SPop(ts, cmd.Args[1], cnt)
	if err != nil {
		return nil, err
	}
	if len(cmd.Args) == 3 {
		return vals, nil
	}
	if len(vals) > 0 {
		return string(vals[0]), nil
	}
	return nil, nil
}

func (kvsm *kvStoreSM) localSclear(cmd redcon.Command, ts int64) (interface{}, error) {
	return kvsm.store.SClear(cmd.Args[1])
}
func (kvsm *kvStoreSM) localSmclear(cmd redcon.Command, ts int64) (interface{}, error) {
	return kvsm.store.SMclear(cmd.Args[1:]...)
}
