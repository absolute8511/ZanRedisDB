package node

import (
	"errors"
	"fmt"
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

func (nd *KVNode) spopCommand(cmd redcon.Command) (interface{}, error) {
	if len(cmd.Args) != 2 && len(cmd.Args) != 3 {
		err := fmt.Errorf("ERR wrong number arguments for '%v' command", string(cmd.Args[0]))
		return nil, err
	}
	hasCount := len(cmd.Args) == 3
	if hasCount {
		cnt, err := strconv.Atoi(string(cmd.Args[2]))
		if err != nil {
			return nil, err
		}
		if cnt < 1 {
			return nil, errors.New("Invalid count")
		}
	}
	_, v, err := rebuildFirstKeyAndPropose(nd, cmd, nil)
	if err != nil {
		return nil, err
	}

	// without the count argument, it is bulk string
	if !hasCount {
		if v == nil {
			return nil, nil
		}
		if rsp, ok := v.(string); ok {
			return rsp, nil
		}
		return nil, errInvalidResponse
	} else {
		rsp, ok := v.([][]byte)
		if !ok {
			return nil, errInvalidResponse
		}
		return rsp, nil
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
