package node

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/absolute8511/redcon"
	"github.com/youzan/ZanRedisDB/common"
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

func (nd *KVNode) srandmembersCommand(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 2 && len(cmd.Args) != 3 {
		err := fmt.Errorf("ERR wrong number arguments for '%v' command", string(cmd.Args[0]))
		conn.WriteError(err.Error())
		return
	}
	hasCount := len(cmd.Args) == 3
	cnt := 1
	var err error
	if hasCount {
		cnt, err = strconv.Atoi(string(cmd.Args[2]))
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
		if cnt < 1 {
			conn.WriteError("Invalid count")
			return
		}
	}
	v, err := nd.store.SRandMembers(cmd.Args[1], int64(cnt))
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	conn.WriteArray(len(v))
	for _, vv := range v {
		conn.WriteBulk(vv)
	}
}

func (nd *KVNode) saddCommand(cmd redcon.Command) (interface{}, error) {
	// optimize the sadd to check before propose to raft
	if len(cmd.Args) < 3 {
		err := fmt.Errorf("ERR wrong number arguments for '%v' command", string(cmd.Args[0]))
		return nil, err
	}
	key, err := common.CutNamesapce(cmd.Args[1])
	if err != nil {
		return nil, err
	}

	needChange := false
	for _, m := range cmd.Args[2:] {
		n, _ := nd.store.SIsMember(key, m)
		if n == 0 {
			// found a new member not exist, we need do raft proposal
			needChange = true
			break
		}
	}
	if !needChange {
		return int64(0), nil
	}
	rsp, err := rebuildFirstKeyAndPropose(nd, cmd, checkAndRewriteIntRsp)
	return rsp, err
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
	v, err := rebuildFirstKeyAndPropose(nd, cmd, func(cmd redcon.Command, r interface{}) (interface{}, error) {
		// without the count argument, it is bulk string
		if !hasCount {
			if r == nil {
				return nil, nil
			}
			if rsp, ok := r.(string); ok {
				return rsp, nil
			}
			return nil, errInvalidResponse
		} else {
			rsp, ok := r.([][]byte)
			if !ok {
				return nil, errInvalidResponse
			}
			return rsp, nil
		}
	})
	if err != nil {
		return nil, err
	}
	return v, nil
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
	return kvsm.store.SClear(ts, cmd.Args[1])
}
func (kvsm *kvStoreSM) localSmclear(cmd redcon.Command, ts int64) (interface{}, error) {
	return kvsm.store.SMclear(cmd.Args[1:]...)
}
