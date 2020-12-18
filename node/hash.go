package node

import (
	"strconv"

	"github.com/absolute8511/redcon"
	"github.com/youzan/ZanRedisDB/common"
)

func (nd *KVNode) hgetCommand(conn redcon.Conn, cmd redcon.Command) {
	err := nd.store.HGetWithOp(cmd.Args[1], cmd.Args[2], func(val []byte) error {
		if val == nil {
			conn.WriteNull()
		} else {
			conn.WriteBulk(val)
			// since val will be freed, we need flush before return
			conn.Flush()
		}
		return nil
	})
	if err != nil {
		conn.WriteError(err.Error())
	}
	return
}

func (nd *KVNode) hgetallCommand(conn redcon.Conn, cmd redcon.Command) {
	_, vals, err := nd.store.HGetAll(cmd.Args[1])
	if err != nil {
		conn.WriteError("ERR for " + string(cmd.Args[0]) + " command: " + err.Error())
		return
	}
	conn.WriteArray(len(vals) * 2)
	for _, v := range vals {
		conn.WriteBulk(v.Rec.Key)
		conn.WriteBulk(v.Rec.Value)
	}
}

func (nd *KVNode) hkeysCommand(conn redcon.Conn, cmd redcon.Command) {
	_, vals, err := nd.store.HKeys(cmd.Args[1])
	if err != nil {
		conn.WriteError("ERR for " + string(cmd.Args[0]) + " command: " + err.Error())
		return
	}
	conn.WriteArray(len(vals))
	for _, v := range vals {
		conn.WriteBulk(v.Rec.Key)
	}
}

func (nd *KVNode) hvalsCommand(conn redcon.Conn, cmd redcon.Command) {
	_, vals, err := nd.store.HValues(cmd.Args[1])
	if err != nil {
		conn.WriteError("ERR for " + string(cmd.Args[0]) + " command: " + err.Error())
		return
	}
	conn.WriteArray(len(vals))
	for _, v := range vals {
		conn.WriteBulk(v.Rec.Value)
	}
}

func (nd *KVNode) hexistsCommand(conn redcon.Conn, cmd redcon.Command) {
	val, err := nd.store.HExist(cmd.Args[1], cmd.Args[2])
	if err != nil || !val {
		conn.WriteInt(0)
	} else {
		conn.WriteInt(1)
	}
}

func (nd *KVNode) hmgetCommand(conn redcon.Conn, cmd redcon.Command) {
	vals, _ := nd.store.HMget(cmd.Args[1], cmd.Args[2:]...)
	conn.WriteArray(len(vals))
	for _, v := range vals {
		if v == nil {
			conn.WriteNull()
		} else {
			conn.WriteBulk(v)
		}
	}
}

func (nd *KVNode) hlenCommand(conn redcon.Conn, cmd redcon.Command) {
	val, err := nd.store.HLen(cmd.Args[1])
	if err != nil {
		conn.WriteInt(0)
	} else {
		conn.WriteInt64(val)
	}
}

// local write command execute only on follower or on the local commit of leader
// the return value of follower is ignored, return value of local leader will be
// return to the future response.
func (kvsm *kvStoreSM) localHSetCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	v, err := kvsm.store.HSet(ts, false, cmd.Args[1], cmd.Args[2], cmd.Args[3])
	return v, err
}

func (kvsm *kvStoreSM) localHSetNXCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	v, err := kvsm.store.HSet(ts, true, cmd.Args[1], cmd.Args[2], cmd.Args[3])
	return v, err
}

func (kvsm *kvStoreSM) localHMsetCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	args := cmd.Args[2:]
	if len(args)%2 != 0 {
		return nil, common.ErrInvalidArgs
	}
	fvs := make([]common.KVRecord, 0, len(args)/2)
	for i := 0; i < len(args); i += 2 {
		fvs = append(fvs, common.KVRecord{Key: args[i], Value: args[i+1]})
	}
	err := kvsm.store.HMset(ts, cmd.Args[1], fvs...)
	return nil, err
}

func (kvsm *kvStoreSM) localHIncrbyCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	v, err := strconv.ParseInt(string(cmd.Args[3]), 10, 64)
	if err != nil {
		return 0, err
	}
	ret, err := kvsm.store.HIncrBy(ts, cmd.Args[1], cmd.Args[2], int64(v))
	return ret, err
}

func (kvsm *kvStoreSM) localHDelCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	n, err := kvsm.store.HDel(ts, cmd.Args[1], cmd.Args[2:]...)
	if err != nil {
		// leader write need response
		return int64(0), err
	}
	return n, nil
}

func (kvsm *kvStoreSM) localHclearCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	return kvsm.store.HClear(ts, cmd.Args[1])
}

func (kvsm *kvStoreSM) localHMClearCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	var count int64
	for _, hkey := range cmd.Args[1:] {
		if _, err := kvsm.store.HClear(ts, hkey); err == nil {
			count++
		} else {
			return count, err
		}
	}
	return count, nil
}
