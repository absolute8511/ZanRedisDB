package node

import (
	"fmt"
	"strconv"
	"time"

	"github.com/absolute8511/redcon"
	"github.com/youzan/ZanRedisDB/common"
	"github.com/youzan/ZanRedisDB/rockredis"
)

func (nd *KVNode) Lookup(key []byte) ([]byte, error) {
	key, err := common.CutNamesapce(key)
	if err != nil {
		return nil, err
	}

	v, err := nd.store.KVGet(key)
	return v, err
}

func (nd *KVNode) getNoLockCommand(conn redcon.Conn, cmd redcon.Command) {
	err := nd.store.GetValueWithOpNoLock(cmd.Args[1], func(val []byte) error {
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
}

func (nd *KVNode) getCommand(conn redcon.Conn, cmd redcon.Command) {
	err := nd.store.GetValueWithOp(cmd.Args[1], func(val []byte) error {
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
}

func (nd *KVNode) getRangeCommand(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 4 {
		conn.WriteError(errWrongNumberArgs.Error())
		return
	}
	start, end, err := getRangeArgs(cmd)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	val, err := nd.store.GetRange(cmd.Args[1], start, end)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	if val == nil {
		conn.WriteNull()
	} else {
		conn.WriteBulk(val)
	}
}

func (nd *KVNode) strlenCommand(conn redcon.Conn, cmd redcon.Command) {
	val, err := nd.store.StrLen(cmd.Args[1])
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteInt64(val)
}

func (nd *KVNode) existsCommand(cmd redcon.Command) (interface{}, error) {
	val, err := nd.store.KVExists(cmd.Args[1:]...)
	return val, err
}

func (nd *KVNode) getbitCommand(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) < 3 {
		conn.WriteError(errWrongNumberArgs.Error())
		return
	}
	offset, err := strconv.ParseInt(string(cmd.Args[2]), 10, 64)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	val, err := nd.store.BitGetV2(cmd.Args[1], offset)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteInt64(val)
}

func getRangeArgs(cmd redcon.Command) (int64, int64, error) {
	start, end := int64(0), int64(-1)
	var err error
	if len(cmd.Args) >= 4 {
		start, err = strconv.ParseInt(string(cmd.Args[2]), 10, 64)
		if err != nil {
			return start, end, err
		}
		end, err = strconv.ParseInt(string(cmd.Args[3]), 10, 64)
		if err != nil {
			return start, end, err
		}
	}
	return start, end, nil
}

func (nd *KVNode) bitcountCommand(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 2 && len(cmd.Args) != 4 {
		conn.WriteError(errWrongNumberArgs.Error())
		return
	}
	start, end, err := getRangeArgs(cmd)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	val, err := nd.store.BitCountV2(cmd.Args[1], start, end)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteInt64(val)
}

func (nd *KVNode) mgetCommand(conn redcon.Conn, cmd redcon.Command) {
	vals, _ := nd.store.MGet(cmd.Args[1:]...)
	conn.WriteArray(len(vals))
	for _, v := range vals {
		if v == nil {
			conn.WriteNull()
		} else {
			conn.WriteBulk(v)
		}
	}
}

// current we restrict the pfcount to single key to avoid merge,
// since merge keys may across multi partitions on different nodes
func (nd *KVNode) pfcountCommand(conn redcon.Conn, cmd redcon.Command) {
	val, err := nd.store.PFCount(time.Now().UnixNano(), cmd.Args[1:]...)
	if err != nil {
		conn.WriteError(err.Error())
	} else {
		conn.WriteInt64(val)
	}
}

func (nd *KVNode) setnxCommand(cmd redcon.Command) (interface{}, error) {
	if len(cmd.Args) != 3 {
		err := fmt.Errorf("ERR wrong number arguments for '%v' command", string(cmd.Args[0]))
		return nil, err
	}
	key, err := common.CutNamesapce(cmd.Args[1])
	if err != nil {
		return nil, err
	}
	ex, _ := nd.store.KVExists(key)
	if ex == 1 {
		// already exist
		return int64(0), nil
	}

	rsp, err := rebuildFirstKeyAndPropose(nd, cmd, nil)
	if err != nil {
		return nil, err
	}
	return rsp, nil
}

func (nd *KVNode) setbitCommand(cmd redcon.Command) (interface{}, error) {
	if len(cmd.Args) != 4 {
		return nil, errWrongNumberArgs
	}

	offset, err := strconv.ParseInt(string(cmd.Args[2]), 10, 64)
	if err != nil {
		return nil, err
	}
	on, err := strconv.ParseInt(string(cmd.Args[3]), 10, 64)
	if err != nil {
		return nil, err
	}
	if offset > rockredis.MaxBitOffset || offset < 0 {
		return nil, rockredis.ErrBitOverflow
	}
	if (on & ^1) != 0 {
		return nil, fmt.Errorf("bit should be 0 or 1, got %d", on)
	}

	v, err := rebuildFirstKeyAndPropose(nd, cmd, nil)
	if err != nil {
		return nil, err
	}
	return v, nil
}

func (nd *KVNode) delCommand(cmd redcon.Command, v interface{}) (interface{}, error) {
	if rsp, ok := v.(int64); ok {
		return rsp, nil
	} else {
		return nil, errInvalidResponse
	}
}

func (kvsm *kvStoreSM) localNoOpWriteCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	return nil, nil
}

// local write command execute only on follower or on the local commit of leader
// the return value of follower is ignored, return value of local leader will be
// return to the future response.
func (kvsm *kvStoreSM) localSetCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	err := kvsm.store.KVSet(ts, cmd.Args[1], cmd.Args[2])
	return nil, err
}

func (kvsm *kvStoreSM) localGetSetCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	oldV, err := kvsm.store.KVGetSet(ts, cmd.Args[1], cmd.Args[2])
	if oldV == nil {
		return nil, err
	}
	return oldV, err
}

func (kvsm *kvStoreSM) localSetnxCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	v, err := kvsm.store.SetNX(ts, cmd.Args[1], cmd.Args[2])
	return v, err
}

func (kvsm *kvStoreSM) localMSetCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	args := cmd.Args[1:]
	kvlist := make([]common.KVRecord, 0, len(args)/2)
	for i := 0; i < len(args); i += 2 {
		kvlist = append(kvlist, common.KVRecord{Key: args[i], Value: args[i+1]})
	}
	err := kvsm.store.MSet(ts, kvlist...)
	return nil, err
}

func (kvsm *kvStoreSM) localIncrCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	v, err := kvsm.store.Incr(ts, cmd.Args[1])
	return v, err
}

func (kvsm *kvStoreSM) localIncrByCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	v, err := strconv.ParseInt(string(cmd.Args[2]), 10, 64)
	if err != nil {
		return 0, err
	}
	ret, err := kvsm.store.IncrBy(ts, cmd.Args[1], v)
	return ret, err
}

func (kvsm *kvStoreSM) localDelCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	cnt, err := kvsm.store.DelKeys(cmd.Args[1:]...)
	if err != nil {
		nodeLog.Infof("failed to delete keys: %v, %v", string(cmd.Raw), err)
		return 0, err
	}
	return cnt, nil
}

func (kvsm *kvStoreSM) localPFCountCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	v, err := kvsm.store.PFCount(ts, cmd.Args[1:]...)
	return v, err
}

func (kvsm *kvStoreSM) localPFAddCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	v, err := kvsm.store.PFAdd(ts, cmd.Args[1], cmd.Args[2:]...)
	return v, err
}

func (kvsm *kvStoreSM) localBitSetCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	offset, err := strconv.ParseInt(string(cmd.Args[2]), 10, 64)
	if err != nil {
		return 0, err
	}
	on, err := strconv.ParseInt(string(cmd.Args[3]), 10, 64)
	if err != nil {
		return 0, err
	}
	return kvsm.store.BitSetOld(ts, cmd.Args[1], offset, int(on))
}

func (kvsm *kvStoreSM) localBitSetV2Command(cmd redcon.Command, ts int64) (interface{}, error) {
	offset, err := strconv.ParseInt(string(cmd.Args[2]), 10, 64)
	if err != nil {
		return 0, err
	}
	on, err := strconv.ParseInt(string(cmd.Args[3]), 10, 64)
	if err != nil {
		return 0, err
	}
	return kvsm.store.BitSetV2(ts, cmd.Args[1], offset, int(on))
}

func (kvsm *kvStoreSM) localBitClearCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	return kvsm.store.BitClear(ts, cmd.Args[1])
}

func (kvsm *kvStoreSM) localAppendCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	ret, err := kvsm.store.Append(ts, cmd.Args[1], cmd.Args[2])
	return ret, err
}

func (kvsm *kvStoreSM) localSetRangeCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	offset, err := strconv.ParseInt(string(cmd.Args[2]), 10, 64)
	if err != nil {
		return 0, err
	}
	ret, err := kvsm.store.SetRange(ts, cmd.Args[1], int(offset), cmd.Args[3])
	return ret, err
}
