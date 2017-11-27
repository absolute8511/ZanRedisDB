package node

import (
	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/absolute8511/redcon"
)

func (nd *KVNode) Lookup(key []byte) ([]byte, error) {
	_, key, err := common.ExtractNamesapce(key)
	if err != nil {
		return nil, err
	}

	v, err := nd.store.LocalLookup(key)
	return v, err
}

func (nd *KVNode) getCommand(conn redcon.Conn, cmd redcon.Command) {
	val, err := nd.store.LocalLookup(cmd.Args[1])
	if err != nil || val == nil {
		conn.WriteNull()
	} else {
		conn.WriteBulk(val)
	}
}

func (nd *KVNode) existsCommand(conn redcon.Conn, cmd redcon.Command) {
	val, _ := nd.store.KVExists(cmd.Args[1])
	if val != 1 {
		conn.WriteInt(0)
	} else {
		conn.WriteInt(1)
	}
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

func (nd *KVNode) setCommand(conn redcon.Conn, cmd redcon.Command, v interface{}) {
	conn.WriteString("OK")
}

func (nd *KVNode) setnxCommand(conn redcon.Conn, cmd redcon.Command, v interface{}) {
	if rsp, ok := v.(int64); ok {
		conn.WriteInt64(rsp)
	} else {
		conn.WriteError(errInvalidResponse.Error())
	}
}

func (nd *KVNode) msetCommand(conn redcon.Conn, cmd redcon.Command, v interface{}) {
	conn.WriteString("OK")
}

func (nd *KVNode) incrCommand(conn redcon.Conn, cmd redcon.Command, v interface{}) {
	if rsp, ok := v.(int64); ok {
		conn.WriteInt64(rsp)
	} else {
		conn.WriteError(errInvalidResponse.Error())
	}
}

func (nd *KVNode) delCommand(conn redcon.Conn, cmd redcon.Command, v interface{}) {
	if rsp, ok := v.(int64); ok {
		conn.WriteInt64(rsp)
	} else {
		conn.WriteError(errInvalidResponse.Error())
	}
}

func (nd *KVNode) pfaddCommand(conn redcon.Conn, cmd redcon.Command, v interface{}) {
	if rsp, ok := v.(int64); ok {
		conn.WriteInt64(rsp)
	} else {
		conn.WriteError(errInvalidResponse.Error())
	}
}

// current we restrict the pfcount to single key to avoid merge,
// since merge keys may across multi partitions on different nodes
func (nd *KVNode) pfcountCommand(conn redcon.Conn, cmd redcon.Command, v interface{}) {
	if rsp, ok := v.(int64); ok {
		conn.WriteInt64(rsp)
	} else {
		conn.WriteError(errInvalidResponse.Error())
	}
}

// local write command execute only on follower or on the local commit of leader
// the return value of follower is ignored, return value of local leader will be
// return to the future response.
func (nd *KVNode) localSetCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	err := nd.store.KVSet(ts, cmd.Args[1], cmd.Args[2])
	return nil, err
}

func (nd *KVNode) localSetnxCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	v, err := nd.store.SetNX(ts, cmd.Args[1], cmd.Args[2])
	return v, err
}

func (nd *KVNode) localMSetCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	args := cmd.Args[1:]
	kvlist := make([]common.KVRecord, 0, len(args)/2)
	for i := 0; i < len(args); i += 2 {
		kvlist = append(kvlist, common.KVRecord{Key: args[i], Value: args[i+1]})
	}
	err := nd.store.MSet(ts, kvlist...)
	return nil, err
}

func (nd *KVNode) localIncrCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	v, err := nd.store.Incr(ts, cmd.Args[1])
	return v, err
}

func (nd *KVNode) localDelCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	nd.store.DelKeys(cmd.Args[1:]...)
	return int64(len(cmd.Args[1:])), nil
}

func (nd *KVNode) localPFCountCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	v, err := nd.store.PFCount(ts, cmd.Args[1:]...)
	return v, err
}

func (nd *KVNode) localPFAddCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	v, err := nd.store.PFAdd(ts, cmd.Args[1], cmd.Args[2:]...)
	return v, err
}
