package node

import (
	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/absolute8511/redcon"
)

func (nd *KVNode) jsonGetCommand(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) < 2 {
		conn.WriteError(common.ErrInvalidArgs.Error())
		return
	}
	var vals []string
	var err error
	if len(cmd.Args) == 2 {
		vals, err = nd.store.JGet(cmd.Args[1], []byte(""))
	} else {
		vals, err = nd.store.JGet(cmd.Args[1], cmd.Args[2:]...)
	}
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteArray(len(vals))
	for _, val := range vals {
		conn.WriteBulkString(val)
	}
}

func (nd *KVNode) jsonKeyExistsCommand(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) < 2 {
		conn.WriteError(common.ErrInvalidArgs.Error())
		return
	}
	n, err := nd.store.JKeyExists(cmd.Args[1])
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteInt64(n)
}

func (nd *KVNode) jsonmkGetCommand(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) < 3 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	if len(cmd.Args[1:]) >= common.MAX_BATCH_NUM {
		conn.WriteError(errTooMuchBatchSize.Error())
		return
	}
	keys := cmd.Args[1 : len(cmd.Args)-1]
	path := cmd.Args[len(cmd.Args)-1]
	for i := 0; i < len(keys); i++ {
		_, key, err := common.ExtractNamesapce(keys[i])
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
		keys[i] = key
	}
	nd.store.JMGet(path, keys...)
}

func (nd *KVNode) jsonTypeCommand(conn redcon.Conn, cmd redcon.Command) {
}

func (nd *KVNode) jsonArrayLenCommand(conn redcon.Conn, cmd redcon.Command) {
	var val int64
	var err error
	if len(cmd.Args) == 2 {
		val, err = nd.store.JArrayLen(cmd.Args[1], []byte(""))
	} else {
		val, err = nd.store.JArrayLen(cmd.Args[1], cmd.Args[2])
	}
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteInt64(val)
}

func (nd *KVNode) jsonObjLenCommand(conn redcon.Conn, cmd redcon.Command) {
	var val int64
	var err error
	if len(cmd.Args) == 2 {
		val, err = nd.store.JObjLen(cmd.Args[1], []byte(""))
	} else {
		val, err = nd.store.JObjLen(cmd.Args[1], cmd.Args[2])
	}
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteInt64(val)
}

func (nd *KVNode) jsonObjKeysCommand(conn redcon.Conn, cmd redcon.Command) {
	var vals []string
	var err error
	if len(cmd.Args) == 2 {
		vals, err = nd.store.JObjKeys(cmd.Args[1], []byte(""))
	} else {
		vals, err = nd.store.JObjKeys(cmd.Args[1], cmd.Args[2])
	}
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteArray(len(vals))
	for _, val := range vals {
		conn.WriteBulkString(val)
	}
}

func (nd *KVNode) jsonSetCommand(conn redcon.Conn, cmd redcon.Command, v interface{}) {
	conn.WriteString("OK")
}

func (nd *KVNode) jsonDelCommand(conn redcon.Conn, cmd redcon.Command, v interface{}) {
	if rsp, ok := v.(int64); ok {
		conn.WriteInt64(rsp)
	} else {
		conn.WriteError(errInvalidResponse.Error())
	}
}

func (nd *KVNode) jsonArrayAppendCommand(conn redcon.Conn, cmd redcon.Command, v interface{}) {
	if rsp, ok := v.(int64); ok {
		conn.WriteInt64(rsp)
	} else {
		conn.WriteError(errInvalidResponse.Error())
	}
}

func (nd *KVNode) jsonArrayPopCommand(conn redcon.Conn, cmd redcon.Command, v interface{}) {
	rsp, ok := v.(string)
	if !ok {
		conn.WriteError(errInvalidResponse.Error())
		return
	}
	conn.WriteBulkString(rsp)
}

func (nd *KVNode) localJsonSetCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	v, err := nd.store.JSet(ts, cmd.Args[1], cmd.Args[2], cmd.Args[3])
	return v, err
}

func (nd *KVNode) localJsonDelCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	n, err := nd.store.JDel(ts, cmd.Args[1], cmd.Args[2])
	if err != nil {
		return int64(0), err
	}
	return n, nil
}

func (nd *KVNode) localJsonArrayAppendCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	n, err := nd.store.JArrayAppend(ts, cmd.Args[1], cmd.Args[2], cmd.Args[3:]...)
	if err != nil {
		return int64(0), err
	}
	return n, nil
}

func (nd *KVNode) localJsonArrayPopCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	var err error
	var path []byte
	if len(cmd.Args) >= 3 {
		path = cmd.Args[2]
	}
	elem, err := nd.store.JArrayPop(ts, cmd.Args[1], path)
	if err != nil {
		return nil, err
	}
	return elem, nil
}
