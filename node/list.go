package node

import (
	"strconv"

	"github.com/absolute8511/redcon"
)

func (nd *KVNode) lindexCommand(conn redcon.Conn, cmd redcon.Command) {
	index, err := strconv.ParseInt(string(cmd.Args[2]), 10, 64)
	if err != nil {
		conn.WriteError("Invalid index: " + err.Error())
		return
	}
	val, err := nd.store.LIndex(cmd.Args[1], index)
	if err != nil || val == nil {
		conn.WriteNull()
	} else {
		conn.WriteBulk(val)
	}
}

func (nd *KVNode) llenCommand(conn redcon.Conn, cmd redcon.Command) {
	n, err := nd.store.LLen(cmd.Args[1])
	if err != nil {
		conn.WriteError("Err: " + err.Error())
		return
	}
	conn.WriteInt64(n)
}

func (nd *KVNode) lrangeCommand(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 4 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	start, err := strconv.ParseInt(string(cmd.Args[2]), 10, 64)
	if err != nil {
		conn.WriteError("Invalid index: " + err.Error())
		return
	}
	end, err := strconv.ParseInt(string(cmd.Args[3]), 10, 64)
	if err != nil {
		conn.WriteError("Invalid index: " + err.Error())
		return
	}

	vlist, err := nd.store.LRange(cmd.Args[1], start, end)
	if err != nil {
		conn.WriteError("Err: " + err.Error())
		return
	}
	conn.WriteArray(len(vlist))
	for _, d := range vlist {
		conn.WriteBulk(d)
	}
}

func (nd *KVNode) lfixkeyCommand(conn redcon.Conn, cmd redcon.Command, v interface{}) {
	conn.WriteString("OK")
}

func (nd *KVNode) lpopCommand(conn redcon.Conn, cmd redcon.Command, v interface{}) {
	rsp, ok := v.([]byte)
	if !ok {
		conn.WriteError("Invalid response type")
		return
	}
	// wait response
	conn.WriteBulk(rsp)
}

func (nd *KVNode) lpushCommand(conn redcon.Conn, cmd redcon.Command, v interface{}) {
	rsp, ok := v.(int64)
	if !ok {
		conn.WriteError("Invalid response type")
		return
	}
	// wait response
	conn.WriteInt64(rsp)
}

func (nd *KVNode) lsetCommand(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 4 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	_, err := strconv.ParseInt(string(cmd.Args[2]), 10, 64)
	if err != nil {
		conn.WriteError("Invalid index: " + err.Error())
		return
	}
	_, _, ok := rebuildFirstKeyAndPropose(nd, conn, cmd)
	if !ok {
		return
	}
	// wait response
	conn.WriteString("OK")
}

func (nd *KVNode) ltrimCommand(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 4 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	_, err := strconv.ParseInt(string(cmd.Args[2]), 10, 64)
	if err != nil {
		conn.WriteError("Invalid start index: " + err.Error())
		return
	}
	_, err = strconv.ParseInt(string(cmd.Args[3]), 10, 64)
	if err != nil {
		conn.WriteError("Invalid end index: " + err.Error())
		return
	}

	_, _, ok := rebuildFirstKeyAndPropose(nd, conn, cmd)
	if !ok {
		return
	}
	// wait response
	conn.WriteString("OK")
}

func (nd *KVNode) rpopCommand(conn redcon.Conn, cmd redcon.Command, v interface{}) {
	rsp, ok := v.([]byte)
	if !ok {
		conn.WriteError("Invalid response type")
		return
	}
	// wait response
	conn.WriteBulk(rsp)
}

func (nd *KVNode) rpushCommand(conn redcon.Conn, cmd redcon.Command, v interface{}) {
	rsp, ok := v.(int64)
	if !ok {
		conn.WriteError("Invalid response type")
		return
	}
	// wait response
	conn.WriteInt64(rsp)
}

func (nd *KVNode) lclearCommand(conn redcon.Conn, cmd redcon.Command, v interface{}) {
	rsp, ok := v.(int64)
	if !ok {
		conn.WriteError("Invalid response type")
		return
	}
	// wait response
	conn.WriteInt64(rsp)
}

// local write command execute only on follower or on the local commit of leader
// the return value of follower is ignored, return value of local leader will be
// return to the future response.
func (kvsm *kvStoreSM) localLfixkeyCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	kvsm.store.LFixKey(ts, cmd.Args[1])
	return nil, nil
}

func (kvsm *kvStoreSM) localLpopCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	return kvsm.store.LPop(ts, cmd.Args[1])
}

func (kvsm *kvStoreSM) localLpushCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	return kvsm.store.LPush(ts, cmd.Args[1], cmd.Args[2:]...)
}

func (kvsm *kvStoreSM) localLsetCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	index, err := strconv.ParseInt(string(cmd.Args[2]), 10, 64)
	if err != nil {
		return nil, err
	}

	return nil, kvsm.store.LSet(ts, cmd.Args[1], index, cmd.Args[3])
}

func (kvsm *kvStoreSM) localLtrimCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	start, err := strconv.ParseInt(string(cmd.Args[2]), 10, 64)
	if err != nil {
		return nil, err
	}
	stop, err := strconv.ParseInt(string(cmd.Args[3]), 10, 64)
	if err != nil {
		return nil, err
	}

	return nil, kvsm.store.LTrim(ts, cmd.Args[1], start, stop)
}

func (kvsm *kvStoreSM) localRpopCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	return kvsm.store.RPop(ts, cmd.Args[1])
}

func (kvsm *kvStoreSM) localRpushCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	return kvsm.store.RPush(ts, cmd.Args[1], cmd.Args[2:]...)
}

func (kvsm *kvStoreSM) localLclearCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	return kvsm.store.LClear(cmd.Args[1])
}

func (kvsm *kvStoreSM) localLMClearCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	var count int64
	for _, lkey := range cmd.Args[1:] {
		if _, err := kvsm.store.LClear(lkey); err != nil {
			return count, err
		} else {
			count++
		}
	}
	return count, nil
}
