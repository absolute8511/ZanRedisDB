package node

import (
	"github.com/tidwall/redcon"
	"strconv"
)

func (self *KVNode) lindexCommand(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 3 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	index, err := strconv.ParseInt(string(cmd.Args[2]), 10, 64)
	if err != nil {
		conn.WriteError("Invalid index: " + err.Error())
		return
	}
	val, err := self.store.LIndex(cmd.Args[1], index)
	if err != nil {
		conn.WriteNull()
	} else {
		conn.WriteBulk(val)
	}
}

func (self *KVNode) llenCommand(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	n, err := self.store.LLen(cmd.Args[1])
	if err != nil {
		conn.WriteError("Err: " + err.Error())
		return
	}
	conn.WriteInt64(n)
}

func (self *KVNode) lrangeCommand(conn redcon.Conn, cmd redcon.Command) {
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

	vlist, err := self.store.LRange(cmd.Args[1], start, end)
	if err != nil {
		conn.WriteError("Err: " + err.Error())
		return
	}
	conn.WriteArray(len(vlist))
	for _, d := range vlist {
		conn.WriteBulk(d)
	}
}

func (self *KVNode) lpopCommand(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	v, err := self.Propose(cmd.Raw)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	rsp, ok := v.([]byte)
	if !ok {
		conn.WriteError("Invalid response type")
		return
	}
	// wait response
	conn.WriteBulk(rsp)
}

func (self *KVNode) lpushCommand(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) < 3 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	v, err := self.Propose(cmd.Raw)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	rsp, ok := v.(int64)
	if !ok {
		conn.WriteError("Invalid response type")
		return
	}
	// wait response
	conn.WriteInt64(rsp)
}

func (self *KVNode) lsetCommand(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 3 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	_, err := strconv.ParseInt(string(cmd.Args[2]), 10, 64)
	if err != nil {
		conn.WriteError("Invalid index: " + err.Error())
		return
	}

	_, err = self.Propose(cmd.Raw)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	// wait response
	conn.WriteString("OK")
}

func (self *KVNode) ltrimCommand(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 3 {
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

	_, err = self.Propose(cmd.Raw)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	// wait response
	conn.WriteString("OK")
}

func (self *KVNode) rpopCommand(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	v, err := self.Propose(cmd.Raw)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	rsp, ok := v.([]byte)
	if !ok {
		conn.WriteError("Invalid response type")
		return
	}
	// wait response
	conn.WriteBulk(rsp)
}

func (self *KVNode) rpushCommand(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) < 3 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	v, err := self.Propose(cmd.Raw)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	rsp, ok := v.(int64)
	if !ok {
		conn.WriteError("Invalid response type")
		return
	}
	// wait response
	conn.WriteInt64(rsp)
}

func (self *KVNode) lclearCommand(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	v, err := self.Propose(cmd.Raw)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
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
func (self *KVNode) localLpopCommand(cmd redcon.Command) (interface{}, error) {
	return self.store.LPop(cmd.Args[1])
}

func (self *KVNode) localLpushCommand(cmd redcon.Command) (interface{}, error) {
	return self.store.LPush(cmd.Args[1], cmd.Args[2:]...)
}

func (self *KVNode) localLsetCommand(cmd redcon.Command) (interface{}, error) {
	index, err := strconv.ParseInt(string(cmd.Args[2]), 10, 64)
	if err != nil {
		return nil, err
	}

	return nil, self.store.LSet(cmd.Args[1], index, cmd.Args[3])
}

func (self *KVNode) localLtrimCommand(cmd redcon.Command) (interface{}, error) {
	start, err := strconv.ParseInt(string(cmd.Args[2]), 10, 64)
	if err != nil {
		return nil, err
	}
	stop, err := strconv.ParseInt(string(cmd.Args[2]), 10, 64)
	if err != nil {
		return nil, err
	}

	return nil, self.store.LTrim(cmd.Args[1], start, stop)
}

func (self *KVNode) localRpopCommand(cmd redcon.Command) (interface{}, error) {
	return self.store.RPop(cmd.Args[1])
}

func (self *KVNode) localRpushCommand(cmd redcon.Command) (interface{}, error) {
	return self.store.RPush(cmd.Args[1], cmd.Args[2:]...)
}

func (self *KVNode) localLclearCommand(cmd redcon.Command) (interface{}, error) {
	return self.store.LClear(cmd.Args[1])
}
