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
	}
	conn.WriteInt64(n)
}

func (self *KVNode) lpopCommand(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	var val []byte
	self.Propose(cmd.Raw)
	// wait response
	conn.WriteBulk(val)
}

// local write command execute only on follower or on the local commit of leader
// the return value of follower is ignored, return value of local leader will be
// return to the future response.
func (self *KVNode) locallPopCommand(cmd redcon.Command) (interface{}, error) {
	return self.store.LPop(cmd.Args[1])
}
