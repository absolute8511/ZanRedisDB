package node

import (
	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/tidwall/redcon"
	"strconv"
)

func (self *KVNode) hgetCommand(conn redcon.Conn, cmd redcon.Command) {
	val, err := self.store.HGet(cmd.Args[1], cmd.Args[2])
	if err != nil || val == nil {
		conn.WriteNull()
	} else {
		conn.WriteBulk(val)
	}
}

func (self *KVNode) hgetallCommand(conn redcon.Conn, cmd redcon.Command) {
	n, valCh, err := self.store.HGetAll(cmd.Args[1])
	if err != nil {
		conn.WriteError("ERR for " + string(cmd.Args[0]) + " command: " + err.Error())
	}
	conn.WriteArray(int(n) * 2)
	for v := range valCh {
		conn.WriteBulk(v.Rec.Key)
		conn.WriteBulk(v.Rec.Value)
	}
}

func (self *KVNode) hkeysCommand(conn redcon.Conn, cmd redcon.Command) {
	n, valCh, _ := self.store.HKeys(cmd.Args[1])
	conn.WriteArray(int(n))
	for v := range valCh {
		conn.WriteBulk(v.Rec.Key)
	}
}

func (self *KVNode) hexistsCommand(conn redcon.Conn, cmd redcon.Command) {
	val, err := self.store.HGet(cmd.Args[1], cmd.Args[2])
	if err != nil || val == nil {
		conn.WriteInt(0)
	} else {
		conn.WriteInt(1)
	}
}

func (self *KVNode) hmgetCommand(conn redcon.Conn, cmd redcon.Command) {
	vals, _ := self.store.HMget(cmd.Args[1], cmd.Args[2:]...)
	conn.WriteArray(len(vals))
	for _, v := range vals {
		conn.WriteBulk(v)
	}
}

func (self *KVNode) hlenCommand(conn redcon.Conn, cmd redcon.Command) {
	val, err := self.store.HLen(cmd.Args[1])
	if err != nil {
		conn.WriteInt(0)
	} else {
		conn.WriteInt64(val)
	}
}

func (self *KVNode) hsetCommand(conn redcon.Conn, cmd redcon.Command, v interface{}) {
	if rsp, ok := v.(int64); ok {
		conn.WriteInt64(rsp)
	} else {
		conn.WriteError(errInvalidResponse.Error())
	}
}

func (self *KVNode) hmsetCommand(conn redcon.Conn, cmd redcon.Command, v interface{}) {
	conn.WriteString("OK")
}

func (self *KVNode) hsetnxCommand(conn redcon.Conn, cmd redcon.Command, v interface{}) {
	conn.WriteString("OK")
}

func (self *KVNode) hdelCommand(conn redcon.Conn, cmd redcon.Command, v interface{}) {
	if rsp, ok := v.(int64); ok {
		conn.WriteInt64(rsp)
	} else {
		conn.WriteError(errInvalidResponse.Error())
	}
}

func (self *KVNode) hincrbyCommand(conn redcon.Conn, cmd redcon.Command, v interface{}) {
	if rsp, ok := v.(int64); ok {
		conn.WriteInt64(rsp)
	} else {
		conn.WriteError(errInvalidResponse.Error())
	}
}

func (self *KVNode) hclearCommand(conn redcon.Conn, cmd redcon.Command, v interface{}) {
	if rsp, ok := v.(int64); ok {
		conn.WriteInt64(rsp)
	} else {
		conn.WriteError(errInvalidResponse.Error())
	}
}

// local write command execute only on follower or on the local commit of leader
// the return value of follower is ignored, return value of local leader will be
// return to the future response.
func (self *KVNode) localHSetCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	v, err := self.store.HSet(cmd.Args[1], cmd.Args[2], cmd.Args[3])
	return v, err
}

func (self *KVNode) localHMsetCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	args := cmd.Args[2:]
	if len(args)%2 != 0 {
		return nil, common.ErrInvalidArgs
	}
	fvs := make([]common.KVRecord, 0, len(args)/2)
	for i := 0; i < len(args); i += 2 {
		fvs = append(fvs, common.KVRecord{Key: args[i], Value: args[i+1]})
	}
	err := self.store.HMset(cmd.Args[1], fvs...)
	return nil, err
}

func (self *KVNode) localHIncrbyCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	v, _ := strconv.Atoi(string(cmd.Args[3]))
	ret, err := self.store.HIncrBy(cmd.Args[1], cmd.Args[2], int64(v))
	return ret, err
}

func (self *KVNode) localHDelCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	n, err := self.store.HDel(cmd.Args[1], cmd.Args[2:]...)
	if err != nil {
		// leader write need response
		return int64(0), err
	} else {
		return n, nil
	}
}

func (self *KVNode) localHclearCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	return self.store.HClear(cmd.Args[1])
}
