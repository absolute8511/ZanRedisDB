package node

import (
	"strconv"

	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/tidwall/redcon"
)

func (self *KVNode) setexCommand(conn redcon.Conn, cmd redcon.Command, v interface{}) {
	conn.WriteString("OK")
}

func (self *KVNode) expireCommand(conn redcon.Conn, cmd redcon.Command, v interface{}) {
	if rsp, ok := v.(int64); ok {
		conn.WriteInt64(rsp)
	} else {
		conn.WriteError(errInvalidResponse.Error())
	}
}

func (self *KVNode) listExpireCommand(conn redcon.Conn, cmd redcon.Command, v interface{}) {
	if rsp, ok := v.(int64); ok {
		conn.WriteInt64(rsp)
	} else {
		conn.WriteError(errInvalidResponse.Error())
	}
}

func (self *KVNode) hashExpireCommand(conn redcon.Conn, cmd redcon.Command, v interface{}) {
	if rsp, ok := v.(int64); ok {
		conn.WriteInt64(rsp)
	} else {
		conn.WriteError(errInvalidResponse.Error())
	}
}

func (self *KVNode) setExpireCommand(conn redcon.Conn, cmd redcon.Command, v interface{}) {
	if rsp, ok := v.(int64); ok {
		conn.WriteInt64(rsp)
	} else {
		conn.WriteError(errInvalidResponse.Error())
	}
}

func (self *KVNode) zsetExpireCommand(conn redcon.Conn, cmd redcon.Command, v interface{}) {
	if rsp, ok := v.(int64); ok {
		conn.WriteInt64(rsp)
	} else {
		conn.WriteError(errInvalidResponse.Error())
	}
}

func (self *KVNode) localSetexCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	if duration, err := strconv.Atoi(string(cmd.Args[2])); err != nil {
		return nil, err
	} else {
		return nil, self.store.SetEx(ts, cmd.Args[1], int64(duration), cmd.Args[3], self.store.GetTTLChecker())
	}
}

func (self *KVNode) localExpireCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	if duration, err := strconv.Atoi(string(cmd.Args[2])); err != nil {
		return int64(0), err
	} else {
		return self.store.Expire(cmd.Args[1], int64(duration), self.store.GetTTLChecker())
	}
}

func (self *KVNode) localHashExpireCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	if duration, err := strconv.Atoi(string(cmd.Args[2])); err != nil {
		return int64(0), err
	} else {
		return self.store.HExpire(cmd.Args[1], int64(duration), self.store.GetTTLChecker())
	}
}

func (self *KVNode) localListExpireCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	if duration, err := strconv.Atoi(string(cmd.Args[2])); err != nil {
		return int64(0), err
	} else {
		return self.store.LExpire(cmd.Args[1], int64(duration), self.store.GetTTLChecker())
	}
}

func (self *KVNode) localSetExpireCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	if duration, err := strconv.Atoi(string(cmd.Args[2])); err != nil {
		return int64(0), err
	} else {
		return self.store.SExpire(cmd.Args[1], int64(duration), self.store.GetTTLChecker())
	}
}

func (self *KVNode) localZSetExpireCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	if duration, err := strconv.Atoi(string(cmd.Args[2])); err != nil {
		return int64(0), err
	} else {
		return self.store.ZExpire(cmd.Args[1], int64(duration), self.store.GetTTLChecker())
	}
}

func (self *KVNode) persistCommand(conn redcon.Conn, cmd redcon.Command, v interface{}) {
	if rsp, ok := v.(int64); ok {
		conn.WriteInt64(rsp)
	} else {
		conn.WriteError(errInvalidResponse.Error())
	}
}

func (self *KVNode) localPersistCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	return self.store.Persist(cmd.Args[1])
}

func (self *KVNode) localHashPersistCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	return self.store.HPersist(cmd.Args[1])
}

func (self *KVNode) localListPersistCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	return self.store.LPersist(cmd.Args[1])
}

func (self *KVNode) localSetPersistCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	return self.store.SPersist(cmd.Args[1])
}

func (self *KVNode) localZSetPersistCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	return self.store.ZPersist(cmd.Args[1])
}

//read commands related to TTL
func (self *KVNode) ttlCommand(conn redcon.Conn, cmd redcon.Command) {
	if v, err := self.store.KVTtl(cmd.Args[1]); err != nil {
		conn.WriteError(err.Error())
	} else {
		conn.WriteInt64(v)
	}
}

func (self *KVNode) httlCommand(conn redcon.Conn, cmd redcon.Command) {
	if v, err := self.store.HashTtl(cmd.Args[1]); err != nil {
		conn.WriteError(err.Error())
	} else {
		conn.WriteInt64(v)
	}
}

func (self *KVNode) lttlCommand(conn redcon.Conn, cmd redcon.Command) {
	if v, err := self.store.ListTtl(cmd.Args[1]); err != nil {
		conn.WriteError(err.Error())
	} else {
		conn.WriteInt64(v)
	}
}

func (self *KVNode) sttlCommand(conn redcon.Conn, cmd redcon.Command) {
	if v, err := self.store.SetTtl(cmd.Args[1]); err != nil {
		conn.WriteError(err.Error())
	} else {
		conn.WriteInt64(v)
	}
}

func (self *KVNode) zttlCommand(conn redcon.Conn, cmd redcon.Command) {
	if v, err := self.store.ZSetTtl(cmd.Args[1]); err != nil {
		conn.WriteError(err.Error())
	} else {
		conn.WriteInt64(v)
	}
}

func (self *KVNode) createOnExpiredFunc(cmd string) common.OnExpiredFunc {
	return func(key []byte) error {
		cmd := buildCommand([][]byte{[]byte(cmd), key})
		_, err := self.Propose(cmd.Raw)
		return err
	}
}
