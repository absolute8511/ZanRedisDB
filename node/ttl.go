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
	if rsp, ok := v.(int); ok {
		conn.WriteInt(rsp)
	} else {
		conn.WriteError(errInvalidResponse.Error())
	}
}

func (self *KVNode) listExpireCommand(conn redcon.Conn, cmd redcon.Command, v interface{}) {
	if rsp, ok := v.(int); ok {
		conn.WriteInt(rsp)
	} else {
		conn.WriteError(errInvalidResponse.Error())
	}

}

func (self *KVNode) hashExpireCommand(conn redcon.Conn, cmd redcon.Command, v interface{}) {
	if rsp, ok := v.(int); ok {
		conn.WriteInt(rsp)
	} else {
		conn.WriteError(errInvalidResponse.Error())
	}
}

func (self *KVNode) setExpireCommand(conn redcon.Conn, cmd redcon.Command, v interface{}) {
	if rsp, ok := v.(int); ok {
		conn.WriteInt(rsp)
	} else {
		conn.WriteError(errInvalidResponse.Error())
	}
}

func (self *KVNode) zsetExpireCommand(conn redcon.Conn, cmd redcon.Command, v interface{}) {
	if rsp, ok := v.(int); ok {
		conn.WriteInt(rsp)
	} else {
		conn.WriteError(errInvalidResponse.Error())
	}
}

func (self *KVNode) localSetexCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	duration, err := strconv.Atoi(string(cmd.Args[2]))
	if err != nil {
		return nil, err
	}

	if err := self.store.KVSet(ts, cmd.Args[1], cmd.Args[3]); err != nil {
		return nil, err
	} else {
		return nil, self.store.KVExpire(cmd.Args[1], int64(duration), self.store.GetTTLChecker())
	}
}

func (self *KVNode) localExpireCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	if val, err := self.store.LocalLookup(cmd.Args[1]); err != nil {
		return 0, err
	} else if val == nil {
		return 0, nil
	} else {
		if duration, err := strconv.Atoi(string(cmd.Args[2])); err != nil {
			return 0, err
		} else {
			return 1, self.store.KVExpire(cmd.Args[1], int64(duration), self.store.GetTTLChecker())
		}
	}
}

func (self *KVNode) localHashExpireCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	if len, err := self.store.HLen(cmd.Args[1]); err != nil {
		return 0, err
	} else if len <= 0 {
		return 0, nil
	} else {
		if duration, err := strconv.Atoi(string(cmd.Args[2])); err != nil {
			return 0, err
		} else {
			return 1, self.store.HashExpire(cmd.Args[1], int64(duration), self.store.GetTTLChecker())
		}
	}
}

func (self *KVNode) localListExpireCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	if len, err := self.store.LLen(cmd.Args[1]); err != nil {
		return 0, err
	} else if len <= 0 {
		return 0, nil
	} else {
		if duration, err := strconv.Atoi(string(cmd.Args[2])); err != nil {
			return 0, err
		} else {
			return 1, self.store.ListExpire(cmd.Args[1], int64(duration), self.store.GetTTLChecker())
		}
	}
}

func (self *KVNode) localSetExpireCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	if card, err := self.store.SCard(cmd.Args[1]); err != nil {
		return 0, err
	} else if card <= 0 {
		return 0, nil
	} else {
		if duration, err := strconv.Atoi(string(cmd.Args[2])); err != nil {
			return 0, err
		} else {
			return 1, self.store.SetExpire(cmd.Args[1], int64(duration), self.store.GetTTLChecker())
		}
	}
}

func (self *KVNode) localZSetExpireCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	if card, err := self.store.ZCard(cmd.Args[1]); err != nil {
		return 0, err
	} else if card <= 0 {
		return 0, nil
	} else {
		if duration, err := strconv.Atoi(string(cmd.Args[2])); err != nil {
			return 0, err
		} else {
			return 1, self.store.ZSetExpire(cmd.Args[1], int64(duration), self.store.GetTTLChecker())
		}
	}
}

func (self *KVNode) persistCommand(conn redcon.Conn, cmd redcon.Command, v interface{}) {
	if rsp, ok := v.(int); ok {
		conn.WriteInt(rsp)
	} else {
		conn.WriteError(errInvalidResponse.Error())
	}
}

func (self *KVNode) localPersistCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	if val, err := self.store.LocalLookup(cmd.Args[1]); err != nil {
		return 0, err
	} else if val == nil {
		return 0, nil
	}

	if ttl, err := self.store.KVTtl(cmd.Args[1]); err != nil {
		return 0, err
	} else if ttl < 0 {
		return 0, nil
	}

	return 1, self.store.KVPersist(cmd.Args[1])
}

func (self *KVNode) localHashPersistCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	if len, err := self.store.HLen(cmd.Args[1]); err != nil {
		return 0, err
	} else if len <= 0 {
		return 0, nil
	}

	if ttl, err := self.store.HashTtl(cmd.Args[1]); err != nil {
		return 0, err
	} else if ttl < 0 {
		return 0, nil
	}

	return 1, self.store.HashPersist(cmd.Args[1])
}

func (self *KVNode) localListPersistCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	if len, err := self.store.LLen(cmd.Args[1]); err != nil {
		return 0, err
	} else if len <= 0 {
		return 0, nil
	}

	if ttl, err := self.store.ListTtl(cmd.Args[1]); err != nil {
		return 0, err
	} else if ttl < 0 {
		return 0, nil
	}

	return 1, self.store.ListPersist(cmd.Args[1])
}

func (self *KVNode) localSetPersistCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	if card, err := self.store.SCard(cmd.Args[1]); err != nil {
		return 0, err
	} else if card <= 0 {
		return 0, nil
	}

	if ttl, err := self.store.SetTtl(cmd.Args[1]); err != nil {
		return 0, err
	} else if ttl < 0 {
		return 0, nil
	}

	return 1, self.store.SetPersist(cmd.Args[1])
}

func (self *KVNode) localZSetPersistCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	if card, err := self.store.ZCard(cmd.Args[1]); err != nil {
		return 0, err
	} else if card <= 0 {
		return 0, nil
	}

	if ttl, err := self.store.ZSetTtl(cmd.Args[1]); err != nil {
		return 0, err
	} else if ttl < 0 {
		return 0, nil
	}

	return 1, self.store.ZSetPersist(cmd.Args[1])
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
