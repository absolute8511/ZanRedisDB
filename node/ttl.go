package node

import (
	"errors"
	"strconv"

	"github.com/absolute8511/redcon"
	"github.com/youzan/ZanRedisDB/common"
)

var (
	expireCmds                [common.ALL - common.NONE][]byte
	ErrExpiredBatchedBuffFull = errors.New("the expired data batched buffer is full now")
)

const (
	expiredBuffedCommitInterval = 1
	raftBatchBufferSize         = 1024 * 4
)

func init() {
	expireCmds[common.KV] = []byte("del")
	// make sure we registered these internal command for clear,
	// api is not registered for these commands
	expireCmds[common.HASH] = []byte("hmclear")
	expireCmds[common.LIST] = []byte("lmclear")
	expireCmds[common.SET] = []byte("smclear")
	expireCmds[common.ZSET] = []byte("zmclear")
}

func (nd *KVNode) setexCommand(cmd redcon.Command, v interface{}) (interface{}, error) {
	return "OK", nil
}

func (kvsm *kvStoreSM) localSetexCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	if duration, err := strconv.Atoi(string(cmd.Args[2])); err != nil {
		return nil, err
	} else {
		return nil, kvsm.store.SetEx(ts, cmd.Args[1], int64(duration), cmd.Args[3])
	}
}

func (kvsm *kvStoreSM) localExpireCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	if duration, err := strconv.Atoi(string(cmd.Args[2])); err != nil {
		return int64(0), err
	} else {
		return kvsm.store.Expire(ts, cmd.Args[1], int64(duration))
	}
}

func (kvsm *kvStoreSM) localHashExpireCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	if duration, err := strconv.Atoi(string(cmd.Args[2])); err != nil {
		return int64(0), err
	} else {
		return kvsm.store.HExpire(ts, cmd.Args[1], int64(duration))
	}
}

func (kvsm *kvStoreSM) localListExpireCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	if duration, err := strconv.Atoi(string(cmd.Args[2])); err != nil {
		return int64(0), err
	} else {
		return kvsm.store.LExpire(ts, cmd.Args[1], int64(duration))
	}
}

func (kvsm *kvStoreSM) localSetExpireCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	if duration, err := strconv.Atoi(string(cmd.Args[2])); err != nil {
		return int64(0), err
	} else {
		return kvsm.store.SExpire(ts, cmd.Args[1], int64(duration))
	}
}

func (kvsm *kvStoreSM) localZSetExpireCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	if duration, err := strconv.Atoi(string(cmd.Args[2])); err != nil {
		return int64(0), err
	} else {
		return kvsm.store.ZExpire(ts, cmd.Args[1], int64(duration))
	}
}

func (kvsm *kvStoreSM) localBitExpireCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	if duration, err := strconv.Atoi(string(cmd.Args[2])); err != nil {
		return int64(0), err
	} else {
		return kvsm.store.BitExpire(ts, cmd.Args[1], int64(duration))
	}
}

func (kvsm *kvStoreSM) localPersistCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	return kvsm.store.Persist(ts, cmd.Args[1])
}

func (kvsm *kvStoreSM) localHashPersistCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	return kvsm.store.HPersist(ts, cmd.Args[1])
}

func (kvsm *kvStoreSM) localListPersistCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	return kvsm.store.LPersist(ts, cmd.Args[1])
}

func (kvsm *kvStoreSM) localSetPersistCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	return kvsm.store.SPersist(ts, cmd.Args[1])
}

func (kvsm *kvStoreSM) localZSetPersistCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	return kvsm.store.ZPersist(ts, cmd.Args[1])
}

func (kvsm *kvStoreSM) localBitPersistCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	return kvsm.store.BitPersist(ts, cmd.Args[1])
}

//read commands related to TTL
func (nd *KVNode) ttlCommand(conn redcon.Conn, cmd redcon.Command) {
	if v, err := nd.store.KVTtl(cmd.Args[1]); err != nil {
		conn.WriteError(err.Error())
	} else {
		conn.WriteInt64(v)
	}
}

func (nd *KVNode) httlCommand(conn redcon.Conn, cmd redcon.Command) {
	if v, err := nd.store.HashTtl(cmd.Args[1]); err != nil {
		conn.WriteError(err.Error())
	} else {
		conn.WriteInt64(v)
	}
}

func (nd *KVNode) lttlCommand(conn redcon.Conn, cmd redcon.Command) {
	if v, err := nd.store.ListTtl(cmd.Args[1]); err != nil {
		conn.WriteError(err.Error())
	} else {
		conn.WriteInt64(v)
	}
}

func (nd *KVNode) sttlCommand(conn redcon.Conn, cmd redcon.Command) {
	if v, err := nd.store.SetTtl(cmd.Args[1]); err != nil {
		conn.WriteError(err.Error())
	} else {
		conn.WriteInt64(v)
	}
}

func (nd *KVNode) zttlCommand(conn redcon.Conn, cmd redcon.Command) {
	if v, err := nd.store.ZSetTtl(cmd.Args[1]); err != nil {
		conn.WriteError(err.Error())
	} else {
		conn.WriteInt64(v)
	}
}

func (nd *KVNode) bttlCommand(conn redcon.Conn, cmd redcon.Command) {
	if v, err := nd.store.BitTtl(cmd.Args[1]); err != nil {
		conn.WriteError(err.Error())
	} else {
		conn.WriteInt64(v)
	}
}

func (nd *KVNode) hKeyExistCommand(conn redcon.Conn, cmd redcon.Command) {
	if v, err := nd.store.HKeyExists(cmd.Args[1]); err != nil {
		conn.WriteError(err.Error())
	} else {
		conn.WriteInt64(v)
	}
}

func (nd *KVNode) lKeyExistCommand(conn redcon.Conn, cmd redcon.Command) {
	if v, err := nd.store.LKeyExists(cmd.Args[1]); err != nil {
		conn.WriteError(err.Error())
	} else {
		conn.WriteInt64(v)
	}
}

func (nd *KVNode) sKeyExistCommand(conn redcon.Conn, cmd redcon.Command) {
	if v, err := nd.store.SKeyExists(cmd.Args[1]); err != nil {
		conn.WriteError(err.Error())
	} else {
		conn.WriteInt64(v)
	}
}

func (nd *KVNode) zKeyExistCommand(conn redcon.Conn, cmd redcon.Command) {
	if v, err := nd.store.ZKeyExists(cmd.Args[1]); err != nil {
		conn.WriteError(err.Error())
	} else {
		conn.WriteInt64(v)
	}
}

func (nd *KVNode) bKeyExistCommand(conn redcon.Conn, cmd redcon.Command) {
	if v, err := nd.store.BitKeyExist(cmd.Args[1]); err != nil {
		conn.WriteError(err.Error())
	} else {
		conn.WriteInt64(v)
	}
}
