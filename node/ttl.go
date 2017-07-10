package node

import (
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/tidwall/redcon"
)

var (
	expireCmds [common.ALL - common.NONE][]byte
)

func init() {
	expireCmds[common.KV] = []byte("del")
	expireCmds[common.HASH] = []byte("hmclear")
	expireCmds[common.LIST] = []byte("lmclear")
	expireCmds[common.SET] = []byte("smclear")
	expireCmds[common.ZSET] = []byte("zmclear")
}

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
		return nil, self.store.SetEx(ts, cmd.Args[1], int64(duration), cmd.Args[3])
	}
}

func (self *KVNode) localExpireCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	if duration, err := strconv.Atoi(string(cmd.Args[2])); err != nil {
		return int64(0), err
	} else {
		return self.store.Expire(cmd.Args[1], int64(duration))
	}
}

func (self *KVNode) localHashExpireCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	if duration, err := strconv.Atoi(string(cmd.Args[2])); err != nil {
		return int64(0), err
	} else {
		return self.store.HExpire(cmd.Args[1], int64(duration))
	}
}

func (self *KVNode) localListExpireCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	if duration, err := strconv.Atoi(string(cmd.Args[2])); err != nil {
		return int64(0), err
	} else {
		return self.store.LExpire(cmd.Args[1], int64(duration))
	}
}

func (self *KVNode) localSetExpireCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	if duration, err := strconv.Atoi(string(cmd.Args[2])); err != nil {
		return int64(0), err
	} else {
		return self.store.SExpire(cmd.Args[1], int64(duration))
	}
}

func (self *KVNode) localZSetExpireCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	if duration, err := strconv.Atoi(string(cmd.Args[2])); err != nil {
		return int64(0), err
	} else {
		return self.store.ZExpire(cmd.Args[1], int64(duration))
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

type ExpireHandler struct {
	node    *KVNode
	resetC  chan struct{}
	quitC   chan struct{}
	wg      sync.WaitGroup
	running int32
}

func NewExpireHandler(node *KVNode) *ExpireHandler {
	return &ExpireHandler{
		node:   node,
		resetC: make(chan struct{}),
	}
}

func (self *ExpireHandler) Start() {
	if !atomic.CompareAndSwapInt32(&self.running, 0, 1) {
		return
	}

	self.quitC = make(chan struct{})

	types := []common.DataType{
		common.KV, common.LIST, common.SET, common.ZSET, common.HASH,
	}

	for {
		stopCh := make(chan struct{})
		for _, t := range types {
			self.wg.Add(1)

			go func(t common.DataType, stopCh chan struct{}) {
				defer self.wg.Done()
				expiredCh := self.node.store.GetExpiredDataChan(t)

				//apply expiration of different data types
				applyExpiration(self.node, t, expiredCh, stopCh)
			}(t, stopCh)
		}

		select {
		case <-self.resetC:
			//the underlying store has changed and the expired data channel should be reacquired
			close(stopCh)
			self.wg.Wait()

		case <-self.quitC:
			close(stopCh)
			return
		}
	}

}

func (self *ExpireHandler) Reset() {
	self.resetC <- struct{}{}
}

func (self *ExpireHandler) Stop() {
	if atomic.CompareAndSwapInt32(&self.running, 1, 0) {
		close(self.quitC)
		self.wg.Wait()
	}
}

func (self *ExpireHandler) Running() bool {
	return atomic.LoadInt32(&self.running) == 1
}

func rawExpireCommand(dt common.DataType, keys [][]byte) []byte {
	cmd := expireCmds[dt]
	buf := make([]byte, 0, 128)

	buf = append(buf, '*')
	buf = append(buf, strconv.FormatInt(int64(len(keys)+1), 10)...)
	buf = append(buf, '\r', '\n')

	buf = append(buf, '$')
	buf = append(buf, strconv.FormatInt(int64(len(cmd)), 10)...)
	buf = append(buf, '\r', '\n')
	buf = append(buf, cmd...)
	buf = append(buf, '\r', '\n')

	for _, key := range keys {
		buf = append(buf, '$')
		buf = append(buf, strconv.FormatInt(int64(len(key)), 10)...)
		buf = append(buf, '\r', '\n')
		buf = append(buf, key...)
		buf = append(buf, '\r', '\n')
	}
	return buf
}

func applyExpiration(node *KVNode, dataType common.DataType, expiredCh chan *common.ExpiredData, stopCh chan struct{}) {
	for {
		select {
		case v, ok := <-expiredCh:
			if ok {
				node.Propose(rawExpireCommand(dataType, v.Keys))
			} else {
				return
			}
		case <-stopCh:
			return
		}
	}
}
