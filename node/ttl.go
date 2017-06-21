package node

import (
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

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

type batchedExpire struct {
	common.DataType
	*KVNode
	pendingArgs [][]byte
	proposeC    chan []byte
	commitC     chan struct{}
}

func newBatchedExpire(t common.DataType, node *KVNode) *batchedExpire {
	var batchedCmd []byte

	switch t {
	case common.KV:
		batchedCmd = []byte("del")
	case common.HASH:
		batchedCmd = []byte("hmclear")
	case common.LIST:
		batchedCmd = []byte("lmclear")
	case common.SET:
		batchedCmd = []byte("smclear")
	case common.ZSET:
		batchedCmd = []byte("zmclear")
	}

	batExp := &batchedExpire{
		pendingArgs: make([][]byte, 1, 1000+1),
		KVNode:      node,
		DataType:    t,
		proposeC:    make(chan []byte, 1000),
		commitC:     make(chan struct{}),
	}

	batExp.pendingArgs[0] = batchedCmd

	return batExp
}

func (self *batchedExpire) Start() {
	//TODO, pick a appropriate check interval
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case k := <-self.proposeC:
			self.pendingArgs = append(self.pendingArgs, k)
			if len(self.pendingArgs) >= 1000 {
				self.batchCommit()
			}
		case <-ticker.C:
			self.batchCommit()
		case _, ok := <-self.commitC:
			self.batchCommit()
			if !ok {
				return
			}
		}
	}
}

func (self *batchedExpire) Clear() {
	self.pendingArgs = self.pendingArgs[0:1]
}

func (self *batchedExpire) Propose(k []byte) {
	self.proposeC <- k
}

func (self *batchedExpire) batchCommit() (err error) {
	if len(self.pendingArgs) <= 1 {
		return
	}

	cmd := buildCommand(self.pendingArgs)
	if _, err := self.KVNode.Propose(cmd.Raw); err != nil {
		err = fmt.Errorf("failed to propose the command which handles expired data, type:%d,"+
			"pending:%s, err:%s", self.DataType, len(self.pendingArgs), err.Error())
	}
	self.pendingArgs = self.pendingArgs[0:1]
	return
}

func (self *batchedExpire) Commit() {
	self.commitC <- struct{}{}
}

func (self *batchedExpire) Stop() {
	close(self.commitC)
}

type ExpireHandler struct {
	node           *KVNode
	batchedExpires [common.ALL - common.NONE]*batchedExpire
	resetC         chan struct{}
	quitC          chan struct{}
	wg             sync.WaitGroup
	running        int32
}

func NewExpireHandler(nd *KVNode) *ExpireHandler {
	h := &ExpireHandler{
		node:   nd,
		resetC: make(chan struct{}),
	}

	dataTypes := []common.DataType{
		common.KV, common.LIST, common.HASH, common.SET, common.ZSET,
	}

	for _, t := range dataTypes {
		h.batchedExpires[t] = newBatchedExpire(t, nd)
	}

	return h
}

func (self *ExpireHandler) Start() {
	if !atomic.CompareAndSwapInt32(&self.running, 0, 1) {
		return
	}

	self.quitC = make(chan struct{})

	for _, t := range self.batchedExpires {
		if t != nil {
			self.wg.Add(1)
			go func(t *batchedExpire) {
				defer self.wg.Done()
				t.Start()
			}(t)
		}
	}

	handleFunc := func(commitC chan struct{}, expiredDataC chan *common.ExpiredData) {
		defer func() {
			for _, t := range self.batchedExpires {
				if t != nil {
					t.Commit()
				}
			}
		}()
		for {
			select {
			case eData := <-expiredDataC:
				self.batchedExpires[eData.DataType].Propose(eData.Key)
			case <-commitC:
				for _, t := range self.batchedExpires {
					if t != nil {
						t.Commit()
					}
				}
			case <-self.quitC:
				return
			case <-self.resetC:
				return
			}
		}
	}

	for {
		commitC, expiredDataC := self.node.store.GetExpiredDataChan()
		handleFunc(commitC, expiredDataC)

		select {
		case <-self.quitC:
			for _, t := range self.batchedExpires {
				if t != nil {
					t.Stop()
				}
			}
			return
		default:
			continue
		}
	}

}

func (self *ExpireHandler) Reset() {
	self.resetC <- struct{}{}
}

func (self *ExpireHandler) Stop() {
	if !atomic.CompareAndSwapInt32(&self.running, 1, 0) {
		return
	} else {
		close(self.quitC)
		self.wg.Wait()
	}
}

func (self *ExpireHandler) Running() bool {
	return atomic.LoadInt32(&self.running) == 1
}
