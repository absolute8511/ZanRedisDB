package node

import (
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/tidwall/redcon"
)

var (
	expireCmds [common.ALL - common.NONE][]byte
)

const (
	expiredBuffedCommitInterval = 1
	raftBatchBufferSize         = 1024
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
	node            *KVNode
	quitC           chan struct{}
	leaderChangedCh chan struct{}
	wg              sync.WaitGroup
	running         int32

	//prevent 'applyExpiration' start more than one time at the same time
	applyLock sync.Mutex

	batchBuffer [common.ALL - common.NONE]*raftBatchBuffer
}

func NewExpireHandler(node *KVNode) *ExpireHandler {
	handler := &ExpireHandler{
		node:            node,
		leaderChangedCh: make(chan struct{}, 8),
		quitC:           make(chan struct{}),
	}

	for t, _ := range handler.batchBuffer {
		handler.batchBuffer[t] = newRaftBatchBuffer(node, common.DataType(t))
	}
	return handler
}

func (self *ExpireHandler) Start() {
	if !atomic.CompareAndSwapInt32(&self.running, 0, 1) {
		return
	}
	self.node.store.StartTTLChecker()

	self.wg.Add(1)
	go func() {
		defer self.wg.Done()
		self.watchLeaderChanged()
	}()

	switch self.node.expirationPolicy {
	case common.LocalDeletion:

	case common.ConsistencyDeletion:
		// the leader should watch the expired data and
		// delete them through RAFT

	case common.PeriodicalRotation:
	}

}

func (self *ExpireHandler) Stop() {
	if atomic.CompareAndSwapInt32(&self.running, 1, 0) {
		close(self.quitC)
		self.node.store.StopTTLChecker()
		self.wg.Wait()
	}
}

func (self *ExpireHandler) LeaderChanged() {
	self.leaderChangedCh <- struct{}{}
}

func (self *ExpireHandler) watchLeaderChanged() {
	var stop chan struct{}
	applying := false

	for {
		select {
		case <-self.leaderChangedCh:
			if self.node.expirationPolicy == common.ConsistencyDeletion {
				if self.node.IsLead() && !applying {
					stop = make(chan struct{})

					self.wg.Add(1)
					go func(stop chan struct{}) {
						defer self.wg.Done()
						if err := self.applyExpiration(stop); err != nil {
							nodeLog.Errorf("apply expiration failed as: %s", err.Error())
						}
					}(stop)

					applying = true
				} else if !self.node.IsLead() && applying {
					close(stop)
					applying = false
				}
			}
		case <-self.quitC:
			if applying {
				close(stop)
				applying = false
			}
			return
		}
	}
}

func buildRawExpireCommand(dt common.DataType, keys [][]byte) []byte {
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

func (self *ExpireHandler) applyExpiration(stop chan struct{}) error {
	defer func() {
		for _, batch := range self.batchBuffer {
			batch.clear()
		}
		self.applyLock.Unlock()
	}()

	self.applyLock.Lock()
	receiver := make(chan *common.ExpiredData, 4*raftBatchBufferSize)

	self.wg.Add(1)
	go func() {
		defer self.wg.Done()
		t := time.NewTicker(time.Second)
		for {
			select {
			case <-t.C:
				for _, batch := range self.batchBuffer {
					batch.commit()
				}
			case <-stop:
				return
			}
		}
	}()

	self.wg.Add(1)
	go func() {
		defer self.wg.Done()
		for v := range receiver {
			self.batchBuffer[v.DataType].propose(v.Key)
		}
	}()

	return self.node.store.WatchExpired(receiver, stop)
}

type raftBatchBuffer struct {
	sync.Mutex
	dataType common.DataType
	keys     [][]byte
	node     *KVNode
}

func newRaftBatchBuffer(nd *KVNode, dt common.DataType) *raftBatchBuffer {
	return &raftBatchBuffer{
		node:     nd,
		keys:     make([][]byte, 0, raftBatchBufferSize),
		dataType: dt,
	}
}

func (rb *raftBatchBuffer) propose(key []byte) {
	rb.Lock()
	rb.keys = append(rb.keys, key)
	if len(rb.keys) >= raftBatchBufferSize {
		rb.node.Propose(buildRawExpireCommand(rb.dataType, rb.keys))
		rb.keys = rb.keys[:0]
	}
	rb.Unlock()
}

func (rb *raftBatchBuffer) commit() {
	rb.Lock()
	if len(rb.keys) > 0 {
		rb.node.Propose(buildRawExpireCommand(rb.dataType, rb.keys))
		rb.keys = rb.keys[:0]
	}
	rb.Unlock()
}

func (rb *raftBatchBuffer) clear() {
	rb.Lock()
	rb.keys = rb.keys[:0]
	rb.Unlock()
}
