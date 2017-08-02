package node

import (
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/absolute8511/redcon"
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
	applyLock       sync.Mutex
	batchBuffer     *raftExpiredBuffer

	running int32
	wg      sync.WaitGroup
}

func NewExpireHandler(node *KVNode) *ExpireHandler {
	return &ExpireHandler{
		node:            node,
		leaderChangedCh: make(chan struct{}, 8),
		quitC:           make(chan struct{}),
		batchBuffer:     newRaftExpiredBuffer(node),
	}
}

func (self *ExpireHandler) Start() {
	if !atomic.CompareAndSwapInt32(&self.running, 0, 1) {
		return
	}

	self.wg.Add(1)
	go func() {
		defer self.wg.Done()
		self.watchLeaderChanged()
	}()

}

func (self *ExpireHandler) Stop() {
	if atomic.CompareAndSwapInt32(&self.running, 1, 0) {
		close(self.quitC)
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
			if self.node.expirationPolicy != common.ConsistencyDeletion {
				continue
			}
			if self.node.IsLead() && !applying {
				stop = make(chan struct{})
				self.wg.Add(1)
				go func(stop chan struct{}) {
					defer self.wg.Done()
					self.applyExpiration(stop)
				}(stop)
				applying = true

			} else if !self.node.IsLead() && applying {
				close(stop)
				applying = false
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

func (self *ExpireHandler) applyExpiration(stop chan struct{}) {
	nodeLog.Infof("begin to apply expiration")
	self.applyLock.Lock()
	checkTicker := time.NewTicker(time.Second)

	defer func() {
		checkTicker.Stop()
		self.batchBuffer.Reset()
		self.applyLock.Unlock()
		nodeLog.Infof("apply expiration has been stopped")
	}()

	self.wg.Add(1)
	go func(buffer *raftExpiredBuffer, stop chan struct{}) {
		defer self.wg.Done()
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				buffer.CommitAll()
			case <-stop:
				return
			}
		}
	}(self.batchBuffer, stop)

	for {
		select {
		case <-checkTicker.C:
			if err := self.node.store.CheckExpiredData(self.batchBuffer, stop); err != nil {
				nodeLog.Errorf("check expired data by the underlying storage system failed, err:%s", err.Error())
			}
			self.batchBuffer.CommitAll()
		case <-stop:
			return
		}
	}
}

type raftExpiredBuffer struct {
	internalBuf [common.ALL - common.NONE]*raftBatchBuffer
}

func newRaftExpiredBuffer(nd *KVNode) *raftExpiredBuffer {
	raftBuff := &raftExpiredBuffer{}

	types := []common.DataType{common.KV, common.LIST, common.HASH,
		common.SET, common.ZSET}

	for _, t := range types {
		raftBuff.internalBuf[t] = newRaftBatchBuffer(nd, t)
	}

	return raftBuff
}

func (raftBuffer *raftExpiredBuffer) Write(dt common.DataType, key []byte) error {
	raftBuffer.internalBuf[dt].propose(key)
	return nil
}

// always return false as the expired data stored at the internal buffers would
// be handled automatically when the buffer is full
func (raftBuffer *raftExpiredBuffer) Full() bool {
	return false
}

func (raftBuffer *raftExpiredBuffer) CommitAll() {
	for _, buff := range raftBuffer.internalBuf {
		if buff != nil {
			buff.commit()
		}
	}
}

func (raftBuffer *raftExpiredBuffer) Reset() {
	for _, buff := range raftBuffer.internalBuf {
		if buff != nil {
			buff.clear()
		}
	}
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
