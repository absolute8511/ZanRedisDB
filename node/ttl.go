package node

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

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

func (nd *KVNode) setexCommand(conn redcon.Conn, cmd redcon.Command, v interface{}) {
	conn.WriteString("OK")
}

func (nd *KVNode) expireCommand(conn redcon.Conn, cmd redcon.Command, v interface{}) {
	if rsp, ok := v.(int64); ok {
		conn.WriteInt64(rsp)
	} else {
		conn.WriteError(errInvalidResponse.Error())
	}
}

func (nd *KVNode) listExpireCommand(conn redcon.Conn, cmd redcon.Command, v interface{}) {
	if rsp, ok := v.(int64); ok {
		conn.WriteInt64(rsp)
	} else {
		conn.WriteError(errInvalidResponse.Error())
	}
}

func (nd *KVNode) hashExpireCommand(conn redcon.Conn, cmd redcon.Command, v interface{}) {
	if rsp, ok := v.(int64); ok {
		conn.WriteInt64(rsp)
	} else {
		conn.WriteError(errInvalidResponse.Error())
	}
}

func (nd *KVNode) setExpireCommand(conn redcon.Conn, cmd redcon.Command, v interface{}) {
	if rsp, ok := v.(int64); ok {
		conn.WriteInt64(rsp)
	} else {
		conn.WriteError(errInvalidResponse.Error())
	}
}

func (nd *KVNode) zsetExpireCommand(conn redcon.Conn, cmd redcon.Command, v interface{}) {
	if rsp, ok := v.(int64); ok {
		conn.WriteInt64(rsp)
	} else {
		conn.WriteError(errInvalidResponse.Error())
	}
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

func (nd *KVNode) persistCommand(conn redcon.Conn, cmd redcon.Command, v interface{}) {
	if rsp, ok := v.(int64); ok {
		conn.WriteInt64(rsp)
	} else {
		conn.WriteError(errInvalidResponse.Error())
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

func (exp *ExpireHandler) Start() {
	if !atomic.CompareAndSwapInt32(&exp.running, 0, 1) {
		return
	}

	exp.wg.Add(1)
	go func() {
		defer exp.wg.Done()
		exp.watchLeaderChanged()
	}()

}

func (exp *ExpireHandler) Stop() {
	if atomic.CompareAndSwapInt32(&exp.running, 1, 0) {
		close(exp.quitC)
		exp.wg.Wait()
	}
}

func (exp *ExpireHandler) LeaderChanged() {
	select {
	case exp.leaderChangedCh <- struct{}{}:
	case <-exp.quitC:
		return
	}
}

func (exp *ExpireHandler) watchLeaderChanged() {
	var stop chan struct{}
	applying := false
	for {
		select {
		case <-exp.leaderChangedCh:
			if exp.node.expirationPolicy != common.ConsistencyDeletion {
				continue
			}
			if exp.node.IsLead() && !applying {
				stop = make(chan struct{})
				exp.wg.Add(1)
				go func(stop chan struct{}) {
					defer exp.wg.Done()
					exp.applyExpiration(stop)
				}(stop)
				applying = true

			} else if !exp.node.IsLead() && applying {
				close(stop)
				applying = false
			}
		case <-exp.quitC:
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

func (exp *ExpireHandler) applyExpiration(stop chan struct{}) {
	nodeLog.Infof("begin to apply expiration")
	exp.applyLock.Lock()
	checkTicker := time.NewTicker(time.Second)

	defer func() {
		checkTicker.Stop()
		exp.batchBuffer.Reset()
		exp.applyLock.Unlock()
		nodeLog.Infof("apply expiration has been stopped")
	}()

	var buffFullTimes int
	for {
		select {
		case <-checkTicker.C:
			if err := exp.node.sm.CheckExpiredData(exp.batchBuffer, stop); err == ErrExpiredBatchedBuffFull {
				if buffFullTimes += 1; buffFullTimes >= 3 {
					nodeLog.Warningf("expired data buffer is filled three times in succession, stats:%s", exp.batchBuffer.GetStats())
					buffFullTimes = 0
				}
			} else if err != nil {
				nodeLog.Errorf("check expired data by the underlying storage system failed, err:%s", err.Error())
			}

			select {
			case <-stop:
				return
			default:
				exp.batchBuffer.CommitAll()
			}
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
	return raftBuffer.internalBuf[dt].propose(key)
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

func (raftBuff *raftExpiredBuffer) GetStats() string {
	stats := bytes.NewBufferString("the stats of expired data buffer:\r\n")
	for _, buff := range raftBuff.internalBuf {
		if buff != nil {
			stats.WriteString(buff.GetStats())
		}
	}
	return stats.String()
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

func (rb *raftBatchBuffer) propose(key []byte) error {
	defer rb.Unlock()
	rb.Lock()

	if len(rb.keys) >= raftBatchBufferSize {
		return ErrExpiredBatchedBuffFull
	} else {
		rb.keys = append(rb.keys, key)
	}
	return nil
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

func (rb *raftBatchBuffer) GetStats() string {
	stats := make(map[string]int)
	rb.Lock()
	for _, k := range rb.keys {
		if t, _, err := common.ExtractTable(k); err != nil {
			continue
		} else {
			stats[string(t)] += 1
		}
	}
	rb.Unlock()

	statsBuf := bytes.NewBufferString(fmt.Sprintf("tables have more than 300 %s keys expired: ", rb.dataType.String()))
	for table, count := range stats {
		if count >= 300 {
			statsBuf.WriteString(fmt.Sprintf("[%s: %d], ", table, count))
		}
	}
	statsBuf.WriteString("\r\n")

	return statsBuf.String()
}
