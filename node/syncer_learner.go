package node

import (
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/absolute8511/ZanRedisDB/raft/raftpb"
)

type logSyncerSM struct {
	fullName      string
	store         *KVStore
	clusterInfo   common.IClusterInfo
	ns            string
	machineConfig MachineConfig
	stopChan      chan struct{}
	ID            uint64
	synced        int64
	syncedIndex   uint64
	sendCh        chan *BatchInternalRaftRequest
	lgSender      *LogSender
	wg            sync.WaitGroup
	stopping      int32
}

func NewLogSyncerSM(fullName string, opts *KVOptions, machineConfig MachineConfig, localID uint64, ns string,
	clusterInfo common.IClusterInfo) (StateMachine, error) {
	store, err := NewKVStore(opts)
	if err != nil {
		return nil, err
	}

	lg := &logSyncerSM{
		fullName:      fullName,
		ns:            ns,
		machineConfig: machineConfig,
		ID:            localID,
		clusterInfo:   clusterInfo,
		store:         store,
		stopChan:      make(chan struct{}),
	}

	return lg, nil
}

func (sm *logSyncerSM) Debugf(f string, args ...interface{}) {
	msg := fmt.Sprintf(f, args...)
	nodeLog.DebugDepth(1, fmt.Sprintf("%v: %s", sm.fullName, msg))
}

func (sm *logSyncerSM) Infof(f string, args ...interface{}) {
	msg := fmt.Sprintf(f, args...)
	nodeLog.InfoDepth(1, fmt.Sprintf("%v: %s", sm.fullName, msg))
}

func (sm *logSyncerSM) Errorf(f string, args ...interface{}) {
	msg := fmt.Sprintf(f, args...)
	nodeLog.ErrorDepth(1, fmt.Sprintf("%v: %s", sm.fullName, msg))
}

func (sm *logSyncerSM) SetRecvChan(c chan *BatchInternalRaftRequest) {
	sm.sendCh = c
}

func (sm *logSyncerSM) Optimize() {
}

func (sm *logSyncerSM) GetDBInternalStats() string {
	return ""
}

func (sm *logSyncerSM) GetStats() common.NamespaceStats {
	var ns common.NamespaceStats
	stat := make(map[string]interface{})
	stat["role"] = common.LearnerRoleLogSyncer
	stat["synced"] = atomic.LoadInt64(&sm.synced)
	stat["synced_index"] = atomic.LoadUint64(&sm.syncedIndex)
	ns.InternalStats = stat
	return ns
}

func (sm *logSyncerSM) CleanData() error {
	return sm.store.CleanData()
}

func (sm *logSyncerSM) Destroy() {
	sm.store.Destroy()
}

func (sm *logSyncerSM) CheckExpiredData(buffer common.ExpiredDataBuffer, stop chan struct{}) error {
	return nil
}

func (sm *logSyncerSM) Start() error {
	lgSender, err := NewLogSender(sm.fullName, sm.machineConfig.RemoteSyncCluster, sm.stopChan)
	if err != nil {
		return err
	}

	sm.SetRecvChan(lgSender.GetChan())
	sm.wg.Add(1)
	go func() {
		defer sm.wg.Done()
		lgSender.Start()
	}()
	return nil
}

func (sm *logSyncerSM) Close() {
	if !atomic.CompareAndSwapInt32(&sm.stopping, 0, 1) {
		return
	}
	close(sm.stopChan)
	sm.wg.Wait()
	sm.store.Close()
}

func (kvsm *logSyncerSM) GetSnapshot(term uint64, index uint64) (*KVSnapInfo, error) {
	var si KVSnapInfo
	return &si, nil
}

func (sm *logSyncerSM) RestoreFromSnapshot(startup bool, raftSnapshot raftpb.Snapshot) error {
	atomic.StoreUint64(&sm.syncedIndex, raftSnapshot.Metadata.Index)
	if sm.clusterInfo == nil {
		// in test, the cluster coordinator is not enabled, we can just ignore restore.
		return nil
	}
	// TODO: get (term-index) from the remote cluster, if the remote cluster has
	// greater (term-index) than snapshot, we can just ignore the snapshot restore
	// since we already synced the data in snapshot.

	// while startup we can use the local snapshot to restart,
	// but while running, we should install the leader's snapshot,
	// so we need remove local and sync from leader
	err := prepareSnapshotForStore(sm.store, sm.machineConfig, sm.clusterInfo, sm.ns,
		sm.ID, sm.stopChan, raftSnapshot, 0)
	if err != nil {
		sm.Infof("failed to prepare snapshot: %v", err)
		return err
	}
	err = sm.store.Restore(raftSnapshot.Metadata.Term, raftSnapshot.Metadata.Index)
	if err != nil {
		sm.Infof("failed to restore snapshot: %v", err)
	}
	// TODO: since we can not restore from checkpoint on leader in the remote cluster, we need send
	// clear request to remote and then scan the db and send all keys to the remote cluster to restore
	// from a clean state.
	return err
}

func (sm *logSyncerSM) ApplyRaftRequest(reqList BatchInternalRaftRequest, term uint64, index uint64) bool {
	if nodeLog.Level() >= common.LOG_DETAIL {
		sm.Debugf("applying in log syncer: %v at (%v, %v)", reqList.String(), term, index)
	}
	forceBackup := false
	reqList.OrigTerm = term
	reqList.OrigIndex = index
	if reqList.Timestamp == 0 {
		sm.Errorf("miss timestamp in raft request: %v", reqList)
	}
	for _, req := range reqList.Reqs {
		if req.Header.DataType == int32(HTTPReq) {
			var p httpProposeData
			err := json.Unmarshal(req.Data, &p)
			if err != nil {
				sm.Infof("failed to unmarshal http propose: %v", req.String())
			}
			if p.ProposeOp == HttpProposeOp_Backup {
				sm.Infof("got force backup request")
				forceBackup = true
				break
			}
		}
	}
	select {
	case sm.sendCh <- &reqList:
	case <-sm.stopChan:
		return false
	}
	atomic.AddInt64(&sm.synced, 1)
	atomic.StoreUint64(&sm.syncedIndex, index)
	return forceBackup
}
