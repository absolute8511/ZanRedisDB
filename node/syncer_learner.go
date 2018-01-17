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
	store         *KVStore
	clusterInfo   common.IClusterInfo
	fullNS        string
	machineConfig MachineConfig
	ID            uint64
	syncedCnt     int64
	syncedIndex   uint64
	syncedTerm    uint64
	lgSender      *RemoteLogSender
	stopping      int32
	sendCh        chan *BatchInternalRaftRequest
	sendStop      chan struct{}
	wg            sync.WaitGroup
}

func NewLogSyncerSM(opts *KVOptions, machineConfig MachineConfig, localID uint64, fullNS string,
	clusterInfo common.IClusterInfo) (StateMachine, error) {
	store, err := NewKVStore(opts)
	if err != nil {
		return nil, err
	}

	lg := &logSyncerSM{
		fullNS:        fullNS,
		machineConfig: machineConfig,
		ID:            localID,
		clusterInfo:   clusterInfo,
		store:         store,
		sendCh:        make(chan *BatchInternalRaftRequest, 10),
		sendStop:      make(chan struct{}),
		//dataDir:       path.Join(opts.DataDir, "logsyncer"),
	}

	var localCluster string
	if clusterInfo != nil {
		localCluster = clusterInfo.GetClusterName()
	}
	lgSender, err := NewRemoteLogSender(localCluster, lg.fullNS, lg.machineConfig.RemoteSyncCluster)
	if err != nil {
		return nil, err
	}
	lg.lgSender = lgSender
	return lg, nil
}

func (sm *logSyncerSM) Debugf(f string, args ...interface{}) {
	msg := fmt.Sprintf(f, args...)
	nodeLog.DebugDepth(1, fmt.Sprintf("%v: %s", sm.fullNS, msg))
}

func (sm *logSyncerSM) Infof(f string, args ...interface{}) {
	msg := fmt.Sprintf(f, args...)
	nodeLog.InfoDepth(1, fmt.Sprintf("%v: %s", sm.fullNS, msg))
}

func (sm *logSyncerSM) Errorf(f string, args ...interface{}) {
	msg := fmt.Sprintf(f, args...)
	nodeLog.ErrorDepth(1, fmt.Sprintf("%v: %s", sm.fullNS, msg))
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
	stat["synced"] = atomic.LoadInt64(&sm.syncedCnt)
	stat["synced_index"] = atomic.LoadUint64(&sm.syncedIndex)
	stat["synced_term"] = atomic.LoadUint64(&sm.syncedTerm)
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
	sm.wg.Add(1)
	go func() {
		defer sm.wg.Done()
		sm.handlerRaftLogs()
	}()
	return nil
}

// the raft node will make sure the raft apply is stopped first
func (sm *logSyncerSM) Close() {
	if !atomic.CompareAndSwapInt32(&sm.stopping, 0, 1) {
		return
	}
	close(sm.sendStop)
	sm.wg.Wait()
	sm.store.Close()
}

func (sm *logSyncerSM) handlerRaftLogs() {
	defer func() {
		sm.lgSender.Stop()
		sm.Infof("raft log syncer send loop exit")
	}()
	raftLogs := make([]*BatchInternalRaftRequest, 0, 100)
	var last *BatchInternalRaftRequest
	state, err := sm.lgSender.getRemoteSyncedRaft(sm.sendStop)
	if err != nil {
		sm.Errorf("failed to get the synced state from remote: %v", err)
	}
	for {
		handled := false
		var err error
		select {
		case req := <-sm.sendCh:
			last = req
			raftLogs = append(raftLogs, req)
			if nodeLog.Level() >= common.LOG_DETAIL {
				sm.Debugf("batching raft log: %v in batch: %v", req.String(), len(raftLogs))
			}
			if len(raftLogs) > 100 {
				handled = true
				if state.SyncedTerm >= last.OrigTerm && state.SyncedIndex >= last.OrigIndex {
					// remote is already replayed this raft log
				} else {
					err = sm.lgSender.sendRaftLog(raftLogs, sm.sendStop)
				}
			}
		default:
			if len(raftLogs) == 0 {
				select {
				case <-sm.sendStop:
					return
				case req := <-sm.sendCh:
					last = req
					raftLogs = append(raftLogs, req)
					if nodeLog.Level() >= common.LOG_DETAIL {
						sm.Debugf("batching raft log: %v in batch: %v", req.String(), len(raftLogs))
					}
				}
				continue
			}
			handled = true
			if state.SyncedTerm >= last.OrigTerm && state.SyncedIndex >= last.OrigIndex {
				// remote is already replayed this raft log
			} else {
				err = sm.lgSender.sendRaftLog(raftLogs, sm.sendStop)
			}

		}
		if err != nil {
			select {
			case <-sm.sendStop:
				return
			default:
				sm.Errorf("failed to send raft log to remote: %v, %v", err, raftLogs)
			}
			continue
		}
		if handled {
			atomic.AddInt64(&sm.syncedCnt, int64(len(raftLogs)))
			atomic.StoreUint64(&sm.syncedIndex, last.OrigIndex)
			atomic.StoreUint64(&sm.syncedTerm, last.OrigTerm)
			raftLogs = raftLogs[:0]
		}
	}
}

func (kvsm *logSyncerSM) GetSnapshot(term uint64, index uint64) (*KVSnapInfo, error) {
	var si KVSnapInfo
	return &si, nil
}

func (sm *logSyncerSM) RestoreFromSnapshot(startup bool, raftSnapshot raftpb.Snapshot, stop chan struct{}) error {
	// get (term-index) from the remote cluster, if the remote cluster has
	// greater (term-index) than snapshot, we can just ignore the snapshot restore
	// since we already synced the data in snapshot.
	state, err := sm.lgSender.getRemoteSyncedRaft(stop)
	if err != nil {
		return err
	}
	if state.SyncedTerm > raftSnapshot.Metadata.Term && state.SyncedIndex > raftSnapshot.Metadata.Index {
		sm.Infof("ignored restore snapshot since remote has newer raft: %v than %v", state, raftSnapshot.Metadata.String())
		return nil
	}

	// TODO: should wait all batched sync raft logs success
	if sm.clusterInfo == nil {
		// in test, the cluster coordinator is not enabled, we can just ignore restore.
		return nil
	}

	// while startup we can use the local snapshot to restart,
	// but while running, we should install the leader's snapshot,
	// so we need remove local and sync from leader
	err = prepareSnapshotForStore(sm.store, sm.machineConfig, sm.clusterInfo, sm.fullNS,
		sm.ID, stop, raftSnapshot, 0)
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

	// load synced offset from snapshot, so we can just send logs from the newest
	return err
}

func (sm *logSyncerSM) ApplyRaftRequest(reqList BatchInternalRaftRequest, term uint64, index uint64, stop chan struct{}) bool {
	if nodeLog.Level() >= common.LOG_DETAIL {
		sm.Debugf("applying in log syncer: %v at (%v, %v)", reqList.String(), term, index)
	}
	forceBackup := false
	reqList.OrigTerm = term
	reqList.OrigIndex = index
	reqList.Type = FromClusterSyncer
	if sm.clusterInfo != nil {
		reqList.OrigCluster = sm.clusterInfo.GetClusterName()
	}
	if reqList.ReqId == 0 {
		for _, e := range reqList.Reqs {
			reqList.ReqId = e.Header.ID
			break
		}
	}
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
	case <-stop:
		return false
	case <-sm.sendStop:
		return false
	}

	return forceBackup
}
