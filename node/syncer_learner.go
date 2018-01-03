package node

import (
	"encoding/json"
	"fmt"

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
}

func NewLogSyncerSM(fullName string, opts *KVOptions, machineConfig MachineConfig, localID uint64, ns string,
	clusterInfo common.IClusterInfo, stopChan chan struct{}) (StateMachine, error) {
	store, err := NewKVStore(opts)
	if err != nil {
		return nil, err
	}
	return &logSyncerSM{
		fullName:      fullName,
		ns:            ns,
		machineConfig: machineConfig,
		ID:            localID,
		clusterInfo:   clusterInfo,
		store:         store,
		stopChan:      stopChan,
	}, nil
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

func (sm *logSyncerSM) Optimize() {
}

func (sm *logSyncerSM) GetDBInternalStats() string {
	return ""
}

func (sm *logSyncerSM) GetStats() common.NamespaceStats {
	var ns common.NamespaceStats
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

func (sm *logSyncerSM) Close() {
	sm.store.Close()
}

func (kvsm *logSyncerSM) GetSnapshot(term uint64, index uint64) (*KVSnapInfo, error) {
	var si KVSnapInfo
	return &si, nil
}

func (sm *logSyncerSM) RestoreFromSnapshot(startup bool, raftSnapshot raftpb.Snapshot) error {
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
	return err
}

func (sm *logSyncerSM) ApplyRaftRequest(reqList BatchInternalRaftRequest) bool {
	forceBackup := false
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
	// TODO: send reqlist to remote cluster
	return forceBackup
}
