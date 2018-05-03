package node

import (
	"encoding/json"
	"errors"
	"sync"
	"time"
)

type SyncedState struct {
	SyncedTerm  uint64 `json:"synced_term,omitempty"`
	SyncedIndex uint64 `json:"synced_index,omitempty"`
}

func (ss *SyncedState) IsNewer(other *SyncedState) bool {
	if ss.SyncedTerm >= other.SyncedTerm && ss.SyncedIndex >= other.SyncedIndex {
		return true
	}
	return false
}

func (ss *SyncedState) IsNewer2(term uint64, index uint64) bool {
	if ss.SyncedTerm >= term && ss.SyncedIndex >= index {
		return true
	}
	return false
}

const (
	ApplySnapUnknown int = iota
	ApplySnapBegin
	ApplySnapTransferring
	ApplySnapTransferred
	ApplySnapApplying
	ApplySnapDone
	ApplySnapFailed
)

var applyStatusMsgs = []string{
	"unknown",
	"begin",
	"transferring",
	"transferred",
	"applying",
	"done",
	"failed",
}

type SnapApplyStatus struct {
	SS          SyncedState
	StatusCode  int
	Status      string
	UpdatedTime time.Time
}
type remoteSyncedStateMgr struct {
	sync.RWMutex
	remoteSyncedStates      map[string]SyncedState
	remoteSnapshotsApplying map[string]*SnapApplyStatus
}

func newRemoteSyncedStateMgr() *remoteSyncedStateMgr {
	return &remoteSyncedStateMgr{
		remoteSyncedStates:      make(map[string]SyncedState),
		remoteSnapshotsApplying: make(map[string]*SnapApplyStatus),
	}
}

func (rss *remoteSyncedStateMgr) RemoveApplyingSnap(name string, state SyncedState) {
	rss.Lock()
	sas, ok := rss.remoteSnapshotsApplying[name]
	if ok && sas.SS == state {
		delete(rss.remoteSnapshotsApplying, name)
	}
	rss.Unlock()
}

func (rss *remoteSyncedStateMgr) AddApplyingSnap(name string, state SyncedState) (*SnapApplyStatus, bool) {
	added := false
	rss.Lock()
	sas, ok := rss.remoteSnapshotsApplying[name]
	canAdd := false
	if !ok {
		canAdd = true
	} else if sas.StatusCode == ApplySnapDone {
		delete(rss.remoteSnapshotsApplying, name)
		canAdd = true
	} else if sas.StatusCode == ApplySnapBegin || sas.StatusCode == ApplySnapFailed || sas.StatusCode == ApplySnapApplying {
		// begin -> transferring, may lost if proposal dropped,
		// so we check the time and restart
		if time.Since(sas.UpdatedTime) > proposeTimeout*10 {
			delete(rss.remoteSnapshotsApplying, name)
			canAdd = true
		}
	}
	if canAdd {
		sas = &SnapApplyStatus{
			SS:          state,
			StatusCode:  ApplySnapBegin,
			Status:      applyStatusMsgs[1],
			UpdatedTime: time.Now(),
		}
		rss.remoteSnapshotsApplying[name] = sas
		added = true
	}
	rss.Unlock()
	return sas, added
}

func (rss *remoteSyncedStateMgr) UpdateApplyingSnapStatus(name string, ss SyncedState, status int) {
	rss.Lock()
	sas, ok := rss.remoteSnapshotsApplying[name]
	if ok && status < len(applyStatusMsgs) && ss == sas.SS {
		if sas.StatusCode != status {
			sas.StatusCode = status
			sas.Status = applyStatusMsgs[status]
			sas.UpdatedTime = time.Now()
		}
	}
	rss.Unlock()
}

func (rss *remoteSyncedStateMgr) GetApplyingSnap(name string) (*SnapApplyStatus, bool) {
	rss.Lock()
	sas, ok := rss.remoteSnapshotsApplying[name]
	rss.Unlock()
	return sas, ok
}

func (rss *remoteSyncedStateMgr) UpdateState(name string, state SyncedState) {
	rss.Lock()
	rss.remoteSyncedStates[name] = state
	rss.Unlock()
}
func (rss *remoteSyncedStateMgr) GetState(name string) (SyncedState, bool) {
	rss.RLock()
	state, ok := rss.remoteSyncedStates[name]
	rss.RUnlock()
	return state, ok
}
func (rss *remoteSyncedStateMgr) RestoreStates(ss map[string]SyncedState) {
	rss.Lock()
	rss.remoteSyncedStates = make(map[string]SyncedState, len(ss))
	for k, v := range rss.remoteSyncedStates {
		rss.remoteSyncedStates[k] = v
	}
	rss.Unlock()
}
func (rss *remoteSyncedStateMgr) Clone() map[string]SyncedState {
	rss.RLock()
	clone := make(map[string]SyncedState, len(rss.remoteSyncedStates))
	for k, v := range rss.remoteSyncedStates {
		clone[k] = v
	}
	rss.RUnlock()
	return clone
}

func (nd *KVNode) isAlreadyApplied(reqList BatchInternalRaftRequest) bool {
	oldState, ok := nd.remoteSyncedStates.GetState(reqList.OrigCluster)
	if ok {
		// check if retrying duplicate req, we can just ignore old retry
		if reqList.OrigTerm < oldState.SyncedTerm {
			nd.rn.Infof("request %v ignored since older than synced:%v",
				reqList.OrigTerm, oldState)
			return true
		}
		if reqList.OrigIndex <= oldState.SyncedIndex {
			nd.rn.Infof("request %v ignored since older than synced index :%v",
				reqList.OrigIndex, oldState.SyncedIndex)
			return true
		}
	}
	return false
}

// return as (cluster name, is transferring remote snapshot, is applying remote snapshot)
func (nd *KVNode) preprocessRemoteSnapApply(reqList BatchInternalRaftRequest) (bool, bool) {
	ss := SyncedState{SyncedTerm: reqList.OrigTerm, SyncedIndex: reqList.OrigIndex}
	for _, req := range reqList.Reqs {
		if req.Header.DataType == int32(CustomReq) {
			var cr customProposeData
			err := json.Unmarshal(req.Data, &cr)
			if err != nil {
				nd.rn.Infof("failed to unmarshal custom propose: %v, err:%v", req.String(), err)
			}
			if cr.ProposeOp == ProposeOp_TransferRemoteSnap {
				// for replica which is not leader, the applying status is not added,
				// so we add here is ok. leader will handle the duplicate.
				nd.remoteSyncedStates.AddApplyingSnap(reqList.OrigCluster, ss)
				nd.remoteSyncedStates.UpdateApplyingSnapStatus(reqList.OrigCluster, ss, ApplySnapTransferring)
				return true, false
			} else if cr.ProposeOp == ProposeOp_ApplyRemoteSnap {
				return false, true
			}
		}
	}
	return false, false
}

func (nd *KVNode) postprocessRemoteSnapApply(reqList BatchInternalRaftRequest,
	isRemoteSnapTransfer bool, isRemoteSnapApply bool, retErr error) {
	ss := SyncedState{SyncedTerm: reqList.OrigTerm, SyncedIndex: reqList.OrigIndex}
	// for remote snapshot transfer, we need wait apply success before update sync state
	if !isRemoteSnapTransfer {
		if retErr != errIgnoredRemoteApply {
			nd.remoteSyncedStates.UpdateState(reqList.OrigCluster, ss)
			if isRemoteSnapApply {
				nd.remoteSyncedStates.UpdateApplyingSnapStatus(reqList.OrigCluster, ss, ApplySnapDone)
			}
		} else {
			if isRemoteSnapApply {
				nd.remoteSyncedStates.UpdateApplyingSnapStatus(reqList.OrigCluster, ss, ApplySnapFailed)
			}
		}
	} else {
		status := ApplySnapTransferred
		if retErr == errRemoteSnapTransferFailed {
			status = ApplySnapFailed
		}
		nd.remoteSyncedStates.UpdateApplyingSnapStatus(reqList.OrigCluster, ss, status)
	}
}

func (nd *KVNode) SetRemoteClusterSyncedRaft(name string, term uint64, index uint64) {
	nd.remoteSyncedStates.UpdateState(name, SyncedState{SyncedTerm: term, SyncedIndex: index})
}
func (nd *KVNode) GetRemoteClusterSyncedRaft(name string) (uint64, uint64) {
	state, _ := nd.remoteSyncedStates.GetState(name)
	return state.SyncedTerm, state.SyncedIndex
}

func (nd *KVNode) ApplyRemoteSnapshot(skip bool, name string, term uint64, index uint64) error {
	// restore the state machine from transferred snap data when transfer success.
	// we do not need restore other cluster member info here.
	nd.rn.Infof("begin recovery from remote cluster %v snapshot here: %v-%v", name, term, index)
	if skip {
		nd.rn.Infof("got skipped snapshot from remote cluster %v : %v-%v", name, term, index)
	} else {
		// we should disallow applying remote snap while we are running as master cluster
		if !IsSyncerOnly() {
			nd.rn.Infof("cluster %v snapshot is not allowed: %v-%v", name, term, index)
			return errors.New("apply remote snapshot is not allowed while not in syncer only mode")
		}
		oldS, ok := nd.remoteSyncedStates.GetApplyingSnap(name)
		if !ok {
			return errors.New("no remote snapshot waiting apply")
		}
		if oldS.SS.SyncedTerm != term || oldS.SS.SyncedIndex != index {
			nd.rn.Infof("remote cluster %v snapshot mismatch: %v-%v, old: %v", name, term, index, oldS)
			return errors.New("apply remote snapshot term-index mismatch")
		}
		if oldS.StatusCode != ApplySnapTransferred {
			nd.rn.Infof("remote cluster %v snapshot not ready for apply: %v", name, oldS)
			return errors.New("apply remote snapshot status invalid")
		}
		// set the snap status to applying and the snap status will be updated if apply done or failed
		nd.remoteSyncedStates.UpdateApplyingSnapStatus(name, oldS.SS, ApplySnapApplying)
	}
	var reqList BatchInternalRaftRequest
	reqList.OrigCluster = name
	reqList.ReqNum = 1
	reqList.Timestamp = time.Now().UnixNano()
	p := &customProposeData{
		ProposeOp:   ProposeOp_ApplyRemoteSnap,
		NeedBackup:  true,
		RemoteTerm:  term,
		RemoteIndex: index,
	}
	if skip {
		p.ProposeOp = ProposeOp_ApplySkippedRemoteSnap
	}
	d, _ := json.Marshal(p)
	h := &RequestHeader{
		ID:       0,
		DataType: int32(CustomReq),
	}
	raftReq := InternalRaftRequest{
		Header: h,
		Data:   d,
	}
	reqList.Reqs = append(reqList.Reqs, &raftReq)
	buf, _ := reqList.Marshal()
	err := nd.ProposeRawAndWait(buf, term, index, reqList.Timestamp)
	if err != nil {
		nd.rn.Infof("cluster %v applying snap %v-%v failed", name, term, index)
		// just wait next retry
	} else {
		nd.rn.Infof("cluster %v applying snap %v-%v done", name, term, index)
	}
	return nil
}

func (nd *KVNode) BeginTransferRemoteSnap(name string, term uint64, index uint64, syncAddr string, syncPath string) error {
	ss := SyncedState{SyncedTerm: term, SyncedIndex: index}
	// set the snap status to begin and the snap status will be updated if transfer begin
	// if transfer failed to propose, after some timeout it will be removed while adding
	old, added := nd.remoteSyncedStates.AddApplyingSnap(name, ss)
	if !added {
		nd.rn.Infof("cluster %v applying snap %v already running while apply %v", name, old, ss)
		return nil
	}

	p := &customProposeData{
		ProposeOp:   ProposeOp_TransferRemoteSnap,
		NeedBackup:  false,
		SyncAddr:    syncAddr,
		SyncPath:    syncPath,
		RemoteTerm:  term,
		RemoteIndex: index,
	}
	d, _ := json.Marshal(p)
	var reqList BatchInternalRaftRequest
	reqList.OrigCluster = name
	reqList.ReqNum = 1
	reqList.Timestamp = time.Now().UnixNano()

	h := &RequestHeader{
		ID:       0,
		DataType: int32(CustomReq),
	}
	raftReq := InternalRaftRequest{
		Header: h,
		Data:   d,
	}
	reqList.Reqs = append(reqList.Reqs, &raftReq)
	buf, _ := reqList.Marshal()
	err := nd.ProposeRawAndWait(buf, term, index, reqList.Timestamp)
	if err != nil {
		nd.rn.Infof("cluster %v applying transfer snap %v failed", name, ss)
	} else {
		nd.rn.Infof("cluster %v applying transfer snap %v done", name, ss)
	}
	return nil
}

func (nd *KVNode) GetApplyRemoteSnapStatus(name string) (*SnapApplyStatus, bool) {
	return nd.remoteSyncedStates.GetApplyingSnap(name)
}
