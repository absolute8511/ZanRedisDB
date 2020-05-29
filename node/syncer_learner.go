package node

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/youzan/ZanRedisDB/common"
	"github.com/youzan/ZanRedisDB/metric"
	"github.com/youzan/ZanRedisDB/pkg/wait"
	"github.com/youzan/ZanRedisDB/raft/raftpb"
	"github.com/youzan/ZanRedisDB/syncerpb"
)

var enableTest = false

func EnableForTest() {
	enableTest = true
}

const (
	logSendBufferLen = 512
)

var syncerNormalInit = false

var remoteSnapRecoverCnt int64

func SetSyncerNormalInit() {
	syncerNormalInit = true
}

var syncLearnerRecvStats metric.WriteStats
var syncLearnerDoneStats metric.WriteStats

type logSyncerSM struct {
	clusterInfo    common.IClusterInfo
	fullNS         string
	machineConfig  MachineConfig
	ID             uint64
	syncedCnt      int64
	receivedState  SyncedState
	syncedState    SyncedState
	lgSender       *RemoteLogSender
	stopping       int32
	sendCh         chan BatchInternalRaftRequest
	sendStop       chan struct{}
	wg             sync.WaitGroup
	waitSendLogChs chan chan struct{}
	// control if we need send the log to remote really
	ignoreSend int32
	w          wait.Wait
}

func NewLogSyncerSM(opts *KVOptions, machineConfig MachineConfig, localID uint64, fullNS string,
	clusterInfo common.IClusterInfo) (*logSyncerSM, error) {

	lg := &logSyncerSM{
		fullNS:         fullNS,
		machineConfig:  machineConfig,
		ID:             localID,
		clusterInfo:    clusterInfo,
		sendCh:         make(chan BatchInternalRaftRequest, logSendBufferLen),
		sendStop:       make(chan struct{}),
		waitSendLogChs: make(chan chan struct{}, 1),
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
	nodeLog.DebugDepth(1, fmt.Sprintf("%v-%v: %s", sm.fullNS, sm.ID, msg))
}

func (sm *logSyncerSM) Infof(f string, args ...interface{}) {
	msg := fmt.Sprintf(f, args...)
	nodeLog.InfoDepth(1, fmt.Sprintf("%v-%v: %s", sm.fullNS, sm.ID, msg))
}

func (sm *logSyncerSM) Errorf(f string, args ...interface{}) {
	msg := fmt.Sprintf(f, args...)
	nodeLog.ErrorDepth(1, fmt.Sprintf("%v-%v: %s", sm.fullNS, sm.ID, msg))
}

func (sm *logSyncerSM) Optimize(t string) {
}
func (sm *logSyncerSM) ClearTopn() {
}
func (sm *logSyncerSM) EnableTopn(on bool) {
}

func (sm *logSyncerSM) GetDBInternalStats() string {
	return ""
}

func GetLogLatencyStats() (*metric.WriteStats, *metric.WriteStats) {
	return syncLearnerRecvStats.Copy(), syncLearnerDoneStats.Copy()
}

func (sm *logSyncerSM) GetLogSyncStats() (metric.LogSyncStats, metric.LogSyncStats) {
	var recvStats metric.LogSyncStats
	var syncStats metric.LogSyncStats
	syncStats.Name = sm.fullNS
	syncStats.Term, syncStats.Index, syncStats.Timestamp = sm.getSyncedState()
	recvStats.Term = atomic.LoadUint64(&sm.receivedState.SyncedTerm)
	recvStats.Index = atomic.LoadUint64(&sm.receivedState.SyncedIndex)
	recvStats.Timestamp = atomic.LoadInt64(&sm.receivedState.Timestamp)
	recvStats.Name = sm.fullNS
	return recvStats, syncStats
}

func (sm *logSyncerSM) GetStats(table string, needTableDetail bool) metric.NamespaceStats {
	var ns metric.NamespaceStats
	stat := make(map[string]interface{})
	stat["role"] = sm.machineConfig.LearnerRole
	stat["synced"] = atomic.LoadInt64(&sm.syncedCnt)
	stat["synced_index"] = atomic.LoadUint64(&sm.syncedState.SyncedIndex)
	stat["synced_term"] = atomic.LoadUint64(&sm.syncedState.SyncedTerm)
	stat["synced_timestamp"] = atomic.LoadInt64(&sm.syncedState.Timestamp)
	ns.InternalStats = stat
	return ns
}

func (sm *logSyncerSM) CleanData() error {
	return nil
}

func (sm *logSyncerSM) Destroy() {
}

func (sm *logSyncerSM) CheckExpiredData(buffer common.ExpiredDataBuffer, stop chan struct{}) error {
	return nil
}

func (sm *logSyncerSM) GetBatchOperator() IBatchOperator {
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
}

func (sm *logSyncerSM) setReceivedState(term uint64, index uint64, ts int64) {
	atomic.StoreUint64(&sm.receivedState.SyncedTerm, term)
	atomic.StoreUint64(&sm.receivedState.SyncedIndex, index)
	atomic.StoreInt64(&sm.receivedState.Timestamp, ts)
}

func (sm *logSyncerSM) setSyncedState(term uint64, index uint64, ts int64) {
	atomic.StoreUint64(&sm.syncedState.SyncedTerm, term)
	atomic.StoreUint64(&sm.syncedState.SyncedIndex, index)
	atomic.StoreInt64(&sm.syncedState.Timestamp, ts)
}

func (sm *logSyncerSM) getSyncedState() (uint64, uint64, int64) {
	syncedTerm := atomic.LoadUint64(&sm.syncedState.SyncedTerm)
	syncedIndex := atomic.LoadUint64(&sm.syncedState.SyncedIndex)
	syncedTs := atomic.LoadInt64(&sm.syncedState.Timestamp)
	return syncedTerm, syncedIndex, syncedTs
}

func (sm *logSyncerSM) switchIgnoreSend(send bool) {
	old := atomic.LoadInt32(&sm.ignoreSend)

	syncedTerm, syncedIndex, syncedTs := sm.getSyncedState()
	if send {
		if old == 0 {
			return
		}
		sm.Infof("switch to send log really at: %v-%v-%v", syncedTerm, syncedIndex, syncedTs)
		atomic.StoreInt32(&sm.ignoreSend, 0)
	} else {
		if old == 1 {
			return
		}
		sm.Infof("switch to ignore send log at: %v-%v-%v", syncedTerm, syncedIndex, syncedTs)
		atomic.StoreInt32(&sm.ignoreSend, 1)
	}
}

func (sm *logSyncerSM) handlerRaftLogs() {
	defer func() {
		sm.lgSender.Stop()
		syncedTerm, syncedIndex, syncedTs := sm.getSyncedState()
		sm.Infof("raft log syncer send loop exit at synced: %v-%v-%v", syncedTerm, syncedIndex, syncedTs)
	}()
	raftLogs := make([]BatchInternalRaftRequest, 0, logSendBufferLen)
	var last BatchInternalRaftRequest
	state, err := sm.lgSender.getRemoteSyncedRaft(sm.sendStop)
	if err != nil {
		sm.Errorf("failed to get the synced state from remote: %v", err)
	}
	in := syncerpb.RaftReqs{}
	logListBuf := make([]syncerpb.RaftLogData, logSendBufferLen*2)
	marshalBufs := make([][]byte, logSendBufferLen*2)
	waitSendNum := 0
	lastIndex := uint64(0)
	for {
		handled := false
		var err error
		sendCh := sm.sendCh
		if waitSendNum > logSendBufferLen*10 {
			sendCh = nil
		}
		select {
		case req := <-sendCh:
			if lastIndex != 0 && req.OrigIndex != lastIndex+1 {
				sm.Infof("syncer log commit is not continue: %v, %v, %v", req, lastIndex, last)
			}
			last = req
			lastIndex = last.OrigIndex
			raftLogs = append(raftLogs, req)
			waitSendNum += len(req.Reqs)
			if nodeLog.Level() > common.LOG_DETAIL {
				sm.Debugf("batching raft log: %v in batch: %v", req.String(), len(raftLogs))
			}
		default:
			if len(raftLogs) == 0 {
				select {
				case <-sm.sendStop:
					return
				case req := <-sm.sendCh:
					if lastIndex != 0 && req.OrigIndex != lastIndex+1 {
						sm.Infof("syncer log commit is not continue: %v, %v, %v", req, lastIndex, last)
					}
					last = req
					lastIndex = last.OrigIndex
					raftLogs = append(raftLogs, req)
					waitSendNum += len(req.Reqs)
					if nodeLog.Level() >= common.LOG_DETAIL {
						sm.Debugf("batching raft log: %v in batch: %v", req.String(), len(raftLogs))
					}
				case waitCh := <-sm.waitSendLogChs:
					select {
					case req := <-sm.sendCh:

						if lastIndex != 0 && req.OrigIndex != lastIndex+1 {
							sm.Infof("syncer log commit is not continue: %v, %v, %v", req, lastIndex, last)
						}
						last = req
						lastIndex = last.OrigIndex

						raftLogs = append(raftLogs, req)
						waitSendNum += len(req.Reqs)
						go func() {
							select {
							// put back to wait next again
							case sm.waitSendLogChs <- waitCh:
							case <-sm.sendStop:
							}
						}()
					default:
						sm.Infof("wake up waiting buffered send logs since no more logs")
						close(waitCh)
					}
				}
				continue
			}
			handled = true
			if state.IsNewer2(last.OrigTerm, last.OrigIndex) {
				// remote is already replayed this raft log
			} else {
				if len(logListBuf) < len(raftLogs) {
					logListBuf = append(logListBuf, make([]syncerpb.RaftLogData, len(raftLogs)-len(logListBuf))...)
				}
				if len(marshalBufs) < len(raftLogs) {
					marshalBufs = append(marshalBufs, make([][]byte, len(raftLogs)-len(marshalBufs))...)
				}
				in.RaftLog = logListBuf[:len(raftLogs)]
				for i, e := range raftLogs {
					logs := in.RaftLog
					logs[i].Type = syncerpb.EntryNormalRaw
					dbuf := marshalBufs[i]
					if len(dbuf) < e.Size() {
						dbuf = make([]byte, e.Size())
						marshalBufs[i] = dbuf
					}
					used, err := e.MarshalTo(dbuf)
					if err != nil {
						sm.Errorf("failed to marshal %v: %v", e.String(), err)
						panic(err)
					}
					logs[i].Data = dbuf[:used]
					logs[i].Term = e.OrigTerm
					logs[i].Index = e.OrigIndex
					logs[i].RaftTimestamp = e.Timestamp
					logs[i].RaftGroupName = sm.lgSender.grpName
					logs[i].ClusterName = sm.lgSender.localCluster
				}
				err = sm.lgSender.sendRaftLog(in, sm.sendStop)
			}
		}
		if err != nil {
			select {
			case <-sm.sendStop:
				return
			default:
				syncedTerm, syncedIndex, syncedTs := sm.getSyncedState()
				sm.Errorf("failed to send raft log to remote: %v, %v, current: %v-%v-%v",
					err, len(raftLogs), syncedTerm, syncedIndex, syncedTs)
			}
			continue
		}
		if handled {
			atomic.AddInt64(&sm.syncedCnt, int64(len(raftLogs)))
			sm.setSyncedState(last.OrigTerm, last.OrigIndex, last.Timestamp)
			t := time.Now().UnixNano()
			for i, rl := range raftLogs {
				syncLearnerDoneStats.UpdateLatencyStats((t - rl.Timestamp) / time.Microsecond.Nanoseconds())
				raftLogs[i].Reqs = nil
			}
			raftLogs = raftLogs[:0]
			waitSendNum = 0
		}
	}
}

func (sm *logSyncerSM) waitBufferedLogs(timeout time.Duration) error {
	waitCh := make(chan struct{})
	sm.Infof("wait buffered send logs")
	var waitT <-chan time.Time
	if timeout > 0 {
		tm := time.NewTimer(timeout)
		defer tm.Stop()
		waitT = tm.C
	}
	select {
	case sm.waitSendLogChs <- waitCh:
	case <-waitT:
		return errors.New("wait log commit timeout")
	case <-sm.sendStop:
		return common.ErrStopped
	}
	select {
	case <-waitCh:
	case <-waitT:
		return errors.New("wait log commit timeout")
	case <-sm.sendStop:
		return common.ErrStopped
	}
	return nil
}

// snapshot should wait all buffered commit logs
func (sm *logSyncerSM) GetSnapshot(term uint64, index uint64) (*KVSnapInfo, error) {
	var si KVSnapInfo
	err := sm.waitBufferedLogs(time.Second * 10)
	return &si, err
}

func (sm *logSyncerSM) UpdateSnapshotState(term uint64, index uint64) {
}

func (sm *logSyncerSM) waitIgnoreUntilChanged(term uint64, index uint64, stop chan struct{}) (bool, error) {
	for {
		if atomic.LoadInt32(&sm.ignoreSend) == 1 {
			// check local to avoid call rpc too much
			syncTerm, syncIndex, _ := sm.getSyncedState()
			if syncTerm >= term && syncIndex >= index {
				return true, nil
			}
			state, err := sm.lgSender.getRemoteSyncedRaft(sm.sendStop)
			if err != nil {
				sm.Infof("failed to get the synced state from remote: %v, at %v-%v", err, term, index)
			} else {
				if state.IsNewer2(term, index) {
					sm.setSyncedState(state.SyncedTerm, state.SyncedIndex, state.Timestamp)
					return true, nil
				}
			}
			t := time.NewTimer(time.Second)
			select {
			case <-stop:
				t.Stop()
				return true, common.ErrStopped
			case <-sm.sendStop:
				t.Stop()
				return true, common.ErrStopped
			case <-t.C:
				t.Stop()
			}
		} else {
			return false, nil
		}
	}
}

func (sm *logSyncerSM) waitAndCheckTransferLimit(start time.Time, stop chan struct{}) (newMyRun int64, err error) {
	r := rand.Int31n(10) + 10
	t := time.NewTimer(time.Second * time.Duration(r))
	atomic.AddInt64(&remoteSnapRecoverCnt, -1)
	defer func() {
		newMyRun = atomic.AddInt64(&remoteSnapRecoverCnt, 1)
	}()
	select {
	case <-stop:
		t.Stop()
		err = common.ErrStopped
		return newMyRun, err
	case <-t.C:
		t.Stop()
		if time.Since(start) > common.SnapWaitTimeout {
			err = common.ErrTransferOutofdate
			return newMyRun, err
		}
	}
	return newMyRun, err
}

func (sm *logSyncerSM) RestoreFromSnapshot(raftSnapshot raftpb.Snapshot, stop chan struct{}) error {
	return nil
}

func (sm *logSyncerSM) PrepareSnapshot(raftSnapshot raftpb.Snapshot, stop chan struct{}) error {
	// get (term-index) from the remote cluster, if the remote cluster has
	// greater (term-index) than snapshot, we can just ignore the snapshot restore
	// since we already synced the data in snapshot.
	sm.Infof("restore snapshot : %v", raftSnapshot.Metadata.String())
	state, err := sm.lgSender.getRemoteSyncedRaft(stop)
	if err != nil {
		return err
	}
	if state.IsNewer2(raftSnapshot.Metadata.Term, raftSnapshot.Metadata.Index) {
		sm.Infof("ignored restore snapshot since remote has newer raft: %v than %v", state, raftSnapshot.Metadata.String())
		sm.setSyncedState(raftSnapshot.Metadata.Term, raftSnapshot.Metadata.Index, 0)
		return nil
	}

	// TODO: should wait all batched sync raft logs success
	if sm.clusterInfo == nil {
		// in test, the cluster coordinator is not enabled, we can just ignore restore.
		sm.Infof("nil cluster info, only for test: %v", raftSnapshot.Metadata.String())
		return nil
	}

	sm.setReceivedState(raftSnapshot.Metadata.Term, raftSnapshot.Metadata.Index, 0)
	sm.Infof("wait buffered send logs while restore from snapshot")
	err = sm.waitBufferedLogs(0)
	if err != nil {
		return err
	}

	ignore, err := sm.waitIgnoreUntilChanged(raftSnapshot.Metadata.Term, raftSnapshot.Metadata.Index, stop)
	if err != nil {
		return err
	}
	if ignore {
		return nil
	}

	if syncerNormalInit {
		// set term-index to remote cluster with skipped snap so we can
		// avoid transfer the snapshot while the two clusters have the exactly same logs
		err := sm.lgSender.sendAndWaitApplySkippedSnap(raftSnapshot, stop)
		if err != nil {
			return err
		}
		sm.Infof("init snap done %v-%v", raftSnapshot.Metadata.Term, raftSnapshot.Metadata.Index)
		sm.setSyncedState(raftSnapshot.Metadata.Term, raftSnapshot.Metadata.Index, 0)
		return nil
	}

	myRun := atomic.AddInt64(&remoteSnapRecoverCnt, 1)
	defer atomic.AddInt64(&remoteSnapRecoverCnt, -1)
	start := time.Now()
	for myRun > int64(common.GetIntDynamicConf(common.ConfMaxRemoteRecover)) {
		oldRun := myRun
		myRun, err = sm.waitAndCheckTransferLimit(start, stop)
		if err != nil {
			sm.Infof("waiting restore snapshot failed: %v", raftSnapshot.Metadata.String())
			return err
		}
		sm.Infof("waiting restore snapshot %v, my run: %v, old: %v", raftSnapshot.Metadata.String(), myRun, oldRun)
	}
	// while startup we can use the local snapshot to restart,
	// but while running, we should install the leader's snapshot,
	// so we need remove local and sync from leader
	retry := 0
	var restoreErr error
	for retry < 3 {
		forceRemote := true
		if enableTest {
			// for test we use local
			forceRemote = false
		}
		state, err := sm.lgSender.getRemoteSyncedRaft(stop)
		if err == nil {
			if state.IsNewer2(raftSnapshot.Metadata.Term, raftSnapshot.Metadata.Index) {
				sm.Infof("ignored restore snapshot since remote has newer raft: %v than %v", state, raftSnapshot.Metadata.String())
				sm.setSyncedState(raftSnapshot.Metadata.Term, raftSnapshot.Metadata.Index, 0)
				return nil
			}
		} else {
			restoreErr = err
		}
		syncAddr, syncDir := GetValidBackupInfo(sm.machineConfig, sm.clusterInfo, sm.fullNS, sm.ID, stop, raftSnapshot, retry, forceRemote)
		// note the local sync path not supported, so we need try another replica if syncAddr is empty
		if syncAddr == "" && syncDir == "" {
			// the snap may be out of date on others, so we can not restore from old snapshot
			restoreErr = errNobackupAvailable
		} else {
			restoreErr = sm.lgSender.waitTransferSnapStatus(raftSnapshot, syncAddr, syncDir, stop)
			if restoreErr != nil {
				sm.Infof("wait transfer snap %v,%v,%v failed: %v", raftSnapshot.Metadata, syncAddr, syncDir, restoreErr)
			} else {
				restoreErr = sm.lgSender.waitApplySnapStatus(raftSnapshot, stop)
				if restoreErr != nil {
					sm.Infof("wait apply snap %v,%v,%v failed: %v", raftSnapshot.Metadata, syncAddr, syncDir, restoreErr)
				}
			}
			// the snapshot maybe out of date, so we do not retry here
			break
		}
		retry++
		select {
		case <-stop:
			return restoreErr
		case <-time.After(time.Second):
		}
	}
	if restoreErr != nil {
		sm.Infof("restore snapshot %v failed: %v", raftSnapshot.Metadata.String(), restoreErr)
		if restoreErr == errNobackupAvailable {
			sm.Infof("restore snapshot %v while startup failed due to no snapshot, we can ignore in learner while startup", raftSnapshot.Metadata.String())
		}
		return restoreErr
	}

	sm.Infof("apply snap done %v", raftSnapshot.Metadata.String())
	sm.setSyncedState(raftSnapshot.Metadata.Term, raftSnapshot.Metadata.Index, 0)
	return nil
}

// raft config change request should be send to the remote cluster since this will update the term-index for raft logs.
func (sm *logSyncerSM) ApplyRaftConfRequest(req raftpb.ConfChange, term uint64, index uint64, stop chan struct{}) error {
	var reqList BatchInternalRaftRequest
	reqList.Timestamp = time.Now().UnixNano()
	reqList.ReqNum = 1
	var rreq InternalRaftRequest
	var p customProposeData
	p.ProposeOp = ProposeOp_RemoteConfChange
	p.RemoteTerm = term
	p.RemoteIndex = index
	p.Data, _ = req.Marshal()
	rreq.Data, _ = json.Marshal(p)
	rreq.Header = RequestHeader{
		DataType:  int32(CustomReq),
		ID:        0,
		Timestamp: reqList.Timestamp,
	}
	reqList.Reqs = append(reqList.Reqs, rreq)
	reqList.ReqId = rreq.Header.ID
	_, err := sm.ApplyRaftRequest(false, nil, reqList, term, index, stop)
	return err
}

func (sm *logSyncerSM) ApplyRaftRequest(isReplaying bool, batch IBatchOperator, reqList BatchInternalRaftRequest, term uint64, index uint64, stop chan struct{}) (bool, error) {
	if nodeLog.Level() >= common.LOG_DETAIL {
		sm.Debugf("applying in log syncer: %v at (%v, %v)", reqList.String(), term, index)
	}
	if reqList.Type == FromClusterSyncer && reqList.OrigCluster != sm.clusterInfo.GetClusterName() {
		sm.Infof("ignore sync from cluster syncer, %v-%v:%v", term, index, reqList.String())
		return false, nil
	}
	ts := reqList.Timestamp
	if ts == 0 {
		// for some event such as leader transfer, the reqList will be empty, so no timestamp in it
		sm.Infof("miss timestamp in raft request: %v", reqList.String())
		reqList.Timestamp = time.Now().UnixNano()
	} else {
		latency := time.Now().UnixNano() - ts
		syncLearnerRecvStats.UpdateLatencyStats(latency / time.Microsecond.Nanoseconds())
	}
	sm.setReceivedState(term, index, reqList.Timestamp)

	forceBackup := false
	reqList.OrigTerm = term
	reqList.OrigIndex = index
	reqList.Type = FromClusterSyncer
	if sm.clusterInfo != nil {
		reqList.OrigCluster = sm.clusterInfo.GetClusterName()
	}
	ignore, err := sm.waitIgnoreUntilChanged(term, index, stop)
	for _, e := range reqList.Reqs {
		sm.w.Trigger(e.Header.ID, err)
	}
	if err != nil {
		return forceBackup, err
	}
	if ignore {
		if nodeLog.Level() >= common.LOG_DEBUG {
			sm.Debugf("ignored on slave, %v-%v:%v", term, index, reqList.String())
		}
		return forceBackup, nil
	}

	if nodeLog.Level() >= common.LOG_DEBUG {
		sm.Debugf("begin sync, %v-%v:%v", term, index, reqList.String())
	}
	if reqList.ReqId == 0 {
		for _, e := range reqList.Reqs {
			reqList.ReqId = e.Header.ID
			break
		}
	}
	// TODO: stats latency raft write begin to begin sync.
	for _, req := range reqList.Reqs {
		if req.Header.DataType == int32(CustomReq) {
			var p customProposeData
			err := json.Unmarshal(req.Data, &p)
			if err != nil {
				sm.Infof("failed to unmarshal http propose: %v", req.String())
			}
			if p.ProposeOp == ProposeOp_Backup {
				sm.Infof("got force backup request")
				forceBackup = true
				break
			}
		}
	}
	select {
	case sm.sendCh <- reqList:
	case <-stop:
		return false, nil
	case <-sm.sendStop:
		return false, nil
	}

	return forceBackup, nil
}
