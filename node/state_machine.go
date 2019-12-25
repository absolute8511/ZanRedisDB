package node

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/absolute8511/redcon"
	"github.com/youzan/ZanRedisDB/common"
	"github.com/youzan/ZanRedisDB/pkg/wait"
	"github.com/youzan/ZanRedisDB/raft/raftpb"
	"github.com/youzan/ZanRedisDB/rockredis"
)

const (
	maxDBBatchCmdNum = 100
	dbWriteSlow      = time.Millisecond * 100
)

// this error is used while the raft is applying the remote raft logs and notify we should
// not update the remote raft term-index state.
var errIgnoredRemoteApply = errors.New("remote raft apply should be ignored")
var errRemoteSnapTransferFailed = errors.New("remote raft snapshot transfer failed")
var errNobackupAvailable = errors.New("no backup available from others")

func isUnrecoveryError(err error) bool {
	if strings.HasPrefix(err.Error(), "IO error: No space left on device") {
		return true
	}
	return false
}

type StateMachine interface {
	ApplyRaftRequest(isReplaying bool, b IBatchOperator, req BatchInternalRaftRequest, term uint64, index uint64, stop chan struct{}) (bool, error)
	ApplyRaftConfRequest(req raftpb.ConfChange, term uint64, index uint64, stop chan struct{}) error
	GetSnapshot(term uint64, index uint64) (*KVSnapInfo, error)
	UpdateSnapshotState(term uint64, index uint64)
	PrepareSnapshot(raftSnapshot raftpb.Snapshot, stop chan struct{}) error
	RestoreFromSnapshot(raftSnapshot raftpb.Snapshot, stop chan struct{}) error
	Destroy()
	CleanData() error
	Optimize(string)
	GetStats(table string) common.NamespaceStats
	Start() error
	Close()
	CheckExpiredData(buffer common.ExpiredDataBuffer, stop chan struct{}) error
	GetBatchOperator() IBatchOperator
}

func NewStateMachine(opts *KVOptions, machineConfig MachineConfig, localID uint64,
	fullNS string, clusterInfo common.IClusterInfo, w wait.Wait) (StateMachine, error) {
	if machineConfig.LearnerRole == "" {
		if machineConfig.StateMachineType == "empty_sm" {
			nodeLog.Infof("%v using empty sm for test", fullNS)
			return &emptySM{w: w}, nil
		}
		kvsm, err := NewKVStoreSM(opts, machineConfig, localID, fullNS, clusterInfo)
		if err != nil {
			return nil, err
		}
		kvsm.w = w
		return kvsm, err
	} else if common.IsRoleLogSyncer(machineConfig.LearnerRole) {
		lssm, err := NewLogSyncerSM(opts, machineConfig, localID, fullNS, clusterInfo)
		if err != nil {
			return nil, err
		}
		lssm.w = w
		return lssm, err
	} else {
		return nil, errors.New("unknown learner role")
	}
}

type IBatchOperator interface {
	SetBatched(bool)
	IsBatched() bool
	BeginBatch() error
	AddBatchKey(string)
	AddBatchRsp(uint64, interface{})
	IsBatchable(string, string) bool
	CommitBatch()
}

type kvbatchOperator struct {
	batchReqIDList  []uint64
	batchReqRspList []interface{}
	batchStart      time.Time
	batching        bool
	dupCheckMap     map[string]bool
	kvsm            *kvStoreSM
}

func (bo *kvbatchOperator) SetBatched(b bool) {
	bo.batching = b
	if b {
		bo.batchStart = time.Now()
	}
}

func (bo *kvbatchOperator) IsBatched() bool {
	return bo.batching
}

func (bo *kvbatchOperator) BeginBatch() error {
	err := bo.kvsm.store.BeginBatchWrite()
	if err != nil {
		return err
	}
	bo.SetBatched(true)
	return nil
}

func (bo *kvbatchOperator) AddBatchKey(pk string) {
	bo.dupCheckMap[string(pk)] = true
}

func (bo *kvbatchOperator) AddBatchRsp(reqID uint64, v interface{}) {
	bo.batchReqIDList = append(bo.batchReqIDList, reqID)
	bo.batchReqRspList = append(bo.batchReqRspList, v)
}

func (bo *kvbatchOperator) IsBatchable(cmdName string, pk string) bool {
	_, ok := bo.dupCheckMap[string(pk)]
	if rockredis.IsBatchableWrite(cmdName) &&
		len(bo.batchReqIDList) < maxDBBatchCmdNum &&
		!ok {
		return true
	}
	return false
}

func (bo *kvbatchOperator) CommitBatch() {
	if !bo.IsBatched() {
		return
	}
	err := bo.kvsm.store.CommitBatchWrite()
	bo.SetBatched(false)
	bo.dupCheckMap = make(map[string]bool)
	batchCost := time.Since(bo.batchStart)
	if nodeLog.Level() >= common.LOG_DETAIL && len(bo.batchReqIDList) > 1 {
		bo.kvsm.Infof("batching command number: %v", len(bo.batchReqIDList))
	}
	// write the future response or error
	for idx, rid := range bo.batchReqIDList {
		if err != nil {
			bo.kvsm.w.Trigger(rid, err)
		} else {
			bo.kvsm.w.Trigger(rid, bo.batchReqRspList[idx])
		}
	}
	if batchCost > dbWriteSlow || (nodeLog.Level() >= common.LOG_DEBUG && batchCost > dbWriteSlow/2) {
		bo.kvsm.Infof("slow batch write db, batch: %v, cost: %v",
			len(bo.batchReqIDList), batchCost)
	}
	if len(bo.batchReqIDList) > 0 {
		bo.kvsm.dbWriteStats.BatchUpdateLatencyStats(batchCost.Nanoseconds()/1000, int64(len(bo.batchReqIDList)))
	}
	bo.batchReqIDList = bo.batchReqIDList[:0]
	bo.batchReqRspList = bo.batchReqRspList[:0]
}

type emptySM struct {
	w wait.Wait
}

func (esm *emptySM) ApplyRaftRequest(isReplaying bool, batch IBatchOperator, reqList BatchInternalRaftRequest, term uint64, index uint64, stop chan struct{}) (bool, error) {
	ts := reqList.Timestamp
	tn := time.Now()
	if ts > 0 && !isReplaying {
		cost := tn.UnixNano() - ts
		if cost > raftSlow.Nanoseconds()*2 {
			nodeLog.Infof("receive raft requests in state machine slow cost: %v, %v, %v", reqList.ReqId, len(reqList.Reqs), cost)
		}
	}
	for _, req := range reqList.Reqs {
		reqID := req.Header.ID
		if reqID == 0 {
			reqID = reqList.ReqId
		}
		esm.w.Trigger(reqID, nil)
	}
	return false, nil
}

func (esm *emptySM) ApplyRaftConfRequest(req raftpb.ConfChange, term uint64, index uint64, stop chan struct{}) error {
	return nil
}

func (esm *emptySM) GetSnapshot(term uint64, index uint64) (*KVSnapInfo, error) {
	var s KVSnapInfo
	return &s, nil
}

func (esm *emptySM) UpdateSnapshotState(term uint64, index uint64) {
}

func (esm *emptySM) PrepareSnapshot(raftSnapshot raftpb.Snapshot, stop chan struct{}) error {
	return nil
}

func (esm *emptySM) RestoreFromSnapshot(raftSnapshot raftpb.Snapshot, stop chan struct{}) error {
	return nil
}

func (esm *emptySM) GetBatchOperator() IBatchOperator {
	return nil
}

func (esm *emptySM) Destroy() {
}

func (esm *emptySM) CleanData() error {
	return nil
}
func (esm *emptySM) Optimize(t string) {

}
func (esm *emptySM) GetStats(table string) common.NamespaceStats {
	return common.NamespaceStats{}
}
func (esm *emptySM) Start() error {
	return nil
}
func (esm *emptySM) Close() {

}
func (esm *emptySM) CheckExpiredData(buffer common.ExpiredDataBuffer, stop chan struct{}) error {
	return nil
}

type kvStoreSM struct {
	fullName      string
	store         *KVStore
	clusterInfo   common.IClusterInfo
	fullNS        string
	machineConfig MachineConfig
	ID            uint64
	dbWriteStats  common.WriteStats
	w             wait.Wait
	router        *common.SMCmdRouter
	stopping      int32
	cRouter       *conflictRouter
}

func NewKVStoreSM(opts *KVOptions, machineConfig MachineConfig, localID uint64, ns string,
	clusterInfo common.IClusterInfo) (*kvStoreSM, error) {
	store, err := NewKVStore(opts)
	if err != nil {
		return nil, err
	}
	sm := &kvStoreSM{
		fullNS:        ns,
		machineConfig: machineConfig,
		ID:            localID,
		clusterInfo:   clusterInfo,
		store:         store,
		router:        common.NewSMCmdRouter(),
		cRouter:       NewConflictRouter(),
	}
	sm.registerHandlers()
	sm.registerConflictHandlers()
	return sm, nil
}

func (kvsm *kvStoreSM) Debugf(f string, args ...interface{}) {
	msg := fmt.Sprintf(f, args...)
	nodeLog.DebugDepth(1, fmt.Sprintf("%v: %s", kvsm.fullNS, msg))
}

func (kvsm *kvStoreSM) Infof(f string, args ...interface{}) {
	msg := fmt.Sprintf(f, args...)
	nodeLog.InfoDepth(1, fmt.Sprintf("%v: %s", kvsm.fullNS, msg))
}

func (kvsm *kvStoreSM) Errorf(f string, args ...interface{}) {
	msg := fmt.Sprintf(f, args...)
	nodeLog.ErrorDepth(1, fmt.Sprintf("%v: %s", kvsm.fullNS, msg))
}

func (kvsm *kvStoreSM) Start() error {
	return nil
}

func (kvsm *kvStoreSM) Close() {
	if !atomic.CompareAndSwapInt32(&kvsm.stopping, 0, 1) {
		return
	}
	kvsm.store.Close()
}

func (kvsm *kvStoreSM) GetBatchOperator() IBatchOperator {
	return &kvbatchOperator{
		dupCheckMap: make(map[string]bool),
		kvsm:        kvsm,
	}
}

func (kvsm *kvStoreSM) Optimize(table string) {
	if table == "" {
		kvsm.store.CompactRange()
	} else {
		kvsm.store.CompactTableRange(table)
	}
}

func (kvsm *kvStoreSM) GetDBInternalStats() string {
	return kvsm.store.GetStatistics()
}

func (kvsm *kvStoreSM) GetStats(table string) common.NamespaceStats {
	var tbs [][]byte
	if len(table) > 0 {
		tbs = [][]byte{[]byte(table)}
	} else {
		tbs = kvsm.store.GetTables()
	}
	var ns common.NamespaceStats
	ns.InternalStats = kvsm.store.GetInternalStatus()
	ns.DBWriteStats = kvsm.dbWriteStats.Copy()
	diskUsages := kvsm.store.GetBTablesSizes(tbs)
	for i, t := range tbs {
		cnt, _ := kvsm.store.GetTableKeyCount(t)
		var ts common.TableStats
		ts.ApproximateKeyNum = kvsm.store.GetTableApproximateNumInRange(string(t), nil, nil)
		if cnt <= 0 {
			cnt = ts.ApproximateKeyNum
		}
		ts.Name = string(t)
		ts.KeyNum = cnt
		ts.DiskBytesUsage = diskUsages[i]
		ns.TStats = append(ns.TStats, ts)
	}

	return ns
}

func (kvsm *kvStoreSM) CleanData() error {
	return kvsm.store.CleanData()
}

func (kvsm *kvStoreSM) Destroy() {
	kvsm.store.Destroy()
}

func (kvsm *kvStoreSM) CheckExpiredData(buffer common.ExpiredDataBuffer, stop chan struct{}) error {
	return kvsm.store.CheckExpiredData(buffer, stop)
}

func (kvsm *kvStoreSM) UpdateSnapshotState(term uint64, index uint64) {
	if kvsm.store != nil {
		kvsm.store.SetLatestSnapIndex(index)
	}
}

func (kvsm *kvStoreSM) GetSnapshot(term uint64, index uint64) (*KVSnapInfo, error) {
	var si KVSnapInfo
	// use the rocksdb backup/checkpoint interface to backup data
	si.BackupInfo = kvsm.store.Backup(term, index)
	if si.BackupInfo == nil {
		return nil, errors.New("failed to begin backup: maybe too much backup running")
	}
	si.WaitReady()
	return &si, nil
}

func checkLocalBackup(store *KVStore, rs raftpb.Snapshot) (bool, error) {
	return store.IsLocalBackupOK(rs.Metadata.Term, rs.Metadata.Index)
}

func handleReuseOldCheckpoint(srcInfo string, localPath string, term uint64, index uint64, skipReuseN int) (string, string) {
	newPath := path.Join(localPath, rockredis.GetCheckpointDir(term, index))
	reused := ""

	latest := rockredis.GetLatestCheckpoint(localPath, skipReuseN, func(dir string) bool {
		// reuse last synced to speed up rsync
		// check if source node info is matched current snapshot source
		d, _ := ioutil.ReadFile(path.Join(dir, "source_node_info"))
		if d != nil && string(d) == srcInfo {
			return true
		} else if dir == newPath {
			// it has the same dir with the transferring snap but with the different node info,
			// we should clean the dir to avoid reuse the data from different node
			os.RemoveAll(dir)
			nodeLog.Infof("clean old path: %v since node info mismatch and same with new", dir)
		}
		return false
	})
	if latest != "" && latest != newPath {
		nodeLog.Infof("transfer reuse old path: %v to new: %v", latest, newPath)
		reused = latest
		// we use hard link to avoid change the old stable checkpoint files which should keep unchanged in case of
		// crashed during new checkpoint transferring
		files, _ := filepath.Glob(path.Join(latest, "*.sst"))
		for _, fn := range files {
			nfn := path.Join(newPath, filepath.Base(fn))
			nodeLog.Infof("hard link for: %v, %v", fn, nfn)
			CopyFileForHardLink(fn, nfn)
		}
	}
	return reused, newPath
}

func postFileSync(newPath string, srcInfo string) {
	// write source node info to allow reuse next time
	ioutil.WriteFile(path.Join(newPath, "source_node_info"), []byte(srcInfo), common.FILE_PERM)
}

func prepareSnapshotForStore(store *KVStore, machineConfig MachineConfig,
	clusterInfo common.IClusterInfo, fullNS string,
	localID uint64, stopChan chan struct{},
	raftSnapshot raftpb.Snapshot, retry int) error {

	hasBackup, _ := checkLocalBackup(store, raftSnapshot)
	if hasBackup {
		return nil
	}
	if clusterInfo == nil {
		return errors.New("cluster info is not available.")
	}
	syncAddr, syncDir := GetValidBackupInfo(machineConfig, clusterInfo, fullNS, localID, stopChan, raftSnapshot, retry, false)
	if syncAddr == "" && syncDir == "" {
		return errNobackupAvailable
	}
	select {
	case <-stopChan:
		return common.ErrStopped
	default:
	}
	localPath := store.GetBackupDir()
	srcInfo := syncAddr + syncDir
	srcPath := path.Join(rockredis.GetBackupDir(syncDir),
		rockredis.GetCheckpointDir(raftSnapshot.Metadata.Term, raftSnapshot.Metadata.Index))

	// since most backup on local is not transferred by others,
	// if we need reuse we need check all backups that has source node info,
	// and skip the latest snap file in snap dir.
	_, newPath := handleReuseOldCheckpoint(srcInfo, localPath,
		raftSnapshot.Metadata.Term, raftSnapshot.Metadata.Index,
		0)

	// copy backup data from the remote leader node, and recovery backup from it
	// if local has some old backup data, we should use rsync to sync the data file
	// use the rocksdb backup/checkpoint interface to backup data
	err := common.RunFileSync(syncAddr,
		srcPath,
		localPath, stopChan)

	postFileSync(newPath, srcInfo)
	return err
}

func GetValidBackupInfo(machineConfig MachineConfig,
	clusterInfo common.IClusterInfo, fullNS string,
	localID uint64, stopChan chan struct{},
	raftSnapshot raftpb.Snapshot, retryIndex int, useRsyncForLocal bool) (string, string) {
	// we need find the right backup data match with the raftsnapshot
	// for each cluster member, it need check the term+index and the backup meta to
	// make sure the data is valid
	syncAddr := ""
	syncDir := ""
	h := machineConfig.BroadcastAddr

	innerRetry := 0
	var snapSyncInfoList []common.SnapshotSyncInfo
	var err error
	for innerRetry < 3 {
		innerRetry++
		snapSyncInfoList, err = clusterInfo.GetSnapshotSyncInfo(fullNS)
		if err != nil {
			nodeLog.Infof("%v get snapshot info failed: %v", fullNS, err)
			select {
			case <-stopChan:
				break
			case <-time.After(time.Second):
			}
		} else {
			break
		}
	}

	nodeLog.Infof("%v current cluster raft nodes info: %v", fullNS, snapSyncInfoList)
	syncAddrList := make([]string, 0)
	syncDirList := make([]string, 0)
	for _, ssi := range snapSyncInfoList {
		if ssi.ReplicaID == localID {
			continue
		}

		body, _ := raftSnapshot.Marshal()
		uri := "http://" + ssi.RemoteAddr + ":" +
			ssi.HttpAPIPort + common.APICheckBackup + "/" + fullNS

		// check may use long time, so we need use large timeout here, some slow disk
		// may cause 10 min to check
		to := time.Second * time.Duration(common.GetIntDynamicConf(common.ConfCheckSnapTimeout))
		sc, err := common.APIRequest("GET", uri, bytes.NewBuffer(body), to, nil)
		if err != nil {
			nodeLog.Infof("request %v error: %v", uri, err)
			continue
		}
		if sc != http.StatusOK {
			nodeLog.Infof("request %v error: %v", uri, sc)
			continue
		}
		if ssi.RemoteAddr == h {
			if ssi.DataRoot == machineConfig.DataRootDir {
				// the leader is old mine, try find another leader
				nodeLog.Infof("data dir can not be same if on local: %v, %v", ssi, machineConfig)
				continue
			}
			if useRsyncForLocal {
				syncAddrList = append(syncAddrList, ssi.RemoteAddr)
				syncDirList = append(syncDirList, path.Join(ssi.RsyncModule, fullNS))
			} else {
				// local node with different directory
				syncAddrList = append(syncAddrList, "")
				syncDirList = append(syncDirList, path.Join(ssi.DataRoot, fullNS))
			}
		} else {
			// for remote snapshot, we do rsync from remote module
			syncAddrList = append(syncAddrList, ssi.RemoteAddr)
			syncDirList = append(syncDirList, path.Join(ssi.RsyncModule, fullNS))
		}
	}
	if len(syncAddrList) > 0 {
		syncAddr = syncAddrList[retryIndex%len(syncAddrList)]
		syncDir = syncDirList[retryIndex%len(syncDirList)]
	}
	nodeLog.Infof("%v should recovery from : %v, %v", fullNS, syncAddr, syncDir)
	return syncAddr, syncDir
}

func (kvsm *kvStoreSM) PrepareSnapshot(raftSnapshot raftpb.Snapshot, stop chan struct{}) error {
	// while startup we can use the local snapshot to restart,
	// but while running, we should install the leader's snapshot,
	// so we need remove local and sync from leader
	retry := 0
	var finalErr error
	for retry < 3 {
		err := prepareSnapshotForStore(kvsm.store, kvsm.machineConfig, kvsm.clusterInfo, kvsm.fullNS,
			kvsm.ID, stop, raftSnapshot, retry)
		if err != nil {
			kvsm.Infof("failed to prepare snapshot: %v", err)
			if err == common.ErrTransferOutofdate ||
				err == common.ErrRsyncFailed ||
				err == common.ErrStopped {
				return err
			}
			finalErr = err
		} else {
			return nil
		}
		retry++
		kvsm.Infof("failed to restore snapshot: %v", err)
		select {
		case <-stop:
			return err
		case <-time.After(time.Second):
		}
	}
	if finalErr == errNobackupAvailable {
		kvsm.Infof("failed to restore snapshot at startup since no any backup from anyware")
		return finalErr
	}
	return finalErr
}

func (kvsm *kvStoreSM) RestoreFromSnapshot(raftSnapshot raftpb.Snapshot, stop chan struct{}) error {
	if enableSnapApplyRestoreStorageTest {
		return errors.New("failed to restore from snapshot in failed test")
	}
	return kvsm.store.Restore(raftSnapshot.Metadata.Term, raftSnapshot.Metadata.Index)
}

func (kvsm *kvStoreSM) ApplyRaftConfRequest(req raftpb.ConfChange, term uint64, index uint64, stop chan struct{}) error {
	return nil
}

func (kvsm *kvStoreSM) preCheckConflict(cmd redcon.Command, reqTs int64) ConflictState {
	cmdName := strings.ToLower(string(cmd.Args[0]))
	h, ok := kvsm.cRouter.GetHandler(cmdName)
	if !ok {
		return Conflict
	}
	return h(cmd, reqTs)
}

func (kvsm *kvStoreSM) ApplyRaftRequest(isReplaying bool, batch IBatchOperator, reqList BatchInternalRaftRequest, term uint64, index uint64, stop chan struct{}) (bool, error) {
	forceBackup := false
	start := time.Now()
	ts := reqList.Timestamp
	if reqList.Type == FromClusterSyncer {
		if nodeLog.Level() >= common.LOG_DETAIL {
			kvsm.Debugf("recv write from cluster syncer at (%v-%v): %v", term, index, reqList.String())
		}
	}
	if ts > 0 {
		cost := start.UnixNano() - ts
		if cost > raftSlow.Nanoseconds()/2 {
			kvsm.Debugf("receive raft requests in state machine slow cost: %v, %v", len(reqList.Reqs), cost)
		}
	}
	// TODO: maybe we can merge the same write with same key and value to avoid too much hot write on the same key-value
	var retErr error
	for _, req := range reqList.Reqs {
		reqTs := ts
		if reqTs == 0 {
			reqTs = req.Header.Timestamp
		}
		reqID := req.Header.ID
		if reqID == 0 {
			reqID = reqList.ReqId
		}
		if req.Header.DataType == int32(RedisReq) {
			cmd, err := redcon.Parse(req.Data)
			if err != nil {
				kvsm.w.Trigger(reqID, err)
			} else {
				// we need compare the key timestamp in this cluster and the timestamp from raft request to handle
				// the conflict change between two cluster.
				//
				if !isReplaying && reqList.Type == FromClusterSyncer && !IsSyncerOnly() {
					// syncer only no need check conflict since it will be no write from redis api
					conflict := kvsm.preCheckConflict(cmd, reqTs)
					if conflict == Conflict {
						kvsm.Infof("conflict sync: %v, %v, %v", string(cmd.Raw), req.String(), reqTs)
						// just ignore sync, should not return error because the syncer will block retrying for error sync
						kvsm.w.Trigger(reqID, nil)
						continue
					}
					if reqTs > GetSyncedOnlyChangedTs() {
						// maybe unconsistence if write on slave after cluster switched,
						// so we need log write here to know what writes are synced after we
						// became the master cluster.
						kvsm.Infof("write from syncer after syncer state changed, conflict state:%v: %v, %v, %v", conflict, string(cmd.Raw), req.String(), reqTs)
					}
				}
				cmdStart := time.Now()
				cmdName := strings.ToLower(string(cmd.Args[0]))
				pk, _ := common.CutNamesapce(cmd.Args[1])
				if batch.IsBatchable(cmdName, string(pk)) {
					if !batch.IsBatched() {
						err := batch.BeginBatch()
						if err != nil {
							kvsm.Infof("begin batch command %v failed: %v, %v", cmdName, string(cmd.Raw), err)
							kvsm.w.Trigger(reqID, err)
							continue
						}
					}
				} else {
					batch.CommitBatch()
				}
				h, ok := kvsm.router.GetInternalCmdHandler(cmdName)
				if !ok {
					kvsm.Infof("unsupported redis command: %v", cmdName)
					kvsm.w.Trigger(reqID, common.ErrInvalidCommand)
				} else {
					if pk != nil && batch.IsBatched() {
						batch.AddBatchKey(string(pk))
					}
					v, err := h(cmd, reqTs)
					if err != nil {
						kvsm.Errorf("redis command %v error: %v, cmd: %v", cmdName, err, string(cmd.Raw))
						kvsm.w.Trigger(reqID, err)
						if isUnrecoveryError(err) {
							panic(err)
						}
					} else {
						if batch.IsBatched() {
							batch.AddBatchRsp(reqID, v)
							if nodeLog.Level() > common.LOG_DETAIL {
								kvsm.Infof("batching write command:%v, %v", cmdName, string(cmd.Raw))
							}
							kvsm.dbWriteStats.UpdateSizeStats(int64(len(cmd.Raw)))
						} else {
							kvsm.w.Trigger(reqID, v)
							cmdCost := time.Since(cmdStart)
							if cmdCost > dbWriteSlow || nodeLog.Level() > common.LOG_DETAIL ||
								(nodeLog.Level() >= common.LOG_DEBUG && cmdCost > dbWriteSlow/2) {
								kvsm.Infof("slow write command: %v, cost: %v", string(cmd.Raw), cmdCost)
							}
							kvsm.dbWriteStats.UpdateWriteStats(int64(len(cmd.Raw)), cmdCost.Nanoseconds()/1000)
						}
					}
				}
			}
		} else {
			batch.CommitBatch()

			if req.Header.DataType == int32(CustomReq) {
				forceBackup, retErr = kvsm.handleCustomRequest(&req, reqID, stop)
			} else if req.Header.DataType == int32(SchemaChangeReq) {
				kvsm.Infof("handle schema change: %v", string(req.Data))
				var sc SchemaChange
				err := sc.Unmarshal(req.Data)
				if err != nil {
					kvsm.Infof("schema data error: %v, %v", string(req.Data), err)
					kvsm.w.Trigger(reqID, err)
				} else {
					err = kvsm.handleSchemaUpdate(sc)
					kvsm.w.Trigger(reqID, err)
				}
			} else {
				kvsm.w.Trigger(reqID, errUnknownData)
			}
		}
	}
	// TODO: add test case for this
	if reqList.ReqId > 0 {
		// reqid only be used for cluster sync grpc.
		// we commit here to allow grpc get notify earlier.
		batch.CommitBatch()
	}
	if !batch.IsBatched() {
		for _, req := range reqList.Reqs {
			if kvsm.w.IsRegistered(req.Header.ID) {
				kvsm.Infof("missing process request: %v", req.String())
				kvsm.w.Trigger(req.Header.ID, errUnknownData)
			}
		}
	}

	cost := time.Since(start)
	if cost >= raftSlow {
		kvsm.Infof("slow for batch write db: %v, cost %v", len(reqList.Reqs), cost)
	}
	// used for grpc raft proposal, will notify that all the raft logs in this batch is done.
	if reqList.ReqId > 0 {
		kvsm.w.Trigger(reqList.ReqId, nil)
	}
	return forceBackup, retErr
}

func (kvsm *kvStoreSM) handleCustomRequest(req *InternalRaftRequest, reqID uint64, stop chan struct{}) (bool, error) {
	var p customProposeData
	var forceBackup bool
	var retErr error
	err := json.Unmarshal(req.Data, &p)
	if err != nil {
		kvsm.Infof("failed to unmarshal custom propose: %v, err: %v", req.String(), err)
		kvsm.w.Trigger(reqID, err)
		return forceBackup, retErr
	}
	if p.ProposeOp == ProposeOp_Backup {
		kvsm.Infof("got force backup request")
		forceBackup = true
		kvsm.w.Trigger(reqID, nil)
	} else if p.ProposeOp == ProposeOp_DeleteTable {
		var dr DeleteTableRange
		err = json.Unmarshal(p.Data, &dr)
		if err != nil {
			kvsm.Infof("invalid delete table range data: %v", string(p.Data))
		} else {
			err = kvsm.store.DeleteTableRange(dr.Dryrun, dr.Table, dr.StartFrom, dr.EndTo)
		}
		kvsm.w.Trigger(reqID, err)
	} else if p.ProposeOp == ProposeOp_RemoteConfChange {
		var cc raftpb.ConfChange
		cc.Unmarshal(p.Data)
		kvsm.Infof("remote config changed: %v, %v ", p, cc.String())
		kvsm.w.Trigger(reqID, nil)
	} else if p.ProposeOp == ProposeOp_TransferRemoteSnap {
		localPath := kvsm.store.GetBackupDirForRemote()
		kvsm.Infof("transfer remote snap request: %v to local: %v", p, localPath)
		retErr = errRemoteSnapTransferFailed
		err := os.MkdirAll(localPath, common.DIR_PERM)
		// trigger early to allow client api return quickly
		// the transfer status already be saved.
		kvsm.w.Trigger(reqID, err)
		if err == nil {
			srcInfo := p.SyncAddr + p.SyncPath
			srcPath := path.Join(rockredis.GetBackupDir(p.SyncPath),
				rockredis.GetCheckpointDir(p.RemoteTerm, p.RemoteIndex))

			_, newPath := handleReuseOldCheckpoint(srcInfo, localPath, p.RemoteTerm, p.RemoteIndex, 0)

			if common.IsConfSetted(common.ConfIgnoreRemoteFileSync) {
				err = nil
			} else {
				err = common.RunFileSync(p.SyncAddr,
					srcPath,
					localPath, stop,
				)
				postFileSync(newPath, srcInfo)
			}
			if err != nil {
				kvsm.Infof("transfer remote snap request: %v to local: %v failed: %v", p, localPath, err)
			} else {
				// TODO: check the transferred snapshot file
				//rockredis.IsLocalBackupOK()
				retErr = nil
			}
		}
	} else if p.ProposeOp == ProposeOp_ApplyRemoteSnap {
		kvsm.Infof("begin apply remote snap : %v", p)
		retErr = errIgnoredRemoteApply
		err := kvsm.store.RestoreFromRemoteBackup(p.RemoteTerm, p.RemoteIndex)
		kvsm.w.Trigger(reqID, err)
		if err != nil {
			kvsm.Infof("apply remote snap %v failed : %v", p, err)
		} else {
			retErr = nil
			forceBackup = true
		}
	} else if p.ProposeOp == ProposeOp_ApplySkippedRemoteSnap {
		kvsm.Infof("apply remote skip snap %v ", p)
		kvsm.w.Trigger(reqID, nil)
	} else {
		kvsm.w.Trigger(reqID, errUnknownData)
	}
	return forceBackup, retErr
}
