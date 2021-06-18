package node

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spaolacci/murmur3"
	"github.com/youzan/ZanRedisDB/common"
	"github.com/youzan/ZanRedisDB/engine"
	"github.com/youzan/ZanRedisDB/metric"
	"github.com/youzan/ZanRedisDB/transport/rafthttp"
	"golang.org/x/net/context"
)

var (
	ErrNamespaceAlreadyExist      = errors.New("namespace already exist")
	ErrNamespaceAlreadyStarting   = errors.New("namespace is starting")
	ErrRaftIDMismatch             = errors.New("raft id mismatch")
	ErrRaftConfMismatch           = errors.New("raft config mismatch")
	errTimeoutLeaderTransfer      = errors.New("raft leader transfer failed")
	errStopping                   = errors.New("ERR_CLUSTER_CHANGED: the namespace is stopping")
	ErrNamespaceNotFound          = errors.New("ERR_CLUSTER_CHANGED: namespace is not found")
	ErrNamespacePartitionNotFound = errors.New("ERR_CLUSTER_CHANGED: partition of the namespace is not found")
	ErrNamespaceNotLeader         = errors.New("ERR_CLUSTER_CHANGED: partition of the namespace is not leader on the node")
	ErrNodeNoLeader               = errors.New("ERR_CLUSTER_CHANGED: partition of the node has no leader")
	ErrRaftGroupNotReady          = errors.New("ERR_CLUSTER_CHANGED: raft group not ready")
	ErrProposalCanceled           = errors.New("ERR_CLUSTER_CHANGED: raft proposal " + context.Canceled.Error())
	errNamespaceConfInvalid       = errors.New("namespace config is invalid")
	ErrLocalMagicCodeConflict     = errors.New("namespace magic code conflict on local")
)

var perfLevel int32

type NamespaceNode struct {
	Node      *KVNode
	conf      *NamespaceConfig
	ready     int32
	magicCode int64
	nsDataDir string
}

func (nn *NamespaceNode) IsReady() bool {
	return atomic.LoadInt32(&nn.ready) == 1
}

func (nn *NamespaceNode) FullName() string {
	return nn.conf.Name
}

func (nn *NamespaceNode) SwitchForLearnerLeader(isLearnerLeader bool) {
	nn.Node.switchForLearnerLeader(isLearnerLeader)
}

func (nn *NamespaceNode) SetDynamicInfo(dync NamespaceDynamicConf) {
	nn.Node.SetDynamicInfo(dync)
}

func getMagicCodeFileName(dataPath string, grp string) string {
	return path.Join(dataPath, "magic_"+grp)
}

func saveMagicCode(fileName string, magicCode int64) error {
	var f *os.File
	var err error

	f, err = os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, common.FILE_PERM)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = fmt.Fprintf(f, "%d\n",
		magicCode)
	if err != nil {
		return err
	}
	f.Sync()
	return nil
}

func LoadMagicCode(fileName string) (int64, error) {
	var f *os.File
	var err error

	f, err = os.OpenFile(fileName, os.O_RDONLY, common.FILE_PERM)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	defer f.Close()

	var code int64
	_, err = fmt.Fscanf(f, "%d\n",
		&code)
	if err != nil {
		return 0, err
	}
	return code, nil
}

func (nn *NamespaceNode) getMagicCode() int64 {
	return atomic.LoadInt64(&nn.magicCode)
}

func (nn *NamespaceNode) SetMagicCode(magic int64) error {
	fileName := getMagicCodeFileName(nn.nsDataDir, nn.conf.Name)
	if nn.getMagicCode() == 0 {
		// try read old magic code from file
		mc, err := LoadMagicCode(fileName)
		if err != nil {
			return err
		}
		atomic.CompareAndSwapInt64(&nn.magicCode, 0, mc)
	}
	// check if already magic code is different, if conflicted should report error
	if nn.getMagicCode() != 0 && nn.getMagicCode() != magic {
		nodeLog.Warningf("set magic code to %v conflict on local: %v", magic, nn.getMagicCode())
		return ErrLocalMagicCodeConflict
	}
	if nn.getMagicCode() == magic {
		return nil
	}
	changed := atomic.CompareAndSwapInt64(&nn.magicCode, 0, magic)
	if !changed {
		return fmt.Errorf("set magic code to %v conflict on local: %v", magic, nn.getMagicCode())
	}
	return saveMagicCode(fileName, magic)
}

func (nn *NamespaceNode) SetDataFixState(needFix bool) {
}

func (nn *NamespaceNode) GetLastLeaderChangedTime() int64 {
	return nn.Node.GetLastLeaderChangedTime()
}

func (nn *NamespaceNode) GetRaftID() uint64 {
	return nn.Node.rn.config.ID
}

func (nn *NamespaceNode) CheckRaftConf(raftID uint64, conf *NamespaceConfig) error {
	if nn.conf.EngType != conf.EngType ||
		nn.conf.RaftGroupConf.GroupID != conf.RaftGroupConf.GroupID {
		nodeLog.Infof("mine :%v, check raft conf:%v", nn.conf, conf)
		return ErrRaftConfMismatch
	}
	if raftID != nn.Node.rn.config.ID {
		nodeLog.Infof("mine :%v, check raft conf:%v", nn.Node.rn.config.ID, raftID)
		return ErrRaftIDMismatch
	}
	return nil
}

func (nn *NamespaceNode) Close() {
	nn.Node.Stop()
	atomic.StoreInt32(&nn.ready, 0)
	nodeLog.Infof("namespace stopped: %v", nn.conf.Name)
}

func (nn *NamespaceNode) Destroy() error {
	return nn.Node.destroy()
}

func (nn *NamespaceNode) IsDataNeedFix() bool {
	return false
}

// full ready node means: all local commit log replay done and we are aware of leader and
// maybe we have done all the newest commit log in state machine.
func (nn *NamespaceNode) IsNsNodeFullReady(checkCommitIndex bool) bool {
	if !nn.IsReady() {
		return false
	}
	if !nn.Node.rn.IsReplayFinished() {
		return false
	}
	return nn.Node.IsRaftSynced(checkCommitIndex)
}

func (nn *NamespaceNode) GetLearners() []*common.MemberInfo {
	return nn.Node.GetLearners()
}

func (nn *NamespaceNode) GetMembers() []*common.MemberInfo {
	return nn.Node.GetMembers()
}

func (nn *NamespaceNode) Start(forceStandaloneCluster bool) error {
	if !atomic.CompareAndSwapInt32(&nn.ready, 0, -1) {
		if atomic.LoadInt32(&nn.ready) == 1 {
			// already started
			return nil
		}
		// starting
		return ErrNamespaceAlreadyStarting
	}
	if err := nn.Node.Start(forceStandaloneCluster); err != nil {
		atomic.StoreInt32(&nn.ready, 0)
		return err
	}
	atomic.StoreInt32(&nn.ready, 1)
	return nil
}

func (nn *NamespaceNode) TransferMyLeader(to uint64, toRaftID uint64) error {
	err := nn.Node.TransferLeadership(toRaftID)
	if err != nil {
		return err
	}
	nodeLog.Infof("finished transfer to %v:%v", to, toRaftID)
	return nil
}

type NamespaceMeta struct {
	PartitionNum int
	walEng       engine.KVEngine
}

type NamespaceMgr struct {
	mutex         sync.RWMutex
	kvNodes       map[string]*NamespaceNode
	nsMetas       map[string]*NamespaceMeta
	groups        map[uint64]string
	machineConf   *MachineConfig
	raftTransport *rafthttp.Transport
	stopping      int32
	stopC         chan struct{}
	wg            sync.WaitGroup
	clusterInfo   common.IClusterInfo
	newLeaderChan chan string
}

func NewNamespaceMgr(transport *rafthttp.Transport, conf *MachineConfig) *NamespaceMgr {
	ns := &NamespaceMgr{
		kvNodes:       make(map[string]*NamespaceNode),
		groups:        make(map[uint64]string),
		nsMetas:       make(map[string]*NamespaceMeta),
		raftTransport: transport,
		machineConf:   conf,
		newLeaderChan: make(chan string, 2048),
	}
	regID, err := ns.LoadMachineRegID()
	if err != nil {
		nodeLog.Infof("load my register node id failed: %v", err)
	} else if regID > 0 {
		ns.machineConf.NodeID = regID
	}
	common.RegisterConfChangedHandler(common.ConfSlowLimiterSwitch, ns.HandleSlowLimiterSwitchChanged)
	RegisterSlowConfChanged()

	return ns
}

func (nsm *NamespaceMgr) SetDBOptions(key string, value string) error {
	key = strings.ToLower(key)
	switch key {
	case "rate_limiter_bytes_per_sec":
		bytes, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return err
		}
		nsm.SetRateLimiterBytesPerSec(bytes)
	case "max_background_compactions":
		maxCompact, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		err = nsm.SetMaxBackgroundOptions(maxCompact, 0)
		if err != nil {
			return err
		}
	case "max_background_jobs":
		maxBackJobs, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		err = nsm.SetMaxBackgroundOptions(0, maxBackJobs)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("db options %v not support", key)
	}
	return nil
}

func (nsm *NamespaceMgr) SetMaxBackgroundOptions(maxCompact int, maxBackJobs int) error {
	var err error
	nsm.mutex.RLock()
	defer nsm.mutex.RUnlock()
	for _, n := range nsm.kvNodes {
		if !n.IsReady() {
			continue
		}
		err = n.Node.SetMaxBackgroundOptions(maxCompact, maxBackJobs)
		if err != nil {
			break
		}
	}
	return err
}

func (nsm *NamespaceMgr) SetRateLimiterBytesPerSec(bytesPerSec int64) {
	if nsm.machineConf.RocksDBSharedConfig == nil {
		return
	}
	nsm.machineConf.RocksDBSharedConfig.ChangeLimiter(bytesPerSec)
}

func (nsm *NamespaceMgr) SetIClusterInfo(clusterInfo common.IClusterInfo) {
	nsm.clusterInfo = clusterInfo
}

func (nsm *NamespaceMgr) LoadMachineRegID() (uint64, error) {
	d, err := ioutil.ReadFile(
		path.Join(nsm.machineConf.DataRootDir, "myid"),
	)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	v, err := strconv.ParseInt(strings.Trim(string(d), "\r\n"), 10, 64)
	return uint64(v), err
}

func (nsm *NamespaceMgr) SaveMachineRegID(regID uint64) error {
	nsm.machineConf.NodeID = regID
	return ioutil.WriteFile(
		path.Join(nsm.machineConf.DataRootDir, "myid"),
		[]byte(strconv.FormatInt(int64(regID), 10)),
		common.FILE_PERM)
}

func (nsm *NamespaceMgr) GetDataRoot() string {
	return nsm.machineConf.DataRootDir
}

func (nsm *NamespaceMgr) CheckLocalNamespaces() map[string]int64 {
	// scan local namespace and magic number
	scanDir := nsm.machineConf.DataRootDir
	// scan all local undeleted ns-part dirs and read the magic code
	dirList, err := filepath.Glob(path.Join(scanDir, "*-*"))
	if err != nil {
		nodeLog.Infof("found local data root %s error: %v", scanDir, err.Error())
		return nil
	}
	magicList := make(map[string]int64)
	for _, dir := range dirList {
		nodeLog.Infof("found local dir in data root: %v", dir)
		grpName := path.Base(dir)
		ns, _ := common.GetNamespaceAndPartition(grpName)
		if ns != "" {
			code, err := LoadMagicCode(getMagicCodeFileName(dir, grpName))
			if err != nil {
				continue
			}
			nodeLog.Infof("add valid namespace magic : %v, %v", grpName, code)
			magicList[grpName] = code
		}
	}
	return magicList
}

func (nsm *NamespaceMgr) Start() {
	nsm.stopC = make(chan struct{})
	atomic.StoreInt32(&nsm.stopping, 0)
	nsm.mutex.Lock()
	for _, kv := range nsm.kvNodes {
		kv.Start(false)
	}
	nsm.mutex.Unlock()
	nsm.wg.Add(1)
	go func() {
		defer nsm.wg.Done()
		nsm.clearUnusedRaftPeer()
	}()

	nsm.wg.Add(1)
	go func() {
		defer nsm.wg.Done()
		nsm.checkNamespaceRaftLeader()
	}()

	nsm.wg.Add(1)
	go func() {
		defer nsm.wg.Done()
		nsm.processRaftTick()
	}()
}

func (nsm *NamespaceMgr) Stop() {
	if !atomic.CompareAndSwapInt32(&nsm.stopping, 0, 1) {
		return
	}
	close(nsm.stopC)
	tmp := nsm.GetNamespaces()
	for _, n := range tmp {
		n.Close()
	}
	nsm.wg.Wait()
	start := time.Now()
	for {
		// wait stopped callback done
		time.Sleep(time.Second)
		if len(nsm.GetNamespaces()) == 0 {
			break
		}
		if time.Since(start) > time.Second*10 {
			nodeLog.Warningf("some namespace not stopped while waiting timeout")
			break
		}
	}
	if nsm.machineConf.RocksDBSharedConfig != nil {
		nsm.machineConf.RocksDBSharedConfig.Destroy()
	}
	nsm.mutex.RLock()
	defer nsm.mutex.RUnlock()
	for _, meta := range nsm.nsMetas {
		if meta.walEng != nil {
			meta.walEng.CloseAll()
		}
	}
	if nsm.machineConf.WALRocksDBSharedConfig != nil {
		nsm.machineConf.WALRocksDBSharedConfig.Destroy()
	}
	nodeLog.Infof("namespace manager stopped")
}

func (nsm *NamespaceMgr) IsAllRecoveryDone() bool {
	done := true
	nsm.mutex.RLock()
	for _, n := range nsm.kvNodes {
		if !n.IsNsNodeFullReady(true) {
			done = false
			break
		}
	}
	nsm.mutex.RUnlock()
	return done
}

func (nsm *NamespaceMgr) GetNamespaces() map[string]*NamespaceNode {
	tmp := make(map[string]*NamespaceNode)
	nsm.mutex.RLock()
	for k, n := range nsm.kvNodes {
		tmp[k] = n
	}
	nsm.mutex.RUnlock()
	return tmp
}

func initRaftStorageEng(cfg *engine.RockEngConfig) engine.KVEngine {
	nodeLog.Infof("using rocksdb raft storage dir:%v", cfg.DataDir)
	cfg.DisableWAL = true
	cfg.DisableMergeCounter = true
	cfg.EnableTableCounter = false
	cfg.OptimizeFiltersForHits = true
	// basically, we no need compress wal since it will be cleaned after snapshot
	cfg.MinLevelToCompress = 5
	// TODO: check memtable_insert_with_hint_prefix_extractor and DeleteRange bug
	if cfg.InsertHintFixedLen == 0 {
		cfg.InsertHintFixedLen = 10
	}
	cfg.AutoCompacted = true
	db, err := engine.NewKVEng(cfg)
	if err == nil {
		err = db.OpenEng()
		if err == nil {
			db.SetOptsForLogStorage()
			go db.CompactAllRange()
			return db
		}
	}
	nodeLog.Warningf("failed to open rocks raft db: %v, fallback to memory entries", err.Error())
	return nil
}

func (nsm *NamespaceMgr) getWALEng(ns string, dataDir string, id uint64, gid uint32, meta *NamespaceMeta) engine.KVEngine {
	if !nsm.machineConf.UseRocksWAL || meta == nil {
		return nil
	}
	rsDir := path.Join(dataDir, "rswal", ns)

	if nsm.machineConf.SharedRocksWAL {
		if meta.walEng != nil {
			return meta.walEng
		}
	} else {
		sharding := fmt.Sprintf("%v-%v", gid, id)
		rsDir = path.Join(rsDir, sharding)
	}

	walEngCfg := engine.NewRockConfig()
	walEngCfg.DataDir = rsDir
	walEngCfg.RockOptions = nsm.machineConf.WALRocksDBOpts
	walEngCfg.SharedConfig = nsm.machineConf.WALRocksDBSharedConfig
	engine.FillDefaultOptions(&walEngCfg.RockOptions)
	eng := initRaftStorageEng(walEngCfg)
	if nsm.machineConf.SharedRocksWAL {
		meta.walEng = eng
	}
	return eng
}

func (nsm *NamespaceMgr) InitNamespaceNode(conf *NamespaceConfig, raftID uint64, join bool) (*NamespaceNode, error) {
	if atomic.LoadInt32(&nsm.stopping) == 1 {
		return nil, errStopping
	}

	expPolicy, err := common.StringToExpirationPolicy(conf.ExpirationPolicy)
	if err != nil {
		nodeLog.Infof("namespace %v invalid expire policy : %v", conf.Name, conf.ExpirationPolicy)
		return nil, err
	}

	dv, err := common.StringToDataVersionType(conf.DataVersion)
	if err != nil {
		nodeLog.Infof("namespace %v invalid data version: %v", conf.Name, conf.DataVersion)
		return nil, err
	}
	if expPolicy == common.WaitCompact && dv == common.DefaultDataVer {
		return nil, errors.New("can not use compact ttl for old data version")
	}
	if dv != common.DefaultDataVer {
		nodeLog.Infof("namespace %v data version: %v, expire policy: %v", conf.Name, conf.DataVersion, expPolicy)
	}

	kvOpts := &KVOptions{
		DataDir:          path.Join(nsm.machineConf.DataRootDir, conf.Name),
		KeepBackup:       nsm.machineConf.KeepBackup,
		EngType:          conf.EngType,
		RockOpts:         nsm.machineConf.RocksDBOpts,
		ExpirationPolicy: expPolicy,
		DataVersion:      dv,
		SharedConfig:     nsm.machineConf.RocksDBSharedConfig,
	}
	engine.FillDefaultOptions(&kvOpts.RockOpts)

	if conf.PartitionNum <= 0 {
		return nil, errNamespaceConfInvalid
	}
	if conf.Replicator <= 0 {
		return nil, errNamespaceConfInvalid
	}
	clusterNodes := make(map[uint64]ReplicaInfo)
	for _, v := range conf.RaftGroupConf.SeedNodes {
		clusterNodes[v.ReplicaID] = v
	}
	_, ok := clusterNodes[uint64(raftID)]
	if !ok {
		join = true
	}

	nsm.mutex.Lock()
	defer nsm.mutex.Unlock()
	if n, ok := nsm.kvNodes[conf.Name]; ok {
		return n, ErrNamespaceAlreadyExist
	}

	d, _ := json.MarshalIndent(&conf, "", " ")
	nodeLog.Infof("namespace load config: %v", string(d))
	d, _ = json.MarshalIndent(&kvOpts, "", " ")
	nodeLog.Infof("namespace kv config: %v", string(d))
	nodeLog.Infof("local namespace node %v start with raft cluster: %v", raftID, clusterNodes)
	raftConf := &RaftConfig{
		GroupID:        conf.RaftGroupConf.GroupID,
		GroupName:      conf.Name,
		ID:             uint64(raftID),
		RaftAddr:       nsm.machineConf.LocalRaftAddr,
		DataDir:        kvOpts.DataDir,
		RaftPeers:      clusterNodes,
		SnapCount:      conf.SnapCount,
		SnapCatchup:    conf.SnapCatchup,
		Replicator:     int32(conf.Replicator),
		OptimizedFsync: conf.OptimizedFsync,
		nodeConfig:     nsm.machineConf,
	}
	d, _ = json.MarshalIndent(&raftConf, "", " ")
	nodeLog.Infof("namespace raft config: %v", string(d))

	var meta *NamespaceMeta
	if oldMeta, ok := nsm.nsMetas[conf.BaseName]; !ok {
		meta = &NamespaceMeta{
			PartitionNum: conf.PartitionNum,
		}
		nsm.nsMetas[conf.BaseName] = meta
		nodeLog.Infof("namespace meta init: %v", conf)
	} else {
		if oldMeta.PartitionNum != conf.PartitionNum {
			nodeLog.Errorf("namespace meta mismatch: %v, old: %v", conf, oldMeta)
			// update the meta if mismatch, it may happen if create the same namespace with different
			// config for old deleted namespace
			if oldMeta.walEng != nil {
				oldMeta.walEng.CloseAll()
			}
			meta = &NamespaceMeta{
				PartitionNum: conf.PartitionNum,
			}
			nsm.nsMetas[conf.BaseName] = meta
		} else {
			meta = oldMeta
		}
	}
	rs := nsm.getWALEng(conf.BaseName, nsm.machineConf.DataRootDir, raftConf.ID, uint32(raftConf.GroupID), meta)
	raftConf.SetEng(rs)

	kv, err := NewKVNode(kvOpts, raftConf, nsm.raftTransport,
		join, nsm.onNamespaceStopped(raftConf.GroupID, conf.Name),
		nsm.clusterInfo, nsm.newLeaderChan)
	if err != nil {
		return nil, err
	}

	n := &NamespaceNode{
		Node:      kv,
		conf:      conf,
		nsDataDir: kvOpts.DataDir,
	}

	nsm.kvNodes[conf.Name] = n
	nsm.groups[raftConf.GroupID] = conf.Name
	return n, nil
}

func GetHashedPartitionID(pk []byte, pnum int) int {
	return int(murmur3.Sum32(pk)) % pnum
}

func (nsm *NamespaceMgr) GetNamespaceNodeWithPrimaryKeySum(nsBaseName string, pk []byte, pkSum int) (*NamespaceNode, error) {
	nsm.mutex.RLock()
	defer nsm.mutex.RUnlock()
	v, ok := nsm.nsMetas[nsBaseName]
	if !ok {
		nodeLog.Infof("namespace %v meta not found", nsBaseName)
		return nil, ErrNamespaceNotFound
	}
	pid := pkSum % v.PartitionNum
	fullName := common.GetNsDesp(nsBaseName, pid)
	n, ok := nsm.kvNodes[fullName]
	if !ok {
		nodeLog.Debugf("namespace %v partition %v not found for pk: %v", nsBaseName, pid, string(pk))
		return nil, ErrNamespacePartitionNotFound
	}
	if !n.IsReady() {
		return nil, ErrRaftGroupNotReady
	}
	return n, nil
}

func (nsm *NamespaceMgr) GetNamespaceNodeWithPrimaryKey(nsBaseName string, pk []byte) (*NamespaceNode, error) {
	pkSum := int(murmur3.Sum32(pk))
	return nsm.GetNamespaceNodeWithPrimaryKeySum(nsBaseName, pk, pkSum)
}

func (nsm *NamespaceMgr) GetNamespaceNodes(nsBaseName string, leaderOnly bool) (map[string]*NamespaceNode, error) {
	nsNodes := make(map[string]*NamespaceNode)

	tmp := nsm.GetNamespaces()
	for k, v := range tmp {
		ns, _ := common.GetNamespaceAndPartition(k)
		if ns == nsBaseName && v.IsReady() {
			if leaderOnly && !v.Node.IsLead() {
				continue
			}
			nsNodes[k] = v
		}
	}

	if len(nsNodes) <= 0 {
		return nil, ErrNamespaceNotFound
	}

	return nsNodes, nil
}

func (nsm *NamespaceMgr) GetNamespaceNode(ns string) *NamespaceNode {
	nsm.mutex.RLock()
	v, _ := nsm.kvNodes[ns]
	nsm.mutex.RUnlock()
	if v != nil {
		if !v.IsReady() {
			return nil
		}
	}
	return v
}

func (nsm *NamespaceMgr) GetNamespaceNodeFromGID(gid uint64) *NamespaceNode {
	nsm.mutex.RLock()
	defer nsm.mutex.RUnlock()
	gn, ok := nsm.groups[gid]
	if !ok {
		nodeLog.Debugf("group name not found %v ", gid)
		return nil
	}
	kv, ok := nsm.kvNodes[gn]
	if !ok {
		nodeLog.Infof("kv namespace not found %v ", gn)
		return nil
	}

	if !kv.IsReady() {
		return nil
	}
	return kv
}

func (nsm *NamespaceMgr) GetWALDBStats(leaderOnly bool) map[string]map[string]interface{} {
	nsm.mutex.RLock()
	nsStats := make(map[string]map[string]interface{}, len(nsm.kvNodes))
	for k, n := range nsm.kvNodes {
		if !n.IsReady() {
			continue
		}
		if leaderOnly && !n.Node.IsLead() {
			continue
		}
		dbStats := n.Node.GetWALDBInternalStats()
		nsStats[k] = dbStats
	}
	nsm.mutex.RUnlock()
	return nsStats
}

func (nsm *NamespaceMgr) GetDBStats(leaderOnly bool) map[string]string {
	nsm.mutex.RLock()
	nsStats := make(map[string]string, len(nsm.kvNodes))
	for k, n := range nsm.kvNodes {
		if !n.IsReady() {
			continue
		}
		if leaderOnly && !n.Node.IsLead() {
			continue
		}
		dbStats := n.Node.GetDBInternalStats()
		nsStats[k] = dbStats
	}
	nsm.mutex.RUnlock()
	return nsStats
}

func (nsm *NamespaceMgr) GetLogSyncStatsInSyncer() ([]metric.LogSyncStats, []metric.LogSyncStats) {
	nsm.mutex.RLock()
	nsRecvStats := make([]metric.LogSyncStats, 0, len(nsm.kvNodes))
	nsSyncStats := make([]metric.LogSyncStats, 0, len(nsm.kvNodes))
	for k, n := range nsm.kvNodes {
		if !n.IsReady() {
			continue
		}
		recvStats, syncStats := n.Node.GetLogSyncStatsInSyncLearner()
		if recvStats == nil || syncStats == nil {
			continue
		}
		recvStats.Name = k
		syncStats.Name = k
		nsRecvStats = append(nsRecvStats, *recvStats)
		nsSyncStats = append(nsSyncStats, *syncStats)
	}
	nsm.mutex.RUnlock()
	return nsRecvStats, nsSyncStats
}

func (nsm *NamespaceMgr) GetLogSyncStats(leaderOnly bool, srcClusterName string) []metric.LogSyncStats {
	if srcClusterName == "" {
		return nil
	}
	nsm.mutex.RLock()
	nsStats := make([]metric.LogSyncStats, 0, len(nsm.kvNodes))
	for k, n := range nsm.kvNodes {
		if !n.IsReady() {
			continue
		}
		if leaderOnly && !n.Node.IsLead() {
			continue
		}
		term, index, ts := n.Node.GetRemoteClusterSyncedRaft(srcClusterName)
		if term == 0 && index == 0 {
			continue
		}
		var s metric.LogSyncStats
		s.Name = k
		s.IsLeader = n.Node.IsLead()
		s.Term = term
		s.Index = index
		s.Timestamp = ts
		nsStats = append(nsStats, s)
	}
	nsm.mutex.RUnlock()
	return nsStats
}

func (nsm *NamespaceMgr) GetStats(leaderOnly bool, table string, needTableDetail bool) []metric.NamespaceStats {
	nsm.mutex.RLock()
	nsStats := make([]metric.NamespaceStats, 0, len(nsm.kvNodes))
	for k, n := range nsm.kvNodes {
		if !n.IsReady() {
			continue
		}
		if leaderOnly && !n.Node.IsLead() {
			continue
		}
		ns := n.Node.GetStats(table, needTableDetail)
		ns.Name = k
		ns.EngType = n.conf.EngType
		ns.IsLeader = n.Node.IsLead()
		nsStats = append(nsStats, ns)
	}
	nsm.mutex.RUnlock()
	return nsStats
}

func (nsm *NamespaceMgr) getNsNodeList(ns string) []*NamespaceNode {
	nsm.mutex.RLock()
	nodeList := make([]*NamespaceNode, 0, len(nsm.kvNodes))
	for k, n := range nsm.kvNodes {
		baseName, _ := common.GetNamespaceAndPartition(k)
		if ns != "" && ns != baseName {
			continue
		}
		nodeList = append(nodeList, n)
	}
	nsm.mutex.RUnlock()
	return nodeList
}

func (nsm *NamespaceMgr) BackupDB(ns string, checkLast bool) {
	nodeList := nsm.getNsNodeList(ns)
	for _, n := range nodeList {
		if atomic.LoadInt32(&nsm.stopping) == 1 {
			return
		}
		if n.IsReady() {
			n.Node.BackupDB(checkLast)
		}
	}
}

func (nsm *NamespaceMgr) EnableTopn(ns string, on bool) {
	nodeList := nsm.getNsNodeList(ns)
	for _, n := range nodeList {
		if atomic.LoadInt32(&nsm.stopping) == 1 {
			return
		}
		if n.IsReady() {
			n.Node.sm.EnableTopn(on)
		}
	}
}

func (nsm *NamespaceMgr) ClearTopn(ns string) {
	nodeList := nsm.getNsNodeList(ns)
	for _, n := range nodeList {
		if atomic.LoadInt32(&nsm.stopping) == 1 {
			return
		}
		if n.IsReady() {
			n.Node.sm.ClearTopn()
		}
	}
}

func (nsm *NamespaceMgr) DisableOptimizeDB(disable bool) {
	nodeList := nsm.getNsNodeList("")
	for _, n := range nodeList {
		if atomic.LoadInt32(&nsm.stopping) == 1 {
			return
		}
		if n.IsReady() {
			n.Node.DisableOptimizeDB(disable)
		}
	}
}

func (nsm *NamespaceMgr) OptimizeDB(ns string, table string) {
	nodeList := nsm.getNsNodeList(ns)
	for _, n := range nodeList {
		if atomic.LoadInt32(&nsm.stopping) == 1 {
			return
		}
		if n.IsReady() {
			n.Node.OptimizeDB(table)
		}
	}
}

func (nsm *NamespaceMgr) OptimizeDBExpire(ns string) {
	nodeList := nsm.getNsNodeList(ns)
	for _, n := range nodeList {
		if atomic.LoadInt32(&nsm.stopping) == 1 {
			return
		}
		if n.IsReady() {
			n.Node.OptimizeDBExpire()
		}
	}
}

func (nsm *NamespaceMgr) OptimizeDBAnyRange(ns string, r CompactAPIRange) {
	nodeList := nsm.getNsNodeList(ns)
	for _, n := range nodeList {
		if atomic.LoadInt32(&nsm.stopping) == 1 {
			return
		}
		if n.IsReady() {
			n.Node.OptimizeDBAnyRange(r)
		}
	}
}

func (nsm *NamespaceMgr) DeleteRange(ns string, dtr DeleteTableRange) error {
	if ns == "" {
		return errors.New("namespace can not be empty")
	}
	nodeList := nsm.getNsNodeList(ns)
	for _, n := range nodeList {
		if atomic.LoadInt32(&nsm.stopping) == 1 {
			return common.ErrStopped
		}
		if n.IsReady() {
			err := n.Node.DeleteRange(dtr)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// this is used to clean some shared namespace data bewteen all partitions, only remove them until
// all the partitions are removed and the whole namespace is deleted from cluster
func (nsm *NamespaceMgr) CleanSharedNsFiles(baseNS string) error {
	nsm.mutex.Lock()
	defer nsm.mutex.Unlock()
	// check if all parts of this namespace is deleted, if so we should remove meta
	for fullName, _ := range nsm.kvNodes {
		n, _ := common.GetNamespaceAndPartition(fullName)
		if n == baseNS {
			err := fmt.Errorf("some node is still running while clean namespace files: %s, %s", baseNS, fullName)
			nodeLog.Info(err.Error())
			return err
		}
	}
	meta, ok := nsm.nsMetas[baseNS]
	if ok && meta.walEng != nil {
		nodeLog.Infof("all partitions of namespace %v stopped, removing shared data", baseNS)
		meta.walEng.CloseAll()
	}
	delete(nsm.nsMetas, baseNS)
	if nsm.machineConf.UseRocksWAL && baseNS != "" {
		// clean the rock wal files
		rsDir := path.Join(nsm.machineConf.DataRootDir, "rswal", baseNS)
		ts := strconv.Itoa(int(time.Now().UnixNano()))
		err := os.Rename(rsDir,
			rsDir+"-deleted-"+ts)
		if err != nil {
			nodeLog.Warningf("remove shared data failed: %s", err.Error())
			return err
		}
		nodeLog.Infof("remove shared wal data for namespace: %s", baseNS)
	}
	return nil
}

func (nsm *NamespaceMgr) onNamespaceStopped(gid uint64, ns string) func() {
	return func() {
		nsm.mutex.Lock()
		defer nsm.mutex.Unlock()
		_, ok := nsm.kvNodes[ns]
		if !ok {
			return
		}
		// note we should not delete any data here, since it is not known we just stopped or it is destroyed
		nodeLog.Infof("namespace stopped: %v-%v", ns, gid)
		nsm.kvNodes[ns] = nil
		delete(nsm.kvNodes, ns)
		delete(nsm.groups, gid)
		baseNS, _ := common.GetNamespaceAndPartition(ns)
		meta, ok := nsm.nsMetas[baseNS]
		if !ok {
			return
		}
		found := false
		// check if all parts of this namespace is deleted, if so we should remove meta
		for fullName, _ := range nsm.kvNodes {
			n, _ := common.GetNamespaceAndPartition(fullName)
			if n == baseNS {
				found = true
				break
			}
		}
		if !found {
			nodeLog.Infof("all partitions of namespace %v stopped, removing meta", baseNS)
			if meta.walEng != nil {
				meta.walEng.CloseAll()
			}
			delete(nsm.nsMetas, baseNS)
		}
	}
}

func (nsm *NamespaceMgr) processRaftTick() {
	ticker := time.NewTicker(time.Duration(nsm.machineConf.TickMs) * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			// send tick for all raft group
			nsm.mutex.RLock()
			nodes := make([]*KVNode, 0, len(nsm.kvNodes))
			for _, v := range nsm.kvNodes {
				if v.IsReady() {
					nodes = append(nodes, v.Node)
				}
			}
			nsm.mutex.RUnlock()
			for _, n := range nodes {
				n.Tick()
			}

		case <-nsm.stopC:
			return
		}
	}
}

func (nsm *NamespaceMgr) checkNamespaceRaftLeader() {
	ticker := time.NewTicker(time.Second * 15)
	defer ticker.Stop()
	leaderNodes := make([]*NamespaceNode, 0)
	// report to leader may failed, so we need retry and check period
	doCheck := func() {
		leaderNodes = leaderNodes[:0]
		nsm.mutex.RLock()
		for _, v := range nsm.kvNodes {
			if v.IsReady() {
				if v.Node.IsLead() {
					leaderNodes = append(leaderNodes, v)
				}
			}
		}
		nsm.mutex.RUnlock()
		for _, v := range leaderNodes {
			v.Node.ReportMeLeaderToCluster()
		}
	}

	for {
		select {
		case <-ticker.C:
			doCheck()
		case ns := <-nsm.newLeaderChan:
			nsm.mutex.RLock()
			v, ok := nsm.kvNodes[ns]
			nsm.mutex.RUnlock()
			if !ok {
				nodeLog.Infof("leader changed namespace not found: %v", ns)
			} else if v.IsReady() {
				v.Node.OnRaftLeaderChanged()
			}
		case <-nsm.stopC:
			return
		}
	}
}

func (nsm *NamespaceMgr) clearUnusedRaftPeer() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	// while close or remove raft node, we need check if any remote transport peer
	// should be closed.
	doCheck := func() {
		nsm.mutex.RLock()
		defer nsm.mutex.RUnlock()
		peers := nsm.raftTransport.GetAllPeers()
		currentNodeIDs := make(map[uint64]bool)
		for _, v := range nsm.kvNodes {
			mems := v.Node.GetMembers()
			for _, m := range mems {
				currentNodeIDs[m.NodeID] = true
			}
			// handle learners here
			mems = v.Node.GetLearners()
			for _, m := range mems {
				currentNodeIDs[m.NodeID] = true
			}
		}
		for _, p := range peers {
			if _, ok := currentNodeIDs[uint64(p)]; !ok {
				nodeLog.Infof("remove peer %v from transport since no any raft is related", p)
				nsm.raftTransport.RemovePeer(p)
			}
		}
	}

	for {
		select {
		case <-ticker.C:
			doCheck()
		case <-nsm.stopC:
			return
		}
	}
}

func (nsm *NamespaceMgr) HandleSlowLimiterSwitchChanged(v interface{}) {
	nodeLog.Infof("config for slow limiter changed to : %v", v)
	on, ok := v.(int)
	if !ok {
		return
	}
	nsm.mutex.RLock()
	for _, n := range nsm.kvNodes {
		if !n.IsReady() || n.Node == nil {
			continue
		}
		if on > 0 {
			n.Node.slowLimiter.TurnOn()
		} else {
			n.Node.slowLimiter.TurnOff()
		}
	}
	nsm.mutex.RUnlock()
}
