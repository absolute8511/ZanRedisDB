package cluster

import (
	"encoding/json"
	"errors"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coreos/etcd/client"
	"github.com/youzan/ZanRedisDB/common"
	"golang.org/x/net/context"
)

const (
	EVENT_WATCH_L_CREATE = iota
	EVENT_WATCH_L_DELETE
)

const (
	ETCD_TTL = 60
)

const (
	ROOT_DIR               = "ZanRedisDBMetaData"
	CLUSTER_META_INFO      = "ClusterMeta"
	NAMESPACE_DIR          = "Namespaces"
	NAMESPACE_META         = "NamespaceMeta"
	NAMESPACE_SCHEMA       = "NamespaceSchema"
	NAMESPACE_REPLICA_INFO = "ReplicaInfo"
	NAMESPACE_REAL_LEADER  = "RealLeader"
	DATA_NODE_DIR          = "DataNodes"
	PD_ROOT_DIR            = "PDInfo"
	PD_NODE_DIR            = "PDNodes"
	PD_LEADER_SESSION      = "PDLeaderSession"
)

const (
	ETCD_LOCK_NAMESPACE = "zanredisdb"
)

type MasterChanInfo struct {
	processStopCh chan bool
	stoppedCh     chan bool
}

func IsEtcdNotFile(err error) bool {
	return isEtcdErrorNum(err, client.ErrorCodeNotFile)
}

func IsEtcdNodeExist(err error) bool {
	return isEtcdErrorNum(err, client.ErrorCodeNodeExist)
}

func isEtcdErrorNum(err error, errorCode int) bool {
	if err != nil {
		if etcdError, ok := err.(client.Error); ok {
			return etcdError.Code == errorCode
		}
		// NOTE: There are other error types returned
	}
	return false
}

func exchangeNodeValue(c *EtcdClient, nodePath string, initValue string,
	valueChangeFn func(bool, string) (string, error)) error {
	rsp, err := c.Get(nodePath, false, false)
	isNew := false
	if err != nil {
		if client.IsKeyNotFound(err) {
			isNew = true
			rsp, err = c.Create(nodePath, initValue, 0)
			if err != nil {
				if !IsEtcdNotFile(err) {
					return err
				}
			}
		} else {
			return err
		}
	}
	var newValue string
	retry := 5
	for retry > 0 {
		retry--
		newValue, err = valueChangeFn(isNew, rsp.Node.Value)
		if err != nil {
			return err
		}
		isNew = false
		rsp, err = c.CompareAndSwap(nodePath, newValue, 0, "", rsp.Node.ModifiedIndex)
		if err != nil {
			time.Sleep(time.Millisecond * 10)
			rsp, err = c.Get(nodePath, false, false)
			if err != nil {
				return err
			}
		} else {
			return nil
		}
	}
	return err
}

type EtcdRegister struct {
	nsMutex sync.Mutex

	client               *EtcdClient
	clusterID            string
	namespaceRoot        string
	clusterPath          string
	pdNodeRootPath       string
	allNamespaceInfos    map[string]map[int]PartitionMetaInfo
	nsEpoch              EpochType
	ifNamespaceChanged   int32
	watchNamespaceStopCh chan struct{}
	nsChangedChan        chan struct{}
	triggerScanCh        chan struct{}
	wg                   sync.WaitGroup
}

func NewEtcdRegister(host string) (*EtcdRegister, error) {
	client, err := NewEClient(host)
	if err != nil {
		return nil, err
	}
	r := &EtcdRegister{
		allNamespaceInfos:    make(map[string]map[int]PartitionMetaInfo),
		watchNamespaceStopCh: make(chan struct{}),
		client:               client,
		ifNamespaceChanged:   1,
		nsChangedChan:        make(chan struct{}, 3),
		triggerScanCh:        make(chan struct{}, 3),
	}
	return r, nil
}

func (etcdReg *EtcdRegister) InitClusterID(id string) {
	etcdReg.clusterID = id
	etcdReg.namespaceRoot = etcdReg.getNamespaceRootPath()
	etcdReg.clusterPath = etcdReg.getClusterPath()
	etcdReg.pdNodeRootPath = etcdReg.getPDNodeRootPath()
}

func (etcdReg *EtcdRegister) Start() {
	etcdReg.watchNamespaceStopCh = make(chan struct{})
	etcdReg.wg.Add(2)
	go func() {
		defer etcdReg.wg.Done()
		etcdReg.watchNamespaces(etcdReg.watchNamespaceStopCh)
	}()
	go func() {
		defer etcdReg.wg.Done()
		etcdReg.refreshNamespaces(etcdReg.watchNamespaceStopCh)
	}()
}

func (etcdReg *EtcdRegister) Stop() {
	coordLog.Infof("stopping etcd register")
	defer coordLog.Infof("stopped etcd register")
	if etcdReg.watchNamespaceStopCh != nil {
		select {
		case <-etcdReg.watchNamespaceStopCh:
		default:
			close(etcdReg.watchNamespaceStopCh)
		}
	}
	etcdReg.wg.Wait()
}

func (etcdReg *EtcdRegister) GetAllPDNodes() ([]NodeInfo, error) {
	rsp, err := etcdReg.client.Get(etcdReg.pdNodeRootPath, false, false)
	if err != nil {
		if client.IsKeyNotFound(err) {
			return nil, ErrKeyNotFound
		}
		return nil, err
	}
	nodeList := make([]NodeInfo, 0)
	for _, node := range rsp.Node.Nodes {
		var nodeInfo NodeInfo
		if err = json.Unmarshal([]byte(node.Value), &nodeInfo); err != nil {
			continue
		}
		nodeList = append(nodeList, nodeInfo)
	}
	return nodeList, nil
}

func (etcdReg *EtcdRegister) GetAllNamespaces() (map[string]map[int]PartitionMetaInfo, EpochType, error) {
	if atomic.LoadInt32(&etcdReg.ifNamespaceChanged) == 1 {
		return etcdReg.scanNamespaces()
	}

	etcdReg.nsMutex.Lock()
	nsInfos := etcdReg.allNamespaceInfos
	nsEpoch := etcdReg.nsEpoch
	etcdReg.nsMutex.Unlock()
	return nsInfos, nsEpoch, nil
}

func (etcdReg *EtcdRegister) GetNamespaceSchemas(ns string) (map[string]SchemaInfo, error) {
	rsp, err := etcdReg.client.Get(etcdReg.getNamespaceSchemaPath(ns), false, true)
	if err != nil {
		if client.IsKeyNotFound(err) {
			return nil, ErrKeyNotFound
		}
		return nil, err
	}
	schemas := make(map[string]SchemaInfo)
	for _, node := range rsp.Node.Nodes {
		if node.Dir {
			continue
		}
		_, table := path.Split(node.Key)
		var sInfo SchemaInfo
		sInfo.Schema = []byte(node.Value)
		sInfo.Epoch = EpochType(node.ModifiedIndex)
		schemas[table] = sInfo
	}

	return schemas, nil
}

func (etcdReg *EtcdRegister) GetNamespacesNotifyChan() chan struct{} {
	return etcdReg.nsChangedChan
}

func (etcdReg *EtcdRegister) refreshNamespaces(stopC <-chan struct{}) {
	ticker := time.NewTicker(time.Second * 3)
	defer ticker.Stop()
	for {
		select {
		case <-stopC:
			return
		case <-etcdReg.triggerScanCh:
			if atomic.LoadInt32(&etcdReg.ifNamespaceChanged) == 1 {
				etcdReg.scanNamespaces()
			}
		case <-ticker.C:
			if atomic.LoadInt32(&etcdReg.ifNamespaceChanged) == 1 {
				etcdReg.scanNamespaces()
			}
		}
	}
}

func (etcdReg *EtcdRegister) watchNamespaces(stopC <-chan struct{}) {
	watcher := etcdReg.client.Watch(etcdReg.namespaceRoot, 0, true)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		select {
		case <-stopC:
			cancel()
		}
	}()
	for {
		_, err := watcher.Next(ctx)
		if err != nil {
			if err == context.Canceled {
				coordLog.Infof("watch key[%s] canceled.", etcdReg.namespaceRoot)
				return
			}
			atomic.StoreInt32(&etcdReg.ifNamespaceChanged, 1)
			coordLog.Errorf("watcher key[%s] error: %s", etcdReg.namespaceRoot, err.Error())
			if IsEtcdWatchExpired(err) {
				rsp, err := etcdReg.client.Get(etcdReg.namespaceRoot, false, true)
				if err != nil {
					coordLog.Errorf("rewatch and get key[%s] error: %s", etcdReg.namespaceRoot, err.Error())
					time.Sleep(time.Second)
					continue
				}
				coordLog.Errorf("watch expired key[%s] : %v", etcdReg.namespaceRoot, rsp)
				watcher = etcdReg.client.Watch(etcdReg.namespaceRoot, rsp.Index+1, true)
				// watch expired should be treated as changed of node
			} else {
				time.Sleep(5 * time.Second)
				continue
			}
		}
		coordLog.Debugf("namespace changed.")
		atomic.StoreInt32(&etcdReg.ifNamespaceChanged, 1)
		select {
		case etcdReg.triggerScanCh <- struct{}{}:
		default:
		}
		select {
		case etcdReg.nsChangedChan <- struct{}{}:
		default:
		}
	}
}

func (etcdReg *EtcdRegister) scanNamespaces() (map[string]map[int]PartitionMetaInfo, EpochType, error) {
	coordLog.Infof("refreshing namespaces")
	atomic.StoreInt32(&etcdReg.ifNamespaceChanged, 0)

	// since the scan is triggered by watch, we need get newest from quorum
	// to avoid get the old data from follower and no any more update event on leader.
	rsp, err := etcdReg.client.GetNewest(etcdReg.namespaceRoot, true, true)
	if err != nil {
		atomic.StoreInt32(&etcdReg.ifNamespaceChanged, 1)
		if client.IsKeyNotFound(err) {
			return nil, 0, ErrKeyNotFound
		}
		etcdReg.nsMutex.Lock()
		nsInfos := etcdReg.allNamespaceInfos
		nsEpoch := etcdReg.nsEpoch
		etcdReg.nsMutex.Unlock()
		coordLog.Infof("refreshing namespaces failed: %v, use old info instead", err)
		return nsInfos, nsEpoch, err
	}

	metaMap := make(map[string]NamespaceMetaInfo)
	replicasMap := make(map[string]map[string]PartitionReplicaInfo)
	leaderMap := make(map[string]map[string]RealLeader)
	maxEpoch := etcdReg.processNamespaceNode(rsp.Node.Nodes, metaMap, replicasMap, leaderMap)

	nsInfos := make(map[string]map[int]PartitionMetaInfo)
	if EpochType(rsp.Node.ModifiedIndex) > maxEpoch {
		maxEpoch = EpochType(rsp.Node.ModifiedIndex)
	}
	for k, v := range replicasMap {
		meta, ok := metaMap[k]
		if !ok {
			continue
		}
		partInfos, ok := nsInfos[k]
		if !ok {
			partInfos = make(map[int]PartitionMetaInfo, meta.PartitionNum)
			nsInfos[k] = partInfos
		}
		for k2, v2 := range v {
			partition, err := strconv.Atoi(k2)
			if err != nil {
				continue
			}
			if partition >= meta.PartitionNum {
				coordLog.Infof("invalid partition id : %v ", k2)
				continue
			}
			var info PartitionMetaInfo
			info.Name = k
			info.Partition = partition
			info.NamespaceMetaInfo = meta
			info.PartitionReplicaInfo = v2
			if leaders, ok := leaderMap[k]; ok {
				info.currentLeader = leaders[k2]
			}
			partInfos[partition] = info
		}
	}

	etcdReg.nsMutex.Lock()
	etcdReg.allNamespaceInfos = nsInfos
	if maxEpoch != etcdReg.nsEpoch {
		coordLog.Infof("ns epoch changed from %v to : %v ", etcdReg.nsEpoch, maxEpoch)
	}
	etcdReg.nsEpoch = maxEpoch
	etcdReg.nsMutex.Unlock()

	return nsInfos, maxEpoch, nil
}

func (etcdReg *EtcdRegister) processNamespaceNode(nodes client.Nodes,
	metaMap map[string]NamespaceMetaInfo,
	replicasMap map[string]map[string]PartitionReplicaInfo,
	leaderMap map[string]map[string]RealLeader) EpochType {
	maxEpoch := EpochType(0)
	for _, node := range nodes {
		if node.Nodes != nil {
			newEpoch := etcdReg.processNamespaceNode(node.Nodes, metaMap, replicasMap, leaderMap)
			if newEpoch > maxEpoch {
				maxEpoch = newEpoch
			}
		}
		if EpochType(node.ModifiedIndex) > maxEpoch {
			maxEpoch = EpochType(node.ModifiedIndex)
		}

		if node.Dir {
			continue
		}
		_, key := path.Split(node.Key)
		if key == NAMESPACE_REPLICA_INFO {
			var rInfo PartitionReplicaInfo
			if err := json.Unmarshal([]byte(node.Value), &rInfo); err != nil {
				coordLog.Infof("unmarshal replica info %v failed: %v", node.Value, err)
				continue
			}
			rInfo.epoch = EpochType(node.ModifiedIndex)
			keys := strings.Split(node.Key, "/")
			keyLen := len(keys)
			if keyLen < 3 {
				continue
			}
			nsName := keys[keyLen-3]
			partition := keys[keyLen-2]
			v, ok := replicasMap[nsName]
			if ok {
				v[partition] = rInfo
			} else {
				pMap := make(map[string]PartitionReplicaInfo)
				pMap[partition] = rInfo
				replicasMap[nsName] = pMap
			}
		} else if key == NAMESPACE_META {
			var mInfo NamespaceMetaInfo
			if err := json.Unmarshal([]byte(node.Value), &mInfo); err != nil {
				continue
			}
			keys := strings.Split(node.Key, "/")
			keyLen := len(keys)
			if keyLen < 2 {
				continue
			}
			mInfo.metaEpoch = EpochType(node.ModifiedIndex)
			nsName := keys[keyLen-2]
			metaMap[nsName] = mInfo
		} else if key == NAMESPACE_REAL_LEADER {
			var rInfo RealLeader
			if err := json.Unmarshal([]byte(node.Value), &rInfo); err != nil {
				continue
			}
			rInfo.epoch = EpochType(node.ModifiedIndex)
			keys := strings.Split(node.Key, "/")
			keyLen := len(keys)
			if keyLen < 3 {
				continue
			}
			nsName := keys[keyLen-3]
			partition := keys[keyLen-2]
			v, ok := leaderMap[nsName]
			if ok {
				v[partition] = rInfo
			} else {
				pMap := make(map[string]RealLeader)
				pMap[partition] = rInfo
				leaderMap[nsName] = pMap
			}
		}
	}
	return maxEpoch
}

func (etcdReg *EtcdRegister) GetNamespacePartInfo(ns string, partition int) (*PartitionMetaInfo, error) {
	etcdReg.nsMutex.Lock()
	defer etcdReg.nsMutex.Unlock()
	nsInfo, ok := etcdReg.allNamespaceInfos[ns]
	if !ok {
		return nil, ErrKeyNotFound
	}
	p, ok := nsInfo[partition]
	if !ok {
		return nil, ErrKeyNotFound
	}
	if p.Partition != partition {
		panic(p)
	}
	return p.GetCopy(), nil
}

func (etcdReg *EtcdRegister) GetRemoteNamespaceReplicaInfo(ns string, partition int) (*PartitionReplicaInfo, error) {
	rsp, err := etcdReg.client.Get(etcdReg.getNamespaceReplicaInfoPath(ns, partition), false, false)
	if err != nil {
		if client.IsKeyNotFound(err) {
			atomic.StoreInt32(&etcdReg.ifNamespaceChanged, 1)
			return nil, ErrKeyNotFound
		}
		return nil, err
	}
	var rInfo PartitionReplicaInfo
	if err = json.Unmarshal([]byte(rsp.Node.Value), &rInfo); err != nil {
		return nil, err
	}
	rInfo.epoch = EpochType(rsp.Node.ModifiedIndex)
	return &rInfo, nil
}

func (etcdReg *EtcdRegister) GetNamespaceTableSchema(ns string, table string) (*SchemaInfo, error) {
	rsp, err := etcdReg.client.Get(etcdReg.getNamespaceTableSchemaPath(ns, table), false, false)
	if err != nil {
		if client.IsKeyNotFound(err) {
			return nil, ErrKeyNotFound
		}
		return nil, err
	}
	var info SchemaInfo
	info.Schema = []byte(rsp.Node.Value)
	info.Epoch = EpochType(rsp.Node.ModifiedIndex)
	return &info, nil
}

func (etcdReg *EtcdRegister) GetNamespaceInfo(ns string) ([]PartitionMetaInfo, error) {
	etcdReg.nsMutex.Lock()
	defer etcdReg.nsMutex.Unlock()
	nsInfo, ok := etcdReg.allNamespaceInfos[ns]
	if !ok {
		return nil, ErrKeyNotFound
	}
	parts := make([]PartitionMetaInfo, 0, len(nsInfo))
	for _, v := range nsInfo {
		parts = append(parts, *v.GetCopy())
	}
	return parts, nil
}

func (etcdReg *EtcdRegister) GetNamespaceMetaInfo(ns string) (NamespaceMetaInfo, error) {
	etcdReg.nsMutex.Lock()
	parts, ok := etcdReg.allNamespaceInfos[ns]
	etcdReg.nsMutex.Unlock()
	var meta NamespaceMetaInfo
	if !ok || len(parts) == 0 {
		rsp, err := etcdReg.client.Get(etcdReg.getNamespaceMetaPath(ns), false, false)
		if err != nil {
			if client.IsKeyNotFound(err) {
				return meta, ErrKeyNotFound
			}
			return meta, err
		}
		err = json.Unmarshal([]byte(rsp.Node.Value), &meta)
		if err != nil {
			return meta, err
		}
		meta.metaEpoch = EpochType(rsp.Node.ModifiedIndex)
		return meta, nil
	}
	return parts[0].NamespaceMetaInfo, nil
}

func (etcdReg *EtcdRegister) getClusterPath() string {
	return path.Join("/", ROOT_DIR, etcdReg.clusterID)
}

func (etcdReg *EtcdRegister) getClusterMetaPath() string {
	return path.Join(etcdReg.getClusterPath(), CLUSTER_META_INFO)
}

func (etcdReg *EtcdRegister) getPDNodePath(value *NodeInfo) string {
	return path.Join(etcdReg.getPDNodeRootPath(), "Node-"+value.ID)
}

func (etcdReg *EtcdRegister) getPDNodeRootPath() string {
	return path.Join(etcdReg.getClusterPath(), PD_ROOT_DIR, PD_NODE_DIR)
}

func (etcdReg *EtcdRegister) getPDLeaderPath() string {
	return path.Join(etcdReg.getClusterPath(), PD_ROOT_DIR, PD_LEADER_SESSION)
}

func (etcdReg *EtcdRegister) getDataNodeRootPath() string {
	return path.Join(etcdReg.getClusterPath(), DATA_NODE_DIR)
}

func (etcdReg *EtcdRegister) getNamespaceRootPath() string {
	return path.Join(etcdReg.getClusterPath(), NAMESPACE_DIR)
}

func (etcdReg *EtcdRegister) getNamespacePath(ns string) string {
	return path.Join(etcdReg.namespaceRoot, ns)
}

func (etcdReg *EtcdRegister) getNamespaceMetaPath(ns string) string {
	return path.Join(etcdReg.getNamespacePath(ns), NAMESPACE_META)
}

func (etcdReg *EtcdRegister) getNamespaceSchemaPath(ns string) string {
	return path.Join(etcdReg.getNamespacePath(ns), NAMESPACE_SCHEMA)
}

func (etcdReg *EtcdRegister) getNamespaceTableSchemaPath(ns string, table string) string {
	return path.Join(etcdReg.getNamespaceSchemaPath(ns), table)
}

func (etcdReg *EtcdRegister) getNamespacePartitionPath(ns string, partition int) string {
	return path.Join(etcdReg.getNamespacePath(ns), strconv.Itoa(partition))
}

func (etcdReg *EtcdRegister) getNamespaceReplicaInfoPath(ns string, partition int) string {
	return path.Join(etcdReg.getNamespacePartitionPath(ns, partition), NAMESPACE_REPLICA_INFO)
}

// placement driver register
type PDEtcdRegister struct {
	*EtcdRegister

	leaderSessionPath string
	leaderStr         string
	nodeInfo          *NodeInfo
	nodeKey           string
	nodeValue         string

	refreshStopCh chan bool
}

func NewPDEtcdRegister(host string) (*PDEtcdRegister, error) {
	reg, err := NewEtcdRegister(host)
	if err != nil {
		return nil, err
	}
	return &PDEtcdRegister{
		EtcdRegister:  reg,
		refreshStopCh: make(chan bool, 1),
	}, nil
}

func (etcdReg *PDEtcdRegister) Register(value *NodeInfo) error {
	etcdReg.leaderSessionPath = etcdReg.getPDLeaderPath()
	etcdReg.nodeInfo = value
	valueB, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if etcdReg.refreshStopCh != nil {
		close(etcdReg.refreshStopCh)
	}

	etcdReg.leaderStr = string(valueB)
	etcdReg.nodeKey = etcdReg.getPDNodePath(value)
	etcdReg.nodeValue = string(valueB)
	_, err = etcdReg.client.Set(etcdReg.nodeKey, etcdReg.nodeValue, ETCD_TTL)
	if err != nil {
		return err
	}
	etcdReg.refreshStopCh = make(chan bool)
	// start to refresh
	go etcdReg.refresh(etcdReg.refreshStopCh)

	return nil
}

func (etcdReg *PDEtcdRegister) refresh(stopC <-chan bool) {
	for {
		select {
		case <-stopC:
			return
		case <-time.After(time.Second * time.Duration(ETCD_TTL/10)):
			_, err := etcdReg.client.SetWithTTL(etcdReg.nodeKey, ETCD_TTL)
			if err != nil {
				coordLog.Errorf("update error: %s", err.Error())
				_, err := etcdReg.client.Set(etcdReg.nodeKey, etcdReg.nodeValue, ETCD_TTL)
				if err != nil {
					coordLog.Errorf("set key error: %s", err.Error())
				}
			}
		}
	}
}

func (etcdReg *PDEtcdRegister) Unregister(value *NodeInfo) error {
	// stop to refresh
	if etcdReg.refreshStopCh != nil {
		close(etcdReg.refreshStopCh)
		etcdReg.refreshStopCh = nil
	}

	_, err := etcdReg.client.Delete(etcdReg.getPDNodePath(value), false)
	if err != nil {
		coordLog.Warningf("cluser[%s] node[%v] unregister failed: %v", etcdReg.clusterID, value, err)
		return err
	}

	return nil
}

func (etcdReg *PDEtcdRegister) PrepareNamespaceMinGID() (int64, error) {
	var clusterMeta ClusterMetaInfo
	initValue, _ := json.Marshal(clusterMeta)
	err := exchangeNodeValue(
		etcdReg.client,
		etcdReg.getClusterMetaPath(),
		string(initValue),
		func(isNew bool, oldValue string) (string, error) {
			if !isNew && oldValue != "" {
				err := json.Unmarshal([]byte(oldValue), &clusterMeta)
				if err != nil {
					coordLog.Infof("cluster meta: %v", string(oldValue))
					return "", err
				}
			}
			clusterMeta.MaxGID += 10000
			newValue, err := json.Marshal(clusterMeta)
			coordLog.Infof("updating cluster meta: %v", clusterMeta)
			return string(newValue), err
		})

	return clusterMeta.MaxGID, err
}

func (etcdReg *PDEtcdRegister) GetClusterMetaInfo() (ClusterMetaInfo, error) {
	var clusterMeta ClusterMetaInfo
	rsp, err := etcdReg.client.Get(etcdReg.getClusterMetaPath(), false, false)
	if err != nil {
		if client.IsKeyNotFound(err) {
			return clusterMeta, ErrKeyNotFound
		}
		return clusterMeta, err
	}
	err = json.Unmarshal([]byte(rsp.Node.Value), &clusterMeta)
	return clusterMeta, err
}

func (etcdReg *PDEtcdRegister) GetClusterEpoch() (EpochType, error) {
	rsp, err := etcdReg.client.Get(etcdReg.clusterPath, false, false)
	if err != nil {
		if client.IsKeyNotFound(err) {
			return 0, ErrKeyNotFound
		}
		return 0, err
	}

	return EpochType(rsp.Node.ModifiedIndex), nil
}

func (etcdReg *PDEtcdRegister) AcquireAndWatchLeader(leader chan *NodeInfo, stop chan struct{}) {
	master := NewMaster(etcdReg.client, etcdReg.leaderSessionPath, etcdReg.leaderStr, ETCD_TTL)
	go etcdReg.processMasterEvents(master, leader, stop)
	master.Start()
}

func (etcdReg *PDEtcdRegister) processMasterEvents(master Master, leader chan *NodeInfo, stop chan struct{}) {
	for {
		select {
		case e := <-master.GetEventsChan():
			if e.Type == MASTER_ADD || e.Type == MASTER_MODIFY {
				// Acquired the lock || lock change.
				var node NodeInfo
				if err := json.Unmarshal([]byte(e.Master), &node); err != nil {
					leader <- &node
					continue
				}
				coordLog.Infof("master event type[%d] Node[%v].", e.Type, node)
				leader <- &node
			} else if e.Type == MASTER_DELETE {
				coordLog.Infof("master event delete.")
				// Lost the lock.
				var node NodeInfo
				leader <- &node
			} else {
				// TODO: lock error.
				coordLog.Infof("unexpected event: %v", e)
			}
		case <-stop:
			master.Stop()
			close(leader)
			return
		}
	}
}

func (etcdReg *PDEtcdRegister) CheckIfLeader() bool {
	rsp, err := etcdReg.client.Get(etcdReg.leaderSessionPath, false, false)
	if err != nil {
		return false
	}
	if rsp.Node.Value == etcdReg.leaderStr {
		return true
	}
	return false
}

func (etcdReg *PDEtcdRegister) GetDataNodes() ([]NodeInfo, error) {
	n, _, err := etcdReg.getDataNodes(false)
	return n, err
}

func (etcdReg *PDEtcdRegister) WatchDataNodes(dataNodesChan chan []NodeInfo, stop chan struct{}) {
	dataNodes, nIndex, err := etcdReg.getDataNodes(false)
	if err == nil {
		select {
		case dataNodesChan <- dataNodes:
		case <-stop:
			close(dataNodesChan)
			return
		}
	}

	key := etcdReg.getDataNodeRootPath()
	watcher := etcdReg.client.Watch(key, nIndex, true)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		select {
		case <-stop:
			cancel()
		}
	}()
	for {
		rsp, err := watcher.Next(ctx)
		if err != nil {
			if err == context.Canceled {
				coordLog.Infof("watch key[%s] canceled.", key)
				close(dataNodesChan)
				return
			}
			coordLog.Errorf("watcher key[%s] error: %s", key, err.Error())
			//rewatch
			if IsEtcdWatchExpired(err) {
				rsp, err = etcdReg.client.Get(key, false, true)
				if err != nil {
					coordLog.Errorf("rewatch and get key[%s] error: %s", key, err.Error())
					time.Sleep(time.Second)
					continue
				}
				coordLog.Errorf("watch expired key[%s] : %v", key, rsp)
				watcher = etcdReg.client.Watch(key, rsp.Index+1, true)
				// should get the nodes to notify watcher since last watch is expired
			} else {
				time.Sleep(5 * time.Second)
				continue
			}
		}
		// must get the newest data
		// otherwise, the get may get the old data from another follower
		dataNodes, _, err := etcdReg.getDataNodes(true)
		if err != nil {
			coordLog.Errorf("key[%s] getNodes error: %s", key, err.Error())
			continue
		}
		select {
		case dataNodesChan <- dataNodes:
		case <-stop:
			close(dataNodesChan)
			return
		}
	}
}

func (etcdReg *PDEtcdRegister) getDataNodes(upToDate bool) ([]NodeInfo, uint64, error) {
	var rsp *client.Response
	var err error
	if upToDate {
		rsp, err = etcdReg.client.GetNewest(etcdReg.getDataNodeRootPath(), false, false)
	} else {
		rsp, err = etcdReg.client.Get(etcdReg.getDataNodeRootPath(), false, false)
	}
	if err != nil {
		if client.IsKeyNotFound(err) {
			return nil, 0, ErrKeyNotFound
		}
		return nil, 0, err
	}
	dataNodes := make([]NodeInfo, 0)
	for _, node := range rsp.Node.Nodes {
		if node.Dir {
			continue
		}
		var nodeInfo NodeInfo
		err := json.Unmarshal([]byte(node.Value), &nodeInfo)
		if err != nil {
			continue
		}
		dataNodes = append(dataNodes, nodeInfo)
	}
	return dataNodes, rsp.Index, nil
}

func (etcdReg *PDEtcdRegister) CreateNamespacePartition(ns string, partition int) error {
	_, err := etcdReg.client.CreateDir(etcdReg.getNamespacePartitionPath(ns, partition), 0)
	if err != nil {
		if IsEtcdNotFile(err) {
			return ErrKeyAlreadyExist
		}
		return err
	}
	return nil
}

func (etcdReg *PDEtcdRegister) CreateNamespace(ns string, meta *NamespaceMetaInfo) error {
	if meta.MinGID <= 0 {
		return errors.New("namespace MinGID is invalid")
	}
	metaValue, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	rsp, err := etcdReg.client.Create(etcdReg.getNamespaceMetaPath(ns), string(metaValue), 0)
	if err != nil {
		if IsEtcdNodeExist(err) {
			return ErrKeyAlreadyExist
		}
		return err
	}

	meta.metaEpoch = EpochType(rsp.Node.ModifiedIndex)
	return nil
}

func (etcdReg *PDEtcdRegister) IsExistNamespace(ns string) (bool, error) {
	_, err := etcdReg.client.Get(etcdReg.getNamespacePath(ns), false, false)
	if err != nil {
		if client.IsKeyNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (etcdReg *PDEtcdRegister) IsExistNamespacePartition(ns string, partitionNum int) (bool, error) {
	_, err := etcdReg.client.Get(etcdReg.getNamespacePartitionPath(ns, partitionNum), false, false)
	if err != nil {
		if client.IsKeyNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (etcdReg *PDEtcdRegister) UpdateNamespaceMetaInfo(ns string, meta *NamespaceMetaInfo, oldGen EpochType) error {
	value, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	coordLog.Infof("Update meta info: %s %s %d", ns, string(value), oldGen)

	etcdReg.nsMutex.Lock()
	defer etcdReg.nsMutex.Unlock()
	atomic.StoreInt32(&etcdReg.ifNamespaceChanged, 1)
	rsp, err := etcdReg.client.CompareAndSwap(etcdReg.getNamespaceMetaPath(ns), string(value), 0, "", uint64(oldGen))
	if err != nil {
		return err
	}
	err = json.Unmarshal([]byte(rsp.Node.Value), &meta)
	if err != nil {
		coordLog.Errorf("unmarshal meta info failed: %v, %v", err, rsp.Node.Value)
		return err
	}
	meta.metaEpoch = EpochType(rsp.Node.ModifiedIndex)

	return nil
}

func (etcdReg *PDEtcdRegister) DeleteWholeNamespace(ns string) error {
	etcdReg.nsMutex.Lock()
	atomic.StoreInt32(&etcdReg.ifNamespaceChanged, 1)
	rsp, err := etcdReg.client.Delete(etcdReg.getNamespacePath(ns), true)
	coordLog.Infof("delete whole topic: %v, %v, %v", ns, err, rsp)
	etcdReg.nsMutex.Unlock()
	return err
}

func (etcdReg *PDEtcdRegister) DeleteNamespacePart(ns string, partition int) error {
	_, err := etcdReg.client.Delete(etcdReg.getNamespacePartitionPath(ns, partition), true)
	if err != nil {
		if !client.IsKeyNotFound(err) {
			return err
		}
	}
	atomic.StoreInt32(&etcdReg.ifNamespaceChanged, 1)
	return nil
}

func (etcdReg *PDEtcdRegister) UpdateNamespacePartReplicaInfo(ns string, partition int,
	replicaInfo *PartitionReplicaInfo, oldGen EpochType) error {
	value, err := json.Marshal(replicaInfo)
	if err != nil {
		return err
	}
	coordLog.Infof("Update info: %s %d %s %d", ns, partition, string(value), oldGen)
	if oldGen == 0 {
		rsp, err := etcdReg.client.Create(etcdReg.getNamespaceReplicaInfoPath(ns, partition), string(value), 0)
		if err != nil {
			return err
		}
		replicaInfo.epoch = EpochType(rsp.Node.ModifiedIndex)
		atomic.StoreInt32(&etcdReg.ifNamespaceChanged, 1)
		return nil
	}
	rsp, err := etcdReg.client.CompareAndSwap(etcdReg.getNamespaceReplicaInfoPath(ns, partition), string(value), 0, "", uint64(oldGen))
	if err != nil {
		return err
	}
	replicaInfo.epoch = EpochType(rsp.Node.ModifiedIndex)
	atomic.StoreInt32(&etcdReg.ifNamespaceChanged, 1)
	return nil
}

func (etcdReg *PDEtcdRegister) UpdateNamespaceSchema(ns string, table string, schema *SchemaInfo) error {
	if schema.Epoch == 0 {
		rsp, err := etcdReg.client.Create(etcdReg.getNamespaceTableSchemaPath(ns, table), string(schema.Schema), 0)
		if err != nil {
			return err
		}
		schema.Epoch = EpochType(rsp.Node.ModifiedIndex)
		atomic.StoreInt32(&etcdReg.ifNamespaceChanged, 1)
		return nil
	}

	rsp, err := etcdReg.client.CompareAndSwap(etcdReg.getNamespaceTableSchemaPath(ns, table), string(schema.Schema),
		0, "", uint64(schema.Epoch))
	if err != nil {
		return err
	}
	schema.Epoch = EpochType(rsp.Node.ModifiedIndex)
	atomic.StoreInt32(&etcdReg.ifNamespaceChanged, 1)
	return nil
}

type DNEtcdRegister struct {
	*EtcdRegister
	sync.Mutex

	nodeKey       string
	nodeValue     string
	refreshStopCh chan bool
}

func NewDNEtcdRegister(host string) (*DNEtcdRegister, error) {
	reg, err := NewEtcdRegister(host)
	if err != nil {
		return nil, err
	}
	return &DNEtcdRegister{
		EtcdRegister: reg,
	}, nil
}

func (etcdReg *DNEtcdRegister) Register(nodeData *NodeInfo) error {
	if nodeData.LearnerRole != "" &&
		!common.IsRoleLogSyncer(nodeData.LearnerRole) &&
		nodeData.LearnerRole != common.LearnerRoleSearcher {
		return ErrLearnerRoleUnsupported
	}
	value, err := json.Marshal(nodeData)
	if err != nil {
		return err
	}
	etcdReg.nodeKey = etcdReg.getDataNodePath(nodeData)
	rsp, err := etcdReg.client.Get(etcdReg.nodeKey, false, false)
	if err != nil {
		if !client.IsKeyNotFound(err) {
			return err
		}
	} else {
		var node NodeInfo
		err = json.Unmarshal([]byte(rsp.Node.Value), &node)
		if err != nil {
			return err
		}
		if node.LearnerRole != nodeData.LearnerRole {
			coordLog.Warningf("node learner role should never be changed: %v (old %v)", nodeData.LearnerRole, node.LearnerRole)
			return ErrLearnerRoleInvalidChanged
		}
	}
	if etcdReg.refreshStopCh != nil {
		close(etcdReg.refreshStopCh)
	}

	etcdReg.nodeValue = string(value)
	_, err = etcdReg.client.Set(etcdReg.nodeKey, etcdReg.nodeValue, ETCD_TTL)
	if err != nil {
		return err
	}
	coordLog.Infof("registered new node: %v", nodeData)
	etcdReg.refreshStopCh = make(chan bool)
	// start refresh node
	go etcdReg.refresh(etcdReg.refreshStopCh)

	return nil
}

func (etcdReg *DNEtcdRegister) refresh(stopChan chan bool) {
	for {
		select {
		case <-stopChan:
			return
		case <-time.After(time.Second * time.Duration(ETCD_TTL/10)):
			_, err := etcdReg.client.SetWithTTL(etcdReg.nodeKey, ETCD_TTL)
			if err != nil {
				coordLog.Errorf("update error: %s", err.Error())
				_, err := etcdReg.client.Set(etcdReg.nodeKey, etcdReg.nodeValue, ETCD_TTL)
				if err != nil {
					coordLog.Errorf("set key error: %s", err.Error())
				} else {
					coordLog.Infof("refresh registered new node: %v", etcdReg.nodeValue)
				}
			}
		}
	}
}

func (etcdReg *DNEtcdRegister) Unregister(nodeData *NodeInfo) error {
	etcdReg.Lock()
	defer etcdReg.Unlock()

	// stop refresh
	if etcdReg.refreshStopCh != nil {
		close(etcdReg.refreshStopCh)
		etcdReg.refreshStopCh = nil
	}

	_, err := etcdReg.client.Delete(etcdReg.getDataNodePath(nodeData), false)
	if err != nil {
		coordLog.Warningf("cluser[%s] node[%v] unregister failed: %v", etcdReg.clusterID, nodeData, err)
		return err
	}

	coordLog.Infof("cluser[%s] node[%v] unregistered", etcdReg.clusterID, nodeData)
	return nil
}

func (etcdReg *DNEtcdRegister) GetNamespaceLeader(ns string, partition int) (string, EpochType, error) {
	etcdReg.nsMutex.Lock()
	defer etcdReg.nsMutex.Unlock()
	nsInfo, ok := etcdReg.allNamespaceInfos[ns]
	if !ok {
		return "", 0, ErrKeyNotFound
	}
	p, ok := nsInfo[partition]
	if !ok {
		return "", 0, ErrKeyNotFound
	}
	return p.GetRealLeader(), p.currentLeader.epoch, nil
}

func (etcdReg *DNEtcdRegister) UpdateNamespaceLeader(ns string, partition int, rl RealLeader, oldGen EpochType) (EpochType, error) {
	value, err := json.Marshal(rl)
	if err != nil {
		return oldGen, err
	}
	if oldGen == 0 {
		rsp, err := etcdReg.client.Create(etcdReg.getNamespaceLeaderPath(ns, partition), string(value), 0)
		if err != nil {
			return 0, err
		}
		rl.epoch = EpochType(rsp.Node.ModifiedIndex)
		return rl.epoch, nil
	}
	rsp, err := etcdReg.client.CompareAndSwap(etcdReg.getNamespaceLeaderPath(ns, partition), string(value), 0, "", uint64(oldGen))
	if err != nil {
		return 0, err
	}
	rl.epoch = EpochType(rsp.Node.ModifiedIndex)
	return rl.epoch, nil
}

func (etcdReg *DNEtcdRegister) GetNodeInfo(nid string) (NodeInfo, error) {
	var node NodeInfo
	rsp, err := etcdReg.client.Get(etcdReg.getDataNodePathFromID(nid), false, false)
	if err != nil {
		if client.IsKeyNotFound(err) {
			return node, ErrKeyNotFound
		}
		return node, err
	}
	err = json.Unmarshal([]byte(rsp.Node.Value), &node)
	if err != nil {
		return node, err
	}
	return node, nil
}

func (etcdReg *DNEtcdRegister) NewRegisterNodeID() (uint64, error) {
	var clusterMeta ClusterMetaInfo
	initValue, _ := json.Marshal(clusterMeta)
	exchangeErr := exchangeNodeValue(etcdReg.client, etcdReg.getClusterMetaPath(), string(initValue), func(isNew bool, oldValue string) (string, error) {
		if !isNew && oldValue != "" {
			err := json.Unmarshal([]byte(oldValue), &clusterMeta)
			if err != nil {
				return "", err
			}
		}
		clusterMeta.MaxRegID += 1
		newValue, err := json.Marshal(clusterMeta)
		return string(newValue), err
	})
	return clusterMeta.MaxRegID, exchangeErr
}

func (etcdReg *DNEtcdRegister) WatchPDLeader(leader chan *NodeInfo, stop chan struct{}) error {
	key := etcdReg.getPDLeaderPath()

	rsp, err := etcdReg.client.Get(key, false, false)
	if err == nil {
		coordLog.Infof("key: %s value: %s", rsp.Node.Key, rsp.Node.Value)
		var node NodeInfo
		err = json.Unmarshal([]byte(rsp.Node.Value), &node)
		if err == nil {
			select {
			case leader <- &node:
			case <-stop:
				close(leader)
				return nil
			}
		}
	} else {
		coordLog.Errorf("get error: %s", err.Error())
	}

	watcher := etcdReg.client.Watch(key, 0, true)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		select {
		case <-stop:
			cancel()
		}
	}()
	isMissing := true
	for {
		rsp, err = watcher.Next(ctx)
		if err != nil {
			if err == context.Canceled {
				coordLog.Infof("watch key[%s] canceled.", key)
				close(leader)
				return nil
			}
			coordLog.Errorf("watcher key[%s] error: %s", key, err.Error())
			//rewatch
			if IsEtcdWatchExpired(err) {
				isMissing = true
				rsp, err = etcdReg.client.Get(key, false, true)
				if err != nil {
					coordLog.Errorf("rewatch and get key[%s] error: %s", key, err.Error())
					time.Sleep(time.Second)
					continue
				}
				coordLog.Errorf("watch expired key[%s] : %s", key, rsp.Node.String())
				watcher = etcdReg.client.Watch(key, rsp.Index+1, true)
			} else {
				time.Sleep(5 * time.Second)
				continue
			}
		}
		if rsp == nil {
			continue
		}
		var node NodeInfo
		if rsp.Action == "expire" || rsp.Action == "delete" {
			coordLog.Infof("key[%s] action[%s]", key, rsp.Action)
			isMissing = true
		} else if rsp.Action == "create" || rsp.Action == "update" || rsp.Action == "set" {
			err := json.Unmarshal([]byte(rsp.Node.Value), &node)
			if err != nil {
				continue
			}
			if node.ID != "" {
				isMissing = false
			}
		} else {
			if isMissing {
				coordLog.Infof("key[%s] new data : %s", key, rsp.Node.String())
				err := json.Unmarshal([]byte(rsp.Node.Value), &node)
				if err != nil {
					continue
				}
				if node.ID != "" {
					isMissing = false
				}
			} else {
				continue
			}
		}
		select {
		case leader <- &node:
		case <-stop:
			close(leader)
			return nil
		}
	}
}

func (etcdReg *DNEtcdRegister) getDataNodePathFromID(nid string) string {
	return path.Join(etcdReg.getClusterPath(), DATA_NODE_DIR, "Node-"+nid)
}

func (etcdReg *DNEtcdRegister) getDataNodePath(nodeData *NodeInfo) string {
	return path.Join(etcdReg.getClusterPath(), DATA_NODE_DIR, "Node-"+nodeData.ID)
}

func (etcdReg *DNEtcdRegister) getNamespaceLeaderPath(ns string, partition int) string {
	return path.Join(etcdReg.getNamespacePartitionPath(ns, partition), NAMESPACE_REAL_LEADER)
}
