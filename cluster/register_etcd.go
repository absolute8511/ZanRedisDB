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
	etcdlock "github.com/reechou/xlock2"
	"golang.org/x/net/context"
)

const (
	EVENT_WATCH_L_CREATE = iota
	EVENT_WATCH_L_DELETE
)

const (
	ETCD_TTL = 15
)

const (
	ROOT_DIR               = "ZanRedisDBMetaData"
	CLUSTER_META_INFO      = "ClusterMeta"
	NAMESPACE_DIR          = "Namespaces"
	NAMESPACE_META         = "NamespaceMeta"
	NAMESPACE_REPLICA_INFO = "ReplicaInfo"
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

func exchangeNodeValue(c *etcdlock.EtcdClient, nodePath string, initValue string,
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

	client               *etcdlock.EtcdClient
	clusterID            string
	namespaceRoot        string
	clusterPath          string
	pdNodeRootPath       string
	allNamespaceInfos    map[string]map[int]PartitionMetaInfo
	nsEpoch              EpochType
	namespaceMetaMap     map[string]NamespaceMetaInfo
	ifNamespaceChanged   int32
	watchNamespaceStopCh chan bool
	nsChangedChan        chan struct{}
}

func NewEtcdRegister(host string) *EtcdRegister {
	client := etcdlock.NewEClient(host)
	r := &EtcdRegister{
		allNamespaceInfos:    make(map[string]map[int]PartitionMetaInfo),
		namespaceMetaMap:     make(map[string]NamespaceMetaInfo),
		watchNamespaceStopCh: make(chan bool),
		client:               client,
		ifNamespaceChanged:   1,
		nsChangedChan:        make(chan struct{}, 3),
	}
	return r
}

func (self *EtcdRegister) InitClusterID(id string) {
	self.clusterID = id
	self.namespaceRoot = self.getNamespaceRootPath()
	self.clusterPath = self.getClusterPath()
	self.pdNodeRootPath = self.getPDNodeRootPath()
	go self.watchNamespaces()
}

func (self *EtcdRegister) updateNamespaceMeta(ns string, meta NamespaceMetaInfo) {
	self.nsMutex.Lock()
	self.namespaceMetaMap[ns] = meta
	self.nsMutex.Unlock()
}

func (self *EtcdRegister) GetAllPDNodes() ([]NodeInfo, error) {
	rsp, err := self.client.Get(self.pdNodeRootPath, false, false)
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

func (self *EtcdRegister) GetAllNamespaces() (map[string]map[int]PartitionMetaInfo, EpochType, error) {
	if atomic.LoadInt32(&self.ifNamespaceChanged) == 1 {
		return self.scanNamespaces()
	}

	self.nsMutex.Lock()
	nsInfos := self.allNamespaceInfos
	nsEpoch := self.nsEpoch
	self.nsMutex.Unlock()
	return nsInfos, nsEpoch, nil
}

func (self *EtcdRegister) GetNamespacesNotifyChan() chan struct{} {
	return self.nsChangedChan
}

func (self *EtcdRegister) watchNamespaces() {
	watcher := self.client.Watch(self.namespaceRoot, 0, true)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		select {
		case <-self.watchNamespaceStopCh:
			cancel()
		}
	}()
	for {
		_, err := watcher.Next(ctx)
		if err != nil {
			if err == context.Canceled {
				coordLog.Infof("watch key[%s] canceled.", self.namespaceRoot)
				return
			} else {
				coordLog.Errorf("watcher key[%s] error: %s", self.namespaceRoot, err.Error())
				if etcdlock.IsEtcdWatchExpired(err) {
					rsp, err := self.client.Get(self.namespaceRoot, false, true)
					if err != nil {
						coordLog.Errorf("rewatch and get key[%s] error: %s", self.namespaceRoot, err.Error())
						continue
					}
					watcher = self.client.Watch(self.namespaceRoot, rsp.Index+1, true)
					// watch expired should be treated as changed of node
				} else {
					time.Sleep(5 * time.Second)
					continue
				}
			}
		}
		coordLog.Debugf("namespace changed.")
		atomic.StoreInt32(&self.ifNamespaceChanged, 1)
		select {
		case self.nsChangedChan <- struct{}{}:
		default:
		}
	}
}

func (self *EtcdRegister) scanNamespaces() (map[string]map[int]PartitionMetaInfo, EpochType, error) {
	rsp, err := self.client.Get(self.namespaceRoot, true, true)
	if err != nil {
		atomic.StoreInt32(&self.ifNamespaceChanged, 1)
		if client.IsKeyNotFound(err) {
			return nil, 0, ErrKeyNotFound
		}
		return nil, 0, err
	}
	atomic.StoreInt32(&self.ifNamespaceChanged, 0)

	metaMap := make(map[string]NamespaceMetaInfo)
	replicasMap := make(map[string]map[string]PartitionReplicaInfo)
	self.processNamespaceNode(rsp.Node.Nodes, metaMap, replicasMap)

	nsInfos := make(map[string]map[int]PartitionMetaInfo)
	nsEpoch := EpochType(rsp.Node.ModifiedIndex)
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
			partInfos[partition] = info
		}
	}

	self.nsMutex.Lock()
	self.allNamespaceInfos = nsInfos
	self.nsEpoch = nsEpoch
	self.nsMutex.Unlock()

	return nsInfos, nsEpoch, nil
}

func (self *EtcdRegister) processNamespaceNode(nodes client.Nodes,
	metaMap map[string]NamespaceMetaInfo,
	replicasMap map[string]map[string]PartitionReplicaInfo) {
	for _, node := range nodes {
		if node.Nodes != nil {
			self.processNamespaceNode(node.Nodes, metaMap, replicasMap)
		}
		if node.Dir {
			continue
		}
		_, key := path.Split(node.Key)
		if key == NAMESPACE_REPLICA_INFO {
			var rInfo PartitionReplicaInfo
			if err := json.Unmarshal([]byte(node.Value), &rInfo); err != nil {
				continue
			}
			rInfo.Epoch = EpochType(node.ModifiedIndex)
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
			nsName := keys[keyLen-2]
			metaMap[nsName] = mInfo
		}
	}
}

func (self *EtcdRegister) GetNamespacePartInfo(ns string, partition int) (*PartitionMetaInfo, error) {
	self.nsMutex.Lock()
	nsInfo, ok := self.allNamespaceInfos[ns]
	self.nsMutex.Unlock()
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
	return &p, nil
}

func (self *EtcdRegister) GetNamespaceInfo(ns string) ([]PartitionMetaInfo, error) {
	self.nsMutex.Lock()
	nsInfo, ok := self.allNamespaceInfos[ns]
	self.nsMutex.Unlock()
	if !ok {
		return nil, ErrKeyNotFound
	}
	parts := make([]PartitionMetaInfo, 0, len(nsInfo))
	for _, v := range nsInfo {
		parts = append(parts, v)
	}
	return parts, nil
}

func (self *EtcdRegister) GetNamespaceMetaInfo(ns string) (NamespaceMetaInfo, error) {
	self.nsMutex.Lock()
	meta, ok := self.namespaceMetaMap[ns]
	self.nsMutex.Unlock()
	if !ok {
		return meta, ErrKeyNotFound
	}
	return meta, nil
}

func (self *EtcdRegister) getClusterPath() string {
	return path.Join("/", ROOT_DIR, self.clusterID)
}

func (self *EtcdRegister) getClusterMetaPath() string {
	return path.Join(self.getClusterPath(), CLUSTER_META_INFO)
}

func (self *EtcdRegister) getPDNodePath(value *NodeInfo) string {
	return path.Join(self.getPDNodeRootPath(), "Node-"+value.ID)
}

func (self *EtcdRegister) getPDNodeRootPath() string {
	return path.Join("/", ROOT_DIR, self.clusterID, PD_ROOT_DIR, PD_NODE_DIR)
}

func (self *EtcdRegister) getPDLeaderPath() string {
	return path.Join("/", ROOT_DIR, self.clusterID, PD_ROOT_DIR, PD_LEADER_SESSION)
}

func (self *EtcdRegister) getDataNodeRootPath() string {
	return path.Join("/", ROOT_DIR, self.clusterID, DATA_NODE_DIR)
}

func (self *EtcdRegister) getNamespaceRootPath() string {
	return path.Join("/", ROOT_DIR, self.clusterID, NAMESPACE_DIR)
}

func (self *EtcdRegister) getNamespacePath(ns string) string {
	return path.Join(self.namespaceRoot, ns)
}

func (self *EtcdRegister) getNamespaceMetaPath(ns string) string {
	return path.Join(self.namespaceRoot, ns, NAMESPACE_META)
}

func (self *EtcdRegister) getNamespacePartitionPath(ns string, partition int) string {
	return path.Join(self.namespaceRoot, ns, strconv.Itoa(partition))
}

func (self *EtcdRegister) getNamespaceReplicaInfoPath(ns string, partition int) string {
	return path.Join(self.namespaceRoot, ns, strconv.Itoa(partition), NAMESPACE_REPLICA_INFO)
}

// placement driver register
type PDEtcdRegister struct {
	*EtcdRegister

	leaderSessionPath string
	leaderStr         string
	nodeInfo          *NodeInfo
	nodeKey           string
	nodeValue         string

	refreshStopCh    chan bool
	watchNodesStopCh chan bool
}

func NewPDEtcdRegister(host string) *PDEtcdRegister {
	return &PDEtcdRegister{
		EtcdRegister:     NewEtcdRegister(host),
		watchNodesStopCh: make(chan bool, 1),
		refreshStopCh:    make(chan bool, 1),
	}
}

func (self *PDEtcdRegister) Register(value *NodeInfo) error {
	self.leaderSessionPath = self.getPDLeaderPath()
	self.nodeInfo = value
	valueB, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if self.refreshStopCh != nil {
		close(self.refreshStopCh)
	}

	self.leaderStr = string(valueB)
	self.nodeKey = self.getPDNodePath(value)
	self.nodeValue = string(valueB)
	_, err = self.client.Set(self.nodeKey, self.nodeValue, ETCD_TTL)
	if err != nil {
		return err
	}
	self.refreshStopCh = make(chan bool)
	// start to refresh
	go self.refresh(self.refreshStopCh)

	return nil
}

func (self *PDEtcdRegister) refresh(stopC <-chan bool) {
	for {
		select {
		case <-stopC:
			return
		case <-time.After(time.Second * time.Duration(ETCD_TTL*4/10)):
			_, err := self.client.SetWithTTL(self.nodeKey, ETCD_TTL)
			if err != nil {
				coordLog.Errorf("update error: %s", err.Error())
				_, err := self.client.Set(self.nodeKey, self.nodeValue, ETCD_TTL)
				if err != nil {
					coordLog.Errorf("set key error: %s", err.Error())
				}
			}
		}
	}
}

func (self *PDEtcdRegister) Unregister(value *NodeInfo) error {
	// stop to refresh
	if self.refreshStopCh != nil {
		close(self.refreshStopCh)
		self.refreshStopCh = nil
	}

	_, err := self.client.Delete(self.getPDNodePath(value), false)
	if err != nil {
		coordLog.Warningf("cluser[%s] node[%s] unregister failed: %v", self.clusterID, value, err)
		return err
	}

	return nil
}

func (self *PDEtcdRegister) Stop() {
	//	self.Unregister()
	if self.watchNodesStopCh != nil {
		close(self.watchNodesStopCh)
	}
	if self.watchNamespaceStopCh != nil {
		close(self.watchNamespaceStopCh)
	}
}

func (self *PDEtcdRegister) PrepareNamespaceMinGID() (int64, error) {
	var clusterMeta ClusterMetaInfo
	initValue, _ := json.Marshal(clusterMeta)
	err := exchangeNodeValue(
		self.client,
		self.getClusterMetaPath(),
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

func (self *PDEtcdRegister) GetClusterEpoch() (EpochType, error) {
	rsp, err := self.client.Get(self.clusterPath, false, false)
	if err != nil {
		if client.IsKeyNotFound(err) {
			return 0, ErrKeyNotFound
		}
		return 0, err
	}

	return EpochType(rsp.Node.ModifiedIndex), nil
}

func (self *PDEtcdRegister) AcquireAndWatchLeader(leader chan *NodeInfo, stop chan struct{}) {
	master := etcdlock.NewMaster(self.client, self.leaderSessionPath, self.leaderStr, ETCD_TTL)
	go self.processMasterEvents(master, leader, stop)
	master.Start()
}

func (self *PDEtcdRegister) processMasterEvents(master etcdlock.Master, leader chan *NodeInfo, stop chan struct{}) {
	for {
		select {
		case e := <-master.GetEventsChan():
			if e.Type == etcdlock.MASTER_ADD || e.Type == etcdlock.MASTER_MODIFY {
				// Acquired the lock || lock change.
				var node NodeInfo
				if err := json.Unmarshal([]byte(e.Master), &node); err != nil {
					leader <- &node
					continue
				}
				coordLog.Infof("master event type[%d] lookupdNode[%v].", e.Type, node)
				leader <- &node
			} else if e.Type == etcdlock.MASTER_DELETE {
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

func (self *PDEtcdRegister) CheckIfLeader() bool {
	rsp, err := self.client.Get(self.leaderSessionPath, false, false)
	if err != nil {
		return false
	}
	if rsp.Node.Value == self.leaderStr {
		return true
	}
	return false
}

func (self *PDEtcdRegister) GetDataNodes() ([]NodeInfo, error) {
	return self.getDataNodes()
}

func (self *PDEtcdRegister) WatchDataNodes(nsqds chan []NodeInfo, stop chan struct{}) {
	nsqdNodes, err := self.getDataNodes()
	if err == nil {
		select {
		case nsqds <- nsqdNodes:
		case <-stop:
			close(nsqds)
			return
		}
	}

	key := self.getDataNodeRootPath()
	watcher := self.client.Watch(key, 0, true)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		select {
		case <-stop:
			cancel()
		case <-self.watchNodesStopCh:
			cancel()
		}
	}()
	for {
		rsp, err := watcher.Next(ctx)
		if err != nil {
			if err == context.Canceled {
				coordLog.Infof("watch key[%s] canceled.", key)
				close(nsqds)
				return
			} else {
				coordLog.Errorf("watcher key[%s] error: %s", key, err.Error())
				//rewatch
				if etcdlock.IsEtcdWatchExpired(err) {
					rsp, err = self.client.Get(key, false, true)
					if err != nil {
						coordLog.Errorf("rewatch and get key[%s] error: %s", key, err.Error())
						continue
					}
					watcher = self.client.Watch(key, rsp.Index+1, true)
					// should get the nodes to notify watcher since last watch is expired
				} else {
					time.Sleep(5 * time.Second)
					continue
				}
			}
		}
		nsqdNodes, err := self.getDataNodes()
		if err != nil {
			coordLog.Errorf("key[%s] getNsqdNodes error: %s", key, err.Error())
			continue
		}
		select {
		case nsqds <- nsqdNodes:
		case <-stop:
			close(nsqds)
			return
		}
	}
}

func (self *PDEtcdRegister) getDataNodes() ([]NodeInfo, error) {
	rsp, err := self.client.Get(self.getDataNodeRootPath(), false, false)
	if err != nil {
		if client.IsKeyNotFound(err) {
			return nil, ErrKeyNotFound
		}
		return nil, err
	}
	nsqdNodes := make([]NodeInfo, 0)
	for _, node := range rsp.Node.Nodes {
		if node.Dir {
			continue
		}
		var nodeInfo NodeInfo
		err := json.Unmarshal([]byte(node.Value), &nodeInfo)
		if err != nil {
			continue
		}
		nsqdNodes = append(nsqdNodes, nodeInfo)
	}
	return nsqdNodes, nil
}

func (self *PDEtcdRegister) CreateNamespacePartition(ns string, partition int) error {
	_, err := self.client.CreateDir(self.getNamespacePartitionPath(ns, partition), 0)
	if err != nil {
		if IsEtcdNotFile(err) {
			return ErrKeyAlreadyExist
		}
		return err
	}
	return nil
}

func (self *PDEtcdRegister) CreateNamespace(ns string, meta *NamespaceMetaInfo) error {
	if meta.MinGID <= 0 {
		return errors.New("namespace MinGID is invalid")
	}
	metaValue, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	rsp, err := self.client.Create(self.getNamespaceMetaPath(ns), string(metaValue), 0)
	if err != nil {
		if IsEtcdNodeExist(err) {
			return ErrKeyAlreadyExist
		}
		return err
	}

	meta.MetaEpoch = EpochType(rsp.Node.ModifiedIndex)
	self.updateNamespaceMeta(ns, *meta)
	return nil
}

func (self *PDEtcdRegister) IsExistNamespace(ns string) (bool, error) {
	_, err := self.client.Get(self.getNamespacePath(ns), false, false)
	if err != nil {
		if client.IsKeyNotFound(err) {
			return false, nil
		} else {
			return false, err
		}
	}
	return true, nil
}

func (self *PDEtcdRegister) IsExistNamespacePartition(ns string, partitionNum int) (bool, error) {
	_, err := self.client.Get(self.getNamespacePartitionPath(ns, partitionNum), false, false)
	if err != nil {
		if client.IsKeyNotFound(err) {
			return false, nil
		} else {
			return false, err
		}
	}
	return true, nil
}

func (self *PDEtcdRegister) UpdateNamespaceMetaInfo(ns string, meta *NamespaceMetaInfo, oldGen EpochType) error {
	value, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	coordLog.Infof("Update meta info: %s %s %d", ns, string(value), oldGen)

	self.nsMutex.Lock()
	defer self.nsMutex.Unlock()
	delete(self.namespaceMetaMap, ns)
	atomic.StoreInt32(&self.ifNamespaceChanged, 1)
	rsp, err := self.client.CompareAndSwap(self.getNamespaceMetaPath(ns), string(value), 0, "", uint64(oldGen))
	if err != nil {
		return err
	}
	err = json.Unmarshal([]byte(rsp.Node.Value), &meta)
	if err != nil {
		coordLog.Errorf("unmarshal meta info failed: %v, %v", err, rsp.Node.Value)
		return err
	}
	meta.MetaEpoch = EpochType(rsp.Node.ModifiedIndex)
	self.namespaceMetaMap[ns] = *meta

	return nil
}

func (self *PDEtcdRegister) DeleteWholeNamespace(ns string) error {
	self.nsMutex.Lock()
	delete(self.namespaceMetaMap, ns)
	rsp, err := self.client.Delete(self.getNamespacePath(ns), true)
	coordLog.Infof("delete whole topic: %v, %v, %v", ns, err, rsp)
	self.nsMutex.Unlock()
	return err
}

func (self *PDEtcdRegister) DeleteNamespacePart(ns string, partition int) error {
	_, err := self.client.Delete(self.getNamespacePartitionPath(ns, partition), true)
	if err != nil {
		if !client.IsKeyNotFound(err) {
			return err
		}
	}
	return nil
}

func (self *PDEtcdRegister) UpdateNamespacePartReplicaInfo(ns string, partition int,
	replicaInfo *PartitionReplicaInfo, oldGen EpochType) error {
	value, err := json.Marshal(replicaInfo)
	if err != nil {
		return err
	}
	coordLog.Infof("Update info: %s %d %s %d", ns, partition, string(value), oldGen)
	if oldGen == 0 {
		rsp, err := self.client.Create(self.getNamespaceReplicaInfoPath(ns, partition), string(value), 0)
		if err != nil {
			return err
		}
		replicaInfo.Epoch = EpochType(rsp.Node.ModifiedIndex)
		return nil
	}
	rsp, err := self.client.CompareAndSwap(self.getNamespaceReplicaInfoPath(ns, partition), string(value), 0, "", uint64(oldGen))
	if err != nil {
		return err
	}
	replicaInfo.Epoch = EpochType(rsp.Node.ModifiedIndex)
	return nil
}

type DNEtcdRegister struct {
	*EtcdRegister
	sync.Mutex

	nodeKey       string
	nodeValue     string
	refreshStopCh chan bool
}

func SetEtcdLogger(log etcdlock.Logger, level int32) {
	etcdlock.SetLogger(log, int(level))
}

func NewDNEtcdRegister(host string) *DNEtcdRegister {
	return &DNEtcdRegister{
		EtcdRegister: NewEtcdRegister(host),
	}
}

func (self *DNEtcdRegister) Register(nodeData *NodeInfo) error {
	value, err := json.Marshal(nodeData)
	if err != nil {
		return err
	}
	if self.refreshStopCh != nil {
		close(self.refreshStopCh)
	}

	self.nodeKey = self.getDataNodePath(nodeData)
	self.nodeValue = string(value)
	_, err = self.client.Set(self.nodeKey, self.nodeValue, ETCD_TTL)
	if err != nil {
		return err
	}
	coordLog.Infof("registered new node: %v", nodeData)
	self.refreshStopCh = make(chan bool)
	// start refresh node
	go self.refresh(self.refreshStopCh)

	return nil
}

func (self *DNEtcdRegister) refresh(stopChan chan bool) {
	for {
		select {
		case <-stopChan:
			return
		case <-time.After(time.Second * time.Duration(ETCD_TTL*4/10)):
			_, err := self.client.SetWithTTL(self.nodeKey, ETCD_TTL)
			if err != nil {
				coordLog.Errorf("update error: %s", err.Error())
				_, err := self.client.Set(self.nodeKey, self.nodeValue, ETCD_TTL)
				if err != nil {
					coordLog.Errorf("set key error: %s", err.Error())
				}
			}
		}
	}
}

func (self *DNEtcdRegister) Unregister(nodeData *NodeInfo) error {
	self.Lock()
	defer self.Unlock()

	// stop refresh
	if self.refreshStopCh != nil {
		close(self.refreshStopCh)
		self.refreshStopCh = nil
	}

	_, err := self.client.Delete(self.getDataNodePath(nodeData), false)
	if err != nil {
		coordLog.Warningf("cluser[%s] node[%s] unregister failed: %v", self.clusterID, nodeData, err)
		return err
	}

	coordLog.Infof("cluser[%s] node[%v] unregistered", self.clusterID, nodeData)
	return nil
}

func (self *DNEtcdRegister) GetNodeInfo(nid string) (NodeInfo, error) {
	var node NodeInfo
	rsp, err := self.client.Get(self.getDataNodePathFromID(nid), false, false)
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

func (self *DNEtcdRegister) NewRegisterNodeID() (uint64, error) {
	var clusterMeta ClusterMetaInfo
	initValue, _ := json.Marshal(clusterMeta)
	exchangeErr := exchangeNodeValue(self.client, self.getClusterMetaPath(), string(initValue), func(isNew bool, oldValue string) (string, error) {
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

func (self *DNEtcdRegister) WatchPDLeader(leader chan *NodeInfo, stop chan struct{}) error {
	key := self.getPDLeaderPath()

	rsp, err := self.client.Get(key, false, false)
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

	watcher := self.client.Watch(key, 0, true)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		select {
		case <-stop:
			cancel()
		}
	}()
	for {
		rsp, err = watcher.Next(ctx)
		if err != nil {
			if err == context.Canceled {
				coordLog.Infof("watch key[%s] canceled.", key)
				close(leader)
				return nil
			} else {
				coordLog.Errorf("watcher key[%s] error: %s", key, err.Error())
				//rewatch
				if etcdlock.IsEtcdWatchExpired(err) {
					rsp, err = self.client.Get(key, false, true)
					if err != nil {
						coordLog.Errorf("rewatch and get key[%s] error: %s", key, err.Error())
						continue
					}
					watcher = self.client.Watch(key, rsp.Index+1, true)
				} else {
					time.Sleep(5 * time.Second)
					continue
				}
			}
		}
		if rsp == nil {
			continue
		}
		var node NodeInfo
		if rsp.Action == "expire" || rsp.Action == "delete" {
			coordLog.Infof("key[%s] action[%s]", key, rsp.Action)
		} else if rsp.Action == "create" || rsp.Action == "update" || rsp.Action == "set" {
			err := json.Unmarshal([]byte(rsp.Node.Value), &node)
			if err != nil {
				continue
			}
		} else {
			continue
		}
		select {
		case leader <- &node:
		case <-stop:
			close(leader)
			return nil
		}
	}

	return nil
}

func (self *DNEtcdRegister) getDataNodePathFromID(nid string) string {
	return path.Join("/", ROOT_DIR, self.clusterID, DATA_NODE_DIR, "Node-"+nid)
}

func (self *DNEtcdRegister) getDataNodePath(nodeData *NodeInfo) string {
	return path.Join("/", ROOT_DIR, self.clusterID, DATA_NODE_DIR, "Node-"+nodeData.ID)
}
