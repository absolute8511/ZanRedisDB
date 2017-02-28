package cluster

import (
	"errors"
	"fmt"
	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/absolute8511/ZanRedisDB/node"
	"github.com/spaolacci/murmur3"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

var (
	ErrBalanceNodeUnavailable = errors.New("can not find a node to be balanced")
	ErrClusterBalanceRunning  = errors.New("another balance is running, should wait")
)

type balanceOpLevel int

func splitNamespacePartitionID(namespaceFullName string) (string, int, error) {
	partIndex := strings.LastIndex(namespaceFullName, "-")
	if partIndex == -1 {
		return "", 0, fmt.Errorf("invalid namespace full name: %v", namespaceFullName)
	}
	namespaceName := namespaceFullName[:partIndex]
	partitionID, err := strconv.Atoi(namespaceFullName[partIndex+1:])
	return namespaceName, partitionID, err
}

// An IntHeap is a min-heap of ints.
type IntHeap []int

func (h IntHeap) Len() int           { return len(h) }
func (h IntHeap) Less(i, j int) bool { return h[i] < h[j] }
func (h IntHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *IntHeap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(int))
}

func (h *IntHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type DataPlacement struct {
	balanceInterval [2]int
	pdCoord         *PDCoordinator
}

func NewDataPlacement(coord *PDCoordinator) *DataPlacement {
	return &DataPlacement{
		pdCoord:         coord,
		balanceInterval: [2]int{2, 4},
	}
}

// query the raft peers if the nid already in the raft group for the namespace
func (self *DataPlacement) IsRaftNodeJoined(nsInfo *PartitionMetaInfo, nid string) bool {
	if len(nsInfo.RaftNodes) == 0 {
		return false
	}
	remoteNode := nsInfo.RaftNodes[0]
	nip, _, _, httpPort := ExtractNodeInfoFromID(remoteNode)
	var rsp []*node.MemberInfo
	err := common.APIRequest("GET",
		"http://"+net.JoinHostPort(nip, httpPort)+common.APIGetMembers+"/"+nsInfo.GetDesp(),
		nil, time.Second*3, &rsp)
	if err != nil {
		coordLog.Infof("failed to get members for namespace: %v", err)
		return false
	}

	for _, m := range rsp {
		if m.NodeID == ExtractRegIDFromGenID(nid) && m.ID == nsInfo.RaftIDs[nid] {
			return true
		}
	}
	coordLog.Infof("namespace %v members is %v, still waiting %v join", nsInfo.GetDesp(), rsp, nid)
	return false
}

func (self *DataPlacement) SetBalanceInterval(start int, end int) {
	if start == end && start == 0 {
		return
	}
	self.balanceInterval[0] = start
	self.balanceInterval[1] = end
}

func (self *DataPlacement) DoBalance(monitorChan chan struct{}) {
	//check period for the data balance.
	ticker := time.NewTicker(time.Minute * 10)
	defer func() {
		ticker.Stop()
		coordLog.Infof("balance check exit.")
	}()
	for {
		select {
		case <-monitorChan:
			return
		case <-ticker.C:
			// only balance at given interval
			if time.Now().Hour() > self.balanceInterval[1] || time.Now().Hour() < self.balanceInterval[0] {
				continue
			}
			if !self.pdCoord.IsMineLeader() {
				coordLog.Infof("not leader while checking balance")
				continue
			}
			if !self.pdCoord.IsClusterStable() {
				coordLog.Infof("no balance since cluster is not stable while checking balance")
				continue
			}
			coordLog.Infof("begin checking balance of namespace data...")
			currentNodes := self.pdCoord.getCurrentNodes(nil)
			validNum := len(currentNodes)
			if validNum < 2 {
				continue
			}
			self.rebalanceNamespace(monitorChan)
		}
	}
}

func (self *DataPlacement) addNodeToNamespaceAndWaitReady(monitorChan chan struct{}, namespaceInfo *PartitionMetaInfo,
	nodeNameList []string) error {
	retry := 0
	currentSelect := 0
	namespaceName := namespaceInfo.Name
	partitionID := namespaceInfo.Partition
	// since we need add new catchup, we make the replica as replica+1
	partitionNodes, err := self.getRebalancedPartitionsFromNameList(
		namespaceInfo.Name,
		namespaceInfo.PartitionNum,
		namespaceInfo.Replica+1, nodeNameList)
	if err != nil {
		return err.ToErrorType()
	}
	selectedCatchup := make([]string, 0)
	for _, nid := range partitionNodes[namespaceInfo.Partition] {
		if FindSlice(namespaceInfo.RaftNodes, nid) != -1 {
			// already isr, ignore add catchup
			continue
		}
		selectedCatchup = append(selectedCatchup, nid)
	}
	for {
		if currentSelect >= len(selectedCatchup) {
			coordLog.Infof("currently no any node %v can be balanced for namespace: %v, expect isr: %v, nodes:%v",
				selectedCatchup, namespaceName, partitionNodes[namespaceInfo.Partition], nodeNameList)
			return ErrBalanceNodeUnavailable
		}
		nid := selectedCatchup[currentSelect]
		namespaceInfo, err := self.pdCoord.register.GetNamespacePartInfo(namespaceName, partitionID)
		if err != nil {
			coordLog.Infof("failed to get namespace %v info: %v", namespaceInfo.GetDesp(), err)
		} else {
			if self.IsRaftNodeJoined(namespaceInfo, nid) {
				break
			} else if FindSlice(namespaceInfo.RaftNodes, nid) != -1 {
				// wait ready
				select {
				case <-monitorChan:
					return errors.New("quiting")
				case <-time.After(time.Second * 5):
					coordLog.Infof("node: %v is added for namespace %v , still waiting catchup", nid, namespaceInfo.GetDesp())
				}
				continue
			} else {
				coordLog.Infof("node: %v is added for namespace %v", nid, namespaceInfo.GetDesp())
				self.pdCoord.addNamespaceToNode(namespaceInfo, nid)
			}
		}
		select {
		case <-monitorChan:
			return errors.New("quiting")
		case <-time.After(time.Second * 5):
		}
		if retry > 5 {
			coordLog.Infof("add catchup and wait timeout : %v", namespaceName)
			return errors.New("wait timeout")
		}
		retry++
	}
	return nil
}

func (self *DataPlacement) getExcludeNodesForNamespace(namespaceInfo *PartitionMetaInfo) (map[string]struct{}, error) {
	excludeNodes := make(map[string]struct{})
	for _, v := range namespaceInfo.RaftNodes {
		excludeNodes[v] = struct{}{}
	}
	return excludeNodes, nil
}

func (self *DataPlacement) allocNodeForNamespace(namespaceInfo *PartitionMetaInfo,
	currentNodes map[string]NodeInfo) (*NodeInfo, *CoordErr) {
	var chosenNode NodeInfo
	excludeNodes, commonErr := self.getExcludeNodesForNamespace(namespaceInfo)
	if commonErr != nil {
		return nil, ErrRegisterServiceUnstable
	}

	partitionNodes, err := self.getRebalancedNamespacePartitions(
		namespaceInfo.Name,
		namespaceInfo.PartitionNum,
		namespaceInfo.Replica, currentNodes)
	if err != nil {
		return nil, err
	}
	for _, nodeID := range partitionNodes[namespaceInfo.Partition] {
		if _, ok := excludeNodes[nodeID]; ok {
			continue
		}
		chosenNode = currentNodes[nodeID]
		break
	}
	if chosenNode.ID == "" {
		coordLog.Infof("no more available node for namespace: %v, excluding nodes: %v, all nodes: %v",
			namespaceInfo.GetDesp(), excludeNodes, currentNodes)
		return nil, ErrNodeUnavailable
	}
	coordLog.Infof("node %v is alloc for namespace: %v", chosenNode, namespaceInfo.GetDesp())
	return &chosenNode, nil
}

func (self *DataPlacement) checkNamespaceNodeConflict(namespaceInfo *PartitionMetaInfo) bool {
	existSlaves := make(map[string]struct{})
	// isr should be different
	for _, id := range namespaceInfo.RaftNodes {
		if _, ok := existSlaves[id]; ok {
			return false
		}
		existSlaves[id] = struct{}{}
	}
	return true
}

func (self *DataPlacement) allocNamespaceRaftNodes(ns string, currentNodes map[string]NodeInfo,
	replica int, partitionNum int, existPart map[int]*PartitionMetaInfo) ([]PartitionReplicaInfo, *CoordErr) {
	replicaList := make([]PartitionReplicaInfo, partitionNum)
	partitionNodes, err := self.getRebalancedNamespacePartitions(
		ns,
		partitionNum,
		replica, currentNodes)
	if err != nil {
		return nil, err
	}
	for p := 0; p < partitionNum; p++ {
		var replicaInfo PartitionReplicaInfo
		if elem, ok := existPart[p]; ok {
			replicaInfo = elem.PartitionReplicaInfo
		} else {
			replicaInfo.RaftNodes = partitionNodes[p]
			replicaInfo.RaftIDs = make(map[string]uint64)
			for _, nid := range replicaInfo.RaftNodes {
				replicaInfo.MaxRaftID++
				replicaInfo.RaftIDs[nid] = uint64(replicaInfo.MaxRaftID)
			}
		}
		replicaList[p] = replicaInfo
	}

	coordLog.Infof("selected namespace replica list : %v", replicaList)
	return replicaList, nil
}

func (self *DataPlacement) rebalanceNamespace(monitorChan chan struct{}) (bool, bool) {
	moved := false
	isAllBalanced := false
	if !atomic.CompareAndSwapInt32(&self.pdCoord.balanceWaiting, 0, 1) {
		coordLog.Infof("another balance is running, should wait")
		return moved, isAllBalanced
	}
	defer atomic.StoreInt32(&self.pdCoord.balanceWaiting, 0)

	allNamespaces, _, err := self.pdCoord.register.GetAllNamespaces()
	if err != nil {
		coordLog.Infof("scan namespaces error: %v", err)
		return moved, isAllBalanced
	}
	namespaceList := make([]PartitionMetaInfo, 0)
	for _, parts := range allNamespaces {
		namespaceList = append(namespaceList, parts...)
	}
	nodeNameList := make([]string, 0)
	movedNamespace := ""
	isAllBalanced = true
	for _, namespaceInfo := range namespaceList {
		select {
		case <-monitorChan:
			return moved, false
		default:
		}
		if !self.pdCoord.IsClusterStable() {
			return moved, false
		}
		if !self.pdCoord.IsMineLeader() {
			return moved, true
		}
		// balance only one namespace once
		if movedNamespace != "" && movedNamespace != namespaceInfo.Name {
			continue
		}
		currentNodes := self.pdCoord.getCurrentNodes(namespaceInfo.Tags)
		nodeNameList = nodeNameList[0:0]
		for _, n := range currentNodes {
			nodeNameList = append(nodeNameList, n.ID)
		}

		partitionNodes, err := self.getRebalancedNamespacePartitions(
			namespaceInfo.Name,
			namespaceInfo.PartitionNum,
			namespaceInfo.Replica, currentNodes)
		if err != nil {
			isAllBalanced = false
			continue
		}
		moveNodes := make([]string, 0)
		for _, nid := range namespaceInfo.RaftNodes {
			found := false
			for _, expectedNode := range partitionNodes[namespaceInfo.Partition] {
				if nid == expectedNode {
					found = true
					break
				}
			}
			if !found {
				moveNodes = append(moveNodes, nid)
			}
		}
		for _, nid := range moveNodes {
			movedNamespace = namespaceInfo.Name
			coordLog.Infof("node %v need move for namespace %v since %v not in expected isr list: %v", nid,
				namespaceInfo.GetDesp(), namespaceInfo.RaftNodes, partitionNodes[namespaceInfo.Partition])
			var err error
			if len(namespaceInfo.RaftNodes) <= namespaceInfo.Replica {
				err = self.addNodeToNamespaceAndWaitReady(monitorChan, &namespaceInfo,
					nodeNameList)
			}
			if err != nil {
				continue
			} else {
				self.pdCoord.removeNamespaceFromNode(&namespaceInfo, nid)
				moved = true
			}
		}
		expectLeader := partitionNodes[namespaceInfo.Partition][0]
		if len(moveNodes) == 0 && (len(namespaceInfo.RaftNodes) >= namespaceInfo.Replica) &&
			(namespaceInfo.RaftNodes[0] != expectLeader) {
			for index, nid := range namespaceInfo.RaftNodes {
				if nid == expectLeader {
					namespaceInfo.RaftNodes[0], namespaceInfo.RaftNodes[index] = namespaceInfo.RaftNodes[index], namespaceInfo.RaftNodes[0]
					moved = true
				}
			}
			select {
			case <-monitorChan:
			case <-time.After(time.Second):
			}
		}
		if len(moveNodes) > 0 || moved {
			isAllBalanced = false
		}
	}

	return moved, isAllBalanced
}

type SortableStrings []string

func (s SortableStrings) Less(l, r int) bool {
	return s[l] < s[r]
}
func (s SortableStrings) Len() int {
	return len(s)
}
func (s SortableStrings) Swap(l, r int) {
	s[l], s[r] = s[r], s[l]
}

func (self *DataPlacement) getRebalancedNamespacePartitions(ns string,
	partitionNum int, replica int,
	currentNodes map[string]NodeInfo) ([][]string, *CoordErr) {
	if len(currentNodes) < replica {
		return nil, ErrNodeUnavailable
	}
	// for ordered namespace we have much partitions than nodes,
	// so we need make all the nodes have the almost the same leader partitions,
	// and also we need make all the nodes have the almost the same followers.
	// and to avoid the data migration, we should keep the data as much as possible
	// algorithm as below:
	// 1. sort node id ; 2. sort the namespace partitions
	// 3. choose the (leader, follower, follower) for each partition,
	// start from the index of the current node array
	// 4. for next partition, start from the next index of node array.
	//  l -> leader, f-> follower
	//         nodeA   nodeB   nodeC   nodeD
	// p1       l       f        f
	// p2               l        f      f
	// p3       f                l      f
	// p4       f       f               l
	// p5       l       f        f
	// p6               l        f      f
	// p7       f                l      f
	// p8       f       f               l
	// p9       l       f        f
	// p10              l        f      f

	// after nodeB is down, the migration as bellow
	//         nodeA   xxxx   nodeC   nodeD
	// p1       l       x        f     x-f
	// p2      x-f      x       f-l     f
	// p3       f                l      f
	// p4       f       x       x-f     l
	// p5       l       x        f     x-f
	// p6      x-f      x       f-l     f
	// p7       f                l      f
	// p8       f       x       x-f     l
	// p9       l       x        f     x-f
	// p10     x-f      x       f-l     f
	nodeNameList := make(SortableStrings, 0, len(currentNodes))
	for nid, _ := range currentNodes {
		nodeNameList = append(nodeNameList, nid)
	}
	return self.getRebalancedPartitionsFromNameList(ns, partitionNum, replica, nodeNameList)
}

func (self *DataPlacement) getRebalancedPartitionsFromNameList(ns string,
	partitionNum int, replica int,
	nodeNameList SortableStrings) ([][]string, *CoordErr) {
	if len(nodeNameList) < replica {
		return nil, ErrNodeUnavailable
	}
	sort.Sort(nodeNameList)
	partitionNodes := make([][]string, partitionNum)
	selectIndex := int(murmur3.Sum32([]byte(ns)))
	for i := 0; i < partitionNum; i++ {
		nlist := make([]string, replica)
		partitionNodes[i] = nlist
		for j := 0; j < replica; j++ {
			nlist[j] = nodeNameList[(selectIndex+j)%len(nodeNameList)]
		}
		selectIndex++
	}

	return partitionNodes, nil
}

func (self *DataPlacement) decideUnwantedRaftNode(namespaceInfo *PartitionMetaInfo, currentNodes map[string]NodeInfo) string {
	unwantedNode := ""
	//remove the unwanted node in isr
	partitionNodes, err := self.getRebalancedNamespacePartitions(
		namespaceInfo.Name,
		namespaceInfo.PartitionNum,
		namespaceInfo.Replica, currentNodes)
	if err != nil {
		return unwantedNode
	}
	for _, nid := range namespaceInfo.RaftNodes {
		found := false
		for _, validNode := range partitionNodes[namespaceInfo.Partition] {
			if nid == validNode {
				found = true
				break
			}
		}
		if !found {
			unwantedNode = nid
		}
	}
	return unwantedNode
}
