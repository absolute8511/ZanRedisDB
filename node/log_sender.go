package node

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/absolute8511/ZanRedisDB/raft/raftpb"
	"github.com/absolute8511/ZanRedisDB/syncerpb"
	"github.com/absolute8511/go-zanredisdb"
	"google.golang.org/grpc"
)

var (
	errInvalidRemoteCluster = errors.New("remote cluster is not valid")
	errInvalidNamespace     = errors.New("namespace is not valid")
	rpcTimeout              = time.Second * 5
	sendLogTimeout          = time.Second * 30
)

type ccAPIClient struct {
	client syncerpb.CrossClusterAPIClient
	conn   *grpc.ClientConn
}

// RemoteLogSender is the raft log sender. It will send all the raft logs
// to the remote cluster using grpc service.
type RemoteLogSender struct {
	localCluster      string
	grpName           string
	ns                string
	pid               int
	poolMutex         sync.RWMutex
	connPool          map[string]ccAPIClient
	zanCluster        *zanredisdb.Cluster
	remoteClusterAddr string
}

func NewRemoteLogSender(localCluster string, fullName string, remoteCluster string) (*RemoteLogSender, error) {
	if remoteCluster == "" {
		return nil, errInvalidRemoteCluster
	}
	// used for test only
	if remoteCluster == "test://" {
		remoteCluster = ""
	}
	ns, pid := common.GetNamespaceAndPartition(fullName)
	if ns == "" {
		nodeLog.Infof("invalid namespace string: %v", fullName)
		return nil, errInvalidNamespace
	}
	return &RemoteLogSender{
		localCluster:      localCluster,
		ns:                ns,
		pid:               pid,
		remoteClusterAddr: remoteCluster,
		grpName:           fullName,
		connPool:          make(map[string]ccAPIClient),
	}, nil
}

func (s *RemoteLogSender) getZanCluster() *zanredisdb.Cluster {
	if s.remoteClusterAddr == "" || strings.HasPrefix(s.remoteClusterAddr, "test://") {
		return nil
	}
	conf := &zanredisdb.Conf{
		DialTimeout:  rpcTimeout,
		ReadTimeout:  rpcTimeout,
		WriteTimeout: rpcTimeout,
		TendInterval: 5,
		Namespace:    s.ns,
	}
	conf.LookupList = append(conf.LookupList, s.remoteClusterAddr)
	s.zanCluster = zanredisdb.NewCluster(conf)
	return s.zanCluster
}

func (s *RemoteLogSender) Stop() {
	s.poolMutex.RLock()
	for _, c := range s.connPool {
		if c.conn != nil {
			c.conn.Close()
		}
	}
	s.poolMutex.RUnlock()
	if s.zanCluster != nil {
		s.zanCluster.Close()
	}
}

func (s *RemoteLogSender) GetStats() interface{} {
	return nil
}

func (s *RemoteLogSender) getClientFromAddr(addr string) syncerpb.CrossClusterAPIClient {
	s.poolMutex.Lock()
	defer s.poolMutex.Unlock()
	if c, ok := s.connPool[addr]; ok {
		return c.client
	}
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		nodeLog.Infof("failed to get grpc client: %v, %v", addr, err)
		return nil
	}
	c := syncerpb.NewCrossClusterAPIClient(conn)
	s.connPool[addr] = ccAPIClient{client: c, conn: conn}
	return c
}

func (s *RemoteLogSender) getClient() (syncerpb.CrossClusterAPIClient, string, error) {
	var addr string
	if strings.HasPrefix(s.remoteClusterAddr, "test://") {
		addr = s.remoteClusterAddr[len("test://"):]
	} else {
		if s.zanCluster == nil {
			s.zanCluster = s.getZanCluster()
			if s.zanCluster == nil {
				return nil, addr, errors.New("failed to init remote zankv cluster")
			}
		}
		h, err := s.zanCluster.GetHostByPart(s.pid, true)
		if err != nil {
			nodeLog.Infof("failed to get host address :%v", err.Error())
			return nil, addr, err
		}
		addr = h.GrpcAddr()
	}
	c := s.getClientFromAddr(addr)
	return c, addr, nil
}

func (s *RemoteLogSender) getAllAddressesForPart() ([]string, error) {
	var addrs []string
	if strings.HasPrefix(s.remoteClusterAddr, "test://") {
		addrs = append(addrs, s.remoteClusterAddr[len("test://"):])
	} else {
		if s.zanCluster == nil {
			s.zanCluster = s.getZanCluster()
			if s.zanCluster == nil {
				return nil, errors.New("failed to init remote zankv cluster")
			}
		}
		hlist, err := s.zanCluster.GetAllHostsByPart(s.pid)
		if err != nil {
			nodeLog.Infof("failed to get all hosts address :%v", err.Error())
			return nil, err
		}
		for _, h := range hlist {
			addrs = append(addrs, h.GrpcAddr())
		}
	}
	return addrs, nil
}

func (s *RemoteLogSender) doSendOnce(r []*BatchInternalRaftRequest) error {
	if s.remoteClusterAddr == "" {
		nodeLog.Infof("sending log with no remote: %v", r)
		return nil
	}
	c, addr, err := s.getClient()
	if c == nil {
		nodeLog.Infof("sending(%v) log failed to get grpc client: %v", addr, err)
		return errors.New("failed to get grpc client")
	}
	raftLogs := make([]*syncerpb.RaftLogData, len(r))
	for i, e := range r {
		var rld syncerpb.RaftLogData
		raftLogs[i] = &rld
		raftLogs[i].Type = syncerpb.EntryNormalRaw
		raftLogs[i].Data, _ = e.Marshal()
		raftLogs[i].Term = e.OrigTerm
		raftLogs[i].Index = e.OrigIndex
		raftLogs[i].RaftTimestamp = e.Timestamp
		raftLogs[i].RaftGroupName = s.grpName
	}

	in := &syncerpb.RaftReqs{RaftLog: raftLogs}
	if nodeLog.Level() >= common.LOG_DETAIL {
		nodeLog.Debugf("sending log : %v", addr, in.String())
	}
	ctx, cancel := context.WithTimeout(context.Background(), sendLogTimeout)
	defer cancel()
	rpcErr, err := c.ApplyRaftReqs(ctx, in)
	if err != nil {
		nodeLog.Infof("sending(%v) log failed: %v,  %v", addr, err.Error(), in.String())
		return err
	}
	if rpcErr != nil && rpcErr.ErrCode != http.StatusOK &&
		rpcErr.ErrCode != 0 {
		nodeLog.Infof("sending(%v) log failed: %v,  %v", addr, rpcErr, in.String())
		return errors.New(rpcErr.String())
	}
	return nil
}

func (s *RemoteLogSender) notifyTransferSnap(raftSnapshot raftpb.Snapshot, syncAddr string, syncPath string) error {
	if s.remoteClusterAddr == "" {
		return nil
	}
	c, addr, err := s.getClient()
	if c == nil {
		nodeLog.Infof("failed to get grpc client(%v): %v", addr, err)
		return errors.New("failed to get grpc client")
	}
	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()
	req := &syncerpb.RaftApplySnapReq{
		ClusterName:   s.localCluster,
		RaftGroupName: s.grpName,
		Term:          raftSnapshot.Metadata.Term,
		Index:         raftSnapshot.Metadata.Index,
		SyncAddr:      syncAddr,
		SyncPath:      syncPath,
	}
	rsp, err := c.NotifyTransferSnap(ctx, req)
	if err != nil {
		nodeLog.Infof("failed to notify transfer snap from: %v, %v", addr, err)
		return err
	}
	if rsp != nil && rsp.ErrCode != 0 && rsp.ErrCode != http.StatusOK {
		nodeLog.Infof("notify apply snapshot failed: %v,  %v", addr, rsp)
		return errors.New(rsp.String())
	}
	return nil
}

func (s *RemoteLogSender) notifyApplySkippedSnap(raftSnapshot raftpb.Snapshot) error {
	return s.notifyApplySnapWithOption(true, raftSnapshot)
}

func (s *RemoteLogSender) notifyApplySnap(raftSnapshot raftpb.Snapshot) error {
	return s.notifyApplySnapWithOption(false, raftSnapshot)
}

func (s *RemoteLogSender) notifyApplySnapWithOption(skip bool, raftSnapshot raftpb.Snapshot) error {
	if s.remoteClusterAddr == "" {
		return nil
	}
	c, addr, err := s.getClient()
	if c == nil {
		nodeLog.Infof("failed to get grpc client(%v): %v", addr, err)
		return errors.New("failed to get grpc client")
	}
	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()
	req := &syncerpb.RaftApplySnapReq{
		ClusterName:   s.localCluster,
		RaftGroupName: s.grpName,
		Term:          raftSnapshot.Metadata.Term,
		Index:         raftSnapshot.Metadata.Index,
	}
	if skip {
		req.Type = syncerpb.SkippedSnap
	}
	rsp, err := c.NotifyApplySnap(ctx, req)
	if err != nil {
		nodeLog.Infof("failed to notify apply snap from: %v, %v", addr, err)
		return err
	}
	if rsp != nil && rsp.ErrCode != 0 && rsp.ErrCode != http.StatusOK {
		nodeLog.Infof("notify apply snapshot failed: %v,  %v", addr, rsp)
		return errors.New(rsp.String())
	}
	return nil
}

func (s *RemoteLogSender) getApplySnapStatus(raftSnapshot raftpb.Snapshot, addr string) (*syncerpb.RaftApplySnapStatusRsp, error) {
	var applyStatus syncerpb.RaftApplySnapStatusRsp
	if s.remoteClusterAddr == "" {
		return &applyStatus, nil
	}
	c := s.getClientFromAddr(addr)
	if c == nil {
		nodeLog.Infof("failed to get grpc client(%v)", addr)
		return &applyStatus, errors.New("failed to get grpc client")
	}
	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()
	req := &syncerpb.RaftApplySnapStatusReq{
		ClusterName:   s.localCluster,
		RaftGroupName: s.grpName,
		Term:          raftSnapshot.Metadata.Term,
		Index:         raftSnapshot.Metadata.Index,
	}
	rsp, err := c.GetApplySnapStatus(ctx, req)
	if err != nil {
		nodeLog.Infof("failed to get apply snap status from: %v, %v", addr, err)
		return nil, err
	}
	if rsp == nil {
		return &applyStatus, errors.New("nil snap status rsp")
	}
	nodeLog.Infof("apply snapshot status: %v,  %v", addr, rsp.String())
	applyStatus = *rsp
	return &applyStatus, nil
}

func (s *RemoteLogSender) waitApplySnapStatus(raftSnapshot raftpb.Snapshot, stop chan struct{}) error {
	// first, query and wait all replicas to finish snapshot transfer
	// if all done, notify apply the transferred snapshot and wait all done
	// then wait all apply done.
	for {
		select {
		case <-stop:
			return common.ErrStopped
		default:
		}
		addrs, err := s.getAllAddressesForPart()
		if err != nil {
			return err
		}
		// wait all became ApplyTransferSuccess or ApplySuccess
		allReady := true
		allTransferReady := true
		needWait := false
		for _, addr := range addrs {
			applyStatus, err := s.getApplySnapStatus(raftSnapshot, addr)
			if err != nil {
				return err
			}
			if applyStatus.Status != syncerpb.ApplySuccess {
				allReady = false
			}
			if applyStatus.Status != syncerpb.ApplySuccess && applyStatus.Status != syncerpb.ApplyTransferSuccess {
				allTransferReady = false
			}
			if applyStatus.Status == syncerpb.ApplyWaiting || applyStatus.Status == syncerpb.ApplyWaitingTransfer ||
				applyStatus.Status == syncerpb.ApplyUnknown {
				needWait = true
			}
			if applyStatus.Status == syncerpb.ApplyFailed {
				nodeLog.Infof("node %v failed to apply snapshot : %v", addr, applyStatus)
				return errors.New("some node failed to apply snapshot")
			}
		}
		if needWait {
			select {
			case <-stop:
				return common.ErrStopped
			case <-time.After(time.Second):
				continue
			}
		}
		if allReady {
			break
		}
		if allTransferReady {
			s.notifyApplySnap(raftSnapshot)
			time.Sleep(time.Second)
		} else {
			return errors.New("some node failed to apply snapshot")
		}
	}
	return nil
}

func (s *RemoteLogSender) getRemoteSyncedRaftOnce() (SyncedState, error) {
	var state SyncedState
	if s.remoteClusterAddr == "" {
		return state, nil
	}
	c, addr, err := s.getClient()
	if c == nil {
		nodeLog.Infof("failed to get grpc client(%v): %v", addr, err)
		return state, errors.New("failed to get grpc client")
	}
	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()
	req := &syncerpb.SyncedRaftReq{ClusterName: s.localCluster, RaftGroupName: s.grpName}
	rsp, err := c.GetSyncedRaft(ctx, req)
	if err != nil {
		nodeLog.Infof("failed to get synced raft from: %v, %v", addr, err)
		return state, err
	}
	if rsp == nil {
		return state, nil
	}
	state.SyncedTerm = rsp.Term
	state.SyncedIndex = rsp.Index
	nodeLog.Debugf("remote(%v) raft group %v synced : %v", addr, s.grpName, state)
	return state, nil
}

type RaftRpcFunc func() error

func sendRpcAndRetry(raftRpc RaftRpcFunc, rpcMethodName string, stop chan struct{}) error {
	retry := 0
	for {
		retry++
		err := raftRpc()
		if err != nil {
			nodeLog.Infof("failed to do rpc %s (retried %v): %v", rpcMethodName,
				retry, err.Error())
			wait := time.Millisecond * 100 * time.Duration(retry)
			if wait > time.Second*30 {
				wait = time.Second * 30
				nodeLog.Errorf("failed too much times do rpc %s (retried %v): %v", rpcMethodName, retry, err.Error())
			}
			select {
			case <-stop:
				return err
			case <-time.After(wait):
				continue
			}
		} else {
			return nil
		}
	}
}

func (s *RemoteLogSender) getRemoteSyncedRaft(stop chan struct{}) (SyncedState, error) {
	var state SyncedState
	err := sendRpcAndRetry(func() error {
		var err error
		state, err = s.getRemoteSyncedRaftOnce()
		return err
	}, "getRemoteSyncedRaft", stop)
	return state, err
}

func (s *RemoteLogSender) sendRaftLog(r []*BatchInternalRaftRequest, stop chan struct{}) error {
	if len(r) == 0 {
		return nil
	}
	first := r[0]
	err := sendRpcAndRetry(func() error {
		err := s.doSendOnce(r)
		if err != nil {
			nodeLog.Infof("failed to send raft log : %v, at %v-%v",
				err.Error(), first.OrigTerm, first.OrigIndex)
		}
		return err
	}, "sendRaftLog", stop)
	return err
}

func (s *RemoteLogSender) sendAndWaitApplySkippedSnap(raftSnap raftpb.Snapshot, stop chan struct{}) error {
	err := sendRpcAndRetry(func() error {
		return s.notifyApplySkippedSnap(raftSnap)
	}, "notifyApplySkippedSnap", stop)

	return err
}
