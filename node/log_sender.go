package node

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/absolute8511/ZanRedisDB/syncerpb"
	"github.com/absolute8511/go-zanredisdb"
	"google.golang.org/grpc"
)

var (
	errInvalidRemoteCluster = errors.New("remote cluster is not valid")
	errInvalidNamespace     = errors.New("namespace is not valid")
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
		DialTimeout:  time.Second * 2,
		ReadTimeout:  time.Second * 2,
		WriteTimeout: time.Second * 2,
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
		addr = h.Addr()
	}
	c := s.getClientFromAddr(addr)
	return c, addr, nil
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
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	rpcErr, err := c.ApplyRaftReqs(ctx, in)
	if err != nil {
		nodeLog.Infof("sending(%v) log failed: %v,  %v", addr, err.Error(), in.String())
		return err
	}
	if rpcErr != nil && rpcErr.ErrCode != http.StatusOK &&
		rpcErr.ErrCode != 0 {
		nodeLog.Infof("sending(%v) log failed: %v,  %v", addr, rpcErr, in.String())
		return errors.New(rpcErr.ErrMsg)
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
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	req := &syncerpb.SyncedRaftReq{ClusterName: s.localCluster, RaftGroupName: s.grpName}
	rsp, err := c.GetSyncedRaft(ctx, req)
	if err != nil {
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

func (s *RemoteLogSender) getRemoteSyncedRaft(stop chan struct{}) (SyncedState, error) {
	retry := 0
	for {
		retry++
		state, err := s.getRemoteSyncedRaftOnce()
		if err != nil {
			nodeLog.Infof("failed to get remote synced raft (retried %v): %v", retry, err.Error())
			wait := time.Millisecond * 100 * time.Duration(retry)
			if wait > time.Second*30 {
				wait = time.Second * 30
			}
			select {
			case <-stop:
				return state, err
			case <-time.After(wait):
				continue
			}
		} else {
			return state, nil
		}
	}
}

func (s *RemoteLogSender) sendRaftLog(r []*BatchInternalRaftRequest, stop chan struct{}) error {
	retry := 0
	for {
		retry++
		err := s.doSendOnce(r)
		if err != nil {
			nodeLog.Infof("failed to send raft log (retried %v): %v", retry, err.Error())
			wait := time.Millisecond * 100 * time.Duration(retry)
			if wait > time.Second*30 {
				wait = time.Second * 30
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
