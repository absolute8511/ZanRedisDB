package node

import (
	"context"
	"errors"
	"net/http"
	"strings"
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

// LogSender is the raft log sender. It will send all the raft logs
// to the remote cluster using grpc service.
type LogSender struct {
	grpName           string
	ns                string
	pid               int
	connPool          map[string]ccAPIClient
	zanCluster        *zanredisdb.Cluster
	remoteClusterAddr string
}

func NewLogSender(fullName string, remoteCluster string) (*LogSender, error) {
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
	return &LogSender{
		ns:                ns,
		pid:               pid,
		remoteClusterAddr: remoteCluster,
		grpName:           fullName,
		connPool:          make(map[string]ccAPIClient),
	}, nil
}

func (s *LogSender) getZanCluster() *zanredisdb.Cluster {
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

func (s *LogSender) Stop() {
	for _, c := range s.connPool {
		if c.conn != nil {
			c.conn.Close()
		}
	}
	if s.zanCluster != nil {
		s.zanCluster.Close()
	}
}

func (s *LogSender) GetStats() interface{} {
	return nil
}

func (s *LogSender) getClient(addr string) syncerpb.CrossClusterAPIClient {
	if c, ok := s.connPool[addr]; ok {
		return c.client
	}
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return nil
	}
	c := syncerpb.NewCrossClusterAPIClient(conn)
	s.connPool[addr] = ccAPIClient{client: c, conn: conn}
	return c
}

func (s *LogSender) doSendOnce(r []*BatchInternalRaftRequest) error {
	if s.remoteClusterAddr == "" {
		nodeLog.Infof("sending log with no remote: %v", r)
		return nil
	}
	var addr string
	if strings.HasPrefix(s.remoteClusterAddr, "test://") {
		addr = s.remoteClusterAddr[len("test://"):]
	} else {
		if s.zanCluster == nil {
			s.zanCluster = s.getZanCluster()
			if s.zanCluster == nil {
				return errors.New("failed to init remote zankv cluster")
			}
		}
		h, err := s.zanCluster.GetHostByPart(s.pid, true)
		if err != nil {
			nodeLog.Infof("failed to get host address :%v", err.Error())
			return err
		}
		addr = h.Addr()
	}
	c := s.getClient(addr)
	if c == nil {
		nodeLog.Infof("sending(%v) log failed to get grpc client", addr)
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
		nodeLog.Debugf("sending(%v) log : %v", addr, in.String())
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

func (s *LogSender) sendRaftLog(r []*BatchInternalRaftRequest, stop chan struct{}) error {
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
