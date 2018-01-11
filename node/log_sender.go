package node

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/absolute8511/ZanRedisDB/syncerpb"
	"github.com/absolute8511/go-zanredisdb"
	"google.golang.org/grpc"
)

type ccAPIClient struct {
	client syncerpb.CrossClusterAPIClient
	conn   *grpc.ClientConn
}

type LogSender struct {
	recvCh            chan *BatchInternalRaftRequest
	stopChan          chan struct{}
	grpName           string
	ns                string
	pid               int
	connPool          map[string]ccAPIClient
	zanCluster        *zanredisdb.Cluster
	remoteClusterAddr string
}

func NewLogSender(fullName string, remoteCluster string, stopChan chan struct{}) (*LogSender, error) {
	ns, pid := common.GetNamespaceAndPartition(fullName)
	return &LogSender{
		stopChan:          stopChan,
		recvCh:            make(chan *BatchInternalRaftRequest),
		ns:                ns,
		pid:               pid,
		remoteClusterAddr: remoteCluster,
		grpName:           fullName,
	}, nil
}

func (s *LogSender) GetChan() chan *BatchInternalRaftRequest {
	return s.recvCh
}

func (s *LogSender) getZanCluster() *zanredisdb.Cluster {
	if s.remoteClusterAddr == "" {
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

func (s *LogSender) Start() {
	s.getZanCluster()
	s.sendRaftLogLoop()
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
	conn, err := grpc.Dial(addr)
	if err != nil {
		return nil
	}
	c := syncerpb.NewCrossClusterAPIClient(conn)
	s.connPool[addr] = ccAPIClient{client: c, conn: conn}
	return c
}

func (s *LogSender) doSendOnce(r *BatchInternalRaftRequest) error {
	if s.remoteClusterAddr == "" {
		nodeLog.Infof("sending log with no remote: %v-%v, %v", r.OrigTerm, r.OrigIndex, r.String())
		return nil
	}
	if s.zanCluster == nil {
		s.zanCluster = s.getZanCluster()
		if s.zanCluster == nil {
			return errors.New("failed to init remote zankv cluster")
		}
	}
	h, err := s.zanCluster.GetHostByPart(s.pid, true)
	if err != nil {
		nodeLog.Infof("failed to get host address :%v,  %v-%v", err.Error(), r.OrigTerm, r.OrigIndex)
		return err
	}
	addr := h.Addr()
	c := s.getClient(addr)
	if c == nil {
		nodeLog.Infof("sending(%v) log failed to get grpc client", addr, r.OrigTerm, r.OrigIndex)
		return errors.New("failed to get grpc client")
	}
	raftLogs := make([]*syncerpb.RaftLogData, 1)
	raftLogs[0].Type = syncerpb.EntryNormalRaw
	raftLogs[0].Data, _ = r.Marshal()
	raftLogs[0].Term = r.OrigTerm
	raftLogs[0].Index = r.OrigIndex
	raftLogs[0].RaftTimestamp = r.Timestamp
	raftLogs[0].RaftGroupName = s.grpName

	in := &syncerpb.RaftReqs{RaftLog: raftLogs}
	if nodeLog.Level() >= common.LOG_DETAIL {
		nodeLog.Debugf("sending(%v) log : %v-%v, %v", addr, r.OrigTerm, r.OrigIndex, r.String())
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	rpcErr, err := c.ApplyRaftReqs(ctx, in)
	if err != nil {
		nodeLog.Infof("sending(%v) log failed: %v,  %v-%v, %v", addr, err.Error(), r.OrigTerm, r.OrigIndex, r.String())
		return err
	}
	if rpcErr != nil && rpcErr.ErrCode != http.StatusOK {
		nodeLog.Infof("sending(%v) log failed: %v,  %v-%v, %v", addr, rpcErr, r.OrigTerm, r.OrigIndex, r.String())
		return errors.New(rpcErr.ErrMsg)
	}
	return nil
}

func (s *LogSender) sendRaftLogLoop() {
	defer func() {
		nodeLog.Infof("raft log send loop exit")
	}()

	for {
		select {
		case r := <-s.recvCh:
			for {
				err := s.doSendOnce(r)
				if err != nil {
					nodeLog.Infof("failed to send raft log: %v, %v", err.Error(), r.String())
					select {
					case <-s.stopChan:
						return
					case <-time.After(time.Millisecond * 100):
						continue
					}
				} else {
					break
				}
			}
		case <-s.stopChan:
			return
		}
	}
}
