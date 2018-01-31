package server

import (
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof"
	"time"

	context "golang.org/x/net/context"

	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/absolute8511/ZanRedisDB/syncerpb"
	"google.golang.org/grpc"
)

var (
	proposeTimeout = time.Second * 4
)

func (s *Server) GetSyncedRaft(ctx context.Context, req *syncerpb.SyncedRaftReq) (*syncerpb.SyncedRaftRsp, error) {
	var rsp syncerpb.SyncedRaftRsp
	kv := s.GetNamespaceFromFullName(req.RaftGroupName)
	if kv == nil || !kv.IsReady() {
		return &rsp, errRaftGroupNotReady
	}
	term, index := kv.Node.GetRemoteClusterSyncedRaft(req.ClusterName)
	rsp.Term = term
	rsp.Index = index
	return &rsp, nil
}

func (s *Server) ApplyRaftReqs(ctx context.Context, reqs *syncerpb.RaftReqs) (*syncerpb.RpcErr, error) {
	var rpcErr syncerpb.RpcErr
	for _, r := range reqs.RaftLog {
		if sLog.Level() >= common.LOG_DETAIL {
			sLog.Debugf("applying raft log from remote cluster syncer: %v", r.String())
		}
		kv := s.GetNamespaceFromFullName(r.RaftGroupName)
		if kv == nil || !kv.IsReady() {
			rpcErr.ErrCode = http.StatusNotFound
			rpcErr.ErrMsg = "namespace node not found"
			return &rpcErr, nil
		}
		if r.Type != syncerpb.EntryNormalRaw {
			// unsupported other type
		}

		// raft timestamp should be the same with the real raft request in data
		logStart := r.RaftTimestamp
		localStart := time.Now().UnixNano()
		var cost int64
		err := kv.Node.ProposeRawAndWait(r.Data, r.Term, r.Index, r.RaftTimestamp)
		if err != nil {
			sLog.Infof("propose failed: %v, err: %v", r.String(), err.Error())
			rpcErr.ErrCode = http.StatusInternalServerError
			rpcErr.ErrMsg = err.Error()
			return &rpcErr, nil
		}
		cost = time.Now().UnixNano() - localStart
		if cost >= int64(proposeTimeout.Nanoseconds())/2 {
			sLog.Infof("slow for batch propose: %v, cost %v", r.RaftGroupName, cost)
		}
		// TODO: store log sync latency between clusters
		syncLatency := cost + localStart - logStart
		_ = syncLatency
	}
	return &rpcErr, nil
}

// serveHttpKVAPI starts a key-value server with a GET/PUT API and listens.
func (s *Server) serveGRPCAPI(port int, stopC <-chan struct{}) error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return err
	}
	sLog.Infof("begin grpc server at port: %v", port)
	rpcServer := grpc.NewServer()
	syncerpb.RegisterCrossClusterAPIServer(rpcServer, s)
	go func() {
		<-stopC
		sLog.Infof("begin stopping grpc server")
		rpcServer.GracefulStop()
	}()
	err = rpcServer.Serve(lis)
	// exit when raft goes down
	sLog.Infof("grpc server stopped")
	if err != nil {
		return err
	}
	return nil
}
