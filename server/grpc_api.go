package server

import (
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof"
	"time"

	context "golang.org/x/net/context"

	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/absolute8511/ZanRedisDB/node"
	"github.com/absolute8511/ZanRedisDB/syncerpb"
	"google.golang.org/grpc"
)

var (
	proposeTimeout = time.Second * 4
)

var syncClusterNetStats common.WriteStats
var syncClusterTotalStats common.WriteStats

var applyStatusMapping = map[int]syncerpb.RaftApplySnapStatus{
	0: syncerpb.ApplyUnknown,
	1: syncerpb.ApplyWaitingTransfer,
	2: syncerpb.ApplyWaitingTransfer,
	3: syncerpb.ApplyTransferSuccess,
	4: syncerpb.ApplyWaiting,
	5: syncerpb.ApplySuccess,
	6: syncerpb.ApplyFailed,
}

func (s *Server) GetSyncedRaft(ctx context.Context, req *syncerpb.SyncedRaftReq) (*syncerpb.SyncedRaftRsp, error) {
	var rsp syncerpb.SyncedRaftRsp
	kv := s.GetNamespaceFromFullName(req.RaftGroupName)
	if kv == nil || !kv.IsReady() {
		return &rsp, errRaftGroupNotReady
	}
	term, index, ts := kv.Node.GetRemoteClusterSyncedRaft(req.ClusterName)
	rsp.Term = term
	rsp.Index = index
	rsp.Timestamp = ts
	return &rsp, nil
}

func (s *Server) ApplyRaftReqs(ctx context.Context, reqs *syncerpb.RaftReqs) (*syncerpb.RpcErr, error) {
	var rpcErr syncerpb.RpcErr
	receivedTs := time.Now()
	// using batch in batch only when all the nodes are upgraded to support applying batch in batch
	//
	babs := make(map[string]*node.BatchInternalRaftRequest)
	useBatchInBatch := len(reqs.RaftLog) > 1
	for _, r := range reqs.RaftLog {
		if sLog.Level() >= common.LOG_DETAIL {
			sLog.Debugf("applying raft log from remote cluster syncer: %v", r.String())
		}
		kv := s.GetNamespaceFromFullName(r.RaftGroupName)
		if kv == nil || !kv.IsReady() {
			rpcErr.ErrCode = http.StatusNotFound
			rpcErr.ErrMsg = errRaftGroupNotReady.Error()
			return &rpcErr, nil
		}
		if r.Type != syncerpb.EntryNormalRaw {
			// unsupported other type
		}
		term, index, _ := kv.Node.GetRemoteClusterSyncedRaft(r.ClusterName)
		if r.Term < term || r.Index <= index {
			sLog.Infof("raft log already applied : %v, synced: %v-%v", r.String(), term, index)
			continue
		}

		// raft timestamp should be the same with the real raft request in data
		logStart := r.RaftTimestamp
		syncNetLatency := receivedTs.UnixNano() - logStart
		syncClusterNetStats.UpdateLatencyStats(syncNetLatency / time.Microsecond.Nanoseconds())
		if !useBatchInBatch {
			err := kv.Node.ProposeRawSyncerReqAndWait(r.Data, r.Term, r.Index, r.RaftTimestamp)
			if err != nil {
				sLog.Infof("propose failed: %v, err: %v", r.String(), err.Error())
				rpcErr.ErrCode = http.StatusInternalServerError
				rpcErr.ErrMsg = err.Error()
				return &rpcErr, nil
			}
			n := time.Now().UnixNano()
			syncLatency := n - r.RaftTimestamp
			syncClusterTotalStats.UpdateLatencyStats(syncLatency / time.Microsecond.Nanoseconds())
			break
		}
		var breq node.BatchInternalRaftRequest
		err := breq.Unmarshal(r.Data)
		if err != nil {
			rpcErr.ErrCode = http.StatusInternalServerError
			rpcErr.ErrMsg = err.Error()
			return &rpcErr, nil
		}
		bab, ok := babs[r.RaftGroupName]
		if !ok {
			var newReq node.BatchInternalRaftRequest
			newReq.BatchedReqs = append(newReq.BatchedReqs, &breq)
			newReq.OrigCluster = breq.OrigCluster
			newReq.OrigTerm = r.Term
			newReq.OrigIndex = r.Index
			newReq.Timestamp = r.RaftTimestamp
			babs[r.RaftGroupName] = &newReq
		} else {
			bab.BatchedReqs = append(bab.BatchedReqs, &breq)
			bab.OrigCluster = breq.OrigCluster
			bab.OrigTerm = r.Term
			bab.OrigIndex = r.Index
			bab.Timestamp = r.RaftTimestamp
		}
	}
	for name, reqs := range babs {
		kv := s.GetNamespaceFromFullName(name)
		if kv == nil || !kv.IsReady() {
			rpcErr.ErrCode = http.StatusNotFound
			rpcErr.ErrMsg = errRaftGroupNotReady.Error()
			return &rpcErr, nil
		}
		err := kv.Node.ProposeSyncerReqAndWait(nil, *reqs)
		if err != nil {
			sLog.Infof("propose failed: %v, err: %v", reqs.String(), err.Error())
			rpcErr.ErrCode = http.StatusInternalServerError
			rpcErr.ErrMsg = err.Error()
			return &rpcErr, nil
		}
		n := time.Now().UnixNano()
		for _, r := range reqs.BatchedReqs {
			syncLatency := n - r.Timestamp
			syncClusterTotalStats.UpdateLatencyStats(syncLatency / time.Microsecond.Nanoseconds())
		}
	}
	return &rpcErr, nil
}

func (s *Server) NotifyTransferSnap(ctx context.Context, req *syncerpb.RaftApplySnapReq) (*syncerpb.RpcErr, error) {
	var rpcErr syncerpb.RpcErr
	kv := s.GetNamespaceFromFullName(req.RaftGroupName)
	if kv == nil || !kv.IsReady() {
		rpcErr.ErrCode = http.StatusNotFound
		rpcErr.ErrMsg = errRaftGroupNotReady.Error()
		return &rpcErr, errRaftGroupNotReady
	}
	term, index, _ := kv.Node.GetRemoteClusterSyncedRaft(req.ClusterName)
	if req.Term < term || req.Index <= index {
		sLog.Infof("raft already applied : %v, synced: %v-%v", req.String(), term, index)
		return &rpcErr, nil
	}
	sLog.Infof("raft need transfer snapshot from remote: %v", req.String())
	kv.Node.BeginTransferRemoteSnap(req.ClusterName, req.Term, req.Index, req.SyncAddr, req.SyncPath)
	return &rpcErr, nil
}

func (s *Server) NotifyApplySnap(ctx context.Context, req *syncerpb.RaftApplySnapReq) (*syncerpb.RpcErr, error) {
	var rpcErr syncerpb.RpcErr
	kv := s.GetNamespaceFromFullName(req.RaftGroupName)
	if kv == nil || !kv.IsReady() {
		rpcErr.ErrCode = http.StatusNotFound
		rpcErr.ErrMsg = errRaftGroupNotReady.Error()
		return &rpcErr, errRaftGroupNotReady
	}
	term, index, _ := kv.Node.GetRemoteClusterSyncedRaft(req.ClusterName)
	if req.Term < term || req.Index <= index {
		sLog.Infof("raft already applied : %v, synced: %v-%v", req.String(), term, index)
		return &rpcErr, nil
	}
	sLog.Infof("raft need apply snapshot from remote: %v", req.String())
	skipSnap := false
	if req.Type == syncerpb.SkippedSnap {
		skipSnap = true
	}
	err := kv.Node.ApplyRemoteSnapshot(skipSnap, req.ClusterName, req.Term, req.Index)
	if err != nil {
		rpcErr.ErrMsg = err.Error()
		rpcErr.ErrCode = http.StatusInternalServerError
	}
	return &rpcErr, nil
}

func (s *Server) GetApplySnapStatus(ctx context.Context, req *syncerpb.RaftApplySnapStatusReq) (*syncerpb.RaftApplySnapStatusRsp, error) {
	var status syncerpb.RaftApplySnapStatusRsp
	kv := s.GetNamespaceFromFullName(req.RaftGroupName)
	if kv == nil || !kv.IsReady() {
		return &status, errRaftGroupNotReady
	}
	// if another is transferring, just return status for waiting
	term, index, _ := kv.Node.GetRemoteClusterSyncedRaft(req.ClusterName)
	if term >= req.Term && index >= req.Index {
		status.Status = syncerpb.ApplySuccess
	} else {
		ss, ok := kv.Node.GetApplyRemoteSnapStatus(req.ClusterName)
		if !ok {
			status.Status = syncerpb.ApplyMissing
		} else {
			status.Status, _ = applyStatusMapping[ss.StatusCode]
			status.StatusMsg = ss.Status
		}
	}
	sLog.Infof("raft apply snapshot from remote %v , status: %v", req.String(), status)
	return &status, nil
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
