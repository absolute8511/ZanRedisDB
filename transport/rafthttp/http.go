// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rafthttp

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"path"
	"strings"

	"github.com/absolute8511/ZanRedisDB/raft/raftpb"
	pioutil "github.com/coreos/etcd/pkg/ioutil"
	"github.com/coreos/etcd/pkg/types"
	"github.com/coreos/etcd/version"
	"golang.org/x/net/context"
)

const (
	// connReadLimitByte limits the number of bytes
	// a single read can read out.
	//
	// 64KB should be large enough for not causing
	// throughput bottleneck as well as small enough
	// for not causing a read timeout.
	connReadLimitByte = 64 * 1024
)

var (
	RaftPrefix         = "/raft"
	ProbingPrefix      = path.Join(RaftPrefix, "probing")
	RaftStreamPrefix   = path.Join(RaftPrefix, "stream")
	RaftSnapshotPrefix = path.Join(RaftPrefix, "snapshot")

	errIncompatibleVersion = errors.New("incompatible version")
	errClusterIDMismatch   = errors.New("cluster ID mismatch")
)

type peerGetter interface {
	Get(id types.ID) Peer
}

type writerToResponse interface {
	WriteTo(w http.ResponseWriter)
}

type pipelineHandler struct {
	tr  Transporter
	r   Raft
	cid string
}

// newPipelineHandler returns a handler for handling raft messages
// from pipeline for RaftPrefix.
//
// The handler reads out the raft message from request body,
// and forwards it to the given raft state machine for processing.
func newPipelineHandler(tr Transporter, r Raft, cid string) http.Handler {
	return &pipelineHandler{
		tr:  tr,
		r:   r,
		cid: cid,
	}
}

func (h *pipelineHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		w.Header().Set("Allow", "POST")
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("X-Etcd-Cluster-ID", h.cid)

	if err := checkClusterCompatibilityFromHeader(r.Header, h.cid); err != nil {
		http.Error(w, err.Error(), http.StatusPreconditionFailed)
		return
	}

	if from, err := types.IDFromString(r.Header.Get("X-Server-From")); err != nil {
		if urls := r.Header.Get("X-PeerURLs"); urls != "" {
			h.tr.AddRemote(from, strings.Split(urls, ","))
		}
	}

	// Limit the data size that could be read from the request body, which ensures that read from
	// connection will not time out accidentally due to possible blocking in underlying implementation.
	limitedr := pioutil.NewLimitedBufferReader(r.Body, connReadLimitByte)
	b, err := ioutil.ReadAll(limitedr)
	if err != nil {
		plog.Errorf("failed to read raft message (%v)", err)
		http.Error(w, "error reading raft message", http.StatusBadRequest)
		recvFailures.WithLabelValues(r.RemoteAddr).Inc()
		return
	}

	var m raftpb.Message
	if err := m.Unmarshal(b); err != nil {
		plog.Errorf("failed to unmarshal raft message (%v)", err)
		http.Error(w, "error unmarshaling raft message", http.StatusBadRequest)
		recvFailures.WithLabelValues(r.RemoteAddr).Inc()
		return
	}

	receivedBytes.WithLabelValues(m.FromGroup.String()).Add(float64(len(b)))

	if err := h.r.Process(context.TODO(), m); err != nil {
		switch v := err.(type) {
		case writerToResponse:
			v.WriteTo(w)
		default:
			plog.Warningf("failed to process raft message (%v)", err)
			http.Error(w, "error processing raft message", http.StatusInternalServerError)
			w.(http.Flusher).Flush()
			// disconnect the http stream
		}
		return
	}

	// Write StatusNoContent header after the message has been processed by
	// raft, which facilitates the client to report MsgSnap status.
	w.WriteHeader(http.StatusNoContent)
}

type ISnapSaver interface {
	SaveDBFrom(io.Reader, raftpb.Message) (int64, error)
}

type snapshotHandler struct {
	tr          Transporter
	r           Raft
	snapshotter ISnapSaver
	cid         string
}

func newSnapshotHandler(tr Transporter, r Raft, snapshotter ISnapSaver, cid string) http.Handler {
	return &snapshotHandler{
		tr:          tr,
		r:           r,
		snapshotter: snapshotter,
		cid:         cid,
	}
}

// ServeHTTP serves HTTP request to receive and process snapshot message.
//
// If request sender dies without closing underlying TCP connection,
// the handler will keep waiting for the request body until TCP keepalive
// finds out that the connection is broken after several minutes.
// This is acceptable because
// 1. snapshot messages sent through other TCP connections could still be
// received and processed.
// 2. this case should happen rarely, so no further optimization is done.
func (h *snapshotHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		w.Header().Set("Allow", "POST")
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("X-Etcd-Cluster-ID", h.cid)

	if err := checkClusterCompatibilityFromHeader(r.Header, h.cid); err != nil {
		http.Error(w, err.Error(), http.StatusPreconditionFailed)
		return
	}

	if from, err := types.IDFromString(r.Header.Get("X-Server-From")); err != nil {
		if urls := r.Header.Get("X-PeerURLs"); urls != "" {
			h.tr.AddRemote(from, strings.Split(urls, ","))
		}
	}

	dec := &messageDecoder{r: r.Body}
	m, err := dec.decode()
	if err != nil {
		msg := fmt.Sprintf("failed to decode raft message (%v)", err)
		plog.Errorf(msg)
		http.Error(w, msg, http.StatusBadRequest)
		recvFailures.WithLabelValues(r.RemoteAddr).Inc()
		return
	}

	receivedBytes.WithLabelValues(m.FromGroup.String()).Add(float64(m.Size()))

	if m.Type != raftpb.MsgSnap {
		plog.Errorf("unexpected raft message type %s on snapshot path", m.Type)
		http.Error(w, "wrong raft message type", http.StatusBadRequest)
		return
	}

	plog.Infof("receiving database snapshot [index:%d, from %s] ...", m.Snapshot.Metadata.Index, types.ID(m.From))
	// save incoming database snapshot.
	n, err := h.snapshotter.SaveDBFrom(r.Body, m)
	if err != nil {
		msg := fmt.Sprintf("failed to save KV snapshot (%v)", err)
		plog.Error(msg)
		http.Error(w, msg, http.StatusInternalServerError)
		return
	}
	receivedBytes.WithLabelValues(m.FromGroup.String()).Add(float64(n))
	plog.Infof("received and saved database snapshot [index: %d, from: %s] successfully", m.Snapshot.Metadata.Index, types.ID(m.From))

	if err := h.r.Process(context.TODO(), m); err != nil {
		switch v := err.(type) {
		// Process may return writerToResponse error when doing some
		// additional checks before calling raft.Node.Step.
		case writerToResponse:
			v.WriteTo(w)
		default:
			msg := fmt.Sprintf("failed to process raft message (%v)", err)
			plog.Warningf(msg)
			http.Error(w, msg, http.StatusInternalServerError)
		}
		return
	}
	// Write StatusNoContent header after the message has been processed by
	// raft, which facilitates the client to report MsgSnap status.
	w.WriteHeader(http.StatusNoContent)
}

type streamHandler struct {
	tr         *Transport
	peerGetter peerGetter
	r          Raft
	id         types.ID
	cid        string
}

func newStreamHandler(tr *Transport, pg peerGetter, r Raft, id types.ID, cid string) http.Handler {
	return &streamHandler{
		tr:         tr,
		peerGetter: pg,
		r:          r,
		id:         id,
		cid:        cid,
	}
}

func (h *streamHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		w.Header().Set("Allow", "GET")
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("X-Server-Version", version.Version)
	w.Header().Set("X-Etcd-Cluster-ID", h.cid)

	if err := checkClusterCompatibilityFromHeader(r.Header, h.cid); err != nil {
		http.Error(w, err.Error(), http.StatusPreconditionFailed)
		return
	}

	var t streamType
	switch path.Dir(r.URL.Path) {
	case streamTypeMsgAppV2.endpoint():
		t = streamTypeMsgAppV2
	case streamTypeMessage.endpoint():
		t = streamTypeMessage
	default:
		plog.Debugf("ignored unexpected streaming request path %s", r.URL.Path)
		http.Error(w, "invalid path", http.StatusNotFound)
		return
	}

	fromStr := path.Base(r.URL.Path)
	from, err := types.IDFromString(fromStr)
	if err != nil {
		plog.Errorf("failed to parse from %s into ID (%v)", fromStr, err)
		http.Error(w, "invalid from", http.StatusNotFound)
		return
	}
	//if h.r.IsIDRemoved(uint64(from)) {
	//	plog.Warningf("rejected the stream from peer %s since it was removed", from)
	//	http.Error(w, "removed member", http.StatusGone)
	//	return
	//}
	p := h.peerGetter.Get(from)
	if p == nil {
		// This may happen in following cases:
		// 1. user starts a remote peer that belongs to a different cluster
		// with the same cluster ID.
		// 2. local etcd falls behind of the cluster, and cannot recognize
		// the members that joined after its current progress.
		if urls := r.Header.Get("X-PeerURLs"); urls != "" {
			h.tr.AddRemote(from, strings.Split(urls, ","))
		}
		// while in multi raft group mode, it can happen if a new raft group is
		// init and not ready to process raft message
		//plog.Errorf("failed to find member %s in cluster %s", from, h.cid)
		http.Error(w, "error sender not found", http.StatusNotFound)
		return
	}

	wto := h.id.String()
	if gto := r.Header.Get("X-Raft-To"); gto != wto {
		plog.Errorf("streaming request ignored (ID mismatch got %s want %s)", gto, wto)
		http.Error(w, "to field mismatch", http.StatusPreconditionFailed)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.(http.Flusher).Flush()

	c := newCloseNotifier()
	conn := &outgoingConn{
		t:       t,
		Writer:  w,
		Flusher: w.(http.Flusher),
		Closer:  c,
	}
	p.attachOutgoingConn(conn)
	<-c.closeNotify()
}

// checkClusterCompatibilityFromHeader checks the cluster compatibility of
// the local member from the given header.
// It checks whether the version of local member is compatible with
// the versions in the header, and whether the cluster ID of local member
// matches the one in the header.
func checkClusterCompatibilityFromHeader(header http.Header, cid string) error {
	if err := checkVersionCompability(header.Get("X-Server-From"), serverVersion(header), minClusterVersion(header)); err != nil {
		plog.Errorf("request version incompatibility (%v)", err)
		return errIncompatibleVersion
	}
	if gcid := header.Get("X-Etcd-Cluster-ID"); gcid != cid {
		plog.Errorf("request cluster ID mismatch (got %s want %s)", gcid, cid)
		return errClusterIDMismatch
	}
	return nil
}

type closeNotifier struct {
	done chan struct{}
}

func newCloseNotifier() *closeNotifier {
	return &closeNotifier{
		done: make(chan struct{}),
	}
}

func (n *closeNotifier) Close() error {
	close(n.done)
	return nil
}

func (n *closeNotifier) closeNotify() <-chan struct{} { return n.done }
