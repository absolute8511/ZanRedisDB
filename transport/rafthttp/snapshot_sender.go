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
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"time"

	"golang.org/x/net/context"

	"github.com/youzan/ZanRedisDB/pkg/httputil"
	pioutil "github.com/youzan/ZanRedisDB/pkg/ioutil"
	"github.com/youzan/ZanRedisDB/pkg/types"
	"github.com/youzan/ZanRedisDB/raft"
	"github.com/youzan/ZanRedisDB/raft/raftpb"
	"github.com/youzan/ZanRedisDB/snap"
)

var (
	// timeout for reading snapshot response body
	snapResponseReadTimeout   = 5 * time.Second
	snapTransferCheckInterval = 5 * time.Second
)

type snapshotSender struct {
	from, to types.ID
	cid      string

	tr     *Transport
	picker *urlPicker
	status *peerStatus
	r      Raft
	errorc chan error

	stopc chan struct{}
}

func newSnapshotSender(tr *Transport, picker *urlPicker, to types.ID, status *peerStatus) *snapshotSender {
	return &snapshotSender{
		from:   tr.ID,
		to:     to,
		cid:    tr.ClusterID,
		tr:     tr,
		picker: picker,
		status: status,
		r:      tr.Raft,
		errorc: tr.ErrorC,
		stopc:  make(chan struct{}),
	}
}

func (s *snapshotSender) stop() {
	close(s.stopc)
}

func (s *snapshotSender) send(merged snap.Message) {
	m := merged.Message

	body := createSnapBody(merged)
	defer body.Close()

	u := s.picker.pick()
	req := createPostRequest(u, RaftSnapshotPrefix, body, "application/octet-stream", s.tr.URLs, s.from, s.cid)

	plog.Infof("start to send database snapshot [index: %d, to %s]...",
		m.Snapshot.Metadata.Index, m.ToGroup.String())

	err := s.post(req)
	defer merged.CloseWithError(err)
	if err != nil {
		plog.Warningf("database snapshot [index: %d, to: %s] failed to be sent out (%v)",
			m.Snapshot.Metadata.Index, m.ToGroup.String(), err)

		// errMemberRemoved is a critical error since a removed member should
		// always be stopped. So we use reportCriticalError to report it to errorc.
		if err == errMemberRemoved {
			reportCriticalError(err, s.errorc)
		}

		s.picker.unreachable(u)
		s.status.deactivate(failureType{source: sendSnap, action: "post"}, err.Error())
		s.r.ReportUnreachable(m.To, m.ToGroup)
		// report SnapshotFailure to raft state machine. After raft state
		// machine knows about it, it would pause a while and retry sending
		// new snapshot message.
		s.r.ReportSnapshot(m.To, m.ToGroup, raft.SnapshotFailure)
		sentFailures.WithLabelValues(m.ToGroup.String()).Inc()
		return
	}
	sentBytes.WithLabelValues(m.ToGroup.String()).Add(float64(merged.TotalSize))
	// send health check until all snapshot data is done

	s.status.activate()
	go func() {
		tk := time.NewTicker(snapTransferCheckInterval)
		defer tk.Stop()
		done := false
		var err error
		for !done {
			select {
			case <-s.stopc:
				s.r.ReportSnapshot(m.To, m.ToGroup, raft.SnapshotFailure)
				return
			case <-tk.C:
				// check snapshot status, consider status code 404/400 as old and skip
				done, err = s.checkSnapshotStatus(m)
				if err != nil {
					plog.Infof("database snapshot [index: %d, to: %s] sent check failed: %v",
						m.Snapshot.Metadata.Index, m.ToGroup.String(), err.Error())
					s.r.ReportSnapshot(m.To, m.ToGroup, raft.SnapshotFailure)
					return
				}
				if !done {
					plog.Infof("database snapshot [index: %d, to: %s] sent still transferring",
						m.Snapshot.Metadata.Index, m.ToGroup.String())
				}
			}
		}
		s.r.ReportSnapshot(m.To, m.ToGroup, raft.SnapshotFinish)
		plog.Infof("database snapshot [index: %d, to: %s] sent out successfully",
			m.Snapshot.Metadata.Index, m.ToGroup.String())
	}()
}

func (s *snapshotSender) checkSnapshotStatus(m raftpb.Message) (bool, error) {
	buf := &bytes.Buffer{}
	enc := &messageEncoder{w: buf}
	// encode raft message
	if err := enc.encode(&m); err != nil {
		return false, err
	}
	u := s.picker.pick()
	req := createPostRequest(u, RaftSnapshotCheckPrefix, buf, "application/octet-stream", s.tr.URLs, s.from, s.cid)

	ctx, cancel := context.WithTimeout(context.Background(), snapTransferCheckInterval)
	req = req.WithContext(ctx)
	defer cancel()
	resp, err := s.tr.pipelineRt.RoundTrip(req)
	if err != nil {
		return false, err
	}
	switch resp.StatusCode {
	case http.StatusNoContent:
		// snapshot transfer finished
		return true, nil
	case http.StatusAlreadyReported:
		// waiting snapshot transfer
		return false, nil
	case http.StatusNotFound, http.StatusForbidden, http.StatusMethodNotAllowed, http.StatusBadRequest:
		// old version, no waiting
		return true, nil
	default:
		plog.Infof("not expected response while check snapshot: %v", resp.Status)
		return false, errors.New(resp.Status)
	}
}

// post posts the given request.
// It returns nil when request is sent out and processed successfully.
func (s *snapshotSender) post(req *http.Request) (err error) {
	ctx, cancel := context.WithCancel(context.Background())
	req = req.WithContext(ctx)
	defer cancel()

	type responseAndError struct {
		resp *http.Response
		body []byte
		err  error
	}
	result := make(chan responseAndError, 1)

	go func() {
		resp, err := s.tr.pipelineRt.RoundTrip(req)
		if err != nil {
			result <- responseAndError{resp, nil, err}
			return
		}

		// close the response body when timeouts.
		// prevents from reading the body forever when the other side dies right after
		// successfully receives the request body.
		time.AfterFunc(snapResponseReadTimeout, func() { httputil.GracefulClose(resp) })
		body, err := ioutil.ReadAll(resp.Body)
		result <- responseAndError{resp, body, err}
	}()

	select {
	case <-s.stopc:
		return errStopped
	case r := <-result:
		if r.err != nil {
			return r.err
		}
		return checkPostResponse(r.resp, r.body, req, s.to)
	}
}

func createSnapBody(merged snap.Message) io.ReadCloser {
	buf := new(bytes.Buffer)
	enc := &messageEncoder{w: buf}
	// encode raft message
	if err := enc.encode(&merged.Message); err != nil {
		plog.Panicf("encode message error (%v)", err)
	}

	return &pioutil.ReaderAndCloser{
		Reader: io.MultiReader(buf, merged.ReadCloser),
		Closer: merged.ReadCloser,
	}
}
