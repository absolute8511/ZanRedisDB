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
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/absolute8511/ZanRedisDB/raft"
	"github.com/absolute8511/ZanRedisDB/raft/raftpb"
	"github.com/absolute8511/ZanRedisDB/stats"
	"github.com/absolute8511/ZanRedisDB/pkg/types"
	"golang.org/x/net/context"
)

func TestSendMessage(t *testing.T) {
	// member 1
	tr := &Transport{
		ID:         types.ID(1),
		ClusterID:  "1",
		Raft:       &fakeRaft{},
		TrStats:    newTrStats(),
		PeersStats: stats.NewPeersStats(),
	}
	tr.Start()
	srv := httptest.NewServer(tr.Handler())
	defer srv.Close()

	// member 2
	recvc := make(chan raftpb.Message, 1)
	p := &fakeRaft{recvc: recvc}
	tr2 := &Transport{
		ID:         types.ID(2),
		ClusterID:  "1",
		Raft:       p,
		TrStats:    newTrStats(),
		PeersStats: stats.NewPeersStats(),
	}
	tr2.Start()
	srv2 := httptest.NewServer(tr2.Handler())
	defer srv2.Close()

	tr.UpdatePeer(types.ID(2), []string{srv2.URL})
	defer tr.Stop()
	tr2.UpdatePeer(types.ID(1), []string{srv.URL})
	defer tr2.Stop()
	if !waitStreamWorking(tr.Get(types.ID(2)).(*peer)) {
		t.Fatalf("stream from 1 to 2 is not in work as expected")
	}

	data := []byte("some data")
	group1 := raftpb.Group{NodeId: 1, RaftReplicaId: 1, GroupId: 1}
	group2 := raftpb.Group{NodeId: 2, RaftReplicaId: 2, GroupId: 1}
	tests := []raftpb.Message{
		// these messages are set to send to itself, which facilitates testing.
		{Type: raftpb.MsgProp, From: 1, FromGroup: group1, To: 2, ToGroup: group2, Entries: []raftpb.Entry{{Data: data}}},
		{Type: raftpb.MsgApp, From: 1, FromGroup: group1, To: 2, ToGroup: group2, Term: 1, Index: 3, LogTerm: 0, Entries: []raftpb.Entry{{Index: 4, Term: 1, Data: data}}, Commit: 3},
		{Type: raftpb.MsgAppResp, From: 1, FromGroup: group1, To: 2, ToGroup: group2, Term: 1, Index: 3},
		{Type: raftpb.MsgVote, From: 1, FromGroup: group1, To: 2, ToGroup: group2, Term: 1, Index: 3, LogTerm: 0},
		{Type: raftpb.MsgVoteResp, From: 1, FromGroup: group1, To: 2, ToGroup: group2, Term: 1},
		{Type: raftpb.MsgSnap, From: 1, FromGroup: group1, To: 2, ToGroup: group2, Term: 1, Snapshot: raftpb.Snapshot{Metadata: raftpb.SnapshotMetadata{Index: 1000, Term: 1}, Data: data}},
		{Type: raftpb.MsgHeartbeat, From: 1, FromGroup: group1, To: 2, ToGroup: group2, Term: 1, Commit: 3},
		{Type: raftpb.MsgHeartbeatResp, From: 1, FromGroup: group1, To: 2, ToGroup: group2, Term: 1},
	}
	for i, tt := range tests {
		tr.Send([]raftpb.Message{tt})
		msg := <-recvc
		if !reflect.DeepEqual(msg, tt) {
			t.Errorf("#%d: msg = %+v, want %+v", i, msg, tt)
		}
	}
}

// TestSendMessageWhenStreamIsBroken tests that message can be sent to the
// remote in a limited time when all underlying connections are broken.
func TestSendMessageWhenStreamIsBroken(t *testing.T) {
	// member 1
	tr := &Transport{
		ID:         types.ID(1),
		ClusterID:  "1",
		Raft:       &fakeRaft{},
		TrStats:    newTrStats(),
		PeersStats: stats.NewPeersStats(),
	}
	tr.Start()
	srv := httptest.NewServer(tr.Handler())
	defer srv.Close()

	// member 2
	recvc := make(chan raftpb.Message, 1)
	p := &fakeRaft{recvc: recvc}
	tr2 := &Transport{
		ID:         types.ID(2),
		ClusterID:  "1",
		Raft:       p,
		TrStats:    newTrStats(),
		PeersStats: stats.NewPeersStats(),
	}
	tr2.Start()
	srv2 := httptest.NewServer(tr2.Handler())
	defer srv2.Close()

	tr.UpdatePeer(types.ID(2), []string{srv2.URL})
	defer tr.Stop()
	tr2.UpdatePeer(types.ID(1), []string{srv.URL})
	defer tr2.Stop()
	if !waitStreamWorking(tr.Get(types.ID(2)).(*peer)) {
		t.Fatalf("stream from 1 to 2 is not in work as expected")
	}

	// break the stream
	srv.CloseClientConnections()
	srv2.CloseClientConnections()

	group1 := raftpb.Group{NodeId: 1, RaftReplicaId: 1, GroupId: 1}
	group2 := raftpb.Group{NodeId: 2, RaftReplicaId: 2, GroupId: 1}
	var n int
	for {
		select {
		// TODO: remove this resend logic when we add retry logic into the code
		case <-time.After(time.Millisecond):
			n++
			tr.Send([]raftpb.Message{{Type: raftpb.MsgHeartbeat,
				From: 1, FromGroup: group1, To: 2, ToGroup: group2, Term: 1, Commit: 3}})
		case <-recvc:
			if n > 50 {
				t.Errorf("disconnection time = %dms, want < 50ms", n)
			}
			return
		}
	}
}

func newTrStats() *stats.TransportStats {
	ss := &stats.TransportStats{}
	ss.Initialize()
	return ss
}

func waitStreamWorking(p *peer) bool {
	for i := 0; i < 1000; i++ {
		time.Sleep(time.Millisecond)
		if _, ok := p.msgAppV2Writer.writec(); !ok {
			continue
		}
		if _, ok := p.writer.writec(); !ok {
			continue
		}
		return true
	}
	return false
}

type fakeRaft struct {
	recvc     chan<- raftpb.Message
	err       error
	removedID uint64
}

func (p *fakeRaft) Process(ctx context.Context, m raftpb.Message) error {
	select {
	case p.recvc <- m:
	default:
	}
	return p.err
}

func (p *fakeRaft) IsPeerRemoved(id uint64) bool { return id == p.removedID }

func (p *fakeRaft) ReportUnreachable(id uint64, g raftpb.Group) {}

func (p *fakeRaft) ReportSnapshot(id uint64, g raftpb.Group, status raft.SnapshotStatus) {}
