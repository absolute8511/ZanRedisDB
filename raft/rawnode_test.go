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

package raft

import (
	"bytes"
	"fmt"
	"math"
	"reflect"
	"testing"

	"github.com/youzan/ZanRedisDB/raft/raftpb"
)

// TestRawNodeStep ensures that RawNode.Step ignore local message.
func TestRawNodeStep(t *testing.T) {
	for i, msgn := range raftpb.MessageType_name {
		t.Run(msgn, func(t *testing.T) {
			s := NewMemoryStorage()
			defer s.Close()
			s.SetHardState(raftpb.HardState{Term: 1, Commit: 1})
			s.Append([]raftpb.Entry{{Term: 1, Index: 1}})
			peerGrps := make([]*raftpb.Group, 0)
			for _, pid := range []uint64{1} {
				grp := raftpb.Group{
					NodeId:        pid,
					RaftReplicaId: pid,
					GroupId:       1,
				}
				peerGrps = append(peerGrps, &grp)
			}
			if err := s.ApplySnapshot(raftpb.Snapshot{Metadata: raftpb.SnapshotMetadata{
				ConfState: raftpb.ConfState{
					Nodes:  []uint64{1},
					Groups: peerGrps,
				},
				Index: 1,
				Term:  1,
			}}); err != nil {
				t.Fatal(err)
			}

			rawNode, err := NewRawNode(newTestConfig(1, nil, 10, 1, s))
			if err != nil {
				t.Fatal(err)
			}
			msgt := raftpb.MessageType(i)
			err = rawNode.Step(raftpb.Message{Type: msgt})
			// LocalMsg should be ignored.
			if IsLocalMsg(msgt) {
				if err != ErrStepLocalMsg {
					t.Errorf("%d: step should ignore %s", msgt, msgn)
				}
			}
		})
	}
}

// TestNodeStepUnblock from node_test.go has no equivalent in rawNode because there is
// no goroutine in RawNode.

// TestRawNodeProposeAndConfChange ensures that RawNode.Propose and RawNode.ProposeConfChange
// send the given proposal and ConfChange to the underlying raft.
func TestRawNodeProposeAndConfChange(t *testing.T) {
	s := NewMemoryStorage()
	defer s.Close()
	var err error
	rawNode, err := NewRawNode(newTestConfig(1, []uint64{1}, 10, 1, s))
	if err != nil {
		t.Fatal(err)
	}

	rawNode.Campaign()
	proposed := false
	var (
		lastIndex uint64
		ccdata    []byte
	)
	for {
		rd := rawNode.Ready(true)
		s.Append(rd.Entries)
		rawNode.Advance(rd)
		// Once we are the leader, propose a command and a ConfChange.
		if !proposed && rd.SoftState.Lead == rawNode.raft.id {
			if err = rawNode.Propose([]byte("somedata")); err != nil {
				t.Fatal(err)
			}

			grp := raftpb.Group{
				NodeId:        1,
				GroupId:       1,
				RaftReplicaId: 1,
			}
			cc := raftpb.ConfChange{Type: raftpb.ConfChangeAddNode, ReplicaID: 1, NodeGroup: grp}
			ccdata, err = cc.Marshal()
			if err != nil {
				t.Fatal(err)
			}
			rawNode.ProposeConfChange(cc)

			proposed = true
		} else if proposed {
			// We proposed last cycle, which means we appended the conf change
			// in this cycle.
			lastIndex, err = s.LastIndex()
			if err != nil {
				t.Fatal(err)
			}
			break
		}
	}

	entries, err := s.Entries(lastIndex-1, lastIndex+1, noLimit)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("len(entries) = %d, want %d", len(entries), 2)
	}
	if !bytes.Equal(entries[0].Data, []byte("somedata")) {
		t.Errorf("entries[0].Data = %v, want %v", entries[0].Data, []byte("somedata"))
	}
	if entries[1].Type != raftpb.EntryConfChange {
		t.Fatalf("type = %v, want %v", entries[1].Type, raftpb.EntryConfChange)
	}
	if !bytes.Equal(entries[1].Data, ccdata) {
		t.Errorf("data = %v, want %v", entries[1].Data, ccdata)
	}
}

// TestRawNodeProposeAddDuplicateNode ensures that two proposes to add the same node should
// not affect the later propose to add new node.
func TestRawNodeProposeAddDuplicateNode(t *testing.T) {
	s := NewMemoryStorage()
	defer s.Close()
	rawNode, err := NewRawNode(newTestConfig(1, []uint64{1}, 10, 1, s))
	if err != nil {
		t.Fatal(err)
	}
	rd := rawNode.Ready(true)
	s.Append(rd.Entries)
	rawNode.Advance(rd)

	rawNode.Campaign()
	for {
		rd = rawNode.Ready(true)
		s.Append(rd.Entries)
		if rd.SoftState.Lead == rawNode.raft.id {
			rawNode.Advance(rd)
			break
		}
		rawNode.Advance(rd)
	}

	proposeConfChangeAndApply := func(cc raftpb.ConfChange) {
		rawNode.ProposeConfChange(cc)
		rd = rawNode.Ready(true)
		s.Append(rd.Entries)
		for _, entry := range rd.CommittedEntries {
			if entry.Type == raftpb.EntryConfChange {
				var cc raftpb.ConfChange
				cc.Unmarshal(entry.Data)
				rawNode.ApplyConfChange(cc)
			}
		}
		rawNode.Advance(rd)
	}

	cc1 := raftpb.ConfChange{Type: raftpb.ConfChangeAddNode, ReplicaID: 1}
	ccdata1, err := cc1.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	proposeConfChangeAndApply(cc1)

	// try to add the same node again
	proposeConfChangeAndApply(cc1)

	// the new node join should be ok
	cc2 := raftpb.ConfChange{Type: raftpb.ConfChangeAddNode, ReplicaID: 2}
	ccdata2, err := cc2.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	proposeConfChangeAndApply(cc2)

	lastIndex, err := s.LastIndex()
	if err != nil {
		t.Fatal(err)
	}

	// the last three entries should be: ConfChange cc1, cc1, cc2
	entries, err := s.Entries(lastIndex-2, lastIndex+1, noLimit)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 3 {
		t.Fatalf("len(entries) = %d, want %d", len(entries), 3)
	}
	if !bytes.Equal(entries[0].Data, ccdata1) {
		t.Errorf("entries[0].Data = %v, want %v", entries[0].Data, ccdata1)
	}
	if !bytes.Equal(entries[2].Data, ccdata2) {
		t.Errorf("entries[2].Data = %v, want %v", entries[2].Data, ccdata2)
	}
}

// TestRawNodeReadIndex ensures that Rawnode.ReadIndex sends the MsgReadIndex message
// to the underlying raft. It also ensures that ReadState can be read out.
func TestRawNodeReadIndex(t *testing.T) {
	msgs := []raftpb.Message{}
	appendStep := func(r *raft, m raftpb.Message) bool {
		msgs = append(msgs, m)
		return false
	}
	wrs := []ReadState{{Index: uint64(1), RequestCtx: []byte("somedata")}}

	s := NewMemoryStorage()
	defer s.Close()
	c := newTestConfig(1, []uint64{1}, 10, 1, s)
	rawNode, err := NewRawNode(c)
	if err != nil {
		t.Fatal(err)
	}
	rawNode.raft.readStates = wrs
	// ensure the ReadStates can be read out
	hasReady := rawNode.HasReady()
	if !hasReady {
		t.Errorf("HasReady() returns %t, want %t", hasReady, true)
	}
	rd := rawNode.Ready(true)
	if !reflect.DeepEqual(rd.ReadStates, wrs) {
		t.Errorf("ReadStates = %d, want %d", rd.ReadStates, wrs)
	}
	s.Append(rd.Entries)
	rawNode.Advance(rd)
	// ensure raft.readStates is reset after advance
	if rawNode.raft.readStates != nil {
		t.Errorf("readStates = %v, want %v", rawNode.raft.readStates, nil)
	}

	wrequestCtx := []byte("somedata2")
	rawNode.Campaign()
	for {
		rd = rawNode.Ready(true)
		s.Append(rd.Entries)

		if rd.SoftState.Lead == rawNode.raft.id {
			rawNode.Advance(rd)

			// Once we are the leader, issue a ReadIndex request
			rawNode.raft.step = appendStep
			rawNode.ReadIndex(wrequestCtx)
			break
		}
		rawNode.Advance(rd)
	}
	// ensure that MsgReadIndex message is sent to the underlying raft
	if len(msgs) != 1 {
		t.Fatalf("len(msgs) = %d, want %d", len(msgs), 1)
	}
	if msgs[0].Type != raftpb.MsgReadIndex {
		t.Errorf("msg type = %d, want %d", msgs[0].Type, raftpb.MsgReadIndex)
	}
	if !bytes.Equal(msgs[0].Entries[0].Data, wrequestCtx) {
		t.Errorf("data = %v, want %v", msgs[0].Entries[0].Data, wrequestCtx)
	}
}

// TestBlockProposal from node_test.go has no equivalent in rawNode because there is
// no leader check in RawNode.

// TestNodeTick from node_test.go has no equivalent in rawNode because
// it reaches into the raft object which is not exposed.

// TestNodeStop from node_test.go has no equivalent in rawNode because there is
// no goroutine in RawNode.

// TestRawNodeStart ensures that a node can be started correctly. Note that RawNode
// requires the application to bootstrap the state, i.e. it does not accept peers
// and will not create faux configuration change entries.
func TestRawNodeStart(t *testing.T) {
	want := Ready{
		SoftState: &SoftState{Lead: 1, RaftState: StateLeader},
		HardState: raftpb.HardState{Term: 1, Commit: 3, Vote: 1},
		Entries: []raftpb.Entry{
			{Term: 1, Index: 2, Data: nil},
			{Term: 1, Index: 3, Data: []byte("foo")},
		},
		CommittedEntries: []raftpb.Entry{
			{Term: 1, Index: 2, Data: nil},
			{Term: 1, Index: 3, Data: []byte("foo")}},
		MustSync: true,
	}

	storage := NewRealMemoryStorage()
	defer storage.Close()
	storage.ents[0].Index = 1

	// TODO(tbg): this is a first prototype of what bootstrapping could look
	// like (without the annoying faux ConfChanges). We want to persist a
	// ConfState at some index and make sure that this index can't be reached
	// from log position 1, so that followers are forced to pick up the
	// ConfState in order to move away from log position 1 (unless they got
	// bootstrapped in the same way already). Failing to do so would mean that
	// followers diverge from the bootstrapped nodes and don't learn about the
	// initial config.
	//
	// NB: this is exactly what CockroachDB does. The Raft log really begins at
	// index 10, so empty followers (at index 1) always need a snapshot first.
	type appenderStorage interface {
		Storage
		ApplySnapshot(raftpb.Snapshot) error
	}
	bootstrap := func(storage appenderStorage, cs raftpb.ConfState) error {
		if len(cs.Nodes) == 0 {
			return fmt.Errorf("no voters specified")
		}
		fi, err := storage.FirstIndex()
		if err != nil {
			return err
		}
		if fi < 2 {
			return fmt.Errorf("FirstIndex >= 2 is prerequisite for bootstrap")
		}
		if _, err = storage.Entries(fi, fi, math.MaxUint64); err == nil {
			// TODO(tbg): match exact error
			return fmt.Errorf("should not have been able to load first index")
		}
		li, err := storage.LastIndex()
		if err != nil {
			return err
		}
		if _, err = storage.Entries(li, li, math.MaxUint64); err == nil {
			return fmt.Errorf("should not have been able to load last index")
		}
		hs, ics, err := storage.InitialState()
		if err != nil {
			return err
		}
		if !IsEmptyHardState(hs) {
			return fmt.Errorf("HardState not empty")
		}
		if len(ics.Nodes) != 0 {
			return fmt.Errorf("ConfState not empty")
		}

		meta := raftpb.SnapshotMetadata{
			Index:     1,
			Term:      0,
			ConfState: cs,
		}
		snap := raftpb.Snapshot{Metadata: meta}
		return storage.ApplySnapshot(snap)
	}

	grp := raftpb.Group{
		NodeId:        1,
		GroupId:       1,
		RaftReplicaId: 1,
	}
	if err := bootstrap(storage, raftpb.ConfState{Nodes: []uint64{1}, Groups: []*raftpb.Group{&grp}}); err != nil {
		t.Fatal(err)
	}

	rawNode, err := NewRawNode(newTestConfig(1, nil, 10, 1, storage))
	if err != nil {
		t.Fatal(err)
	}

	if rawNode.HasReady() {
		t.Fatalf("unexpected ready: %+v", rawNode.Ready(true))
	}

	rawNode.Campaign()
	rawNode.Propose([]byte("foo"))
	if !rawNode.HasReady() {
		t.Fatal("expected a Ready")
	}
	rd := rawNode.Ready(true)
	storage.Append(rd.Entries)
	rawNode.Advance(rd)

	rd.SoftState, want.SoftState = nil, nil

	if !reflect.DeepEqual(rd, want) {
		t.Fatalf("unexpected Ready:\n%+v\nvs\n%+v", rd, want)
	}

	if rawNode.HasReady() {
		t.Errorf("unexpected Ready: %+v", rawNode.Ready(true))
	}
}

func TestRawNodeRestart(t *testing.T) {
	entries := []raftpb.Entry{
		{Term: 1, Index: 1},
		{Term: 1, Index: 2, Data: []byte("foo")},
	}
	st := raftpb.HardState{Term: 1, Commit: 1}

	want := Ready{
		HardState: emptyState,
		// commit up to commit index in st
		CommittedEntries: entries[:st.Commit],
		MustSync:         false,
	}

	storage := NewMemoryStorage()
	defer storage.Close()
	storage.SetHardState(st)
	storage.Append(entries)
	rawNode, err := NewRawNode(newTestConfig(1, []uint64{1}, 10, 1, storage))
	if err != nil {
		t.Fatal(err)
	}
	rd := rawNode.Ready(true)
	if !reflect.DeepEqual(rd, want) {
		t.Errorf("g = %+v,\n             w   %+v", rd, want)
	}
	rawNode.Advance(rd)
	if rawNode.HasReady() {
		t.Errorf("unexpected Ready: %+v", rawNode.Ready(true))
	}
}

func TestRawNodeRestartFromSnapshot(t *testing.T) {
	peerGrps := make([]*raftpb.Group, 0)
	for _, pid := range []uint64{1, 2} {
		grp := raftpb.Group{
			NodeId:        pid,
			RaftReplicaId: pid,
			GroupId:       1,
		}
		peerGrps = append(peerGrps, &grp)
	}

	snap := raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			ConfState: raftpb.ConfState{Nodes: []uint64{1, 2}, Groups: peerGrps},
			Index:     2,
			Term:      1,
		},
	}
	entries := []raftpb.Entry{
		{Term: 1, Index: 3, Data: []byte("foo")},
	}
	st := raftpb.HardState{Term: 1, Commit: 3}

	want := Ready{
		HardState: emptyState,
		// commit up to commit index in st
		CommittedEntries: entries,
		MustSync:         false,
	}

	s := NewMemoryStorage()
	defer s.Close()
	s.SetHardState(st)
	s.ApplySnapshot(snap)
	s.Append(entries)
	rawNode, err := NewRawNode(newTestConfig(1, nil, 10, 1, s))
	if err != nil {
		t.Fatal(err)
	}
	if rd := rawNode.Ready(true); !reflect.DeepEqual(rd, want) {
		t.Errorf("g = %+v,\n             w   %+v", rd, want)
	} else {
		rawNode.Advance(rd)
	}
	if rawNode.HasReady() {
		t.Errorf("unexpected Ready: %+v", rawNode.HasReady())
	}
}

// TestNodeAdvance from node_test.go has no equivalent in rawNode because there is
// no dependency check between Ready() and Advance()

func TestRawNodeStatus(t *testing.T) {
	storage := NewMemoryStorage()
	defer storage.Close()
	rawNode, err := NewRawNode(newTestConfig(1, nil, 10, 1, storage))
	if err != nil {
		t.Fatal(err)
	}
	status := rawNode.Status()
	if status == nil {
		t.Errorf("expected status struct, got nil")
	}
}

// TestRawNodeCommitPaginationAfterRestart is the RawNode version of
// TestNodeCommitPaginationAfterRestart. The anomaly here was even worse as the
// Raft group would forget to apply entries:
//
// - node learns that index 11 is committed
// - nextEnts returns index 1..10 in CommittedEntries (but index 10 already
//   exceeds maxBytes), which isn't noticed internally by Raft
// - Commit index gets bumped to 10
// - the node persists the HardState, but crashes before applying the entries
// - upon restart, the storage returns the same entries, but `slice` takes a
//   different code path and removes the last entry.
// - Raft does not emit a HardState, but when the app calls Advance(), it bumps
//   its internal applied index cursor to 10 (when it should be 9)
// - the next Ready asks the app to apply index 11 (omitting index 10), losing a
//    write.
func TestRawNodeCommitPaginationAfterRestart(t *testing.T) {
	s := &ignoreSizeHintMemStorage{
		MemoryStorage: NewRealMemoryStorage(),
	}
	persistedHardState := raftpb.HardState{
		Term:   1,
		Vote:   1,
		Commit: 10,
	}

	s.hardState = persistedHardState
	s.ents = make([]raftpb.Entry, 10)
	var size uint64
	for i := range s.ents {
		ent := raftpb.Entry{
			Term:  1,
			Index: uint64(i + 1),
			Type:  raftpb.EntryNormal,
			Data:  []byte("a"),
		}

		s.ents[i] = ent
		size += uint64(ent.Size())
	}

	cfg := newTestConfig(1, []uint64{1}, 10, 1, s)
	// Set a MaxSizePerMsg that would suggest to Raft that the last committed entry should
	// not be included in the initial rd.CommittedEntries. However, our storage will ignore
	// this and *will* return it (which is how the Commit index ended up being 10 initially).
	cfg.MaxSizePerMsg = size - uint64(s.ents[len(s.ents)-1].Size()) - 1

	s.ents = append(s.ents, raftpb.Entry{
		Term:  1,
		Index: uint64(11),
		Type:  raftpb.EntryNormal,
		Data:  []byte("boom"),
	})

	rawNode, err := NewRawNode(cfg)
	if err != nil {
		t.Fatal(err)
	}

	for highestApplied := uint64(0); highestApplied != 11; {
		rd := rawNode.Ready(true)
		n := len(rd.CommittedEntries)
		if n == 0 {
			t.Fatalf("stopped applying entries at index %d", highestApplied)
		}
		if next := rd.CommittedEntries[0].Index; highestApplied != 0 && highestApplied+1 != next {
			t.Fatalf("attempting to apply index %d after index %d, leaving a gap", next, highestApplied)
		}
		highestApplied = rd.CommittedEntries[n-1].Index
		rawNode.Advance(rd)
		rawNode.Step(raftpb.Message{
			Type:   raftpb.MsgHeartbeat,
			To:     1,
			From:   1, // illegal, but we get away with it
			Term:   1,
			Commit: 11,
		})
	}
}

func BenchmarkStatusProgress(b *testing.B) {
	setup := func(members int) *RawNode {
		peers := make([]uint64, members)
		for i := range peers {
			peers[i] = uint64(i + 1)
		}
		cfg := newTestConfig(1, peers, 3, 1, NewMemoryStorage())
		cfg.Logger = discardLogger
		r := newRaft(cfg)
		r.becomeFollower(1, 1)
		r.becomeCandidate()
		r.becomeLeader()
		return &RawNode{raft: r}
	}

	for _, members := range []int{1, 3, 5, 100} {
		b.Run(fmt.Sprintf("members=%d", members), func(b *testing.B) {
			// NB: call getStatus through rn.Status because that incurs an additional
			// allocation.
			rn := setup(members)

			b.Run("Status", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					_ = rn.Status()
				}
			})

			b.Run("Status-example", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					s := rn.Status()
					var n uint64
					for _, pr := range s.Progress {
						n += pr.Match
					}
					_ = n
				}
			})

			b.Run("StatusWithoutProgress", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					_ = rn.StatusWithoutProgress()
				}
			})

			b.Run("WithProgress", func(b *testing.B) {
				b.ReportAllocs()
				visit := func(uint64, ProgressType, Progress) {}

				for i := 0; i < b.N; i++ {
					rn.WithProgress(visit)
				}
			})
			b.Run("WithProgress-example", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					var n uint64
					visit := func(_ uint64, _ ProgressType, pr Progress) {
						n += pr.Match
					}
					rn.WithProgress(visit)
					_ = n
				}
			})
		})
	}
}

func TestRawNodeConsumeReady(t *testing.T) {
	// Check that readyWithoutAccept() does not call acceptReady (which resets
	// the messages) but Ready() does.
	s := NewMemoryStorage()
	rn := newTestRawNode(1, []uint64{1}, 3, 1, s)
	m1 := raftpb.Message{Context: []byte("foo")}
	m2 := raftpb.Message{Context: []byte("bar")}

	// Inject first message, make sure it's visible via readyWithoutAccept.
	rn.raft.msgs = append(rn.raft.msgs, m1)
	rd := rn.readyWithoutAccept(true)
	if len(rd.Messages) != 1 || !reflect.DeepEqual(rd.Messages[0], m1) {
		t.Fatalf("expected only m1 sent, got %+v", rd.Messages)
	}
	if len(rn.raft.msgs) != 1 || !reflect.DeepEqual(rn.raft.msgs[0], m1) {
		t.Fatalf("expected only m1 in raft.msgs, got %+v", rn.raft.msgs)
	}
	// Now call Ready() which should move the message into the Ready (as opposed
	// to leaving it in both places).
	rd = rn.Ready(true)
	if len(rn.raft.msgs) > 0 {
		t.Fatalf("messages not reset: %+v", rn.raft.msgs)
	}
	if len(rd.Messages) != 1 || !reflect.DeepEqual(rd.Messages[0], m1) {
		t.Fatalf("expected only m1 sent, got %+v", rd.Messages)
	}
	// Add a message to raft to make sure that Advance() doesn't drop it.
	rn.raft.msgs = append(rn.raft.msgs, m2)
	rn.Advance(rd)
	if len(rn.raft.msgs) != 1 || !reflect.DeepEqual(rn.raft.msgs[0], m2) {
		t.Fatalf("expected only m2 in raft.msgs, got %+v", rn.raft.msgs)
	}
}
