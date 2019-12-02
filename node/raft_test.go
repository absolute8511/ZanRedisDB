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

package node

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/youzan/ZanRedisDB/common"
	"github.com/youzan/ZanRedisDB/pkg/pbutil"
	"github.com/youzan/ZanRedisDB/pkg/testutil"
	"github.com/youzan/ZanRedisDB/raft"
	"github.com/youzan/ZanRedisDB/raft/raftpb"
	"github.com/youzan/ZanRedisDB/transport/rafthttp"
	"golang.org/x/net/context"
)

type nodeRecorder struct{ testutil.Recorder }

func newNodeRecorder() *nodeRecorder       { return &nodeRecorder{&testutil.RecorderBuffered{}} }
func newNodeRecorderStream() *nodeRecorder { return &nodeRecorder{testutil.NewRecorderStream()} }
func newNodeNop() raft.Node                { return newNodeRecorder() }

func (n *nodeRecorder) Tick() bool {
	n.Record(testutil.Action{Name: "Tick"})
	return true
}
func (n *nodeRecorder) Campaign(ctx context.Context) error {
	n.Record(testutil.Action{Name: "Campaign"})
	return nil
}
func (n *nodeRecorder) Propose(ctx context.Context, data []byte) error {
	n.Record(testutil.Action{Name: "Propose", Params: []interface{}{data}})
	return nil
}

func (n *nodeRecorder) ProposeWithDrop(ctx context.Context, data []byte, cancel context.CancelFunc) error {
	n.Record(testutil.Action{Name: "Propose", Params: []interface{}{data}})
	return nil
}

func (n *nodeRecorder) ProposeConfChange(ctx context.Context, conf raftpb.ConfChange) error {
	n.Record(testutil.Action{Name: "ProposeConfChange"})
	return nil
}
func (n *nodeRecorder) Step(ctx context.Context, msg raftpb.Message) error {
	n.Record(testutil.Action{Name: "Step"})
	return nil
}
func (n *nodeRecorder) ConfChangedCh() <-chan raftpb.ConfChange                         { return nil }
func (n *nodeRecorder) HandleConfChanged(cc raftpb.ConfChange)                          { return }
func (n *nodeRecorder) EventNotifyCh() chan bool                                        { return nil }
func (n *nodeRecorder) NotifyEventCh()                                                  { return }
func (n *nodeRecorder) StepNode(bool, bool) (raft.Ready, bool)                          { return raft.Ready{}, true }
func (n *nodeRecorder) Status() raft.Status                                             { return raft.Status{} }
func (n *nodeRecorder) Ready() <-chan raft.Ready                                        { return nil }
func (n *nodeRecorder) TransferLeadership(ctx context.Context, lead, transferee uint64) {}
func (n *nodeRecorder) ReadIndex(ctx context.Context, rctx []byte) error                { return nil }
func (n *nodeRecorder) Advance(rd raft.Ready)                                           {}
func (n *nodeRecorder) ApplyConfChange(conf raftpb.ConfChange) *raftpb.ConfState {
	n.Record(testutil.Action{Name: "ApplyConfChange", Params: []interface{}{conf}})
	return &raftpb.ConfState{}
}

func (n *nodeRecorder) Stop() {
	n.Record(testutil.Action{Name: "Stop"})
}

func (n *nodeRecorder) ReportUnreachable(id uint64, g raftpb.Group) {}

func (n *nodeRecorder) ReportSnapshot(id uint64, g raftpb.Group, status raft.SnapshotStatus) {}

func (n *nodeRecorder) Compact(index uint64, nodes []uint64, d []byte) {
	n.Record(testutil.Action{Name: "Compact"})
}

// readyNode is a nodeRecorder with a user-writeable ready channel
type readyNode struct {
	nodeRecorder
	readyc chan raft.Ready
	c      chan bool
}

func newReadyNode() *readyNode {
	return &readyNode{
		nodeRecorder: nodeRecorder{testutil.NewRecorderStream()},
		c:            make(chan bool, 1),
		readyc:       make(chan raft.Ready, 1)}
}

func newNopReadyNode() *readyNode {
	return &readyNode{*newNodeRecorder(), make(chan raft.Ready, 1), make(chan bool, 1)}
}

func (n *readyNode) EventNotifyCh() chan bool { return n.c }

func (n *readyNode) pushReady(rd raft.Ready) {
	select {
	case n.readyc <- rd:
	}
	select {
	case n.c <- true:
	default:
	}
}

func (n *readyNode) StepNode(bool, bool) (raft.Ready, bool) {
	select {
	case rd := <-n.readyc:
		return rd, true
	default:
	}
	return raft.Ready{}, false
}

type storageRecorder struct {
	testutil.Recorder
	dbPath string // must have '/' suffix if set
}

func NewStorageRecorder(db string) *storageRecorder {
	return &storageRecorder{&testutil.RecorderBuffered{}, db}
}

func NewStorageRecorderStream(db string) *storageRecorder {
	return &storageRecorder{testutil.NewRecorderStream(), db}
}

func (p *storageRecorder) Save(st raftpb.HardState, ents []raftpb.Entry) error {
	p.Record(testutil.Action{Name: "Save"})
	return nil
}

func (p *storageRecorder) Load() (*raftpb.Snapshot, string, error) {
	p.Record(testutil.Action{Name: "Load"})
	return nil, "", nil
}

func (p *storageRecorder) SaveSnap(st raftpb.Snapshot) error {
	if !raft.IsEmptySnap(st) {
		p.Record(testutil.Action{Name: "SaveSnap"})
	}
	return nil
}

func (p *storageRecorder) DBFilePath(id uint64) (string, error) {
	p.Record(testutil.Action{Name: "DBFilePath"})
	path := p.dbPath
	if path != "" {
		path = path + "/"
	}
	return fmt.Sprintf("%s%016x.snap.db", path, id), nil
}

func (p *storageRecorder) Close() error { return nil }
func TestGetIDs(t *testing.T) {
	node1Group := &raftpb.Group{
		NodeId:        1,
		Name:          "group",
		GroupId:       1,
		RaftReplicaId: 1,
	}
	node2Group := &raftpb.Group{
		NodeId:        2,
		Name:          "testgroup",
		GroupId:       1,
		RaftReplicaId: 2,
	}

	addcc := &raftpb.ConfChange{Type: raftpb.ConfChangeAddNode, ReplicaID: 2, NodeGroup: *node2Group}
	addEntry := raftpb.Entry{Type: raftpb.EntryConfChange, Data: pbutil.MustMarshal(addcc)}
	removecc := &raftpb.ConfChange{Type: raftpb.ConfChangeRemoveNode, ReplicaID: 2, NodeGroup: *node2Group}
	removeEntry := raftpb.Entry{Type: raftpb.EntryConfChange, Data: pbutil.MustMarshal(removecc)}
	normalEntry := raftpb.Entry{Type: raftpb.EntryNormal}
	updatecc := &raftpb.ConfChange{Type: raftpb.ConfChangeUpdateNode, ReplicaID: 2, NodeGroup: *node2Group}
	updateEntry := raftpb.Entry{Type: raftpb.EntryConfChange, Data: pbutil.MustMarshal(updatecc)}

	tests := []struct {
		confState *raftpb.ConfState
		ents      []raftpb.Entry

		widSet []uint64
		grps   []*raftpb.Group
	}{
		{nil, []raftpb.Entry{}, []uint64{}, []*raftpb.Group{}},
		{&raftpb.ConfState{Nodes: []uint64{1}, Groups: []*raftpb.Group{node1Group}},
			[]raftpb.Entry{}, []uint64{1}, []*raftpb.Group{node1Group}},
		{&raftpb.ConfState{Nodes: []uint64{1}, Groups: []*raftpb.Group{node1Group}},
			[]raftpb.Entry{addEntry}, []uint64{1, 2}, []*raftpb.Group{node1Group, node2Group}},
		{&raftpb.ConfState{Nodes: []uint64{1}, Groups: []*raftpb.Group{node1Group}},
			[]raftpb.Entry{addEntry, removeEntry}, []uint64{1}, []*raftpb.Group{node1Group}},
		{&raftpb.ConfState{Nodes: []uint64{1}, Groups: []*raftpb.Group{node1Group}},
			[]raftpb.Entry{addEntry, normalEntry}, []uint64{1, 2}, []*raftpb.Group{node1Group, node2Group}},
		{&raftpb.ConfState{Nodes: []uint64{1}, Groups: []*raftpb.Group{node1Group}},
			[]raftpb.Entry{addEntry, normalEntry, updateEntry}, []uint64{1, 2}, []*raftpb.Group{node1Group, node2Group}},
		{&raftpb.ConfState{Nodes: []uint64{1}, Groups: []*raftpb.Group{node1Group}},
			[]raftpb.Entry{addEntry, removeEntry, normalEntry}, []uint64{1}, []*raftpb.Group{node1Group}},
	}

	for i, tt := range tests {
		var snap raftpb.Snapshot
		if tt.confState != nil {
			snap.Metadata.ConfState = *tt.confState
		}
		idSet, _ := getIDsAndGroups(&snap, tt.ents)
		if !reflect.DeepEqual(idSet, tt.widSet) {
			t.Errorf("#%d: idset = %#v, want %#v", i, idSet, tt.widSet)
		}
	}
}

func TestCreateConfigChangeEnts(t *testing.T) {
	node1Group := &raftpb.Group{
		NodeId:        1,
		Name:          "testgroup",
		GroupId:       1,
		RaftReplicaId: 1,
	}
	node2Group := &raftpb.Group{
		NodeId:        2,
		Name:          "testgroup",
		GroupId:       1,
		RaftReplicaId: 2,
	}

	node3Group := &raftpb.Group{
		NodeId:        3,
		Name:          "testgroup",
		GroupId:       1,
		RaftReplicaId: 3,
	}
	m1 := common.MemberInfo{
		ID:        uint64(1),
		NodeID:    1,
		GroupName: "testgroup",
		GroupID:   1,
	}
	m1.RaftURLs = append(m1.RaftURLs, "http://localhost:2380")
	ctx, err := json.Marshal(m1)
	if err != nil {
		t.Fatal(err)
	}

	addcc1 := &raftpb.ConfChange{Type: raftpb.ConfChangeAddNode, ReplicaID: 1, NodeGroup: *node1Group, Context: ctx}
	removecc2 := &raftpb.ConfChange{Type: raftpb.ConfChangeRemoveNode, ReplicaID: 2, NodeGroup: *node2Group}
	removecc3 := &raftpb.ConfChange{Type: raftpb.ConfChangeRemoveNode, ReplicaID: 3, NodeGroup: *node3Group}
	grps1 := make(map[uint64]raftpb.Group)
	grps1[1] = *node1Group

	grps12 := make(map[uint64]raftpb.Group)
	grps12[1] = *node1Group
	grps12[2] = *node2Group
	grps23 := make(map[uint64]raftpb.Group)
	grps23[2] = *node2Group
	grps23[3] = *node3Group

	grps123 := make(map[uint64]raftpb.Group)
	grps123[1] = *node1Group
	grps123[2] = *node2Group
	grps123[3] = *node3Group

	tests := []struct {
		ids         []uint64
		grps        map[uint64]raftpb.Group
		self        uint64
		term, index uint64

		wents []raftpb.Entry
	}{
		{
			[]uint64{1},
			grps1,
			1,
			1, 1,

			[]raftpb.Entry{},
		},
		{
			[]uint64{1, 2},
			grps12,
			1,
			1, 1,

			[]raftpb.Entry{{Term: 1, Index: 2, Type: raftpb.EntryConfChange, Data: pbutil.MustMarshal(removecc2)}},
		},
		{
			[]uint64{1, 2},
			grps12,
			1,
			2, 2,

			[]raftpb.Entry{{Term: 2, Index: 3, Type: raftpb.EntryConfChange, Data: pbutil.MustMarshal(removecc2)}},
		},
		{
			[]uint64{1, 2, 3},
			grps123,
			1,
			2, 2,

			[]raftpb.Entry{
				{Term: 2, Index: 3, Type: raftpb.EntryConfChange, Data: pbutil.MustMarshal(removecc2)},
				{Term: 2, Index: 4, Type: raftpb.EntryConfChange, Data: pbutil.MustMarshal(removecc3)},
			},
		},
		{
			[]uint64{2, 3},
			grps23,
			2,
			2, 2,

			[]raftpb.Entry{
				{Term: 2, Index: 3, Type: raftpb.EntryConfChange, Data: pbutil.MustMarshal(removecc3)},
			},
		},
		{
			[]uint64{2, 3},
			grps123,
			1,
			2, 2,

			[]raftpb.Entry{
				{Term: 2, Index: 3, Type: raftpb.EntryConfChange, Data: pbutil.MustMarshal(removecc2)},
				{Term: 2, Index: 4, Type: raftpb.EntryConfChange, Data: pbutil.MustMarshal(removecc3)},
				{Term: 2, Index: 5, Type: raftpb.EntryConfChange, Data: pbutil.MustMarshal(addcc1)},
			},
		},
	}

	for i, tt := range tests {
		m := common.MemberInfo{
			ID:        uint64(tt.self),
			NodeID:    tt.self,
			GroupName: "testgroup",
			GroupID:   1,
		}
		m.RaftURLs = append(m.RaftURLs, "http://localhost:2380")

		ctx, err := json.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}

		gents := createConfigChangeEnts(ctx, tt.ids, tt.grps, tt.self, tt.term, tt.index)
		if !reflect.DeepEqual(gents, tt.wents) {
			t.Errorf("#%d: ents = %v, want %v", i, gents, tt.wents)
			t.Errorf("ents = %s, want %s", gents[2].String(), tt.wents[2].String())
		}
	}
}

func TestStopRaftWhenWaitingForApplyDone(t *testing.T) {
	n := newNopReadyNode()
	commitC := make(chan applyInfo, 5000)
	config := &RaftConfig{
		GroupID:   1,
		GroupName: "testgroup",
		ID:        1,
		RaftAddr:  "127.0.0.1:1239",
	}

	r := raftNode{
		config:         config,
		commitC:        commitC,
		node:           n,
		persistStorage: NewStorageRecorder(""),
		raftStorage:    raft.NewMemoryStorage(),
		transport:      rafthttp.NewNopTransporter(),
		stopc:          make(chan struct{}),
	}
	done := make(chan struct{})
	go func() {
		r.serveChannels()
		close(done)
	}()
	n.pushReady(raft.Ready{
		Snapshot: raftpb.Snapshot{
			Metadata: raftpb.SnapshotMetadata{
				Term:  1,
				Index: 1,
			},
		},
	})
	select {
	case <-commitC:
	case <-time.After(time.Second):
		t.Fatalf("failed to receive apply struct")
	}

	close(r.stopc)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("failed to stop raft loop")
	}
}

// TestConfgChangeBlocksApply ensures apply blocks if committed entries contain config-change.
func TestConfgChangeBlocksApply(t *testing.T) {
	n := newNopReadyNode()

	config := &RaftConfig{
		GroupID:   1,
		GroupName: "testgroup",
		ID:        1,
		RaftAddr:  "127.0.0.1:1239",
	}
	commitC := make(chan applyInfo, 5)
	r := &raftNode{
		config:         config,
		commitC:        commitC,
		node:           n,
		persistStorage: NewStorageRecorder(""),
		raftStorage:    raft.NewMemoryStorage(),
		transport:      rafthttp.NewNopTransporter(),
		stopc:          make(chan struct{}),
	}

	go func() {
		r.serveChannels()
	}()
	defer close(r.stopc)

	n.pushReady(raft.Ready{
		SoftState:        &raft.SoftState{RaftState: raft.StateFollower},
		CommittedEntries: []raftpb.Entry{{Type: raftpb.EntryConfChange}},
	})
	blockingEnt := <-commitC
	if blockingEnt.applyWaitDone == nil {
		t.Fatalf("unexpected nil chan, should init wait channel for waiting apply conf change event")
	}

	continueC := make(chan struct{})
	go func() {
		n.pushReady(raft.Ready{
			Snapshot: raftpb.Snapshot{
				Metadata: raftpb.SnapshotMetadata{
					Term:  1,
					Index: 2,
				},
			},
		})
		<-commitC
		close(continueC)
	}()

	select {
	case <-continueC:
		t.Fatalf("unexpected execution: raft routine should block waiting for apply")
	case <-time.After(time.Second):
	}

	// finish apply, unblock raft routine
	close(blockingEnt.applyWaitDone)

	select {
	case <-continueC:
	case <-time.After(time.Second):
		t.Fatalf("unexpected blocking on execution")
	}
}

func TestSnapshotApplyingShouldNotBlock(t *testing.T) {
	// TODO: apply slow snapshot should not block raft loop for ticker and other messaages
}

func TestSnapshotPreTransferRetryOnFail(t *testing.T) {
	// TODO: a failed snapshot receive should retry after report failed
}
