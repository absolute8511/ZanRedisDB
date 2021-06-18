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
	"errors"
	"fmt"
	"time"

	pb "github.com/youzan/ZanRedisDB/raft/raftpb"
	"golang.org/x/net/context"
)

type SnapshotStatus int

const (
	SnapshotFinish   SnapshotStatus = 1
	SnapshotFailure  SnapshotStatus = 2
	queueWaitTime                   = time.Millisecond * 10
	recvQueueLen                    = 1024 * 16
	proposalQueueLen                = 1024 * 4
)

var (
	emptyState = pb.HardState{}

	// ErrStopped is returned by methods on Nodes that have been stopped.
	ErrStopped           = errors.New("raft: stopped")
	errMsgDropped        = errors.New("raft message dropped")
	errProposalAddFailed = errors.New("add proposal to queue failed")
)

// SoftState provides state that is useful for logging and debugging.
// The state is volatile and does not need to be persisted to the WAL.
type SoftState struct {
	Lead      uint64 // must use atomic operations to access; keep 64-bit aligned.
	RaftState StateType
}

func (a *SoftState) equal(b *SoftState) bool {
	return a.Lead == b.Lead && a.RaftState == b.RaftState
}

// Ready encapsulates the entries and messages that are ready to read,
// be saved to stable storage, committed or sent to other peers.
// All fields in Ready are read-only.
type Ready struct {
	// The current volatile state of a Node.
	// SoftState will be nil if there is no update.
	// It is not required to consume or store SoftState.
	*SoftState

	// The current state of a Node to be saved to stable storage BEFORE
	// Messages are sent.
	// HardState will be equal to empty state if there is no update.
	pb.HardState

	// ReadStates can be used for node to serve linearizable read requests locally
	// when its applied index is greater than the index in ReadState.
	// Note that the readState will be returned when raft receives msgReadIndex.
	// The returned is only valid for the request that requested to read.
	ReadStates []ReadState

	// Entries specifies entries to be saved to stable storage BEFORE
	// Messages are sent.
	Entries []pb.Entry

	// Snapshot specifies the snapshot to be saved to stable storage.
	Snapshot pb.Snapshot

	// CommittedEntries specifies entries to be committed to a
	// store/state-machine. These have previously been committed to stable
	// store.
	CommittedEntries []pb.Entry
	// Whether there are more committed entries ready to be applied.
	MoreCommittedEntries bool
	// Messages specifies outbound messages to be sent AFTER Entries are
	// committed to stable storage.
	// If it contains a MsgSnap message, the application MUST report back to raft
	// when the snapshot has been received or has failed by calling ReportSnapshot.
	Messages []pb.Message

	// MustSync indicates whether the HardState and Entries must be synchronously
	// written to disk or if an asynchronous write is permissible.
	MustSync bool
}

func isHardStateEqual(a, b pb.HardState) bool {
	return a.Term == b.Term && a.Vote == b.Vote && a.Commit == b.Commit
}

// IsEmptyHardState returns true if the given HardState is empty.
func IsEmptyHardState(st pb.HardState) bool {
	return isHardStateEqual(st, emptyState)
}

// IsEmptySnap returns true if the given Snapshot is empty.
func IsEmptySnap(sp pb.Snapshot) bool {
	return sp.Metadata.Index == 0
}

func (rd Ready) containsUpdates() bool {
	return rd.SoftState != nil || !IsEmptyHardState(rd.HardState) ||
		!IsEmptySnap(rd.Snapshot) || len(rd.Entries) > 0 ||
		len(rd.CommittedEntries) > 0 || len(rd.Messages) > 0 || len(rd.ReadStates) != 0
}

// appliedCursor extracts from the Ready the highest index the client has
// applied (once the Ready is confirmed via Advance). If no information is
// contained in the Ready, returns zero.
func (rd Ready) appliedCursor() uint64 {
	if n := len(rd.CommittedEntries); n > 0 {
		return rd.CommittedEntries[n-1].Index
	}
	if index := rd.Snapshot.Metadata.Index; index > 0 {
		return index
	}
	return 0
}

type msgWithDrop struct {
	m      pb.Message
	dropCB context.CancelFunc
}

// Node represents a node in a raft cluster.
type Node interface {
	// Tick increments the internal logical clock for the Node by a single tick. Election
	// timeouts and heartbeat timeouts are in units of ticks.
	Tick() bool
	// Campaign causes the Node to transition to candidate state and start campaigning to become leader.
	Campaign(ctx context.Context) error
	// Propose proposes that data be appended to the log.
	Propose(ctx context.Context, data []byte) error
	// Propose proposed that data be appended to the log and cancel it if dropped
	ProposeWithDrop(ctx context.Context, data []byte, cancel context.CancelFunc) error
	ProposeEntryWithDrop(ctx context.Context, e pb.Entry, cancel context.CancelFunc) error
	// ProposeConfChange proposes config change.
	// At most one ConfChange can be in the process of going through consensus.
	// Application needs to call ApplyConfChange when applying EntryConfChange type entry.
	ProposeConfChange(ctx context.Context, cc pb.ConfChange) error
	// Step advances the state machine using the given message. ctx.Err() will be returned, if any.
	Step(ctx context.Context, msg pb.Message) error

	// StepNode handle raft events and returns the current point-in-time state.
	// Users of the Node must call Advance after retrieving the state returned by StepNode.
	//
	// NOTE: No committed entries from the next Ready may be applied until all committed entries
	// and snapshots from the previous one have finished.
	StepNode(moreApplyEntries bool, busySnap bool) (Ready, bool)
	// EventNotifyCh is used to notify or receive the notify for the raft event
	EventNotifyCh() chan bool
	// NotifyEventCh will notify the raft loop event to check new event
	NotifyEventCh()

	ConfChangedCh() <-chan pb.ConfChange
	// HandleConfChanged will  handle configure change event
	HandleConfChanged(cc pb.ConfChange)
	// Advance notifies the Node that the application has saved progress up to the last Ready.
	// It prepares the node to return the next available Ready.
	//
	// The application should generally call Advance after it applies the entries in last Ready.
	//
	// However, as an optimization, the application may call Advance while it is applying the
	// commands. For example. when the last Ready contains a snapshot, the application might take
	// a long time to apply the snapshot data. To continue receiving Ready without blocking raft
	// progress, it can call Advance before finishing applying the last ready.
	Advance(rd Ready)
	// ApplyConfChange applies config change to the local node.
	// Returns an opaque ConfState protobuf which must be recorded
	// in snapshots. Will never return nil; it returns a pointer only
	// to match MemoryStorage.Compact.
	ApplyConfChange(cc pb.ConfChange) *pb.ConfState

	// TransferLeadership attempts to transfer leadership to the given transferee.
	TransferLeadership(ctx context.Context, lead, transferee uint64)

	// ReadIndex request a read state. The read state will be set in the ready.
	// Read state has a read index. Once the application advances further than the read
	// index, any linearizable read requests issued before the read request can be
	// processed safely. The read state will have the same rctx attached.
	ReadIndex(ctx context.Context, rctx []byte) error

	// Status returns the current status of the raft state machine.
	Status() Status
	// ReportUnreachable reports the given node is not reachable for the last send.
	ReportUnreachable(id uint64, group pb.Group)
	// ReportSnapshot reports the status of the sent snapshot.
	ReportSnapshot(id uint64, group pb.Group, status SnapshotStatus)
	// Stop performs any necessary termination of the Node.
	Stop()
	DebugString() string
}

type Peer struct {
	NodeID    uint64
	ReplicaID uint64
	Context   []byte
}

type prevState struct {
	havePrevLastUnstablei bool
	prevLastUnstablei     uint64
	prevLastUnstablet     uint64
	prevSnapi             uint64
	prevSoftSt            *SoftState
	prevHardSt            pb.HardState
	prevLead              uint64
}

func newPrevState(r *raft) *prevState {
	return &prevState{
		prevSoftSt: r.softState(),
		prevHardSt: emptyState,
		prevLead:   None,
	}
}

// StartNode returns a new Node given configuration and a list of raft peers.
// It appends a ConfChangeAddNode entry for each given peer to the initial log.
func StartNode(c *Config, peers []Peer, isLearner bool) Node {
	if isLearner {
		c.learners = append(c.learners, c.Group)
	}
	r := newRaft(c)
	// become the follower at term 1 and apply initial configuration
	// entries of term 1
	r.becomeFollower(1, None)
	for _, peer := range peers {
		cc := pb.ConfChange{Type: pb.ConfChangeAddNode, ReplicaID: peer.ReplicaID,
			NodeGroup: pb.Group{NodeId: peer.NodeID, Name: r.group.Name, GroupId: r.group.GroupId,
				RaftReplicaId: peer.ReplicaID},
			Context: peer.Context}
		d, err := cc.Marshal()
		if err != nil {
			panic("unexpected marshal error")
		}
		e := pb.Entry{Type: pb.EntryConfChange, Term: 1, Index: r.raftLog.lastIndex() + 1, Data: d}
		r.raftLog.append(e)
	}
	// Mark these initial entries as committed.
	// TODO(bdarnell): These entries are still unstable; do we need to preserve
	// the invariant that committed < unstable?
	r.raftLog.committed = r.raftLog.lastIndex()
	// Now apply them, mainly so that the application can call Campaign
	// immediately after StartNode in tests. Note that these nodes will
	// be added to raft twice: here and when the application's Ready
	// loop calls ApplyConfChange. The calls to addNode must come after
	// all calls to raftLog.append so progress.next is set after these
	// bootstrapping entries (it is an error if we try to append these
	// entries since they have already been committed).
	// We do not set raftLog.applied so the application will be able
	// to observe all conf changes via Ready.CommittedEntries.
	for _, peer := range peers {
		r.addNode(peer.ReplicaID, pb.Group{NodeId: peer.NodeID, Name: r.group.Name, GroupId: r.group.GroupId,
			RaftReplicaId: peer.ReplicaID})
	}

	n := newNode()
	n.logger = c.Logger
	n.r = r
	n.prevS = newPrevState(r)
	off := max(r.raftLog.applied+1, r.raftLog.firstIndex())
	n.lastSteppedIndex = off
	n.NotifyEventCh()
	return &n
}

// RestartNode is similar to StartNode but does not take a list of peers.
// The current membership of the cluster will be restored from the Storage.
// If the caller has an existing state machine, pass in the last log index that
// has been applied to it; otherwise use zero.
func RestartNode(c *Config) Node {
	r := newRaft(c)

	n := newNode()
	n.logger = c.Logger
	n.r = r
	n.prevS = newPrevState(r)
	off := max(r.raftLog.applied+1, r.raftLog.firstIndex())
	n.lastSteppedIndex = off
	n.NotifyEventCh()
	return &n
}

// node is the canonical implementation of the Node interface
type node struct {
	propQ            *ProposalQueue
	msgQ             *MessageQueue
	confc            chan pb.ConfChange
	confstatec       chan pb.ConfState
	tickc            chan struct{}
	done             chan struct{}
	stop             chan struct{}
	status           chan chan Status
	eventNotifyCh    chan bool
	r                *raft
	prevS            *prevState
	newReadyFunc     func(*raft, *SoftState, pb.HardState, bool) Ready
	needAdvance      bool
	lastSteppedIndex uint64

	logger Logger
}

func newNode() node {
	return node{
		propQ:      NewProposalQueue(proposalQueueLen, 1),
		msgQ:       NewMessageQueue(recvQueueLen, false, 1),
		confc:      make(chan pb.ConfChange, 1),
		confstatec: make(chan pb.ConfState, 1),
		// make tickc a buffered chan, so raft node can buffer some ticks when the node
		// is busy processing raft messages. Raft node will resume process buffered
		// ticks when it becomes idle.
		tickc:         make(chan struct{}, 128),
		done:          make(chan struct{}),
		stop:          make(chan struct{}),
		status:        make(chan chan Status, 1),
		eventNotifyCh: make(chan bool, 1),
		newReadyFunc:  newReady,
	}
}

func (n *node) EventNotifyCh() chan bool {
	return n.eventNotifyCh
}

func (n *node) Stop() {
	select {
	case <-n.done:
		// already closed
		return
	default:
		close(n.done)
	}
}

func (n *node) StepNode(moreEntriesToApply bool, busySnap bool) (Ready, bool) {
	if n.needAdvance {
		return Ready{}, false
	}
	var hasEvent bool
	msgs := n.msgQ.Get()
	for i, m := range msgs {
		hasEvent = true
		if busySnap && m.Type == pb.MsgApp {
			// ignore msg app while busy snapshot
		} else {
			n.handleReceivedMessage(n.r, m)
		}
		msgs[i].Entries = nil
	}
	if n.handleTicks(n.r) {
		hasEvent = true
	}
	needHandleProposal := n.handleLeaderUpdate(n.r)
	var ev bool
	ev, needHandleProposal = n.handleConfChanged(n.r, needHandleProposal)
	if ev {
		hasEvent = ev
	}
	if needHandleProposal {
		props := n.propQ.Get()
		for _, p := range props {
			hasEvent = true
			n.handleProposal(n.r, p)
		}
	}
	n.handleStatus(n.r)
	_ = hasEvent
	rd := n.newReadyFunc(n.r, n.prevS.prevSoftSt, n.prevS.prevHardSt, moreEntriesToApply)
	if rd.containsUpdates() {
		n.needAdvance = true
		var stepIndex uint64
		if !IsEmptySnap(rd.Snapshot) {
			stepIndex = rd.Snapshot.Metadata.Index
		}
		if len(rd.CommittedEntries) > 0 {
			fi := rd.CommittedEntries[0].Index
			if n.lastSteppedIndex != 0 && fi > n.lastSteppedIndex+1 {
				e := fmt.Sprintf("raft.node: %x(%v) index not continued: %v, %v, %v, snap:%v, prev: %v, logs: %v ",
					n.r.id, n.r.group, fi, n.lastSteppedIndex, stepIndex, rd.Snapshot.Metadata.String(), n.prevS,
					n.r.raftLog.String())
				n.logger.Error(e)
			}
			stepIndex = rd.CommittedEntries[len(rd.CommittedEntries)-1].Index
		}
		n.lastSteppedIndex = stepIndex
		return rd, true
	}
	return Ready{}, false
}

func (n *node) DebugString() string {
	ents := n.r.raftLog.allEntries()
	e := fmt.Sprintf("raft.node: %x(%v) index not continued: %v, prev: %v, logs: %v, %v ",
		n.r.id, n.r.group, n.lastSteppedIndex, n.prevS, len(ents),
		n.r.raftLog.String())
	return e
}

func (n *node) handleLeaderUpdate(r *raft) bool {
	lead := n.prevS.prevLead
	needHandleProposal := lead != None
	if lead != r.lead {
		if r.hasLeader() {
			if lead == None {
				r.logger.Infof("raft.node: %x(%v) elected leader %x at term %d", r.id, r.group.Name, r.lead, r.Term)
			} else {
				r.logger.Infof("raft.node: %x(%v) changed leader from %x to %x at term %d", r.id, r.group.Name, lead, r.lead, r.Term)
			}
			needHandleProposal = true
		} else {
			r.logger.Infof("raft.node: %x(%v) lost leader %x at term %d", r.id, r.group.Name, lead, r.Term)
			needHandleProposal = false
		}
		lead = r.lead
		n.prevS.prevLead = lead
	}
	return needHandleProposal
}

func (n *node) NotifyEventCh() {
	select {
	case n.eventNotifyCh <- true:
	default:
	}
}

func (n *node) addProposalToQueue(ctx context.Context, p msgWithDrop, to time.Duration, stopC chan struct{}) error {
	if added, stopped, err := n.propQ.AddWait(ctx, p, to, stopC); !added || stopped {
		if n.logger != nil {
			n.logger.Warningf("dropped an incoming proposal: %v", p.m.String())
		}
		if err != nil {
			return err
		}
		if stopped {
			return ErrStopped
		}
		if !added {
			return errProposalAddFailed
		}
	}

	n.NotifyEventCh()
	return nil
}

func (n *node) addReqMessageToQueue(req pb.Message) {
	if req.Type == pb.MsgSnap {
		n.msgQ.AddSnapshot(req)
	} else {
		if added, stopped := n.msgQ.Add(req); !added || stopped {
			if n.logger != nil {
				n.logger.Warningf("dropped an incoming message: %v", req.String())
			}
			return
		}
	}

	n.NotifyEventCh()
}

func (n *node) Advance(rd Ready) {
	if rd.SoftState != nil {
		n.prevS.prevSoftSt = rd.SoftState
	}
	if len(rd.Entries) > 0 {
		n.prevS.prevLastUnstablei = rd.Entries[len(rd.Entries)-1].Index
		n.prevS.prevLastUnstablet = rd.Entries[len(rd.Entries)-1].Term
		n.prevS.havePrevLastUnstablei = true
	}
	if !IsEmptyHardState(rd.HardState) {
		n.prevS.prevHardSt = rd.HardState
	}
	if !IsEmptySnap(rd.Snapshot) {
		n.prevS.prevSnapi = rd.Snapshot.Metadata.Index
	}

	n.r.msgs = nil
	n.r.readStates = nil

	appliedI := rd.appliedCursor()
	if appliedI != 0 {
		// since the committed entries may less than the hard commit index due to the
		// limit for buffer len, we should not use the hard state commit index.
		n.r.raftLog.appliedTo(appliedI)
	}
	if n.prevS.havePrevLastUnstablei {
		n.r.raftLog.stableTo(n.prevS.prevLastUnstablei, n.prevS.prevLastUnstablet)
		n.prevS.havePrevLastUnstablei = false
	}
	n.r.raftLog.stableSnapTo(n.prevS.prevSnapi)
	n.needAdvance = false
}

func (n *node) ConfChangedCh() <-chan pb.ConfChange {
	return n.confc
}

func (n *node) HandleConfChanged(cc pb.ConfChange) {
	n.processConfChanged(n.r, cc, true)
}

func (n *node) handleConfChanged(r *raft, needHandleProposal bool) (bool, bool) {
	if len(n.confc) == 0 {
		return false, needHandleProposal
	}
	select {
	case cc := <-n.confc:
		needHandleProposal = n.processConfChanged(r, cc, needHandleProposal)
		return true, needHandleProposal
	default:
		return false, needHandleProposal
	}
}

func (n *node) processConfChanged(r *raft, cc pb.ConfChange, needHandleProposal bool) bool {
	if cc.ReplicaID == None {
		r.resetPendingConf()
		select {
		case n.confstatec <- pb.ConfState{Nodes: r.nodes(),
			Groups:        r.groups(),
			Learners:      r.learnerNodes(),
			LearnerGroups: r.learnerGroups()}:
		case <-n.done:
		}
		return needHandleProposal
	}
	switch cc.Type {
	case pb.ConfChangeAddNode:
		r.addNode(cc.ReplicaID, cc.NodeGroup)
	case pb.ConfChangeAddLearnerNode:
		r.addLearner(cc.ReplicaID, cc.NodeGroup)
	case pb.ConfChangeRemoveNode:
		// block incoming proposal when local node is
		// removed
		if cc.ReplicaID == r.id {
			needHandleProposal = false
		}
		r.removeNode(cc.ReplicaID)
	case pb.ConfChangeUpdateNode:
		r.updateNode(cc.ReplicaID, cc.NodeGroup)
	default:
		panic("unexpected conf type")
	}
	select {
	case n.confstatec <- pb.ConfState{Nodes: r.nodes(),
		Groups:        r.groups(),
		Learners:      r.learnerNodes(),
		LearnerGroups: r.learnerGroups()}:
	case <-n.done:
	}
	return needHandleProposal
}

func (n *node) handleTicks(r *raft) bool {
	tdone := false
	hasEvent := false
	for !tdone {
		select {
		case <-n.tickc:
			hasEvent = true
			r.tick()
		default:
			tdone = true
		}
	}
	return hasEvent
}

func (n *node) handleStatus(r *raft) {
	select {
	case c := <-n.status:
		c <- getStatus(r)
	default:
	}
}

func (n *node) handleReceivedMessage(r *raft, m pb.Message) {
	from := r.getProgress(m.From)
	// filter out response message from unknown From.
	if from == nil && IsResponseMsg(m.Type) {
		m.Entries = nil
		return
	}
	if m.Type == pb.MsgTransferLeader {
		if m.FromGroup.NodeId == 0 {
			if from == nil {
				if m.From == r.id {
					m.FromGroup = r.group
				} else {
					if n.logger != nil {
						n.logger.Errorf("no replica found %v while processing : %v",
							m.From, m.String())
					}
					return
				}
			} else {
				m.FromGroup = from.group
			}
		}
		if m.ToGroup.NodeId == 0 {
			pr := r.getProgress(m.To)
			if pr == nil {
				if m.To == r.id {
					m.ToGroup = r.group
				} else {
					if n.logger != nil {
						n.logger.Errorf("no replica found %v while processing : %v",
							m.To, m.String())
					}
					return
				}
			} else {
				m.ToGroup = pr.group
			}
		}
	} else {
		// if we missing the peer node group info, try update it from
		// raft message
		if from != nil && from.group.NodeId == 0 && m.FromGroup.NodeId > 0 &&
			m.FromGroup.GroupId == r.group.GroupId {
			from.group = m.FromGroup
		}
	}
	r.Step(m)
	m.Entries = nil
}

func (n *node) handleProposal(r *raft, mdrop msgWithDrop) {
	m := mdrop.m
	m.From = r.id
	m.FromGroup = r.group
	err := r.Step(m)
	if err == errMsgDropped && mdrop.dropCB != nil {
		mdrop.dropCB()
	}
}

// Tick increments the internal logical clock for this Node. Election timeouts
// and heartbeat timeouts are in units of ticks.
func (n *node) Tick() bool {
	select {
	case n.tickc <- struct{}{}:
		n.NotifyEventCh()
		return true
	case <-n.done:
		return true
	default:
		if n.logger != nil {
			n.logger.Warningf("A tick missed to fire. Node blocks too long!")
		}
		return false
	}
}

func (n *node) Campaign(ctx context.Context) error { return n.step(ctx, pb.Message{Type: pb.MsgHup}) }

func (n *node) Propose(ctx context.Context, data []byte) error {
	return n.step(ctx, pb.Message{Type: pb.MsgProp, Entries: []pb.Entry{{Data: data}}})
}

func (n *node) ProposeEntryWithDrop(ctx context.Context, e pb.Entry, cancel context.CancelFunc) error {
	return n.stepWithDrop(ctx, pb.Message{Type: pb.MsgProp, Entries: []pb.Entry{e}}, cancel)
}

func (n *node) ProposeWithDrop(ctx context.Context, data []byte, cancel context.CancelFunc) error {
	return n.stepWithDrop(ctx, pb.Message{Type: pb.MsgProp, Entries: []pb.Entry{{Data: data}}}, cancel)
}

func (n *node) Step(ctx context.Context, m pb.Message) error {
	// ignore unexpected local messages receiving over network
	if IsLocalMsg(m.Type) {
		// TODO: return an error?
		return nil
	}
	return n.step(ctx, m)
}

func (n *node) ProposeConfChange(ctx context.Context, cc pb.ConfChange) error {
	data, err := cc.Marshal()
	if err != nil {
		return err
	}
	return n.Step(ctx, pb.Message{Type: pb.MsgProp, Entries: []pb.Entry{{Type: pb.EntryConfChange, Data: data}}})
}

func (n *node) stepWithDrop(ctx context.Context, m pb.Message, cancel context.CancelFunc) error {
	if m.Type != pb.MsgProp {
		n.addReqMessageToQueue(m)
		return nil
	}

	err := n.addProposalToQueue(ctx, msgWithDrop{m: m, dropCB: cancel}, queueWaitTime, n.done)
	return err
}

// Step advances the state machine using msgs. The ctx.Err() will be returned,
// if any.
func (n *node) step(ctx context.Context, m pb.Message) error {
	return n.stepWithDrop(ctx, m, nil)
}

func (n *node) ApplyConfChange(cc pb.ConfChange) *pb.ConfState {
	var cs pb.ConfState
	select {
	case n.confc <- cc:
	case <-n.done:
		return &cs
	}
	// notify event
	n.NotifyEventCh()

	select {
	case cs = <-n.confstatec:
	case <-n.done:
	}
	return &cs
}

func (n *node) Status() Status {
	c := make(chan Status, 1)
	to := time.NewTimer(time.Second)
	defer to.Stop()
	select {
	case n.status <- c:
	case <-n.done:
		return Status{}
	case <-to.C:
		return Status{}
	}
	n.NotifyEventCh()
	select {
	case s := <-c:
		return s
	case <-n.done:
		return Status{}
	case <-to.C:
		return Status{}
	}
}

func (n *node) ReportUnreachable(id uint64, group pb.Group) {
	n.addReqMessageToQueue(pb.Message{Type: pb.MsgUnreachable, From: id, FromGroup: group})
}

func (n *node) ReportSnapshot(id uint64, gp pb.Group, status SnapshotStatus) {
	rej := status == SnapshotFailure
	n.addReqMessageToQueue(pb.Message{Type: pb.MsgSnapStatus, From: id, FromGroup: gp, Reject: rej})
}

func (n *node) TransferLeadership(ctx context.Context, lead, transferee uint64) {
	// manually set 'from' and 'to', so that leader can voluntarily transfers its leadership
	n.addReqMessageToQueue(pb.Message{Type: pb.MsgTransferLeader, From: transferee, To: lead})
}

func (n *node) ReadIndex(ctx context.Context, rctx []byte) error {
	return n.step(ctx, pb.Message{Type: pb.MsgReadIndex, Entries: []pb.Entry{{Data: rctx}}})
}

func newReady(r *raft, prevSoftSt *SoftState, prevHardSt pb.HardState, moreEntriesToApply bool) Ready {
	rd := Ready{
		Entries:  r.raftLog.unstableEntries(),
		Messages: r.msgs,
	}
	if moreEntriesToApply {
		rd.CommittedEntries = r.raftLog.nextEnts()
	}
	if len(rd.CommittedEntries) > 0 {
		lastIndex := rd.CommittedEntries[len(rd.CommittedEntries)-1].Index
		rd.MoreCommittedEntries = r.raftLog.hasMoreNextEnts(lastIndex)
	}
	if softSt := r.softState(); !softSt.equal(prevSoftSt) {
		rd.SoftState = softSt
	}
	if hardSt := r.hardState(); !isHardStateEqual(hardSt, prevHardSt) {
		rd.HardState = hardSt
	}
	if r.raftLog.unstable.snapshot != nil {
		rd.Snapshot = *r.raftLog.unstable.snapshot
	}
	if len(r.readStates) != 0 {
		rd.ReadStates = r.readStates
	}
	// see: https://github.com/etcd-io/etcd/pull/10106
	rd.MustSync = MustSync(r.hardState(), prevHardSt, len(rd.Entries))
	return rd
}

// MustSync returns true if the hard state and count of Raft entries indicate
// that a synchronous write to persistent storage is required.
func MustSync(st, prevst pb.HardState, entsnum int) bool {
	// Persistent state on all servers:
	// (Updated on stable storage before responding to RPCs)
	// currentTerm
	// votedFor
	// log entries[]
	return entsnum != 0 || st.Vote != prevst.Vote || st.Term != prevst.Term
}
