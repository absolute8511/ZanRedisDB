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
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"

	pb "github.com/youzan/ZanRedisDB/raft/raftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0
const noLimit = math.MaxUint64

// Possible values for StateType.
const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
	StatePreCandidate
	numStates
)

type ReadOnlyOption int

const (
	// ReadOnlySafe guarantees the linearizability of the read only request by
	// communicating with the quorum. It is the default and suggested option.
	ReadOnlySafe ReadOnlyOption = iota
	// ReadOnlyLeaseBased ensures linearizability of the read only request by
	// relying on the leader lease. It can be affected by clock drift.
	// If the clock drift is unbounded, leader might keep the lease longer than it
	// should (clock can move backward/pause without any bound). ReadIndex is not safe
	// in that case.
	ReadOnlyLeaseBased
)

// Possible values for CampaignType
const (
	// campaignPreElection represents the first phase of a normal election when
	// Config.PreVote is true.
	campaignPreElection CampaignType = "CampaignPreElection"
	// campaignElection represents a normal (time-based) election (the second phase
	// of the election when Config.PreVote is true).
	campaignElection CampaignType = "CampaignElection"
	// campaignTransfer represents the type of leader transfer
	campaignTransfer CampaignType = "CampaignTransfer"
)

// lockedRand is a small wrapper around rand.Rand to provide
// synchronization. Only the methods needed by the code are exposed
// (e.g. Intn).
type lockedRand struct {
	mu   sync.Mutex
	rand *rand.Rand
}

func (r *lockedRand) Intn(n int) int {
	r.mu.Lock()
	v := r.rand.Intn(n)
	r.mu.Unlock()
	return v
}

var globalRand = &lockedRand{
	rand: rand.New(rand.NewSource(time.Now().UnixNano())),
}

// CampaignType represents the type of campaigning
// the reason we use the type of string instead of uint64
// is because it's simpler to compare and fill in raft entries
type CampaignType string

// StateType represents the role of a node in a cluster.
type StateType uint64

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
	"StatePreCandidate",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	Group pb.Group
	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []pb.Group
	// learners contains the IDs of all leaner nodes (including self if the local node is a leaner) in the raft cluster.
	// learners only receives entries from the leader node. It does not vote or promote itself.
	learners []pb.Group

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64

	// MaxSizePerMsg limits the max size of each append message. Smaller value
	// lowers the raft recovery cost(initial probing and message lost during normal
	// operation). On the other side, it might affect the throughput during normal
	// replication. Note: math.MaxUint64 for unlimited, 0 for at most one entry per
	// message.
	MaxSizePerMsg uint64
	// MaxCommittedSizePerReady limits the size of the committed entries which
	// can be applied.
	MaxCommittedSizePerReady uint64
	// MaxInflightMsgs limits the max number of in-flight append messages during
	// optimistic replication phase. The application transportation layer usually
	// has its own sending buffer over TCP/UDP. Setting MaxInflightMsgs to avoid
	// overflowing that sending buffer. TODO (xiangli): feedback to application to
	// limit the proposal rate?
	MaxInflightMsgs int

	// CheckQuorum specifies if the leader should check quorum activity. Leader
	// steps down when quorum is not active for an electionTimeout.
	CheckQuorum bool

	// PreVote enables the Pre-Vote algorithm described in raft thesis section
	// 9.6. This prevents disruption when a node that has been partitioned away
	// rejoins the cluster.
	PreVote bool

	// ReadOnlyOption specifies how the read only request is processed.
	//
	// ReadOnlySafe guarantees the linearizability of the read only request by
	// communicating with the quorum. It is the default and suggested option.
	//
	// ReadOnlyLeaseBased ensures linearizability of the read only request by
	// relying on the leader lease. It can be affected by clock drift.
	// If the clock drift is unbounded, leader might keep the lease longer than it
	// should (clock can move backward/pause without any bound). ReadIndex is not safe
	// in that case.
	// CheckQuorum MUST be enabled if ReadOnlyOption is ReadOnlyLeaseBased.
	ReadOnlyOption ReadOnlyOption

	// Logger is the logger used for raft log. For multinode which can host
	// multiple raft group, each raft group can have its own logger
	Logger Logger

	// DisableProposalForwarding set to true means that followers will drop
	// proposals, rather than forwarding them to the leader. One use case for
	// this feature would be in a situation where the Raft leader is used to
	// compute the data of a proposal, for example, adding a timestamp from a
	// hybrid logical clock to data in a monotonically increasing way. Forwarding
	// should be disabled to prevent a follower with an innaccurate hybrid
	// logical clock from assigning the timestamp and then forwarding the data
	// to the leader.
	DisableProposalForwarding bool
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.ID != c.Group.RaftReplicaId {
		return errors.New("local raft id should match group replica id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}
	// default MaxCommittedSizePerReady to MaxSizePerMsg because they were
	// previously the same parameter.
	if c.MaxCommittedSizePerReady == 0 {
		c.MaxCommittedSizePerReady = c.MaxSizePerMsg
	}

	if c.MaxInflightMsgs <= 0 {
		return errors.New("max inflight messages must be greater than 0")
	}

	if c.Logger == nil {
		c.Logger = raftLogger
	}
	if c.ReadOnlyOption == ReadOnlyLeaseBased && !c.CheckQuorum {
		return errors.New("CheckQuorum must be enabled when ReadOnlyOption is ReadOnlyLeaseBased")
	}
	return nil
}

func IsFromLocalNodeMsg(r *raft, m *pb.Message) bool {
	if r.group.NodeId == m.FromGroup.NodeId &&
		r.group.GroupId == m.FromGroup.GroupId &&
		r.group.RaftReplicaId == m.FromGroup.RaftReplicaId &&
		r.id == m.From {
		return true
	}
	return false
}

type raft struct {
	id    uint64
	group pb.Group

	Term uint64
	Vote uint64

	readStates []ReadState

	// the log
	raftLog *raftLog

	maxInflight int
	maxMsgSize  uint64
	prs         map[uint64]*Progress
	learnerPrs  map[uint64]*Progress
	matchBuf    uint64Slice

	state StateType
	//isLearner is true if the local raft node is a learner.
	isLearner bool

	votes map[uint64]bool

	msgs []pb.Message

	// the leader id
	lead uint64
	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in raft thesis 3.10.
	leadTransferee uint64
	// New configuration is ignored if there exists unapplied configuration.
	pendingConf bool

	readOnly *readOnly

	// number of ticks since it reached last electionTimeout when it is leader
	// or candidate.
	// number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int

	checkQuorum bool
	preVote     bool

	heartbeatTimeout int
	electionTimeout  int
	// randomizedElectionTimeout is a random number between
	// [electiontimeout, 2 * electiontimeout - 1]. It gets reset
	// when raft changes its state to follower or candidate.
	randomizedElectionTimeout int
	disableProposalForwarding bool

	tick func()
	step stepFunc

	logger Logger
}

func newRaft(c *Config) *raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	raftlog := newLogWithSize(c.Storage, c.Logger, c.MaxCommittedSizePerReady)
	hs, cs, err := c.Storage.InitialState()
	if err != nil {
		panic(err) // TODO(bdarnell)
	}
	peers := c.peers
	learners := c.learners
	if len(cs.Nodes) > 0 || len(cs.Learners) > 0 {
		if len(peers) > 0 || len(learners) > 0 {
			// TODO(bdarnell): the peers argument is always nil except in
			// tests; the argument should be removed and these tests should be
			// updated to specify their nodes through a snapshot.
			panic("cannot specify both newRaft(peers, learners) and ConfState.(Nodes, Learners)")
		}
		if len(cs.Groups) != len(cs.Nodes) {
			panic("initial peers from storage failed: invalid initial state")
		}
		if len(cs.LearnerGroups) != len(cs.Learners) {
			panic("initial learner peers from storage failed: invalid initial state")
		}
		for _, g := range cs.Groups {
			if g.GroupId == 0 {
				g.GroupId = c.Group.GroupId
			}
			peers = append(peers, *g)
		}
		for _, g := range cs.LearnerGroups {
			if g.GroupId == 0 {
				g.GroupId = c.Group.GroupId
			}
			learners = append(learners, *g)
		}
	}
	r := &raft{
		id:                        c.ID,
		group:                     c.Group,
		lead:                      None,
		isLearner:                 false,
		raftLog:                   raftlog,
		maxMsgSize:                c.MaxSizePerMsg,
		maxInflight:               c.MaxInflightMsgs,
		prs:                       make(map[uint64]*Progress),
		learnerPrs:                make(map[uint64]*Progress),
		electionTimeout:           c.ElectionTick,
		heartbeatTimeout:          c.HeartbeatTick,
		logger:                    c.Logger,
		checkQuorum:               c.CheckQuorum,
		preVote:                   c.PreVote,
		readOnly:                  newReadOnly(c.ReadOnlyOption),
		disableProposalForwarding: c.DisableProposalForwarding,
	}
	for _, p := range peers {
		r.prs[p.RaftReplicaId] = &Progress{Next: 1, ins: newInflights(r.maxInflight), group: p}
		r.logger.Debugf("newRaft %x [add peer: %v ]", r.id, r.prs[p.RaftReplicaId])
	}
	for _, p := range learners {
		if _, ok := r.prs[p.RaftReplicaId]; ok {
			panic(fmt.Sprintf("node %x is in both learner and peer list", p))
		}
		r.learnerPrs[p.RaftReplicaId] = &Progress{Next: 1, ins: newInflights(r.maxInflight), group: p, IsLearner: true}
		if r.id == p.RaftReplicaId {
			r.isLearner = true
		}
		r.logger.Debugf("newRaft %x [add learner peer: %v ]", r.id, r.learnerPrs[p.RaftReplicaId])
	}
	if !isHardStateEqual(hs, emptyState) {
		r.loadState(hs)
	}
	if c.Applied > 0 {
		raftlog.appliedTo(c.Applied)
	}
	r.becomeFollower(r.Term, None)

	var nodesStrs []string
	for _, n := range r.nodes() {
		nodesStrs = append(nodesStrs, fmt.Sprintf("%x", n))
	}
	for _, n := range r.learnerNodes() {
		nodesStrs = append(nodesStrs, fmt.Sprintf("learner-%x", n))
	}

	r.logger.Infof("newRaft %x(%v) [peers: [%s], term: %d, commit: %d, applied: %d, lastindex: %d, lastterm: %d]",
		r.id, r.group.Name, strings.Join(nodesStrs, ","), r.Term, r.raftLog.committed, r.raftLog.applied, r.raftLog.lastIndex(), r.raftLog.lastTerm())
	return r
}

func (r *raft) hasLeader() bool { return r.lead != None }

func (r *raft) softState() *SoftState { return &SoftState{Lead: r.lead, RaftState: r.state} }

func (r *raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.raftLog.committed,
	}
}

func (r *raft) quorum() int { return len(r.prs)/2 + 1 }

func (r *raft) nodes() []uint64 {
	nodes := make([]uint64, 0, len(r.prs))
	for id := range r.prs {
		nodes = append(nodes, id)
	}
	sort.Sort(uint64Slice(nodes))
	return nodes
}

func (r *raft) learnerNodes() []uint64 {
	nodes := make([]uint64, 0, len(r.learnerPrs))
	for id := range r.learnerPrs {
		nodes = append(nodes, id)
	}
	sort.Sort(uint64Slice(nodes))
	return nodes
}

func (r *raft) groups() []*pb.Group {
	groups := make([]*pb.Group, 0, len(r.prs)+len(r.learnerPrs))
	for _, pr := range r.prs {
		newg := pr.group
		groups = append(groups, &newg)
	}
	return groups
}

func (r *raft) learnerGroups() []*pb.Group {
	groups := make([]*pb.Group, 0, len(r.learnerPrs))
	for _, pr := range r.learnerPrs {
		newg := pr.group
		groups = append(groups, &newg)
	}
	return groups
}

// send persists state to stable storage and then sends to its mailbox.
func (r *raft) send(m pb.Message) {
	m.From = r.id
	m.FromGroup = r.group
	if m.ToGroup.NodeId == 0 {
		pr := r.getProgress(m.To)
		if pr != nil {
			m.ToGroup = pr.group
		}
	}
	if m.Type == pb.MsgVote || m.Type == pb.MsgVoteResp ||
		m.Type == pb.MsgPreVote || m.Type == pb.MsgPreVoteResp {
		if m.Term == 0 {
			// All {pre-,}campaign messages need to have the term set when
			// sending.
			// - MsgVote: m.Term is the term the node is campaigning for,
			//   non-zero as we increment the term when campaigning.
			// - MsgVoteResp: m.Term is the new r.Term if the MsgVote was
			//   granted, non-zero for the same reason MsgVote is
			// - MsgPreVote: m.Term is the term the node will campaign,
			//   non-zero as we use m.Term to indicate the next term we'll be
			//   campaigning for
			// - MsgPreVoteResp: m.Term is the term received in the original
			//   MsgPreVote if the pre-vote was granted, non-zero for the
			//   same reasons MsgPreVote is
			panic(fmt.Sprintf("term should be set when sending %s", m.Type))
		}
	} else {
		if m.Term != 0 {
			panic(fmt.Sprintf("term should not be set when sending %s (was %d)", m.Type, m.Term))
		}
		// do not attach term to MsgProp, MsgReadIndex
		// proposals are a way to forward to the leader and
		// should be treated as local message.
		// MsgReadIndex is also forwarded to leader.
		if m.Type != pb.MsgProp && m.Type != pb.MsgReadIndex {
			m.Term = r.Term
		}
	}
	r.msgs = append(r.msgs, m)
}

func (r *raft) getProgress(id uint64) *Progress {
	if pr, ok := r.prs[id]; ok {
		return pr
	}

	return r.learnerPrs[id]
}

func (r *raft) forEachProgress(f func(id uint64, pr *Progress)) {
	for id, pr := range r.prs {
		f(id, pr)
	}

	for id, pr := range r.learnerPrs {
		f(id, pr)
	}
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer.
func (r *raft) sendAppend(to uint64) {
	r.maybeSendAppend(to, true)
}

// maybeSendAppend sends an append RPC with new entries to the given peer,
// if necessary. Returns true if a message was sent. The sendIfEmpty
// argument controls whether messages with no entries will be sent
// ("empty" messages are useful to convey updated Commit indexes, but
// are undesirable when we're sending multiple messages in a batch).
func (r *raft) maybeSendAppend(to uint64, sendIfEmpty bool) bool {
	pr := r.getProgress(to)
	if pr.IsPaused() {
		return false
	}
	m := pb.Message{FromGroup: r.group, ToGroup: pr.group}
	m.To = to

	term, errt := r.raftLog.term(pr.Next - 1)
	ents, erre := r.raftLog.entries(pr.Next, r.maxMsgSize)
	if len(ents) == 0 && !sendIfEmpty {
		return false
	}

	if errt != nil || erre != nil { // send snapshot if we failed to get term or entries
		if !pr.RecentActive {
			r.logger.Debugf("ignore sending snapshot to %x since it is not recently active", to)
			return false
		}

		m.Type = pb.MsgSnap
		snapshot, err := r.raftLog.snapshot()
		if err != nil {
			if err == ErrSnapshotTemporarilyUnavailable {
				r.logger.Debugf("%x failed to send snapshot to %x because snapshot is temporarily unavailable", r.id, to)
				return false
			}
			panic(err) // TODO(bdarnell)
		}
		if IsEmptySnap(snapshot) {
			panic("need non-empty snapshot")
		}
		m.Snapshot = snapshot
		sindex, sterm := snapshot.Metadata.Index, snapshot.Metadata.Term
		r.logger.Infof("%x [firstindex: %d, commit: %d] sent snapshot[index: %d, term: %d] to %x [%s]",
			r.id, r.raftLog.firstIndex(), r.raftLog.committed, sindex, sterm, to, pr)
		pr.becomeSnapshot(sindex)
		r.logger.Debugf("%x paused sending replication messages to %x [%s]", r.id, to, pr)
	} else {
		m.Type = pb.MsgApp
		m.Index = pr.Next - 1
		m.LogTerm = term
		m.Entries = ents
		m.Commit = r.raftLog.committed
		if n := len(m.Entries); n != 0 {
			switch pr.State {
			// optimistically increase the next when in ProgressStateReplicate
			case ProgressStateReplicate:
				last := m.Entries[n-1].Index
				pr.optimisticUpdate(last)
				pr.ins.add(last)
			case ProgressStateProbe:
				pr.pause()
			default:
				r.logger.Panicf("%x is sending append in unhandled state %s", r.id, pr.State)
			}
		}
	}
	r.send(m)
	return true
}

// sendHeartbeat sends an empty MsgApp
func (r *raft) sendHeartbeat(to uint64, ctx []byte) {
	// Attach the commit as min(to.matched, r.committed).
	// When the leader sends out heartbeat message,
	// the receiver(follower) might not be matched with the leader
	// or it might not have all the committed entries.
	// The leader MUST NOT forward the follower's commit to
	// an unmatched index.
	pr := r.getProgress(to)
	commit := min(pr.Match, r.raftLog.committed)
	m := pb.Message{
		To:      to,
		ToGroup: pr.group,
		Type:    pb.MsgHeartbeat,
		Commit:  commit,
		Context: ctx,
	}

	r.send(m)
}

// bcastAppend sends RPC, with entries to all peers that are not up-to-date
// according to the progress recorded in r.prs.
func (r *raft) bcastAppend() {
	r.forEachProgress(func(id uint64, _ *Progress) {
		if id == r.id {
			return
		}
		r.sendAppend(id)
	})
}

// bcastHeartbeat sends RPC, without entries to all the peers.
func (r *raft) bcastHeartbeat() {
	lastCtx := r.readOnly.lastPendingRequestCtx()
	if len(lastCtx) == 0 {
		r.bcastHeartbeatWithCtx(nil)
	} else {
		r.bcastHeartbeatWithCtx(lastCtx)
	}
}

func (r *raft) bcastHeartbeatWithCtx(ctx []byte) {
	r.forEachProgress(func(id uint64, _ *Progress) {
		if id == r.id {
			return
		}
		r.sendHeartbeat(id, ctx)
	})
}

func (r *raft) advance(rd Ready) {
	// If entries were applied (or a snapshot), update our cursor for
	// the next Ready. Note that if the current HardState contains a
	// new Commit index, this does not mean that we're also applying
	// all of the new entries due to commit pagination by size.
	if index := rd.appliedCursor(); index > 0 {
		r.raftLog.appliedTo(index)
	}
	if len(rd.Entries) > 0 {
		e := rd.Entries[len(rd.Entries)-1]
		r.raftLog.stableTo(e.Index, e.Term)
	}
	if !IsEmptySnap(rd.Snapshot) {
		r.raftLog.stableSnapTo(rd.Snapshot.Metadata.Index)
	}
}

// maybeCommit attempts to advance the commit index. Returns true if
// the commit index changed (in which case the caller should call
// r.bcastAppend).
func (r *raft) maybeCommit() bool {
	// Preserving matchBuf across calls is an optimization
	// used to avoid allocating a new slice on each call.
	if cap(r.matchBuf) < len(r.prs) {
		r.matchBuf = make(uint64Slice, len(r.prs))
	}
	r.matchBuf = r.matchBuf[:len(r.prs)]
	idx := 0
	for _, p := range r.prs {
		r.matchBuf[idx] = p.Match
		idx++
	}
	sort.Sort(&r.matchBuf)
	mci := r.matchBuf[len(r.matchBuf)-r.quorum()]
	return r.raftLog.maybeCommit(mci, r.Term)
}

func (r *raft) reset(term uint64) {
	if r.Term != term {
		r.Term = term
		r.Vote = None
	}
	r.lead = None

	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.resetRandomizedElectionTimeout()

	r.abortLeaderTransfer()

	r.votes = make(map[uint64]bool)
	r.forEachProgress(func(id uint64, pr *Progress) {
		*pr = Progress{Next: r.raftLog.lastIndex() + 1, ins: newInflights(r.maxInflight), group: pr.group, IsLearner: pr.IsLearner}
		if id == r.id {
			pr.Match = r.raftLog.lastIndex()
		}
	})
	r.pendingConf = false
	r.readOnly = newReadOnly(r.readOnly.option)
}

func (r *raft) appendEntry(es ...pb.Entry) {
	li := r.raftLog.lastIndex()
	for i := range es {
		es[i].Term = r.Term
		es[i].Index = li + 1 + uint64(i)
	}
	li = r.raftLog.append(es...)
	r.getProgress(r.id).maybeUpdate(li)
	// Regardless of maybeCommit's return, our caller will call bcastAppend.
	r.maybeCommit()
}

// tickElection is run by followers and candidates after r.electionTimeout.
func (r *raft) tickElection() {
	r.electionElapsed++

	if r.promotable() && r.pastElectionTimeout() {
		r.electionElapsed = 0
		r.Step(pb.Message{From: r.id, FromGroup: r.group, Type: pb.MsgHup})
	}
}

// tickHeartbeat is run by leaders to send a MsgBeat after r.heartbeatTimeout.
func (r *raft) tickHeartbeat() {
	r.heartbeatElapsed++
	r.electionElapsed++

	if r.electionElapsed >= r.electionTimeout {
		r.electionElapsed = 0
		if r.checkQuorum {
			r.Step(pb.Message{From: r.id, FromGroup: r.group, Type: pb.MsgCheckQuorum})
		}
		// If current leader cannot transfer leadership in electionTimeout, it becomes leader again.
		if r.state == StateLeader && r.leadTransferee != None {
			r.abortLeaderTransfer()
		}
	}

	if r.state != StateLeader {
		return
	}

	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		err := r.Step(pb.Message{From: r.id, FromGroup: r.group, Type: pb.MsgBeat})
		if err != nil {
			r.logger.Infof("%x(%v) step msgbeat failed: %v", r.id, r.group.Name, err.Error())
		}
	}
}

func (r *raft) becomeFollower(term uint64, lead uint64) {
	r.step = stepFollower
	r.reset(term)
	r.tick = r.tickElection
	r.lead = lead
	r.state = StateFollower
	r.logger.Infof("%x(%v) became follower at term %d", r.id, r.group.Name, r.Term)
}

func (r *raft) becomeCandidate() {
	// TODO(xiangli) remove the panic when the raft implementation is stable
	if r.state == StateLeader {
		panic("invalid transition [leader -> candidate]")
	}
	r.step = stepCandidate
	r.reset(r.Term + 1)
	r.tick = r.tickElection
	r.Vote = r.id
	r.state = StateCandidate
	r.logger.Infof("%x(%v) became candidate at term %d", r.id, r.group.Name, r.Term)
}

func (r *raft) becomePreCandidate() {
	// TODO(xiangli) remove the panic when the raft implementation is stable
	if r.state == StateLeader {
		panic("invalid transition [leader -> pre-candidate]")
	}
	// Becoming a pre-candidate changes our step functions and state,
	// but doesn't change anything else. In particular it does not increase
	// r.Term or change r.Vote.
	r.step = stepCandidate
	r.votes = make(map[uint64]bool)
	r.tick = r.tickElection
	// see: https://github.com/etcd-io/etcd/pull/8334
	r.lead = None
	r.state = StatePreCandidate
	r.logger.Infof("%x(%v) became pre-candidate at term %d", r.id, r.group.Name, r.Term)
}

func (r *raft) becomeLeader() {
	// TODO(xiangli) remove the panic when the raft implementation is stable
	if r.state == StateFollower {
		panic("invalid transition [follower -> leader]")
	}
	r.step = stepLeader
	r.reset(r.Term)
	r.tick = r.tickHeartbeat
	r.lead = r.id
	r.state = StateLeader
	// see: https://github.com/etcd-io/etcd/pull/10279
	// Followers enter replicate mode when they've been successfully probed
	// (perhaps after having received a snapshot as a result). The leader is
	// trivially in this state. Note that r.reset() has initialized this
	// progress with the last index already.
	r.prs[r.id].becomeReplicate()

	ents, err := r.raftLog.entries(r.raftLog.committed+1, noLimit)
	if err != nil {
		r.logger.Panicf("unexpected error getting uncommitted entries (%v)", err)
	}

	nconf := numOfPendingConf(ents)
	if nconf > 1 {
		panic("unexpected multiple uncommitted config entry")
	}
	if nconf == 1 {
		r.pendingConf = true
	}

	r.appendEntry(pb.Entry{Data: nil})
	r.logger.Infof("%x(%v) became leader at term %d", r.id, r.group.Name, r.Term)
}

func (r *raft) campaign(t CampaignType) {
	var term uint64
	var voteMsg pb.MessageType
	if t == campaignPreElection {
		r.becomePreCandidate()
		voteMsg = pb.MsgPreVote
		// PreVote RPCs are sent for the next term before we've incremented r.Term.
		term = r.Term + 1
	} else {
		r.becomeCandidate()
		voteMsg = pb.MsgVote
		term = r.Term
	}
	if r.quorum() == r.poll(r.id, voteRespMsgType(voteMsg), true) {
		// We won the election after voting for ourselves (which must mean that
		// this is a single-node cluster). Advance to the next state.
		if t == campaignPreElection {
			r.campaign(campaignElection)
		} else {
			r.becomeLeader()
		}
		return
	}
	for id, p := range r.prs {
		if id == r.id {
			continue
		}
		r.logger.Infof("%x(%v) [logterm: %d, index: %d] sent %s request to %x (%v) at term %d",
			r.id, r.group.Name, r.raftLog.lastTerm(), r.raftLog.lastIndex(), voteMsg, id, p.group, r.Term)

		var ctx []byte
		if t == campaignTransfer {
			ctx = []byte(t)
		}
		r.send(pb.Message{Term: term, To: id, ToGroup: p.group, Type: voteMsg, Index: r.raftLog.lastIndex(),
			LogTerm: r.raftLog.lastTerm(), Context: ctx})
	}
}

func (r *raft) poll(id uint64, t pb.MessageType, v bool) (granted int) {
	if v {
		r.logger.Infof("%x(%v) received %s from %x at term %d", r.id, r.group.Name, t, id, r.Term)
	} else {
		r.logger.Infof("%x(%v) received %s rejection from %x at term %d", r.id, r.group.Name, t, id, r.Term)
	}
	if _, ok := r.votes[id]; !ok {
		r.votes[id] = v
	}
	for _, vv := range r.votes {
		if vv {
			granted++
		}
	}
	return granted
}

func (r *raft) Step(m pb.Message) error {
	if m.FromGroup.NodeId == 0 {
		m.FromGroup = r.group
	}
	// Handle the message term, which may result in our stepping down to a follower.
	switch {
	case m.Term == 0:
		// local message
	case m.Term > r.Term:
		if m.Type == pb.MsgVote || m.Type == pb.MsgPreVote {
			force := bytes.Equal(m.Context, []byte(campaignTransfer))
			inLease := r.checkQuorum && r.lead != None && r.electionElapsed < r.electionTimeout
			if !force && inLease {
				// If a server receives a RequestVote request within the minimum election timeout
				// of hearing from a current leader, it does not update its term or grant its vote
				r.logger.Infof("%x(%v) [logterm: %d, index: %d, vote: %x] ignored %s from %x [logterm: %d, index: %d] at term %d: lease is not expired (remaining ticks: %d)",
					r.id, r.group.Name, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.Vote, m.Type, m.From, m.LogTerm, m.Index, r.Term, r.electionTimeout-r.electionElapsed)
				return nil
			}
		}
		switch {
		case m.Type == pb.MsgPreVote:
			// Never change our term in response to a PreVote
		case m.Type == pb.MsgPreVoteResp && !m.Reject:
			// We send pre-vote requests with a term in our future. If the
			// pre-vote is granted, we will increment our term when we get a
			// quorum. If it is not, the term comes from the node that
			// rejected our vote so we should become a follower at the new
			// term.
		default:
			r.logger.Infof("%x(%v) [term: %d] received a %s message with higher term from %x [term: %d]",
				r.id, r.group.Name, r.Term, m.Type, m.From, m.Term)
			if m.Type == pb.MsgApp || m.Type == pb.MsgHeartbeat || m.Type == pb.MsgSnap {
				r.becomeFollower(m.Term, m.From)
			} else {
				r.becomeFollower(m.Term, None)
			}
		}

	case m.Term < r.Term:
		if (r.checkQuorum || r.preVote) && (m.Type == pb.MsgHeartbeat || m.Type == pb.MsgApp) {
			// We have received messages from a leader at a lower term. It is possible
			// that these messages were simply delayed in the network, but this could
			// also mean that this node has advanced its term number during a network
			// partition, and it is now unable to either win an election or to rejoin
			// the majority on the old term. If checkQuorum is false, this will be
			// handled by incrementing term numbers in response to MsgVote with a
			// higher term, but if checkQuorum is true we may not advance the term on
			// MsgVote and must generate other messages to advance the term. The net
			// result of these two features is to minimize the disruption caused by
			// nodes that have been removed from the cluster's configuration: a
			// removed node will send MsgVotes (or MsgPreVotes) which will be ignored,
			// but it will not receive MsgApp or MsgHeartbeat, so it will not create
			// disruptive term increases
			r.send(pb.Message{To: m.From, ToGroup: m.FromGroup, Type: pb.MsgAppResp})
		} else if m.Type == pb.MsgPreVote {
			// Before Pre-Vote enable, there may have candidate with higher term,
			// but less log. After update to Pre-Vote, the cluster may deadlock if
			// we drop messages with a lower term.
			r.logger.Infof("%x [logterm: %d, index: %d, vote: %x] rejected %s from %x [logterm: %d, index: %d] at term %d",
				r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.Vote, m.Type, m.From, m.LogTerm, m.Index, r.Term)
			r.send(pb.Message{To: m.From, Term: r.Term, Type: pb.MsgPreVoteResp, Reject: true})
		} else {
			// ignore other cases
			r.logger.Infof("%x(%v) [term: %d] ignored a %s message with lower term from %x [term: %d]",
				r.id, r.group.Name, r.Term, m.Type, m.From, m.Term)
		}
		return nil
	}

	switch m.Type {
	case pb.MsgHup:
		if r.state != StateLeader {
			ents, err := r.raftLog.slice(r.raftLog.applied+1, r.raftLog.committed+1, noLimit)
			if err != nil {
				if err == ErrCompacted {
					r.logger.Errorf("%x cannot campaign at term %d since log is compacted: %d", r.id, r.Term, r.raftLog.applied)
					return nil
				}
				r.logger.Panicf("unexpected error getting unapplied entries (%v)", err)
			}
			if n := numOfPendingConf(ents); n != 0 && r.raftLog.committed > r.raftLog.applied {
				r.logger.Warningf("%x cannot campaign at term %d since there are still %d pending configuration changes to apply", r.id, r.Term, n)
				return nil
			}

			r.logger.Infof("%x(%v) is starting a new election at term %d", r.id, r.group.Name, r.Term)
			if r.preVote {
				r.campaign(campaignPreElection)
			} else {
				r.campaign(campaignElection)
			}
		} else {
			r.logger.Debugf("%x ignoring MsgHup because already leader", r.id)
		}

	case pb.MsgVote, pb.MsgPreVote:
		if r.isLearner {
			// TODO: learner may need to vote, in case of node down when confchange.
			r.logger.Debugf("%x [logterm: %d, index: %d, vote: %x] ignored %s from %x [logterm: %d, index: %d] at term %d: learner can not vote",
				r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.Vote, m.Type, m.From, m.LogTerm, m.Index, r.Term)
			return nil
		}
		// We can vote if this is a repeat of a vote we've already cast...
		canVote := r.Vote == m.From ||
			// ...we haven't voted and we don't think there's a leader yet in this term...
			(r.Vote == None && r.lead == None) ||
			// ...or this is a PreVote for a future term...
			(m.Type == pb.MsgPreVote && m.Term > r.Term)
		// ...and we believe the candidate is up to date.
		if canVote && r.raftLog.isUpToDate(m.Index, m.LogTerm) {
			r.logger.Infof("%x(%v) [logterm: %d, index: %d, vote: %x] cast %s for %x(%v) [logterm: %d, index: %d] at term %d",
				r.id, r.group.Name, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.Vote, m.Type,
				m.From, m.FromGroup, m.LogTerm, m.Index, r.Term)
			// When responding to Msg{Pre,}Vote messages we include the term
			// from the message, not the local term. To see why consider the
			// case where a single node was previously partitioned away and
			// it's local term is now of date. If we include the local term
			// (recall that for pre-votes we don't update the local term), the
			// (pre-)campaigning node on the other end will proceed to ignore
			// the message (it ignores all out of date messages).
			// The term in the original message and current local term are the
			// same in the case of regular votes, but different for pre-votes.
			r.send(pb.Message{To: m.From, ToGroup: m.FromGroup, Term: m.Term, Type: voteRespMsgType(m.Type)})
			if m.Type == pb.MsgVote {
				// Only record real votes.
				r.electionElapsed = 0
				r.Vote = m.From
			}
		} else {
			r.logger.Infof("%x(%v) [logterm: %d, index: %d, vote: %x] rejected %s from %x [logterm: %d, index: %d] at term %d",
				r.id, r.group.Name, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.Vote, m.Type, m.From, m.LogTerm, m.Index, r.Term)
			r.send(pb.Message{To: m.From, ToGroup: m.FromGroup, Term: r.Term, Type: voteRespMsgType(m.Type), Reject: true})
		}

	default:
		dropped := r.step(r, m)
		if dropped {
			r.logger.Infof("%x(%v) at term %d dropping message: %v", r.id, r.group.Name, r.Term, m.String())
			return errMsgDropped
		}
	}
	return nil
}

// return true if the message is dropped
type stepFunc func(r *raft, m pb.Message) bool

func stepLeader(r *raft, m pb.Message) bool {
	// These message types do not require any progress for m.From.
	switch m.Type {
	case pb.MsgBeat:
		r.bcastHeartbeat()
		return false
	case pb.MsgCheckQuorum:
		if !r.checkQuorumActive() {
			r.logger.Warningf("%x stepped down to follower since quorum is not active", r.id)
			r.becomeFollower(r.Term, None)
		}
		return false
	case pb.MsgProp:
		if len(m.Entries) == 0 {
			r.logger.Panicf("%x stepped empty MsgProp", r.id)
		}
		if _, ok := r.prs[r.id]; !ok {
			// If we are not currently a member of the range (i.e. this node
			// was removed from the configuration while serving as leader),
			// drop any new proposals.
			return true
		}
		if r.leadTransferee != None {
			r.logger.Debugf("%x [term %d] transfer leadership to %x is in progress; dropping proposal", r.id, r.Term, r.leadTransferee)
			return true
		}

		for i := range m.Entries {
			e := &m.Entries[i]
			if e.Type == pb.EntryConfChange {
				if r.pendingConf {
					r.logger.Infof("propose conf %s ignored since pending unapplied configuration [applied: %d]",
						e, r.raftLog.applied)
					m.Entries[i] = pb.Entry{Type: pb.EntryNormal}
				} else {
					r.logger.Infof("propose conf change %s ", e.String())
				}
				r.pendingConf = true
			}
		}
		r.appendEntry(m.Entries...)
		r.bcastAppend()
		return false
	case pb.MsgReadIndex:
		if r.quorum() > 1 {
			if r.raftLog.zeroTermOnErrCompacted(r.raftLog.term(r.raftLog.committed)) != r.Term {
				// Reject read only request when this leader has not committed any log entry at its term.
				return true
			}

			// thinking: use an interally defined context instead of the user given context.
			// We can express this in terms of the term and index instead of a user-supplied value.
			// This would allow multiple reads to piggyback on the same message.
			switch r.readOnly.option {
			case ReadOnlySafe:
				r.readOnly.addRequest(r.raftLog.committed, m)
				r.bcastHeartbeatWithCtx(m.Entries[0].Data)
			case ReadOnlyLeaseBased:
				ri := r.raftLog.committed
				if m.From == None || IsFromLocalNodeMsg(r, &m) { // from local member
					r.readStates = append(r.readStates, ReadState{Index: r.raftLog.committed, RequestCtx: m.Entries[0].Data})
				} else {
					r.send(pb.Message{To: m.From, ToGroup: m.FromGroup, Type: pb.MsgReadIndexResp, Index: ri,
						Entries: m.Entries})
				}
			}
		} else {
			// there is only one voting member (the leader) in the cluster
			if m.From == None || m.From == r.id { // from leader itself
				r.readStates = append(r.readStates, ReadState{Index: r.raftLog.committed, RequestCtx: m.Entries[0].Data})
			} else { // from learner member
				// see https://github.com/etcd-io/etcd/pull/10590
				r.send(pb.Message{To: m.From, ToGroup: m.FromGroup, Type: pb.MsgReadIndexResp, Index: r.raftLog.committed, Entries: m.Entries})
			}
		}

		return false
	}

	// All other message types require a progress for m.From (pr).
	pr := r.getProgress(m.From)
	if pr == nil {
		//r.logger.Debugf("%x no progress available for %x", r.id, m.From)
		return true
	}
	switch m.Type {
	case pb.MsgAppResp:
		pr.RecentActive = true

		if m.Reject {
			r.logger.Debugf("%x(%v) received msgApp rejection(lastindex: %d) from %x for index %d",
				r.id, r.group.Name, m.RejectHint, m.From, m.Index)
			if pr.maybeDecrTo(m.Index, m.RejectHint) {
				r.logger.Debugf("%x decreased progress of %x to [%s]", r.id, m.From, pr)
				if pr.State == ProgressStateReplicate {
					pr.becomeProbe()
				}
				r.sendAppend(m.From)
			}
		} else {
			oldPaused := pr.IsPaused()
			if pr.maybeUpdate(m.Index) {
				switch {
				case pr.State == ProgressStateProbe:
					pr.becomeReplicate()
				case pr.State == ProgressStateSnapshot && pr.needSnapshotAbort():
					r.logger.Debugf("%x snapshot aborted, resumed sending replication messages to %x [%s]", r.id, m.From, pr)
					// Transition back to replicating state via probing state
					// (which takes the snapshot into account). If we didn't
					// move to replicating state, that would only happen with
					// the next round of appends (but there may not be a next
					// round for a while, exposing an inconsistent RaftStatus).
					pr.becomeProbe()
					pr.becomeReplicate()
				case pr.State == ProgressStateReplicate:
					pr.ins.freeTo(m.Index)
				}

				if r.maybeCommit() {
					r.bcastAppend()
				} else if oldPaused {
					// If we were paused before, this node may be missing the
					// latest commit index, so send it.
					r.sendAppend(m.From)
				}
				// We've updated flow control information above, which may
				// allow us to send multiple (size-limited) in-flight messages
				// at once (such as when transitioning from probe to
				// replicate, or when freeTo() covers multiple messages). If
				// we have more entries to send, send as many messages as we
				// can (without sending empty messages for the commit index)
				for r.maybeSendAppend(m.From, false) {
				}
				// Transfer leadership is in progress.
				if m.From == r.leadTransferee && pr.Match == r.raftLog.lastIndex() {
					r.logger.Infof("%x(%v) sent MsgTimeoutNow to %x after received MsgAppResp", r.id, r.group.Name, m.From)
					r.sendTimeoutNow(m.From, m.FromGroup)
				}
			}
		}
	case pb.MsgHeartbeatResp:
		pr.RecentActive = true
		pr.resume()

		// free one slot for the full inflights window to allow progress.
		if pr.State == ProgressStateReplicate && pr.ins.full() {
			pr.ins.freeFirstOne()
		}
		if pr.Match < r.raftLog.lastIndex() {
			r.sendAppend(m.From)
		}

		if r.readOnly.option != ReadOnlySafe || len(m.Context) == 0 {
			return false
		}

		ackCount := r.readOnly.recvAck(m)
		if ackCount < r.quorum() {
			return false
		}

		rss := r.readOnly.advance(m)
		for _, rs := range rss {
			req := rs.req
			if req.From == None || IsFromLocalNodeMsg(r, &req) { // from local member
				r.readStates = append(r.readStates, ReadState{Index: rs.index, RequestCtx: req.Entries[0].Data})
			} else {
				r.send(pb.Message{To: req.From, ToGroup: req.FromGroup, Type: pb.MsgReadIndexResp, Index: rs.index,
					Entries: req.Entries})
			}
		}
	case pb.MsgSnapStatus:
		if pr.State != ProgressStateSnapshot {
			return false
		}
		if !m.Reject {
			pr.becomeProbe()
			r.logger.Debugf("%x snapshot succeeded, resumed sending replication messages to %x [%s]", r.id, m.From, pr)
		} else {
			pr.snapshotFailure()
			pr.becomeProbe()
			r.logger.Debugf("%x snapshot failed, resumed sending replication messages to %x [%s]", r.id, m.From, pr)
		}
		// If snapshot finish, wait for the msgAppResp from the remote node before sending
		// out the next msgApp.
		// If snapshot failure, wait for a heartbeat interval before next try
		pr.pause()
	case pb.MsgUnreachable:
		// During optimistic replication, if the remote becomes unreachable,
		// there is huge probability that a MsgApp is lost.
		if pr.State == ProgressStateReplicate {
			pr.becomeProbe()
		}
		r.logger.Debugf("%x failed to send message to %x because it is unreachable [%s]", r.id, m.From, pr)
	case pb.MsgTransferLeader:
		if pr.IsLearner {
			r.logger.Debugf("%x is learner. Ignored transferring leadership", r.id)
			return false
		}
		leadTransferee := m.From
		lastLeadTransferee := r.leadTransferee
		if lastLeadTransferee != None {
			if lastLeadTransferee == leadTransferee {
				r.logger.Infof("%x(%v) [term %d] transfer leadership to %x is in progress, ignores request to same node %x",
					r.id, r.group.Name, r.Term, leadTransferee, leadTransferee)
				return false
			}
			r.abortLeaderTransfer()
			r.logger.Infof("%x(%v) [term %d] abort previous transferring leadership to %x", r.id, r.group.Name, r.Term, lastLeadTransferee)
		}
		if leadTransferee == r.id {
			r.logger.Debugf("%x(%v) is already leader. Ignored transferring leadership to self", r.id, r.group.Name)
			return false
		}
		// Transfer leadership to third party.
		r.logger.Infof("%x(%v) [term %d] starts to transfer leadership to %x", r.id, r.group.Name, r.Term, leadTransferee)
		// Transfer leadership should be finished in one electionTimeout, so reset r.electionElapsed.
		r.electionElapsed = 0
		r.leadTransferee = leadTransferee
		if pr.Match == r.raftLog.lastIndex() {
			r.sendTimeoutNow(leadTransferee, m.FromGroup)
			r.logger.Infof("%x sends MsgTimeoutNow to %x immediately as %x already has up-to-date log", r.id, leadTransferee, leadTransferee)
		} else {
			r.sendAppend(leadTransferee)
		}
	}
	return false
}

// stepCandidate is shared by StateCandidate and StatePreCandidate; the difference is
// whether they respond to MsgVoteResp or MsgPreVoteResp.
func stepCandidate(r *raft, m pb.Message) bool {
	// Only handle vote responses corresponding to our candidacy (while in
	// StateCandidate, we may get stale MsgPreVoteResp messages in this term from
	// our pre-candidate state).
	var myVoteRespType pb.MessageType
	if r.state == StatePreCandidate {
		myVoteRespType = pb.MsgPreVoteResp
	} else {
		myVoteRespType = pb.MsgVoteResp
	}
	switch m.Type {
	case pb.MsgProp:
		r.logger.Infof("%x no leader at term %d; dropping proposal", r.id, r.Term)
		return true
	case pb.MsgApp:
		r.becomeFollower(m.Term, m.From) // always m.Term == r.Term
		r.handleAppendEntries(m)
	case pb.MsgHeartbeat:
		r.becomeFollower(m.Term, m.From) // always m.Term == r.Term
		r.handleHeartbeat(m)
	case pb.MsgSnap:
		r.becomeFollower(m.Term, m.From)
		r.handleSnapshot(m)
	case myVoteRespType:
		gr := r.poll(m.From, m.Type, !m.Reject)
		r.logger.Infof("%x [quorum:%d] has received %d %s votes and %d vote rejections", r.id, r.quorum(), gr, m.Type, len(r.votes)-gr)
		switch r.quorum() {
		case gr:
			if r.state == StatePreCandidate {
				r.campaign(campaignElection)
			} else {
				r.becomeLeader()
				r.bcastAppend()
			}
		case len(r.votes) - gr:
			// pb.MsgPreVoteResp contains future term of pre-candidate
			// m.Term > r.Term; reuse r.Term
			r.becomeFollower(r.Term, None)
		}
	case pb.MsgTimeoutNow:
		r.logger.Debugf("%x [term %d state %v] ignored MsgTimeoutNow from %x", r.id, r.Term, r.state, m.From)
	}
	return false
}

func stepFollower(r *raft, m pb.Message) bool {
	switch m.Type {
	case pb.MsgProp:
		if r.lead == None {
			r.logger.Infof("%x no leader at term %d; dropping proposal", r.id, r.Term)
			return true
		} else if r.disableProposalForwarding {
			r.logger.Infof("%x not forwarding to leader %x at term %d; dropping proposal", r.id, r.lead, r.Term)
			return true
		}
		m.To = r.lead
		if pr := r.getProgress(m.To); pr != nil {
			m.ToGroup = pr.group
		}
		r.send(m)
	case pb.MsgApp:
		r.electionElapsed = 0
		r.lead = m.From
		r.handleAppendEntries(m)
	case pb.MsgHeartbeat:
		r.electionElapsed = 0
		r.lead = m.From
		r.handleHeartbeat(m)
	case pb.MsgSnap:
		r.electionElapsed = 0
		r.lead = m.From
		r.handleSnapshot(m)
	case pb.MsgTransferLeader:
		if r.lead == None {
			r.logger.Infof("%x no leader at term %d; dropping leader transfer msg", r.id, r.Term)
			return true
		}
		m.To = r.lead
		if pr := r.getProgress(m.To); pr != nil {
			m.ToGroup = pr.group
		}
		r.send(m)
	case pb.MsgTimeoutNow:
		if r.promotable() {
			r.logger.Infof("%x [term %d] received MsgTimeoutNow from %x and starts an election to get leadership.", r.id, r.Term, m.From)
			// Leadership transfers never use pre-vote even if r.preVote is true; we
			// know we are not recovering from a partition so there is no need for the
			// extra round trip.
			r.campaign(campaignTransfer)
		} else {
			r.logger.Infof("%x received MsgTimeoutNow from %x but is not promotable", r.id, m.From)
		}
	case pb.MsgReadIndex:
		if r.lead == None {
			r.logger.Infof("%x no leader at term %d; dropping index reading msg", r.id, r.Term)
			return true
		}
		m.To = r.lead
		if pr := r.getProgress(m.To); pr != nil {
			m.ToGroup = pr.group
		}
		r.send(m)
	case pb.MsgReadIndexResp:
		if len(m.Entries) != 1 {
			r.logger.Errorf("%x invalid format of MsgReadIndexResp from %x, entries count: %d", r.id, m.From, len(m.Entries))
			return false
		}
		r.readStates = append(r.readStates, ReadState{Index: m.Index, RequestCtx: m.Entries[0].Data})
	}
	return false
}

func (r *raft) handleAppendEntries(m pb.Message) {
	if m.Index < r.raftLog.committed {
		r.send(pb.Message{To: m.From, ToGroup: m.FromGroup, Type: pb.MsgAppResp, Index: r.raftLog.committed})
		return
	}

	if mlastIndex, ok := r.raftLog.maybeAppend(m.Index, m.LogTerm, m.Commit, m.Entries...); ok {
		r.send(pb.Message{To: m.From, ToGroup: m.FromGroup, Type: pb.MsgAppResp, Index: mlastIndex})
	} else {
		r.logger.Debugf("%x(%v) [logterm: %d, index: %d] rejected msgApp [logterm: %d, index: %d] from %x",
			r.id, r.group, r.raftLog.zeroTermOnErrCompacted(r.raftLog.term(m.Index)), m.Index, m.LogTerm, m.Index, m.From)
		r.send(pb.Message{To: m.From, ToGroup: m.FromGroup, Type: pb.MsgAppResp, Index: m.Index, Reject: true,
			RejectHint: r.raftLog.lastIndex()})
	}
}

func (r *raft) handleHeartbeat(m pb.Message) {
	r.raftLog.commitTo(m.Commit)
	r.send(pb.Message{To: m.From, ToGroup: m.FromGroup, Type: pb.MsgHeartbeatResp, Context: m.Context})
}

func (r *raft) handleSnapshot(m pb.Message) {
	sindex, sterm := m.Snapshot.Metadata.Index, m.Snapshot.Metadata.Term
	if r.restore(m.Snapshot) {
		r.logger.Infof("%x [commit: %d] restored snapshot [index: %d, term: %d]",
			r.id, r.raftLog.committed, sindex, sterm)
		r.send(pb.Message{To: m.From, ToGroup: m.FromGroup, Type: pb.MsgAppResp, Index: r.raftLog.lastIndex()})
	} else {
		r.logger.Infof("%x [commit: %d] ignored snapshot [index: %d, term: %d]",
			r.id, r.raftLog.committed, sindex, sterm)
		r.send(pb.Message{To: m.From, ToGroup: m.FromGroup, Type: pb.MsgAppResp, Index: r.raftLog.committed})
	}
}

// restore recovers the state machine from a snapshot. It restores the log and the
// configuration of state machine.
func (r *raft) restore(s pb.Snapshot) bool {
	if s.Metadata.Index <= r.raftLog.committed {
		return false
	}
	if r.raftLog.matchTerm(s.Metadata.Index, s.Metadata.Term) {
		r.logger.Infof("%x [commit: %d, lastindex: %d, lastterm: %d] fast-forwarded commit to snapshot [index: %d, term: %d]",
			r.id, r.raftLog.committed, r.raftLog.lastIndex(), r.raftLog.lastTerm(), s.Metadata.Index, s.Metadata.Term)
		r.raftLog.commitTo(s.Metadata.Index)
		return false
	}

	// Both of `prs` and `learnerPrs` are empty means the peer is new created by
	// conf change, in which case we should accept snapshots make it as learner.
	if (len(r.prs) > 0 || len(r.learnerPrs) > 0) && !r.isLearner {
		for _, id := range s.Metadata.ConfState.Learners {
			if id == r.id {
				r.logger.Errorf("%x can't become learner when restores snapshot [index: %d, term: %d]", r.id, s.Metadata.Index, s.Metadata.Term)
				return false
			}
		}
	}

	r.logger.Infof("%x [commit: %d, lastindex: %d, lastterm: %d] starts to restore snapshot [index: %d, term: %d, prs:%v]",
		r.id, r.raftLog.committed, r.raftLog.lastIndex(), r.raftLog.lastTerm(),
		s.Metadata.Index, s.Metadata.Term, r.prs)

	r.raftLog.restore(s)
	r.prs = make(map[uint64]*Progress)
	r.learnerPrs = make(map[uint64]*Progress)

	success := r.restoreNode(s.Metadata.ConfState.Nodes, s.Metadata.ConfState.Groups, false)
	if !success {
		return false
	}
	success = r.restoreNode(s.Metadata.ConfState.Learners, s.Metadata.ConfState.LearnerGroups, true)
	return success
}

func (r *raft) restoreNode(nodes []uint64, grpsConf []*pb.Group, isLearner bool) bool {
	grps := make(map[uint64]pb.Group)
	for _, g := range grpsConf {
		if g.GroupId != r.group.GroupId {
			r.logger.Errorf("group id mismatch for replica: %v, %v", g, r.group)
			return false
		}
		grps[g.RaftReplicaId] = *g
	}

	for _, n := range nodes {
		match, next := uint64(0), r.raftLog.lastIndex()+1
		if n == r.id {
			match = next - 1
			r.isLearner = isLearner
		}
		r.setProgress(n, match, next, grps[n], isLearner)
		r.logger.Infof("%x(%v) restored progress of %x(%v) [%s]", r.id, r.group, n, grps[n], r.getProgress(n))
	}
	return true
}

// promotable indicates whether state machine can be promoted to leader,
// which is true when its own id is in progress list.
func (r *raft) promotable() bool {
	pr, ok := r.prs[r.id]
	return ok && pr != nil && !pr.IsLearner && !r.raftLog.hasPendingSnapshot()
}

func (r *raft) updateNode(id uint64, g pb.Group) {
	r.pendingConf = false
	if pr := r.getProgress(id); pr != nil {
		r.logger.Infof("%x update progress group info for node : %v, %v", r.id, pr, g)
		if g.NodeId > 0 && g.GroupId == r.group.GroupId && g.RaftReplicaId == id {
			pr.group = g
		}
	}
}

func (r *raft) addNode(id uint64, g pb.Group) {
	r.addNodeOrLearnerNode(id, g, false)
}

func (r *raft) addLearner(id uint64, g pb.Group) {
	r.addNodeOrLearnerNode(id, g, true)
}

func (r *raft) addNodeOrLearnerNode(id uint64, g pb.Group, isLearner bool) {
	r.pendingConf = false
	pr := r.getProgress(id)
	if pr == nil {
		r.setProgress(id, 0, r.raftLog.lastIndex()+1, g, isLearner)
	} else {
		if isLearner && !pr.IsLearner {
			// can only change Learner to Voter
			r.logger.Infof("%x(%v) ignored addLeaner: do not support changing %x from raft peer to learner.", r.id, r.group, id)
			return
		}
		if pr.group.NodeId == 0 || pr.group.GroupId == 0 ||
			pr.group.RaftReplicaId == 0 {
			r.logger.Infof("%x missing progress group info for node : %v, %v", r.id, pr, g)
			pr.group = g
		}
		// Ignore any redundant addNode calls (which can happen because the
		// initial bootstrapping entries are applied twice).
		if isLearner == pr.IsLearner {
			// Ignore any redundant addNode calls (which can happen because the
			// initial bootstrapping entries are applied twice).
			return
		}
		// TODO: add flag to disable learner became voter, this node will be always a learner, working as catchup node only
		//if r.keepLearner {
		//	return
		//}
		//
		// change Learner to Voter, use origin Learner progress
		delete(r.learnerPrs, id)
		pr.IsLearner = false
		r.prs[id] = pr
		r.logger.Infof("%x(%v) changed : %x from raft learner to voter.", r.id, r.group, id)
	}

	if r.id == id {
		r.isLearner = isLearner
	}
	// When a node is first added, we should mark it as recently active.
	// Otherwise, CheckQuorum may cause us to step down if it is invoked
	// before the added node has a chance to communicate with us.
	pr = r.getProgress(id)
	pr.RecentActive = true
}

func (r *raft) removeNode(id uint64) {
	r.delProgress(id)
	r.pendingConf = false

	// do not try to commit or abort transferring if there is no nodes in the cluster.
	if len(r.prs) == 0 && len(r.learnerPrs) == 0 {
		return
	}

	// The quorum size is now smaller, so see if any pending entries can
	// be committed.
	if r.maybeCommit() {
		r.bcastAppend()
	}
	// If the removed node is the leadTransferee, then abort the leadership transferring.
	if r.state == StateLeader && r.leadTransferee == id {
		r.abortLeaderTransfer()
	}
}

func (r *raft) resetPendingConf() { r.pendingConf = false }

func (r *raft) setProgress(id, match, next uint64, g pb.Group, isLearner bool) {
	if !isLearner {
		delete(r.learnerPrs, id)
		r.prs[id] = &Progress{Next: next, Match: match,
			ins: newInflights(r.maxInflight), group: g}
		r.logger.Infof(" %x [peer progress: %v ]", r.id, r.prs[id])
		return
	}
	if _, ok := r.prs[id]; ok {
		panic(fmt.Sprintf("%x unexpected changing from voter to learner for %x", r.id, id))
	}
	r.learnerPrs[id] = &Progress{Next: next, Match: match, ins: newInflights(r.maxInflight),
		group: g, IsLearner: true}
	r.logger.Infof(" %x [peer progress: %v ]", r.id, r.learnerPrs[id])
}

func (r *raft) delProgress(id uint64) {
	delete(r.prs, id)
	delete(r.learnerPrs, id)
}

func (r *raft) loadState(state pb.HardState) {
	if state.Commit < r.raftLog.committed || state.Commit > r.raftLog.lastIndex() {
		r.logger.Panicf("%x state.commit %d is out of range [%d, %d]", r.id, state.Commit, r.raftLog.committed, r.raftLog.lastIndex())
	}
	r.raftLog.committed = state.Commit
	r.Term = state.Term
	r.Vote = state.Vote
}

// pastElectionTimeout returns true iff r.electionElapsed is greater
// than or equal to the randomized election timeout in
// [electiontimeout, 2 * electiontimeout - 1].
func (r *raft) pastElectionTimeout() bool {
	return r.electionElapsed >= r.randomizedElectionTimeout
}

func (r *raft) resetRandomizedElectionTimeout() {
	r.randomizedElectionTimeout = r.electionTimeout + globalRand.Intn(r.electionTimeout)
}

// checkQuorumActive returns true if the quorum is active from
// the view of the local raft state machine. Otherwise, it returns
// false.
// checkQuorumActive also resets all RecentActive to false.
func (r *raft) checkQuorumActive() bool {
	var act int

	r.forEachProgress(func(id uint64, pr *Progress) {
		if id == r.id { // self is always active
			act++
			return
		}

		if pr.RecentActive && !pr.IsLearner {
			act++
		}
		pr.RecentActive = false
	})
	return act >= r.quorum()
}

func (r *raft) sendTimeoutNow(to uint64, toGroup pb.Group) {
	r.send(pb.Message{To: to, ToGroup: toGroup, Type: pb.MsgTimeoutNow})
}

func (r *raft) abortLeaderTransfer() {
	r.leadTransferee = None
}

func numOfPendingConf(ents []pb.Entry) int {
	n := 0
	for i := range ents {
		if ents[i].Type == pb.EntryConfChange {
			n++
		}
	}
	return n
}
