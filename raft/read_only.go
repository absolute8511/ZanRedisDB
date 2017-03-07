// Copyright 2016 The etcd Authors
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
	pb "github.com/absolute8511/ZanRedisDB/raft/raftpb"
)

// ReadState provides state for read only query.
// It's caller's responsibility to call ReadIndex first before getting
// this state from ready, It's also caller's duty to differentiate if this
// state is what it requests through RequestCtx, eg. given a unique id as
// RequestCtx
var (
	empty = []byte("")
)

type ReadState struct {
	Index      uint64
	RequestCtx []byte
}

type readIndexStatus struct {
	req   pb.Message
	index uint64
	acks  map[uint64]struct{}
}

type readOnly struct {
	option           ReadOnlyOption
	pendingReadIndex map[string]*readIndexStatus
	readIndexQueue   [][]byte
	buf              [][]byte
}

func newReadOnly(option ReadOnlyOption) *readOnly {
	r := &readOnly{
		option:           option,
		pendingReadIndex: make(map[string]*readIndexStatus),
		buf:              make([][]byte, 0, 32),
	}
	r.readIndexQueue = r.buf
	return r
}

// addRequest adds a read only reuqest into readonly struct.
// `index` is the commit index of the raft state machine when it received
// the read only request.
// `m` is the original read only request message from the local or remote node.
func (ro *readOnly) addRequest(index uint64, m pb.Message) {
	if _, ok := ro.pendingReadIndex[string(m.Entries[0].Data)]; ok {
		return
	}
	ro.pendingReadIndex[string(m.Entries[0].Data)] = &readIndexStatus{index: index, req: m, acks: make(map[uint64]struct{})}
	ro.readIndexQueue = append(ro.readIndexQueue, m.Entries[0].Data)
}

// recvAck notifies the readonly struct that the raft state machine received
// an acknowledgment of the heartbeat that attached with the read only request
// context.
func (ro *readOnly) recvAck(m pb.Message) int {
	rs, ok := ro.pendingReadIndex[string(m.Context)]
	if !ok {
		return 0
	}

	rs.acks[m.From] = struct{}{}
	// add one to include an ack from local node
	return len(rs.acks) + 1
}

// advance advances the read only request queue kept by the readonly struct.
// It dequeues the requests until it finds the read only request that has
// the same context as the given `m`.
func (ro *readOnly) advance(m pb.Message) []*readIndexStatus {
	var (
		i     int
		found bool
	)

	rss := []*readIndexStatus{}

	for _, okctx := range ro.readIndexQueue {
		i++
		rs, ok := ro.pendingReadIndex[string(okctx)]
		if !ok {
			panic("cannot find corresponding read state from pending map")
		}
		rss = append(rss, rs)
		if bytes.Equal(okctx, m.Context) {
			found = true
			break
		}
	}

	if found {
		ro.readIndexQueue = ro.readIndexQueue[i:]
		for _, rs := range rss {
			delete(ro.pendingReadIndex, string(rs.req.Context))
		}
		if len(ro.readIndexQueue) == 0 {
			ro.readIndexQueue = ro.buf
		} else {
			left := cap(ro.readIndexQueue) - len(ro.readIndexQueue)
			if cap(ro.readIndexQueue) > 2*cap(ro.buf) && cap(ro.readIndexQueue) < 1024 {
				ro.buf = make([][]byte, 0, cap(ro.readIndexQueue))
			}
			if len(ro.readIndexQueue) < cap(ro.buf)/2 &&
				left < 1+cap(ro.readIndexQueue)/8 {
				tmp := ro.readIndexQueue
				ro.readIndexQueue = ro.buf
				ro.readIndexQueue = append(ro.readIndexQueue, tmp...)
			}
		}
		return rss
	}

	return nil
}

// lastPendingRequestCtx returns the context of the last pending read only
// request in readonly struct.
func (ro *readOnly) lastPendingRequestCtx() []byte {
	if len(ro.readIndexQueue) == 0 {
		return empty
	}
	return ro.readIndexQueue[len(ro.readIndexQueue)-1]
}
