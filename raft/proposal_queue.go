// Copyright 2017-2019 Lei Ni (nilei81@gmail.com)
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
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/context"
)

var ErrProposalQueueTimeout = errors.New("queue proposal timeout")
var ErrProposalQueueTooFull = errors.New("queue proposal too full")

// ProposalQueue is the queue used to hold Raft messages.
type ProposalQueue struct {
	size          uint64
	left          []msgWithDrop
	right         []msgWithDrop
	leftInWrite   bool
	stopped       bool
	idx           uint64
	oldIdx        uint64
	cycle         uint64
	lazyFreeCycle uint64
	mu            sync.Mutex
	waitC         chan struct{}
	waitCnt       int64
}

// NewProposalQueue creates a new ProposalQueue instance.
func NewProposalQueue(size uint64, lazyFreeCycle uint64) *ProposalQueue {
	q := &ProposalQueue{
		size:          size,
		lazyFreeCycle: lazyFreeCycle,
		left:          make([]msgWithDrop, size),
		right:         make([]msgWithDrop, size),
		waitC:         make(chan struct{}, 1),
	}
	return q
}

// Close closes the queue so no further messages can be added.
func (q *ProposalQueue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.stopped = true
	if q.waitC != nil {
		close(q.waitC)
	}
}

func (q *ProposalQueue) targetQueue() []msgWithDrop {
	var t []msgWithDrop
	if q.leftInWrite {
		t = q.left
	} else {
		t = q.right
	}
	return t
}

// AddWait adds the specified message to the queue and wait if full.
func (q *ProposalQueue) AddWait(ctx context.Context, msg msgWithDrop,
	to time.Duration, stopC chan struct{}) (bool, bool, error) {
	if to <= 0 {
		added, stopped, _ := q.Add(msg)
		return added, stopped, nil
	}
	var t *time.Timer
	for {
		added, stopped, w := q.Add(msg)
		if added || stopped {
			if t != nil {
				t.Stop()
			}
			return added, stopped, nil
		}
		// too full
		if atomic.LoadInt64(&q.waitCnt) > int64(q.size)*5 {
			if t != nil {
				t.Stop()
			}
			return false, stopped, ErrProposalQueueTooFull
		}
		if t == nil {
			t = time.NewTimer(to)
		}
		atomic.AddInt64(&q.waitCnt, 1)
		select {
		case <-stopC:
			atomic.AddInt64(&q.waitCnt, -1)
			t.Stop()
			return false, false, ErrStopped
		case <-ctx.Done():
			atomic.AddInt64(&q.waitCnt, -1)
			t.Stop()
			return false, false, ctx.Err()
		case <-t.C:
			atomic.AddInt64(&q.waitCnt, -1)
			t.Stop()
			return false, false, ErrProposalQueueTimeout
		case <-w:
		}
		atomic.AddInt64(&q.waitCnt, -1)
	}
}

// Add adds the specified message to the queue.
func (q *ProposalQueue) Add(msg msgWithDrop) (bool, bool, chan struct{}) {
	q.mu.Lock()
	wc := q.waitC
	if q.idx >= q.size {
		q.mu.Unlock()
		return false, q.stopped, wc
	}
	if q.stopped {
		q.mu.Unlock()
		return false, true, wc
	}
	w := q.targetQueue()
	w[q.idx] = msg
	q.idx++
	q.mu.Unlock()
	return true, false, wc
}

func (q *ProposalQueue) gc() {
	if q.lazyFreeCycle > 0 {
		oldq := q.targetQueue()
		if q.lazyFreeCycle == 1 {
			for i := uint64(0); i < q.oldIdx; i++ {
				oldq[i].m.Entries = nil
			}
		} else if q.cycle%q.lazyFreeCycle == 0 {
			for i := uint64(0); i < q.size; i++ {
				oldq[i].m.Entries = nil
			}
		}
	}
}

// Get returns everything current in the queue.
func (q *ProposalQueue) Get() []msgWithDrop {
	q.mu.Lock()
	defer q.mu.Unlock()
	needNotify := false
	if q.idx >= q.size {
		needNotify = true
	}
	q.cycle++
	sz := q.idx
	q.idx = 0
	t := q.targetQueue()
	q.leftInWrite = !q.leftInWrite
	q.gc()
	q.oldIdx = sz
	if needNotify {
		close(q.waitC)
		q.waitC = make(chan struct{}, 1)
	}
	return t[:sz]
}
