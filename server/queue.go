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

package server

import (
	"sync"
	"sync/atomic"
	"time"
)

type internalReq struct {
	d  []byte
	f  func()
	id uint64
}

type elemT internalReq

func newElemWithData(d []byte) elemT {
	e := elemT{}
	e.d = d
	return e
}

func (e elemT) GetID() uint64 {
	return e.id
}

func (e elemT) Func() func() {
	return e.f
}

func (e elemT) GetData() []byte {
	return e.d
}

func (e *elemT) ResetData() {
	e.d = nil
}

func (e *elemT) SetID(index uint64) {
	e.id = index
}

type entryQueue struct {
	size          uint64
	left          []elemT
	right         []elemT
	leftInWrite   bool
	stopped       bool
	paused        bool
	idx           uint64
	oldIdx        uint64
	cycle         uint64
	lazyFreeCycle uint64
	mu            sync.Mutex
	waitC         chan struct{}
	waitCnt       int64
}

func newEntryQueue(size uint64, lazyFreeCycle uint64) *entryQueue {
	e := &entryQueue{
		size:          size,
		lazyFreeCycle: lazyFreeCycle,
		left:          make([]elemT, size),
		right:         make([]elemT, size),
		waitC:         make(chan struct{}, 1),
	}
	return e
}

func (q *entryQueue) close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.stopped = true
	if q.waitC != nil {
		close(q.waitC)
	}
}

func (q *entryQueue) targetQueue() []elemT {
	var t []elemT
	if q.leftInWrite {
		t = q.left
	} else {
		t = q.right
	}
	return t
}

func (q *entryQueue) addWait(ent elemT, to time.Duration) (bool, bool) {
	if to <= 0 {
		added, stopped, _ := q.add(ent)
		return added, stopped
	}
	var t *time.Timer
	for {
		added, stopped, w := q.add(ent)
		if added || stopped {
			if t != nil {
				t.Stop()
			}
			return added, stopped
		}
		// too full
		if atomic.LoadInt64(&q.waitCnt) > int64(q.size)*5 {
			if t != nil {
				t.Stop()
			}
			return false, stopped
		}
		if t == nil {
			t = time.NewTimer(to)
		}
		atomic.AddInt64(&q.waitCnt, 1)
		select {
		case <-t.C:
			atomic.AddInt64(&q.waitCnt, -1)
			t.Stop()
			return false, false
		case <-w:
		}
		atomic.AddInt64(&q.waitCnt, -1)
	}
}

func (q *entryQueue) add(ent elemT) (bool, bool, chan struct{}) {
	q.mu.Lock()
	wc := q.waitC
	if q.paused || q.idx >= q.size {
		q.mu.Unlock()
		return false, q.stopped, wc
	}
	if q.stopped {
		q.mu.Unlock()
		return false, true, wc
	}
	w := q.targetQueue()
	w[q.idx] = ent
	q.idx++
	q.mu.Unlock()
	return true, false, wc
}

func (q *entryQueue) gc() {
	if q.lazyFreeCycle > 0 {
		oldq := q.targetQueue()
		if q.lazyFreeCycle == 1 {
			for i := uint64(0); i < q.oldIdx; i++ {
				oldq[i].ResetData()
			}
		} else if q.cycle%q.lazyFreeCycle == 0 {
			for i := uint64(0); i < q.size; i++ {
				oldq[i].ResetData()
			}
		}
	}
}

func (q *entryQueue) get(paused bool) []elemT {
	q.mu.Lock()
	defer q.mu.Unlock()
	needNotify := false
	if q.paused || q.idx >= q.size {
		needNotify = true
	}
	q.paused = paused
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

type readyCluster struct {
	mu    sync.Mutex
	ready map[uint64]struct{}
	maps  [2]map[uint64]struct{}
	index uint8
}

func newReadyCluster() *readyCluster {
	r := &readyCluster{}
	r.maps[0] = make(map[uint64]struct{})
	r.maps[1] = make(map[uint64]struct{})
	r.ready = r.maps[0]
	return r
}

func (r *readyCluster) setClusterReady(clusterID uint64) {
	r.mu.Lock()
	r.ready[clusterID] = struct{}{}
	r.mu.Unlock()
}

func (r *readyCluster) getReadyClusters() map[uint64]struct{} {
	r.mu.Lock()
	v := r.ready
	r.index++
	selected := r.index % 2
	nm := r.maps[selected]
	for k := range nm {
		delete(nm, k)
	}
	r.ready = nm
	r.mu.Unlock()
	return v
}
