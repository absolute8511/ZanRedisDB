package metric

import (
	"container/heap"
	"sort"
	"sync"
)

const (
	minCollSizeInHeap   = 32
	DefaultHeapCapacity = 100
)

// An Item is something we manage in a priority queue.
type Item struct {
	value    []byte // The value of the item; arbitrary.
	priority int    // The priority of the item in the queue.
	// The index is needed by update and is maintained by the heap.Interface methods.
	index int // The index of the item in the heap.
}

// A PriorityQueue implements heap.Interface and holds Items.
type PriorityQueue []*Item

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].priority < pq[j].priority
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*Item)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

// update modifies the priority and value of an Item in the queue.
func (pq *PriorityQueue) update(item *Item, priority int) {
	item.priority = priority
	heap.Fix(pq, item.index)
}

type CollSizeHeap struct {
	l        sync.Mutex
	pq       PriorityQueue
	items    map[string]*Item
	capacity int
}

func NewCollSizeHeap(cap int) *CollSizeHeap {
	q := make(PriorityQueue, 0)
	heap.Init(&q)
	return &CollSizeHeap{
		pq:       q,
		items:    make(map[string]*Item),
		capacity: cap,
	}
}

func (csh *CollSizeHeap) Update(key []byte, collSize int) {
	csh.l.Lock()
	defer csh.l.Unlock()
	item, ok := csh.items[string(key)]
	if ok {
		if collSize < minCollSizeInHeap {
			heap.Remove(&csh.pq, item.index)
			csh.items[string(key)] = nil
			delete(csh.items, string(key))
		} else {
			csh.pq.update(item, collSize)
		}
	} else {
		if collSize < minCollSizeInHeap {
			return
		}
		item = &Item{
			value:    key,
			priority: collSize,
		}
		heap.Push(&csh.pq, item)
		csh.items[string(key)] = item
		if csh.pq.Len() > csh.capacity {
			old := heap.Pop(&csh.pq)
			if old != nil {
				oldItem := old.(*Item)
				csh.items[string(oldItem.value)] = nil
				delete(csh.items, string(oldItem.value))
			}
		}
	}
}

func (csh *CollSizeHeap) keys() topnList {
	csh.l.Lock()
	defer csh.l.Unlock()
	keys := make(topnList, 0, len(csh.items))
	for _, item := range csh.items {
		keys = append(keys, TopNInfo{Key: string(item.value), Cnt: int32(item.priority)})
	}
	return keys
}

func (csh *CollSizeHeap) TopKeys() []TopNInfo {
	keys := csh.keys()
	sort.Sort(keys)
	return keys
}
