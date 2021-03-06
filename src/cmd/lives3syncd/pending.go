package main

import (
	"container/heap"
)

// PendingSync is an entry for a file that needs to be synced which we manage in a priority queue.
type PendingSync struct {
	Name     string
	Mtime    int64
	Size     int64
	Sequence uint64
	Attempts int

	Uploaded bool

	// The heap index is needed by update and is maintained by the heap.Interface methods.
	index int
}

// A PriorityQueue implements heap.Interface and holds Items.
type PriorityQueue []*PendingSync

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	// We want Pop to give us the highest, not lowest, priority so we use greater than here.
	return pq[i].Mtime > pq[j].Mtime
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*PendingSync)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

// update modifies the priority and value of an Item in the queue.
func (pq *PriorityQueue) update(item *PendingSync, mtime, size int64) {
	// TODO: atomic updates
	item.Mtime = mtime
	item.Size = size
	heap.Fix(pq, item.index)
}

// // This example creates a PriorityQueue with some items, adds and manipulates an item,
// // and then removes the items in priority order.
// func main() {
// 	// Some items and their priorities.
// 	items := map[string]int{
// 		"banana": 3, "apple": 2, "pear": 4,
// 	}
//
// 	// Create a priority queue, put the items in it, and
// 	// establish the priority queue (heap) invariants.
// 	pq := make(PriorityQueue, len(items))
// 	i := 0
// 	for value, priority := range items {
// 		pq[i] = &Item{
// 			value:    value,
// 			priority: priority,
// 			index:    i,
// 		}
// 		i++
// 	}
// 	heap.Init(&pq)
//
// 	// Insert a new item and then modify its priority.
// 	item := &Item{
// 		value:    "orange",
// 		priority: 1,
// 	}
// 	heap.Push(&pq, item)
// 	pq.update(item, item.value, 5)
//
// 	// Take the items out; they arrive in decreasing priority order.
// 	for pq.Len() > 0 {
// 		item := heap.Pop(&pq).(*PendingSync)
// 		fmt.Printf("%.2d:%s ", item.priority, item.value)
// 	}
// }
