package pb

import (
	"bytes"
	"container/heap"
)

// An item is something we manage in a priority queue.
type item struct {
	*RawKeyValue
	chanIndex int
}

// A priorityQueue implements heap.Interface and holds Items.
type priorityQueue []*item

func (pq priorityQueue) Len() int { return len(pq) }

func (pq priorityQueue) Less(i, j int) bool {
	return bytes.Compare(pq[i].Key, pq[j].Key) < 0
}

func (pq priorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *priorityQueue) Push(x interface{}) {
	item := x.(*item)
	*pq = append(*pq, item)
}

func (pq *priorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[0 : n-1]
	return item
}

func MergeSorted(chans []chan *RawKeyValue, fn func(*RawKeyValue) error) error {

	pq := make(priorityQueue, 0, len(chans))

	for i := 0; i < len(chans); i++ {
		if chans[i] == nil {
			continue
		}
		keyValue := <-chans[i]
		if keyValue != nil {
			pq = append(pq, &item{
				RawKeyValue: keyValue,
				chanIndex:   i,
			})
		}
	}
	heap.Init(&pq)

	for pq.Len() > 0 {
		t := heap.Pop(&pq).(*item)
		if err := fn(t.RawKeyValue); err != nil {
			return err
		}
		newT, hasMore := <-chans[t.chanIndex]
		if hasMore {
			heap.Push(&pq, &item{
				RawKeyValue: newT,
				chanIndex:   t.chanIndex,
			})
			heap.Fix(&pq, len(pq)-1)
		}
	}

	return nil
}
