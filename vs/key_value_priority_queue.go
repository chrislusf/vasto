package vs

import (
	"bytes"
	"container/heap"
)

// An typeItem is something we manage in a priority queue.
type typeItem struct {
	*KeyValue
	chanIndex int
}

// A pqKeyTypeValue implements heap.Interface and holds Items.
type pqKeyTypeValue []*typeItem

func (pq pqKeyTypeValue) Len() int { return len(pq) }

func (pq pqKeyTypeValue) Less(i, j int) bool {
	return bytes.Compare(pq[i].KeyObject.key, pq[j].KeyObject.key) < 0
}

func (pq pqKeyTypeValue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *pqKeyTypeValue) Push(x interface{}) {
	typeItem := x.(*typeItem)
	*pq = append(*pq, typeItem)
}

func (pq *pqKeyTypeValue) Pop() interface{} {
	old := *pq
	n := len(old)
	typeItem := old[n-1]
	*pq = old[0 : n-1]
	return typeItem
}

func limitedMergeSorted(chans []chan *KeyValue, limit int) (results []*KeyValue) {

	pq := make(pqKeyTypeValue, 0, len(chans))

	for i := 0; i < len(chans); i++ {
		if chans[i] == nil {
			continue
		}
		keyValue := <-chans[i]
		if keyValue != nil {
			pq = append(pq, &typeItem{
				KeyValue:  keyValue,
				chanIndex: i,
			})
		}
	}
	heap.Init(&pq)

	for pq.Len() > 0 && limit > 0 {
		limit--

		t := heap.Pop(&pq).(*typeItem)
		results = append(results, t.KeyValue)
		newT, hasMore := <-chans[t.chanIndex]
		if hasMore {
			heap.Push(&pq, &typeItem{
				KeyValue:  newT,
				chanIndex: t.chanIndex,
			})
			heap.Fix(&pq, len(pq)-1)
		}
	}

	return
}
