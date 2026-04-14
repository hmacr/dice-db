package core

import "sort"

type PoolItem struct {
	key            string
	lastAccessedAt uint32
}

// TODO: When `lastAccessedAt` of object changes,
// update `PoolItem` corresponding to that
type EvictionPool struct {
	pool   []*PoolItem
	keyset map[string]*PoolItem
}

type ByIdleItem []*PoolItem

func (a ByIdleItem) Len() int {
	return len(a)
}

func (a ByIdleItem) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func (a ByIdleItem) Less(i, j int) bool {
	return getIdleTime(a[i].lastAccessedAt) > getIdleTime(a[j].lastAccessedAt)
}

// TODO: Make the implementation efficient to eliminate need for repeated sorting
func (pq *EvictionPool) Push(key string, lastAccessedAt uint32) {
	_, ok := pq.keyset[key]
	if ok {
		return
	}
	item := &PoolItem{key: key, lastAccessedAt: lastAccessedAt}
	if len(pq.pool) < ePoolSizeMax {
		pq.keyset[key] = item
		pq.pool = append(pq.pool, item)

		// Performance bottleneck
		sort.Sort(ByIdleItem(pq.pool))
	} else if lastAccessedAt > pq.pool[len(pq.pool)-1].lastAccessedAt {
		pq.pool = pq.pool[1:]
		pq.keyset[key] = item
		pq.pool = append(pq.pool, item)
	}
}

func (pq *EvictionPool) Pop() *PoolItem {
	if len(pq.pool) == 0 {
		return nil
	}
	item := pq.pool[0]
	pq.pool = pq.pool[1:]
	delete(pq.keyset, item.key)
	return item
}

func newEvictionPool(size int) *EvictionPool {
	return &EvictionPool{
		pool:   make([]*PoolItem, size),
		keyset: make(map[string]*PoolItem),
	}
}

var ePoolSizeMax int = 16
var ePool *EvictionPool = newEvictionPool(0)
