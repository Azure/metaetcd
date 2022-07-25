package util

import (
	"sync"

	"go.etcd.io/etcd/pkg/v3/adt"
)

// GroupTree extends etcd's ADT package by grouping listeners such that they can overlap.
// Otherwise ADT only supports a single listener per keyspace.
type GroupTree[T any] struct {
	mut  sync.RWMutex
	tree adt.IntervalTree
}

func NewGroupTree[T any]() *GroupTree[T] {
	return &GroupTree[T]{
		tree: adt.NewIntervalTree(),
	}
}

func (g *GroupTree[T]) Add(interval adt.Interval, ch chan T) {
	g.mut.Lock()
	defer g.mut.Unlock()

	val := g.tree.Find(interval)
	if val != nil {
		val.Val.(*watchGroup[T]).Chans[ch] = struct{}{}
		return
	}
	g.tree.Insert(interval, &watchGroup[T]{Chans: map[chan T]struct{}{ch: {}}})
}

func (g *GroupTree[T]) Remove(interval adt.Interval, ch chan T) {
	g.mut.Lock()
	defer g.mut.Unlock()

	val := g.tree.Find(interval)
	if val == nil {
		return
	}

	grp := val.Val.(*watchGroup[T])
	delete(grp.Chans, ch)
	if len(grp.Chans) > 0 {
		return
	}

	g.tree.Delete(interval)
}

func (g *GroupTree[T]) Broadcast(key adt.Interval, event T) {
	g.mut.RLock()
	defer g.mut.RUnlock()

	for _, i := range g.tree.Stab(key) {
		for ch := range i.Val.(*watchGroup[T]).Chans {
			ch <- event
		}
	}
}

type watchGroup[T any] struct {
	Chans map[chan T]struct{}
}
