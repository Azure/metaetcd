package watch

import (
	"sync"

	"go.etcd.io/etcd/pkg/v3/adt"
)

type groupTree[T any] struct {
	mut  sync.RWMutex
	tree adt.IntervalTree
}

func newGroupTree[T any]() *groupTree[T] {
	return &groupTree[T]{
		tree: adt.NewIntervalTree(),
	}
}

func (g *groupTree[T]) Add(interval adt.Interval, ch chan T) {
	g.mut.Lock()
	defer g.mut.Unlock()

	val := g.tree.Find(interval)
	if val != nil {
		val.Val.(*watchGroup[T]).Chans[ch] = struct{}{}
		return
	}
	g.tree.Insert(interval, &watchGroup[T]{Chans: map[chan T]struct{}{ch: {}}})
}

func (g *groupTree[T]) Remove(interval adt.Interval, ch chan T) {
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

func (g *groupTree[T]) Broadcast(key adt.Interval, event T) {
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
