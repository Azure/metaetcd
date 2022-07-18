package watch

import (
	"container/list"
	"context"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"go.etcd.io/etcd/pkg/v3/adt"
	"go.uber.org/zap"
)

// TODO: Consider watching only the metakey instead of maintaining event buffer for entire keyspace

type buffer struct {
	mut                    sync.Mutex
	list                   *list.List
	gapTimeout             time.Duration
	maxLen                 int
	lowerBound, upperBound int64
	bcast                  *broadcast
	upperVal               *list.Element
}

func newBuffer(gapTimeout time.Duration, maxLen int, bcast *broadcast) *buffer {
	return &buffer{list: list.New(), gapTimeout: gapTimeout, maxLen: maxLen, bcast: bcast, lowerBound: -1}
}

func (b *buffer) Run(ctx context.Context) {
	ticker := time.NewTicker(b.gapTimeout)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_, ok := b.bridgeGapUnlocked()
			if ok {
				b.bcast.Send()
			}
		}
	}
}

func (b *buffer) Push(events []*clientv3.Event) {
	b.mut.Lock()
	defer b.mut.Unlock()
	for _, event := range events {
		e := mvccpb.Event(*event)
		b.pushOrDeferUnlocked(&e)
	}
}

func (b *buffer) pushOrDeferUnlocked(event *mvccpb.Event) bool {
	b.pushUnlocked(event)
	ok, _ := b.bridgeGapUnlocked()
	b.trimUnlocked()
	if ok {
		b.bcast.Send()
	}
	return ok
}

func (b *buffer) pushUnlocked(event *mvccpb.Event) {
	lastEl := b.list.Back()
	wrapped := &eventWrapper{Event: event, Timestamp: time.Now(), Key: adt.NewStringAffinePoint(string(event.Kv.Key))}

	// Case 1: first element
	if lastEl == nil {
		b.list.PushFront(wrapped)
		return
	}

	// Case 2: outside of range - insert before or after
	last := lastEl.Value.(*eventWrapper)
	if event.Kv.ModRevision > last.Kv.ModRevision {
		b.list.PushBack(wrapped)
		return
	}

	firstEl := b.list.Front()
	first := firstEl.Value.(*eventWrapper)
	if event.Kv.ModRevision < first.Kv.ModRevision {
		b.list.PushFront(wrapped)
		return
	}

	// Case 3: find place between pairs of events
	for {
		firstEl = lastEl.Prev()
		if firstEl == nil {
			break
		}
		first = firstEl.Value.(*eventWrapper)

		if event.Kv.ModRevision > first.Kv.ModRevision {
			b.list.InsertAfter(wrapped, firstEl)
			return
		}
		lastEl = firstEl
	}
}

func (b *buffer) trimUnlocked() {
	if b.list.Len() <= b.maxLen {
		return
	}
	front := b.list.Front()
	if front == b.upperVal {
		return // don't trim events until the gap has been filled
	}

	next := front.Next()
	if next != nil {
		newFront := next.Value.(*eventWrapper)
		b.lowerBound = newFront.Kv.ModRevision
	}

	b.list.Remove(front)
}

func (b *buffer) StartRange(start int64) (*list.Element, bool) {
	b.mut.Lock()
	defer b.mut.Unlock()

	if start < b.lowerBound {
		return nil, false
	}

	val := b.list.Front()
	for i := 0; true; i++ {
		if val == nil {
			break
		}
		e := val.Value.(*eventWrapper)
		if i == 0 && e.Kv.ModRevision > start {
			break // buffer starts after the requested start rev
		}
		if e.Kv.ModRevision == start {
			return val, true
		}
		val = val.Next()
	}
	return nil, true
}

func (b *buffer) Range(start *list.Element, ivl adt.IntervalTree) (slice []*mvccpb.Event, n int, pos *list.Element) {
	b.mut.Lock()
	defer b.mut.Unlock()

	if start == nil {
		pos = b.list.Front()
	} else {
		pos = start.Next()
	}
	for {
		if pos == nil {
			break
		}
		e := pos.Value.(*eventWrapper)
		if e.Kv.ModRevision > b.upperBound {
			break
		}
		if ivl.Intersects(e.Key) {
			n++
			slice = append(slice, e.Event)
		}
		pos = pos.Next()
	}
	return
}

func (b *buffer) bridgeGapUnlocked() (ok, changed bool) {
	ok = true
	val := b.upperVal
	if val == nil {
		val = b.list.Front()
	}
	for {
		if val == nil || val.Value == nil {
			break
		}
		valE := val.Value.(*eventWrapper)

		if valE.Kv.ModRevision <= b.upperBound {
			val = val.Next()
			continue // this gap has already been closed
		}

		isNextEvent := valE.Kv.ModRevision == b.upperBound+1
		age := time.Since(valE.Timestamp)
		hasTimedout := age > b.gapTimeout
		if hasTimedout && !isNextEvent {
			zap.L().Warn("filled gap in watch stream", zap.Int64("from", b.upperBound), zap.Int64("to", valE.Kv.ModRevision))
			watchGapTimeoutCount.Inc()
		}
		if !isNextEvent && !hasTimedout {
			ok = false
			break
		}

		b.upperBound = valE.Kv.ModRevision
		if b.lowerBound == -1 {
			b.lowerBound = valE.Kv.ModRevision
		}
		currentWatchRev.Set(float64(b.upperBound))
		watchLatency.Observe(age.Seconds())

		changed = true
		b.upperVal = val
		val = val.Next()
		continue

	}
	return ok, changed
}

type eventWrapper struct {
	*mvccpb.Event
	Key       adt.Interval
	Timestamp time.Time
}
