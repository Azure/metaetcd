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

// TODO: Return error for attempts to start watch connection before the oldest message in the buffer

type buffer struct {
	mut        sync.Mutex
	list       *list.List
	gapTimeout time.Duration
	maxLen     int
	upperBound int64
	bcast      *broadcast
	logger     *zap.Logger
	upperVal   *list.Element
}

func newBuffer(gapTimeout time.Duration, maxLen int, bcast *broadcast, logger *zap.Logger) *buffer {
	return &buffer{list: list.New(), gapTimeout: gapTimeout, maxLen: maxLen, bcast: bcast, logger: logger}
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
	b.list.Remove(front)
}

func (b *buffer) Range(start int64, ivl adt.IntervalTree) (slice []*mvccpb.Event, n int, rev int64) {
	b.mut.Lock()
	defer b.mut.Unlock()
	val := b.list.Front()

	// TODO: Somewhere, hold a pointer to current position instead of scanning for start rev every time

	for {
		if val == nil {
			break
		}

		e := val.Value.(*eventWrapper)
		if e.Kv.ModRevision > b.upperBound {
			break
		}
		if e.Kv.ModRevision > start && ivl.Intersects(e.Key) {
			n++
			slice = append(slice, e.Event)
			rev = e.Kv.ModRevision
		}
		next := val.Next()
		val = next
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
		if val == nil {
			break
		}
		valE := val.Value.(*eventWrapper)

		if valE.Kv.ModRevision <= b.upperBound {
			val = val.Next()
			continue // this gap has already been closed
		}

		isNextEvent := valE.Kv.ModRevision == b.upperBound+1
		hasTimedout := time.Since(valE.Timestamp) > b.gapTimeout
		if hasTimedout && !isNextEvent {
			b.logger.Warn("filled gap in watch stream", zap.Int64("from", b.upperBound), zap.Int64("to", valE.Kv.ModRevision))
		}
		if isNextEvent || hasTimedout {
			b.upperBound = valE.Kv.ModRevision
			b.upperVal = val
			changed = true
			val = val.Next()
			continue
		}

		ok = false
		break
	}
	return ok, changed
}

type eventWrapper struct {
	*mvccpb.Event
	Key       adt.Interval
	Timestamp time.Time
}
