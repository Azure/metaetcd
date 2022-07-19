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
	upperVal               *list.Element
	ch                     chan<- *eventWrapper
}

func newBuffer(gapTimeout time.Duration, maxLen int, ch chan<- *eventWrapper) *buffer {
	return &buffer{list: list.New(), gapTimeout: gapTimeout, maxLen: maxLen, ch: ch, lowerBound: -1}
}

func (b *buffer) Run(ctx context.Context) {
	ticker := time.NewTicker(b.gapTimeout)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			b.bridgeGapUnlocked()
		}
	}
}

func (b *buffer) Push(events []*clientv3.Event) {
	watchEventCount.Inc()
	b.mut.Lock()
	defer b.mut.Unlock()
	for _, event := range events {
		e := mvccpb.Event(*event)
		b.pushOrDeferUnlocked(&e)
	}
}

func (b *buffer) pushOrDeferUnlocked(event *mvccpb.Event) {
	b.pushUnlocked(event)
	b.bridgeGapUnlocked()
}

func (b *buffer) pushUnlocked(event *mvccpb.Event) {
	watchBufferLength.Inc()
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
	item := b.list.Front()
	for {
		if b.list.Len() <= b.maxLen || item == nil {
			return
		}
		event := item.Value.(*eventWrapper)

		next := item.Next()
		if b.upperBound > event.Kv.ModRevision {
			watchBufferLength.Dec()
			b.list.Remove(item)
		}
		if next != nil {
			newFront := next.Value.(*eventWrapper)
			b.lowerBound = newFront.Kv.ModRevision
		}
		item = next
	}
}

func (b *buffer) Range(start int64, ivl adt.Interval) ([]*mvccpb.Event, int64, int64) {
	b.mut.Lock()
	defer b.mut.Unlock()

	slice := []*mvccpb.Event{}
	pos := b.list.Front()
	for {
		if pos == nil {
			break
		}
		e := pos.Value.(*eventWrapper)
		if e.Kv.ModRevision <= start {
			pos = pos.Next()
			continue
		}
		if e.Kv.ModRevision > b.upperBound {
			break
		}
		if ivl.Compare(&e.Key) == 0 {
			slice = append(slice, e.Event)
		}
		pos = pos.Next()
	}
	return slice, b.lowerBound, b.upperBound
}

func (b *buffer) bridgeGapUnlocked() {
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
			break
		}

		if b.ch != nil {
			b.ch <- valE
		}
		b.upperBound = valE.Kv.ModRevision
		b.upperVal = val
		if b.lowerBound == -1 {
			b.lowerBound = valE.Kv.ModRevision
		}

		currentWatchRev.Set(float64(b.upperBound))
		watchLatency.Observe(age.Seconds())

		val = val.Next()
		continue

	}
	b.trimUnlocked()
}

type eventWrapper struct {
	*mvccpb.Event
	Key       adt.Interval
	Timestamp time.Time
}
