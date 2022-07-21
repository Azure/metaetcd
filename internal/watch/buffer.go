package watch

import (
	"container/list"
	"context"
	"sync"
	"time"

	"github.com/coreos/etcd/mvcc/mvccpb"
	"go.etcd.io/etcd/pkg/v3/adt"
	"go.uber.org/zap"
)

type bufferableEvent interface {
	GetAge() time.Duration
	GetModRev() int64
	GetKey() *adt.Interval
}

type buffer[T bufferableEvent] struct {
	mut                    sync.Mutex
	list                   *list.List
	gapTimeout             time.Duration
	maxLen                 int
	lowerBound, upperBound int64
	upperVal               *list.Element
	ch                     chan<- T
}

func newBuffer[T bufferableEvent](gapTimeout time.Duration, maxLen int, ch chan<- T) *buffer[T] {
	return &buffer[T]{list: list.New(), gapTimeout: gapTimeout, maxLen: maxLen, ch: ch, lowerBound: -1}
}

func (b *buffer[T]) Run(ctx context.Context) {
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

func (b *buffer[T]) Push(event T) {
	watchEventCount.Inc()
	b.mut.Lock()
	defer b.mut.Unlock()

	b.pushUnlocked(event)
	b.bridgeGapUnlocked()
}

func (b *buffer[T]) pushUnlocked(event T) {
	watchBufferLength.Inc()
	lastEl := b.list.Back()

	// Case 1: first element
	if lastEl == nil {
		b.list.PushFront(event)
		return
	}

	// Case 2: outside of range - insert before or after
	last := lastEl.Value.(T)
	if event.GetModRev() > last.GetModRev() {
		b.list.PushBack(event)
		return
	}

	firstEl := b.list.Front()
	first := firstEl.Value.(T)
	if event.GetModRev() < first.GetModRev() {
		b.list.PushFront(event)
		return
	}

	// Case 3: find place between pairs of events
	for {
		firstEl = lastEl.Prev()
		if firstEl == nil {
			break
		}
		first = firstEl.Value.(T)

		if event.GetModRev() > first.GetModRev() {
			b.list.InsertAfter(event, firstEl)
			return
		}
		lastEl = firstEl
	}
}

func (b *buffer[T]) trimUnlocked() {
	item := b.list.Front()
	for {
		if b.list.Len() <= b.maxLen || item == nil {
			return
		}
		event := item.Value.(T)

		next := item.Next()
		if b.upperBound > event.GetModRev() {
			watchBufferLength.Dec()
			b.list.Remove(item)
		}
		if next != nil {
			newFront := next.Value.(T)
			b.lowerBound = newFront.GetModRev()
		}
		item = next
	}
}

func (b *buffer[T]) Range(start int64, ivl adt.Interval) ([]T, int64, int64) {
	b.mut.Lock()
	defer b.mut.Unlock()

	slice := []T{}
	pos := b.list.Front()
	for {
		if pos == nil {
			break
		}
		e := pos.Value.(T)
		if e.GetModRev() <= start {
			pos = pos.Next()
			continue
		}
		if e.GetModRev() > b.upperBound {
			break
		}
		if ivl.Compare(e.GetKey()) == 0 {
			slice = append(slice, e)
		}
		pos = pos.Next()
	}
	return slice, b.lowerBound, b.upperBound
}

func (b *buffer[T]) bridgeGapUnlocked() {
	val := b.upperVal
	if val == nil {
		val = b.list.Front()
	}
	for {
		if val == nil || val.Value == nil {
			break
		}
		valE := val.Value.(T)

		if valE.GetModRev() <= b.upperBound {
			val = val.Next()
			continue // this gap has already been closed
		}

		isNextEvent := valE.GetModRev() == b.upperBound+1
		age := valE.GetAge()
		hasTimedout := age > b.gapTimeout
		if hasTimedout && !isNextEvent {
			zap.L().Warn("filled gap in watch stream", zap.Int64("from", b.upperBound), zap.Int64("to", valE.GetModRev()))
			watchGapTimeoutCount.Inc()
		}
		if !isNextEvent && !hasTimedout {
			break
		}

		if b.ch != nil {
			b.ch <- valE
		}
		b.upperBound = valE.GetModRev()
		b.upperVal = val
		if b.lowerBound == -1 {
			b.lowerBound = valE.GetModRev()
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

func (e *eventWrapper) GetAge() time.Duration { return time.Since(e.Timestamp) }
func (e *eventWrapper) GetModRev() int64      { return e.Kv.ModRevision }
func (e *eventWrapper) GetKey() *adt.Interval { return &e.Key } // TODO: Remove
