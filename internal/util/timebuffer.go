package util

import (
	"container/list"
	"context"
	"sync"
	"time"

	"go.etcd.io/etcd/pkg/v3/adt"
	"go.uber.org/zap"
)

type BufferableEvent interface {
	GetAge() time.Duration
	GetModRev() int64
	GetKey() *adt.Interval
}

type TimeBuffer[T BufferableEvent] struct {
	mut                    sync.Mutex
	list                   *list.List
	gapTimeout             time.Duration
	maxLen                 int
	lowerBound, upperBound int64
	upperVal               *list.Element
	ch                     chan<- T
}

func NewTimeBuffer[T BufferableEvent](gapTimeout time.Duration, maxLen int, ch chan<- T) *TimeBuffer[T] {
	return &TimeBuffer[T]{list: list.New(), gapTimeout: gapTimeout, maxLen: maxLen, ch: ch, lowerBound: -1}
}

func (t *TimeBuffer[T]) LatestVisibleRev() int64 {
	t.mut.Lock()
	defer t.mut.Unlock()
	return t.upperBound
}

func (t *TimeBuffer[T]) MaxLen() int { return t.maxLen }

func (t *TimeBuffer[T]) Run(ctx context.Context) {
	ticker := time.NewTicker(t.gapTimeout)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			t.bridgeGapUnlocked()
		}
	}
}

func (t *TimeBuffer[T]) Push(event T) {
	watchEventCount.Inc()
	t.mut.Lock()
	defer t.mut.Unlock()

	t.pushUnlocked(event)
	t.bridgeGapUnlocked()
}

func (t *TimeBuffer[T]) pushUnlocked(event T) {
	watchBufferLength.Inc()
	lastEl := t.list.Back()

	// Case 1: first element
	if lastEl == nil {
		t.list.PushFront(event)
		return
	}

	// Case 2: outside of range - insert before or after
	last := lastEl.Value.(T)
	if event.GetModRev() > last.GetModRev() {
		t.list.PushBack(event)
		return
	}

	firstEl := t.list.Front()
	first := firstEl.Value.(T)
	if event.GetModRev() < first.GetModRev() {
		t.list.PushFront(event)
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
			t.list.InsertAfter(event, firstEl)
			return
		}
		lastEl = firstEl
	}
}

func (t *TimeBuffer[T]) trimUnlocked() {
	item := t.list.Front()
	for {
		if t.list.Len() <= t.maxLen || item == nil {
			return
		}
		event := item.Value.(T)

		next := item.Next()
		if t.upperBound > event.GetModRev() {
			watchBufferLength.Dec()
			t.list.Remove(item)
		}
		if next != nil {
			newFront := next.Value.(T)
			t.lowerBound = newFront.GetModRev()
		}
		item = next
	}
}

func (t *TimeBuffer[T]) Range(start int64, ivl adt.Interval) ([]T, int64, int64) {
	t.mut.Lock()
	defer t.mut.Unlock()

	slice := []T{}
	pos := t.list.Front()
	for {
		if pos == nil {
			break
		}
		e := pos.Value.(T)
		if e.GetModRev() <= start {
			pos = pos.Next()
			continue
		}
		if e.GetModRev() > t.upperBound {
			break
		}
		if ivl.Compare(e.GetKey()) == 0 {
			slice = append(slice, e)
		}
		pos = pos.Next()
	}
	return slice, t.lowerBound, t.upperBound
}

func (t *TimeBuffer[T]) bridgeGapUnlocked() {
	val := t.upperVal
	if val == nil {
		val = t.list.Front()
	}
	for {
		if val == nil || val.Value == nil {
			break
		}
		valE := val.Value.(T)

		if valE.GetModRev() <= t.upperBound {
			val = val.Next()
			continue // this gap has already been closed
		}

		isNextEvent := valE.GetModRev() == t.upperBound+1
		age := valE.GetAge()
		hasTimedout := age > t.gapTimeout
		if hasTimedout && !isNextEvent {
			zap.L().Warn("filled gap in watch stream", zap.Int64("from", t.upperBound), zap.Int64("to", valE.GetModRev()))
			watchGapTimeoutCount.Inc()
		}
		if !isNextEvent && !hasTimedout {
			break
		}

		if t.ch != nil {
			t.ch <- valE
		}
		t.upperBound = valE.GetModRev()
		t.upperVal = val
		if t.lowerBound == -1 {
			t.lowerBound = valE.GetModRev()
		}

		currentWatchRev.Set(float64(t.upperBound))
		watchLatency.Observe(age.Seconds())

		val = val.Next()
		continue

	}
	t.trimUnlocked()
}
