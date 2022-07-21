package util

import (
	"container/list"
	"context"
	"sync"
	"time"

	"go.uber.org/zap"
)

type BufferableEvent[T any] interface {
	GetAge() time.Duration
	GetModRev() int64
	InRange(T) bool
}

type TimeBuffer[T any, TT BufferableEvent[T]] struct {
	mut                    sync.Mutex
	list                   *list.List
	gapTimeout             time.Duration
	maxLen                 int
	lowerBound, upperBound int64
	upperVal               *list.Element
	ch                     chan<- TT
}

func NewTimeBuffer[T any, TT BufferableEvent[T]](gapTimeout time.Duration, maxLen int, ch chan<- TT) *TimeBuffer[T, TT] {
	return &TimeBuffer[T, TT]{list: list.New(), gapTimeout: gapTimeout, maxLen: maxLen, ch: ch, lowerBound: -1}
}

func (t *TimeBuffer[T, TT]) LatestVisibleRev() int64 {
	t.mut.Lock()
	defer t.mut.Unlock()
	return t.upperBound
}

func (t *TimeBuffer[T, TT]) MaxLen() int { return t.maxLen }

func (t *TimeBuffer[T, TT]) Run(ctx context.Context) {
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

func (t *TimeBuffer[T, TT]) Push(event TT) {
	watchEventCount.Inc()
	t.mut.Lock()
	defer t.mut.Unlock()

	t.pushUnlocked(event)
	t.bridgeGapUnlocked()
}

func (t *TimeBuffer[T, TT]) pushUnlocked(event TT) {
	watchBufferLength.Inc()
	lastEl := t.list.Back()

	// Case 1: first element
	if lastEl == nil {
		t.list.PushFront(event)
		return
	}

	// Case 2: outside of range - insert before or after
	last := lastEl.Value.(TT)
	if event.GetModRev() > last.GetModRev() {
		t.list.PushBack(event)
		return
	}

	firstEl := t.list.Front()
	first := firstEl.Value.(TT)
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
		first = firstEl.Value.(TT)

		if event.GetModRev() > first.GetModRev() {
			t.list.InsertAfter(event, firstEl)
			return
		}
		lastEl = firstEl
	}
}

func (t *TimeBuffer[T, TT]) trimUnlocked() {
	item := t.list.Front()
	for {
		if t.list.Len() <= t.maxLen || item == nil {
			return
		}
		event := item.Value.(TT)

		next := item.Next()
		if t.upperBound > event.GetModRev() {
			watchBufferLength.Dec()
			t.list.Remove(item)
		}
		if next != nil {
			newFront := next.Value.(TT)
			t.lowerBound = newFront.GetModRev()
		}
		item = next
	}
}

func (t *TimeBuffer[T, TT]) Range(start int64, ivl T) ([]TT, int64, int64) {
	t.mut.Lock()
	defer t.mut.Unlock()

	slice := []TT{}
	pos := t.list.Front()
	for {
		if pos == nil {
			break
		}
		e := pos.Value.(TT)
		if e.GetModRev() <= start {
			pos = pos.Next()
			continue
		}
		if e.GetModRev() > t.upperBound {
			break
		}
		if e.InRange(ivl) {
			slice = append(slice, e)
		}
		pos = pos.Next()
	}
	return slice, t.lowerBound, t.upperBound
}

func (t *TimeBuffer[T, TT]) bridgeGapUnlocked() {
	val := t.upperVal
	if val == nil {
		val = t.list.Front()
	}
	for {
		if val == nil || val.Value == nil {
			break
		}
		valE := val.Value.(TT)

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
