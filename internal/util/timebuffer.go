package util

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"
)

type BufferableEvent[T any] interface {
	GetAge() time.Duration
	GetRevision() int64
	Matches(query T) bool
}

// TimeBuffer buffers events and sorts them by their logical timestamp.
//
// Events are only visible to readers when the preceding event has been received.
// If the preceding event is never received, the following event will still become
// visible after a (wallclock) timeout period in order to prevent deadlocks caused
// dropped events.
//
// In practice, it is useful for merging streams of events that may be out of order due
// to network latency, and have the possibility of missing events due to network partitions.
type TimeBuffer[T any, TT BufferableEvent[T]] struct {
	mut        sync.Mutex
	list       *List[TT]
	gapTimeout time.Duration
	len        int
	min, max   int64
	cursor     *Element[TT]
	ch         chan<- TT
}

func NewTimeBuffer[T any, TT BufferableEvent[T]](gapTimeout time.Duration, len int, ch chan<- TT) *TimeBuffer[T, TT] {
	return &TimeBuffer[T, TT]{list: &List[TT]{}, gapTimeout: gapTimeout, len: len, min: -1, ch: ch}
}

func (t *TimeBuffer[T, TT]) Run(ctx context.Context) {
	ticker := time.NewTicker(t.gapTimeout)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			func() {
				t.mut.Lock()
				defer t.mut.Unlock()
				t.bridgeGapUnlocked()
			}()
		}
	}
}

func (t *TimeBuffer[T, TT]) LatestVisibleRev() int64 {
	t.mut.Lock()
	defer t.mut.Unlock()
	return t.max
}

func (t *TimeBuffer[T, TT]) Len() int { return t.len }

func (t *TimeBuffer[T, TT]) Push(event TT) {
	t.mut.Lock()
	defer t.mut.Unlock()
	t.pushUnlocked(event)
	t.bridgeGapUnlocked()
}

func (t *TimeBuffer[T, TT]) pushUnlocked(event TT) {
	timeBufferLength.Inc()

	cursorItem := t.list.Last() // start at the newest event
	for {
		if cursorItem == nil {
			t.list.PushFront(event)
			return // prepend to back of list
		}

		cursorEvent := cursorItem.Value
		if event.GetRevision() < cursorEvent.GetRevision() {
			cursorItem = cursorItem.Prev()
			continue // keep scanning back in time
		}

		t.list.InsertAfter(event, cursorItem)
		return
	}
}

func (t *TimeBuffer[T, TT]) trimUnlocked() {
	item := t.list.First()
	for {
		if t.list.Len <= t.len || item == nil {
			break
		}
		event := item.Value
		next := item.Next()

		// Trim if the buffer is too long
		if t.max > event.GetRevision() {
			timeBufferLength.Dec()
			t.list.Remove(item)
		}

		// Keep track of the buffer's tail
		if next != nil {
			t.min = next.Value.GetRevision()
		}
		item = next
	}
}

func (t *TimeBuffer[T, TT]) All() (slice []TT) {
	t.mut.Lock()
	defer t.mut.Unlock()

	item := t.list.First() // start at the oldest event
	for {
		if item == nil {
			break
		}
		slice = append(slice, item.Value)
		item = item.Next()
	}
	return
}

func (t *TimeBuffer[T, TT]) Range(start int64, query T) ([]TT, int64, int64) {
	t.mut.Lock()
	defer t.mut.Unlock()

	slice := []TT{}
	item := t.list.First() // start at the oldest event
	for {
		if item == nil {
			break
		}
		event := item.Value
		if event.GetRevision() <= start {
			item = item.Next()
			continue
		}
		if event.GetRevision() > t.max {
			break
		}
		if event.Matches(query) {
			slice = append(slice, event)
		}
		item = item.Next()
	}
	return slice, t.min, t.max
}

func (t *TimeBuffer[T, TT]) bridgeGapUnlocked() {
	item := t.cursor // start at the oldest visible event and scan forwards
	if item == nil {
		item = t.list.First()
	}
	for {
		if item == nil {
			break // at end of list
		}
		event := item.Value

		// Not a gap - keep scanning
		if event.GetRevision() <= t.max {
			item = item.Next()
			continue
		}

		isNextEvent := event.GetRevision() == t.max+1
		age := event.GetAge()
		hasTimedout := age > t.gapTimeout

		// Report on gap timeouts
		if !isNextEvent && hasTimedout {
			zap.L().Warn("filled gap in event buffer", zap.Int64("from", t.max), zap.Int64("to", event.GetRevision()))
			timeBufferTimeoutCount.Inc()
		}

		// We've reached a gap and it hasn't timed out yet
		if !isNextEvent && !hasTimedout {
			break
		}

		t.advanceCursorUnlocked(item, event)
		item = item.Next()
		continue

	}
	t.trimUnlocked()
}

func (t *TimeBuffer[T, TT]) advanceCursorUnlocked(item *Element[TT], event TT) {
	t.ch <- event
	t.max = event.GetRevision()
	t.cursor = item

	if t.min == -1 {
		t.min = event.GetRevision()
	}

	timeBufferVisibleMax.Set(float64(t.max))
}
