package watch

import (
	"bytes"
	"container/list"
	"context"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
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
		case <-ticker.C:
			b.bridgeGapUnlocked()
			b.bcast.Send() // TODO: Only on change
		case <-ctx.Done():
			return
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
	ok := b.pushUnlocked(event)
	b.trimUnlocked()
	if ok {
		b.bcast.Send()
	}
	return ok
}

func (b *buffer) pushUnlocked(event *mvccpb.Event) bool {
	lastEl := b.list.Back()
	wrapped := &eventWrapper{Event: event, Timestamp: time.Now()}

	// Case 1: first element
	if lastEl == nil {
		b.list.PushFront(wrapped)
		return b.bridgeGapUnlocked()
	}

	// Case 2: outside of range - insert before or after
	last := lastEl.Value.(*eventWrapper)
	if event.Kv.ModRevision > last.Kv.ModRevision {
		b.list.PushBack(wrapped)
		return b.bridgeGapUnlocked()
	}

	firstEl := b.list.Front()
	first := firstEl.Value.(*eventWrapper)
	if event.Kv.ModRevision < first.Kv.ModRevision {
		b.list.PushFront(wrapped)
		return b.bridgeGapUnlocked()
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
			return b.bridgeGapUnlocked()
		}
		lastEl = firstEl
	}

	return b.bridgeGapUnlocked()
}

func (b *buffer) trimUnlocked() {
	if b.list.Len() <= b.maxLen {
		return
	}
	// TODO: Don't remove if after b.lastEl
	b.list.Remove(b.list.Front())
}

func (b *buffer) Range(start int64, key, end []byte) (slice []*mvccpb.Event, n int, rev int64) {
	b.mut.Lock()
	defer b.mut.Unlock()
	val := b.list.Front()

	for {
		if val == nil {
			break
		}

		e := val.Value.(*eventWrapper)
		if e.Kv.ModRevision > b.upperBound {
			break
		}
		// TODO: https://github.com/etcd-io/etcd/blob/6c5dfcf140319d7815e0a303aefc0f45cc49dd0b/server/storage/mvcc/watcher_group.go#L186
		if e.Kv.ModRevision > start &&
			bytes.Compare(e.Kv.Key, key) >= 0 &&
			(len(end) == 0 || bytes.Compare(e.Kv.Key, end) < 0) {
			n++
			slice = append(slice, e.Event)
			rev = e.Kv.ModRevision
		}
		next := val.Next()
		val = next
	}
	return
}

func (b *buffer) bridgeGapUnlocked() (ok bool) {
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
			val = val.Next()
			continue
		}

		ok = false
		break
	}
	return ok
}

type eventWrapper struct {
	*mvccpb.Event
	Timestamp time.Time
}
