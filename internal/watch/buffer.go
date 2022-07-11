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
	maxLen     int
	upperBound int64
	deferral   *deferralQueue
	bcast      *broadcast
	logger     *zap.Logger
}

func newBuffer(gapTimeout time.Duration, maxLen int, bcast *broadcast, logger *zap.Logger) *buffer {
	return &buffer{list: list.New(), maxLen: maxLen, deferral: newDeferralQueue(gapTimeout), bcast: bcast, logger: logger}
}

func (b *buffer) Run(ctx context.Context) {
	go b.deferral.Run(ctx)
	for val := range b.deferral.Chan {
		b.raiseUpperBound(val)
	}
}

func (b *buffer) raiseUpperBound(val int64) {
	b.mut.Lock()
	defer b.mut.Unlock()
	if b.upperBound >= val {
		return
	}
	b.logger.Error("closing watch gap after timeout", zap.Int64("currentRev", b.upperBound), zap.Int64("newRev", val))

	b.upperBound = val
	b.bridgeGapUnlocked()
	b.bcast.Send()
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
	} else {
		b.deferral.Defer(event.Kv.ModRevision)
	}
	return ok
}

func (b *buffer) pushUnlocked(event *mvccpb.Event) bool {
	lastEl := b.list.Back()

	// Case 1: first element
	if lastEl == nil {
		b.list.PushFront(event)
		return b.bridgeGapUnlocked()
	}

	// Case 2: outside of range - insert before or after
	last := lastEl.Value.(*mvccpb.Event)
	if event.Kv.ModRevision > last.Kv.ModRevision {
		b.list.PushBack(event)
		return b.bridgeGapUnlocked()
	}

	firstEl := b.list.Front()
	first := firstEl.Value.(*mvccpb.Event)
	if event.Kv.ModRevision < first.Kv.ModRevision {
		b.list.PushFront(event)
		return b.bridgeGapUnlocked()
	}

	// Case 3: find place between pairs of events
	for {
		firstEl = lastEl.Prev()
		if firstEl == nil {
			break
		}
		first = firstEl.Value.(*mvccpb.Event)

		if event.Kv.ModRevision > first.Kv.ModRevision {
			b.list.InsertAfter(event, firstEl)
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

		e := val.Value.(*mvccpb.Event)
		if e.Kv.ModRevision > b.upperBound {
			break
		}
		if e.Kv.ModRevision > start &&
			bytes.Compare(e.Kv.Key, key) >= 0 &&
			(len(end) == 0 || bytes.Compare(e.Kv.Key, end) < 0) {
			n++
			slice = append(slice, e)
			rev = e.Kv.ModRevision
		}
		next := val.Next()
		val = next
	}
	return
}

func (b *buffer) bridgeGapUnlocked() (ok bool) {
	// TODO: Keep pointer to oldest rev before gap to avoid scanning the entire buffer
	ok = true
	val := b.list.Front() // oldest to newest
	for {
		if val == nil {
			break
		}
		valE := val.Value.(*mvccpb.Event)

		if valE.Kv.ModRevision <= b.upperBound {
			val = val.Next()
			continue // this gap has already been closed
		}

		if r := valE.Kv.ModRevision; r == b.upperBound+1 {
			b.upperBound = r
			val = val.Next()
			continue
		}
		ok = false
		break
	}
	return ok
}
