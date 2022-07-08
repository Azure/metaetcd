package watch

import (
	"container/list"
	"context"
	"sync"
	"time"
)

type deferralQueue struct {
	Chan chan int64

	mut         sync.Mutex
	list        *list.List
	timer       *time.Timer
	latency     time.Duration
	timerResets chan struct{}
}

func newDeferralQueue(latency time.Duration) *deferralQueue {
	return &deferralQueue{
		Chan:        make(chan int64),
		list:        list.New(),
		timer:       time.NewTimer(0),
		latency:     latency,
		timerResets: make(chan struct{}, 2),
	}
}

func (d *deferralQueue) Defer(val int64) {
	d.mut.Lock()
	defer d.mut.Unlock()
	d.list.PushFront(&deferredValue{PushTime: time.Now(), Val: val})
	select {
	case d.timerResets <- struct{}{}:
	default:
	}
}

func (d *deferralQueue) Run(ctx context.Context) {
	defer close(d.Chan)
	for {
		select {
		case <-d.timerResets:
		case <-d.timer.C:
			el := d.list.Back()
			if el == nil {
				continue // should be impossible
			}
			val := el.Value.(*deferredValue)
			d.Chan <- val.Val
			d.list.Remove(el)
		case <-ctx.Done():
			return
		}

		// Set timer for next item
		el := d.list.Back()
		if el == nil {
			continue // no more items in the list
		}
		val := el.Value.(*deferredValue)
		d.timer.Stop()
		d.timer.Reset(d.latency - time.Since(val.PushTime))
	}
}

type deferredValue struct {
	PushTime time.Time
	Val      int64
}
