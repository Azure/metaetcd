package util

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/pkg/v3/adt"

	"github.com/Azure/metaetcd/internal/testutil"
)

func TestTimeBufferOrdering(t *testing.T) {
	ch := make(chan *testEvent, 100)
	b := NewTimeBuffer[adt.Interval](time.Millisecond*10, 4, ch)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		b.Run(ctx)
		close(done)
	}()

	// The first event starts at rev 2, wait for the initial gap
	b.Push(newTestEvent(2))
	buf, lb, up := b.Range(0, defaultKeyRange)
	assert.Equal(t, int64(-1), lb)
	assert.Equal(t, int64(0), up)
	assert.Len(t, buf, 0)
	<-ch

	// Create gap
	b.Push(newTestEvent(4))

	// Full range - but only the first should be returned since there is a gap
	buf, lb, up = b.Range(0, defaultKeyRange)
	assert.Equal(t, int64(2), lb)
	assert.Equal(t, int64(2), up)
	assert.Equal(t, []int64{2}, testutil.EventModRevs(buf))

	// Fill the gap
	b.Push(newTestEvent(3))

	// Full range
	buf, lb, up = b.Range(0, defaultKeyRange)
	assert.Equal(t, int64(2), lb)
	assert.Equal(t, int64(4), up)
	assert.Equal(t, []int64{2, 3, 4}, testutil.EventModRevs(buf))

	// Partial range
	buf, lb, up = b.Range(2, defaultKeyRange)
	assert.Equal(t, int64(2), lb)
	assert.Equal(t, int64(4), up)
	assert.Equal(t, []int64{3, 4}, testutil.EventModRevs(buf))

	// Push event to create another gap
	b.Push(newTestEvent(6))

	// This gap is never filled - wait for the timeout
	for {
		buf, lb, up = b.Range(0, defaultKeyRange)
		if len(buf) == 4 {
			assert.Equal(t, int64(2), lb)
			assert.Equal(t, int64(6), up)
			break
		}
		time.Sleep(time.Millisecond * 5)
	}
	assert.Equal(t, []int64{2, 3, 4, 6}, testutil.EventModRevs(buf))

	// Push another event, which will cause the earliest event to fall off
	b.Push(newTestEvent(7))
	buf, lb, up = b.Range(0, defaultKeyRange)
	assert.Equal(t, int64(3), lb)
	assert.Equal(t, int64(7), up)
	assert.Equal(t, []int64{3, 4, 6, 7}, testutil.EventModRevs(buf))

	cancel()
	<-done
}

func TestTimeBufferBridgeGap(t *testing.T) {
	b := NewTimeBuffer[adt.Interval, *testEvent](time.Second, 10, nil)

	events := []int64{4, 3, 1, 2}
	expectedUpperBounds := []int64{0, 0, 1, 4}

	for i, rev := range events {
		b.Push(newTestEvent(rev))
		assert.Equal(t, expectedUpperBounds[i], b.upperBound, "iteration: %d", i)
	}
}

func TestTimeBufferTrimWhenGap(t *testing.T) {
	b := NewTimeBuffer[adt.Interval, *testEvent](time.Millisecond, 2, nil)

	// Fill the buffer and more
	const n = 10
	for i := 0; i < n; i++ {
		b.Push(newTestEvent(int64(i + 3)))
	}
	assert.Equal(t, n, b.list.Len())

	// Bridge the gap and prove the buffer was shortened
	time.Sleep(time.Millisecond * 2)
	b.bridgeGapUnlocked()
	assert.Equal(t, 2, b.list.Len())
}

var defaultKeyRange = adt.NewStringAffineInterval("foo", "foo0")

type testEvent struct {
	Rev       int64
	Key       adt.Interval
	Timestamp time.Time
}

func newTestEvent(rev int64) *testEvent {
	return &testEvent{
		Rev:       rev,
		Timestamp: time.Now(),
		Key:       adt.NewStringAffinePoint("foo/test"),
	}
}
func (e *testEvent) GetAge() time.Duration         { return time.Since(e.Timestamp) }
func (e *testEvent) GetModRev() int64              { return e.Rev }
func (e *testEvent) InRange(ivl adt.Interval) bool { return ivl.Compare(&e.Key) == 0 }

// TODO: Don't use adt here and better tests for InRange
