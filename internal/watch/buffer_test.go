package watch

import (
	"context"
	"testing"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/pkg/v3/adt"

	"github.com/Azure/metaetcd/internal/testutil"
)

func TestBufferOrdering(t *testing.T) {
	ch := make(chan *eventWrapper, 100)
	b := newBuffer(time.Millisecond*10, 4, ch)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		b.Run(ctx)
		close(done)
	}()

	// The first event starts at rev 2, wait for the initial gap
	b.Push(eventWithModRev(2))
	buf, lb, up := b.Range(0, defaultKeyRange)
	assert.Equal(t, int64(-1), lb)
	assert.Equal(t, int64(0), up)
	assert.Len(t, buf, 0)
	<-ch

	// Create gap
	b.Push(eventWithModRev(4))

	// Full range - but only the first should be returned since there is a gap
	buf, lb, up = b.Range(0, defaultKeyRange)
	assert.Equal(t, int64(2), lb)
	assert.Equal(t, int64(2), up)
	assert.Equal(t, []int64{2}, testutil.EventModRevs(buf))

	// Fill the gap
	b.Push(eventWithModRev(3))

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
	b.Push(eventWithModRev(6))

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
	b.Push(eventWithModRev(7))
	buf, lb, up = b.Range(0, defaultKeyRange)
	assert.Equal(t, int64(3), lb)
	assert.Equal(t, int64(7), up)
	assert.Equal(t, []int64{3, 4, 6, 7}, testutil.EventModRevs(buf))

	cancel()
	<-done
}

func TestBufferKeyFiltering(t *testing.T) {
	b := newBuffer(time.Millisecond*10, 10, nil)

	b.Push([]*clientv3.Event{{Kv: &mvccpb.KeyValue{
		ModRevision: 1,
		Key:         []byte("foo/1"),
	}}})
	b.Push([]*clientv3.Event{{Kv: &mvccpb.KeyValue{
		ModRevision: 2,
		Key:         []byte("bar/2"),
	}}})
	b.Push([]*clientv3.Event{{Kv: &mvccpb.KeyValue{
		ModRevision: 3,
		Key:         []byte("bar/3"),
	}}})
	b.Push([]*clientv3.Event{{Kv: &mvccpb.KeyValue{
		ModRevision: 4,
		Key:         []byte("foo/4"),
	}}})

	slice, _, _ := b.Range(0, adt.NewStringAffineInterval("bar", "bar0"))
	require.Len(t, slice, 2)
	assert.Equal(t, []int64{2, 3}, testutil.EventModRevs(slice))
}

func TestBufferBridgeGap(t *testing.T) {
	b := newBuffer(time.Second, 10, nil)

	events := []int64{4, 3, 1, 2}
	expectedUpperBounds := []int64{0, 0, 1, 4}

	for i, rev := range events {
		b.Push(eventWithModRev(rev))
		assert.Equal(t, expectedUpperBounds[i], b.upperBound, "iteration: %d", i)
	}
}

func TestBufferTrimWhenGap(t *testing.T) {
	b := newBuffer(time.Millisecond, 2, nil)

	// Fill the buffer and more
	const n = 10
	for i := 0; i < n; i++ {
		b.Push(eventWithModRev(int64(i + 3)))
	}
	assert.Equal(t, n, b.list.Len())

	// Bridge the gap and prove the buffer was shortened
	time.Sleep(time.Millisecond * 2)
	b.bridgeGapUnlocked()
	assert.Equal(t, 2, b.list.Len())
}

func eventWithModRev(rev int64) []*clientv3.Event {
	return []*clientv3.Event{{Kv: &mvccpb.KeyValue{Key: []byte("foo/test"), ModRevision: rev}}}
}

var defaultKeyRange = adt.NewStringAffineInterval("foo", "foo0")
