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
	"go.uber.org/zap"

	"github.com/Azure/metaetcd/internal/testutil"
)

func TestBufferOrdering(t *testing.T) {
	bcast := newBroadcast()
	b := newBuffer(time.Millisecond*10, 4, bcast, zap.NewNop())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := make(chan struct{}, 1)
	bcast.Watch(ch)

	// TODO: Helper for this - many tests do the same thing
	done := make(chan struct{})
	go func() {
		b.Run(ctx)
		close(done)
	}()

	// The first event starts at rev 2, wait for the initial gap
	b.Push(eventWithModRev(2))
	_, n, _ := b.Range(0, defaultKeyRange)
	assert.Equal(t, 0, n)
	<-ch

	// Create gap
	b.Push(eventWithModRev(4))

	// Full range - but only the first should be returned since there is a gap
	buf, n, rev := b.Range(0, defaultKeyRange)
	assert.Equal(t, 1, n)
	assert.Equal(t, int64(2), rev)
	assert.Equal(t, []int64{2}, testutil.EventModRevs(buf))

	// Fill the gap
	b.Push(eventWithModRev(3))

	// Full range
	buf, n, rev = b.Range(0, defaultKeyRange)
	assert.Equal(t, 3, n)
	assert.Equal(t, int64(4), rev)
	assert.Equal(t, []int64{2, 3, 4}, testutil.EventModRevs(buf))

	// Partial range
	buf, n, rev = b.Range(2, defaultKeyRange)
	assert.Equal(t, 2, n)
	assert.Equal(t, int64(4), rev)
	assert.Equal(t, []int64{3, 4}, testutil.EventModRevs(buf))

	// Push event to create another gap
	b.Push(eventWithModRev(6))

	// This gap is never filled - wait for the timeout
	for {
		buf, n, _ = b.Range(0, defaultKeyRange)
		if n == 4 {
			break
		}
		time.Sleep(time.Millisecond * 10)
	}
	assert.Equal(t, []int64{2, 3, 4, 6}, testutil.EventModRevs(buf))

	// Push another event, which will cause the earliest event to fall off
	b.Push(eventWithModRev(7))
	buf, n, _ = b.Range(0, defaultKeyRange)
	assert.Equal(t, 4, n)
	assert.Equal(t, []int64{3, 4, 6, 7}, testutil.EventModRevs(buf))

	cancel()
	<-done
}

func TestBufferKeyFiltering(t *testing.T) {
	b := newBuffer(time.Millisecond*10, 10, newBroadcast(), zap.NewNop())

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

	slice, _, _ := b.Range(0, keyRange("bar", "bar0"))
	require.Len(t, slice, 2)
	assert.Equal(t, []int64{2, 3}, testutil.EventModRevs(slice))
}

func TestBufferBridgeGap(t *testing.T) {
	b := newBuffer(time.Second, 10, newBroadcast(), zap.NewNop())

	events := []int64{4, 3, 1, 2}
	expectedUpperBounds := []int64{0, 0, 1, 4}

	for i, rev := range events {
		b.Push(eventWithModRev(rev))
		assert.Equal(t, expectedUpperBounds[i], b.upperBound, "iteration: %d", i)
	}
}

func eventWithModRev(rev int64) []*clientv3.Event {
	return []*clientv3.Event{{Kv: &mvccpb.KeyValue{Key: []byte("foo/test"), ModRevision: rev}}}
}

func keyRange(from, to string) adt.IntervalTree {
	tree := adt.NewIntervalTree()
	tree.Insert(adt.NewStringAffineInterval(string(from), string(to)), nil)
	return tree
}

var defaultKeyRange = keyRange("foo", "foo0")
