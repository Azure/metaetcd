package watch

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/pkg/v3/adt"
)

func TestGroupTree(t *testing.T) {
	g := newGroupTree[int]()

	// Initial listener
	i1 := adt.NewStringAffineInterval("foo", "foo0")
	ch1 := make(chan int, 10)
	g.Add(i1, ch1)

	// Duplicate
	i2 := adt.NewStringAffineInterval("foo", "foo0")
	ch2 := make(chan int, 10)
	g.Add(i2, ch2)

	// Overlapping
	i3 := adt.NewStringAffineInterval("foo", "foo9") // TODO
	ch3 := make(chan int, 10)
	g.Add(i3, ch3)

	// Different keyspace
	i4 := adt.NewStringAffineInterval("bar", "bar0")
	g.Add(i4, nil) // expect this never to be used - nil chan on purpose

	// Prove the right listeners get the event
	key := adt.NewStringAffinePoint("foo-test")
	const val = 123
	g.Broadcast(key, val)

	assert.Equal(t, val, <-ch1)
	assert.Equal(t, val, <-ch2)
	assert.Equal(t, val, <-ch3)

	// Remove one of the duplicate listeners and prove the other still receives
	g.Remove(i1, ch1)
	g.Broadcast(key, val)
	assert.Equal(t, val, <-ch2)
	assert.Equal(t, val, <-ch3)

	// Remove all listeners and prove it doesn't panic when sending
	g.Remove(i2, ch2)
	g.Remove(i3, ch3)
	g.Remove(i3, ch3)
	g.Remove(i4, nil)
	g.Broadcast(key, val)
}
