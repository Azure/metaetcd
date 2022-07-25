package util

import (
	"math/rand"
	"testing"
	"time"

	"github.com/Azure/metaetcd/internal/testutil"
	"github.com/stretchr/testify/assert"
)

// FuzzTimeBufferPush proves that the buffer is always sorted when given random input.
func FuzzTimeBufferPush(f *testing.F) {
	numbers := make([]int64, 1000)
	for i := range numbers {
		numbers[i] = int64(rand.Int())
	}
	for _, n := range numbers {
		f.Add(n)
	}

	b := NewTimeBuffer[struct{}](time.Millisecond*10, 4, make(chan<- *testEvent, 100))
	f.Fuzz(func(t *testing.T, n int64) {
		b.Push(newTestEvent(n))

		var prev int64
		for i, n := range b.All() {
			if i == 0 {
				prev = n.GetRevision()
				continue
			}
			if n.GetRevision() < prev {
				t.Errorf("buffer is out of order - %d < %d", n.GetRevision(), prev)
			}
			prev = n.GetRevision()
		}
	})
}

// TestTimeBufferCursor proves that buffer's cursor moves forward as out-of-order events (without gaps) are added.
func TestTimeBufferCursor(t *testing.T) {
	b := NewTimeBuffer[struct{}](time.Second, 10, make(chan<- *testEvent, 100))

	events := []int64{4, 3, 1, 2, 5}
	expectedUpperBounds := []int64{0, 0, 1, 4, 5}

	for i, rev := range events {
		b.Push(newTestEvent(rev))
		assert.Equal(t, expectedUpperBounds[i], b.LatestVisibleRev(), "iteration: %d", i)
	}
}

// TestTimeBufferPruningCursor proves that events are not pruned until they are visible.
func TestTimeBufferPruningCursor(t *testing.T) {
	b := NewTimeBuffer[struct{}](time.Millisecond, 2, make(chan<- *testEvent, 100))

	// Fill the buffer and more
	const n = 10
	for i := 0; i < n; i++ {
		b.Push(newTestEvent(int64(i + 3)))
	}
	assert.Equal(t, n, b.list.Len)

	// Bridge the gap and prove the buffer was shortened
	time.Sleep(time.Millisecond * 2)
	b.bridgeGapUnlocked()
	assert.Equal(t, 2, b.Len())
}

// TestTimeBufferRange proves that ranges filter on start revision, the visibility window, and the query.
func TestTimeBufferRange(t *testing.T) {
	b := NewTimeBuffer[struct{}](time.Second, 10, make(chan<- *testEvent, 100))

	for i, rev := range []int64{4, 3, 1, 2, 5, 6, 7, 9} {
		event := newTestEvent(rev)
		if i == 0 {
			event.Invisible = true
		}
		b.Push(event)
	}

	results, min, max := b.Range(2, struct{}{})
	assert.Equal(t, int64(1), min)
	assert.Equal(t, int64(7), max)
	assert.Equal(t, []int64{3, 5, 6, 7}, testutil.GetRevisions(results))
}

type testEvent struct {
	Rev       int64
	Timestamp time.Time
	Invisible bool
}

func newTestEvent(rev int64) *testEvent {
	return &testEvent{
		Rev:       rev,
		Timestamp: time.Now(),
	}
}
func (e *testEvent) GetAge() time.Duration       { return time.Since(e.Timestamp) }
func (e *testEvent) GetRevision() int64          { return e.Rev }
func (e *testEvent) Matches(query struct{}) bool { return !e.Invisible }
