package watch

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestMapRing(t *testing.T) {
	m := newMapRing(3, zap.NewNop())
	m.Push(10, 20)
	m.Push(11, 21)
	m.Push(12, 22)
	m.Push(13, 23)

	val, ok := m.Get(10)
	assert.False(t, ok)
	assert.Equal(t, int64(0), val)

	val, ok = m.Get(11)
	assert.True(t, ok)
	assert.Equal(t, int64(21), val)

	val, ok = m.Get(12)
	assert.True(t, ok)
	assert.Equal(t, int64(22), val)

	val, ok = m.Get(13)
	assert.True(t, ok)
	assert.Equal(t, int64(23), val)

	max := m.latestKey()
	assert.Equal(t, int64(13), max)
}

func TestMapRingWaiting(t *testing.T) {
	m := newMapRing(2, zap.NewNop())

	t.Run("future value", func(t *testing.T) {
		done := make(chan struct{})
		go func() {
			defer close(done)
			val, ok := m.WaitGet(context.Background(), 1, time.Second)
			assert.Equal(t, int64(2), val)
			assert.True(t, ok)
		}()
		time.Sleep(time.Millisecond)
		m.Push(1, 2)
		<-done
	})

	t.Run("existing value", func(t *testing.T) {
		val, ok := m.WaitGet(context.Background(), 1, time.Second)
		assert.Equal(t, int64(2), val)
		assert.True(t, ok)
	})

	t.Run("timeout", func(t *testing.T) {
		val, ok := m.WaitGet(context.Background(), 2, time.Millisecond)
		assert.Equal(t, int64(-1), val)
		assert.False(t, ok)
	})
}
