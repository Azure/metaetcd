package watch

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDeferralQueue(t *testing.T) {
	d := newDeferralQueue(time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		d.Run(ctx)
		close(done)
	}()

	d.Defer(1)
	d.Defer(2)
	d.Defer(3)

	val := <-d.Chan
	assert.Equal(t, int64(1), val)

	val = <-d.Chan
	assert.Equal(t, int64(2), val)

	val = <-d.Chan
	assert.Equal(t, int64(3), val)

	cancel()
	<-done
}
