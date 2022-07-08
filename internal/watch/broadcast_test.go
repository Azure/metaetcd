package watch

import (
	"testing"
)

func TestBroadcast(t *testing.T) {
	b := newBroadcast()

	ch := make(chan struct{}, 1)
	closeFn := b.Watch(ch)
	closeFn()

	b.Watch(ch)
	b.Send()
	b.Send()
	b.Send()
	<-ch
}
