package watch

import "sync"

type broadcast struct {
	mut sync.Mutex
	chs map[interface{}]chan<- struct{}
}

func newBroadcast() *broadcast {
	return &broadcast{chs: make(map[interface{}]chan<- struct{})}
}

func (b *broadcast) Send() {
	b.mut.Lock()
	defer b.mut.Unlock()
	for _, ch := range b.chs {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

func (b *broadcast) Watch(ch chan<- struct{}) func() {
	func() {
		b.mut.Lock()
		defer b.mut.Unlock()
		b.chs[ch] = ch
		// TODO: Close channel?
	}()

	return func() {
		b.mut.Lock()
		defer b.mut.Unlock()
		delete(b.chs, ch)
	}
}
