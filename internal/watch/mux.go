package watch

import (
	"container/list"
	"context"
	"fmt"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/etcdserver/etcdserverpb"
	"go.etcd.io/etcd/pkg/v3/adt"
	"go.uber.org/zap"

	"github.com/Azure/metaetcd/internal/scheme"
)

type Mux struct {
	buffer *buffer
	bcast  *broadcast
}

func NewMux(gapTimeout time.Duration, bufferLen int) *Mux {
	bcast := newBroadcast()
	m := &Mux{
		buffer: newBuffer(gapTimeout, bufferLen, bcast),
		bcast:  bcast,
	}
	return m
}

func (m *Mux) Run(ctx context.Context) { m.buffer.Run(ctx) }

func (m *Mux) StartWatch(client *clientv3.Client) (*Status, error) {
	resp, err := client.KV.Get(context.Background(), scheme.MetaKey)
	if err != nil {
		return nil, fmt.Errorf("getting current revision: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	s := &Status{
		cancel: cancel,
		done:   make(chan struct{}),
	}

	nextEvent := (resp.Header.Revision + 1)
	startRev := nextEvent - int64(m.buffer.maxLen)
	if startRev < 0 {
		startRev = 0
	}

	ctx = clientv3.WithRequireLeader(ctx)
	w := client.Watch(ctx, "", clientv3.WithPrefix(), clientv3.WithRev(startRev), clientv3.WithPrevKV())
	go func() {
		close(s.done)
		m.watchLoop(w)
		if ctx.Err() == nil {
			zap.L().Sugar().Panicf("watch of client with endpoints '%+s' closed unexpectedly", client.Endpoints())
		}
	}()

	return s, nil
}

func (m *Mux) watchLoop(w clientv3.WatchChan) {
	for msg := range w {
		meta, ok := scheme.FindMetaEvent(msg.Events)
		if !ok {
			continue // not a metaetcd event
		}
		zap.L().Info("observed watch event", zap.Int64("metaRev", meta))
		scheme.TransformEvents(meta, msg.Events)
		m.buffer.Push(msg.Events)
	}
}

func (m *Mux) Watch(key, end []byte, rev int64) *Watch {
	pos, ok := m.buffer.StartRange(rev)
	if !ok {
		return nil
	}

	// TODO: Consider one tree per incoming watch connection (like etcd does)
	tree := adt.NewIntervalTree()
	tree.Insert(adt.NewStringAffineInterval(string(key), string(end)), nil)

	return &Watch{m: m, pos: pos, tree: tree}
}

type Watch struct {
	m    *Mux
	pos  *list.Element
	tree adt.IntervalTree
}

func (w *Watch) Run(ctx context.Context, ch chan<- *etcdserverpb.WatchResponse) *Status {
	broadcast := make(chan struct{}, 2)
	close := w.m.bcast.Watch(broadcast)
	go func() {
		<-ctx.Done()
		close()
	}()

	broadcast <- struct{}{}

	var n int
	for range broadcast {
		resp := &etcdserverpb.WatchResponse{Header: &etcdserverpb.ResponseHeader{}}
		resp.Events, n, w.pos = w.m.buffer.Range(w.pos, w.tree)
		if n > 0 {
			ch <- resp
		}
	}
	return nil
}

type Status struct {
	cancel context.CancelFunc
	done   chan struct{}
}

func (s *Status) Close() {
	s.cancel()
	<-s.done
}
