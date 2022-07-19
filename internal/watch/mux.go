package watch

import (
	"context"
	"fmt"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/etcdserver/etcdserverpb"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"go.etcd.io/etcd/pkg/v3/adt"
	"go.uber.org/zap"

	"github.com/Azure/metaetcd/internal/scheme"
)

type Mux struct {
	buffer *buffer
	ch     chan *eventWrapper
	tree   *groupTree[*mvccpb.Event]
}

func NewMux(gapTimeout time.Duration, bufferLen int) *Mux {
	ch := make(chan *eventWrapper)
	m := &Mux{
		buffer: newBuffer(gapTimeout, bufferLen, ch),
		ch:     ch,
		tree:   newGroupTree[*mvccpb.Event](),
	}
	return m
}

func (m *Mux) Run(ctx context.Context) {
	go m.buffer.Run(ctx)
	go func() {
		<-ctx.Done()
		close(m.ch)
	}()
	for event := range m.ch {
		m.tree.Broadcast(event.Key, event.Event)
	}
}

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

func (m *Mux) Watch(ctx context.Context, key, end []byte, rev int64, ch chan<- *etcdserverpb.WatchResponse) {
	eventCh := make(chan *mvccpb.Event, 2000) // TODO: Make this tunable
	i := adt.NewStringAffineInterval(string(key), string(end))

	// Start listening for new events
	var chanStartRev int64
	func() {
		m.buffer.mut.Lock()
		defer m.buffer.mut.Unlock()
		m.tree.Add(i, eventCh)
		chanStartRev = m.buffer.upperBound
	}()

	// Clean up when the context is canceled
	go func() {
		<-ctx.Done()

		m.tree.Remove(i, eventCh)
		defer close(eventCh)
	}()

	// Backfill old events
	var startingRev int64
	for {
		events, upperBound := m.buffer.Range(startingRev, i)
		for _, event := range events {
			ch <- &etcdserverpb.WatchResponse{Header: &etcdserverpb.ResponseHeader{}, Events: []*mvccpb.Event{event}}
		}
		startingRev = upperBound
		if upperBound > chanStartRev {
			break
		}
		time.Sleep(time.Millisecond * 100)
	}

	// Map the event channel into the watch response channel
	for event := range eventCh {
		if event.Kv.ModRevision < startingRev {
			continue
		}
		ch <- &etcdserverpb.WatchResponse{Header: &etcdserverpb.ResponseHeader{}, Events: []*mvccpb.Event{event}}
	}
}

type Status struct {
	cancel context.CancelFunc
	done   chan struct{}
}

func (s *Status) Close() {
	s.cancel()
	<-s.done
}
