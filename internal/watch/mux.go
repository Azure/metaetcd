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
)

type EventTransformer interface {
	MungeEvents([]*clientv3.Event) (metaRev int64, events []*mvccpb.Event, ok bool)
}

type Mux struct {
	buffer      *buffer
	ch          chan *eventWrapper
	tree        *groupTree[*mvccpb.Event]
	transformer EventTransformer
}

func NewMux(gapTimeout time.Duration, bufferLen int, et EventTransformer) *Mux {
	ch := make(chan *eventWrapper)
	m := &Mux{
		buffer:      newBuffer(gapTimeout, bufferLen, ch),
		ch:          ch,
		tree:        newGroupTree[*mvccpb.Event](),
		transformer: et,
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
	resp, err := client.KV.Get(context.Background(), "a")
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
		meta, events, ok := m.transformer.MungeEvents(msg.Events)
		if !ok {
			continue
		}

		for _, event := range events {
			zap.L().Info("observed watch event", zap.Int64("metaRev", meta))
			m.buffer.Push(&eventWrapper{
				Event:     event,
				Timestamp: time.Now(),
				Key:       adt.NewStringAffinePoint(string(event.Kv.Key)),
			})
		}

	}
}

func (m *Mux) Watch(ctx context.Context, req *etcdserverpb.WatchCreateRequest, ch chan<- *etcdserverpb.WatchResponse) (func(), int64) {
	eventCh := make(chan *mvccpb.Event, 2000) // TODO: Make this tunable
	i := adt.NewStringAffineInterval(string(req.Key), string(req.RangeEnd))

	// Start listening for new events
	var chanStartRev int64
	func() {
		m.buffer.mut.Lock()
		defer m.buffer.mut.Unlock()
		m.tree.Add(i, eventCh)
		chanStartRev = m.buffer.upperBound
	}()

	ch <- &etcdserverpb.WatchResponse{WatchId: req.WatchId, Created: true, Header: &etcdserverpb.ResponseHeader{}}

	// Backfill old events
	var startingRev int64
	for j := 0; true; j++ {
		events, lowerBound, upperBound := m.buffer.Range(startingRev, i)
		if j == 0 && lowerBound > req.StartRevision {
			staleWatchCount.Inc()
			return nil, lowerBound
		}
		for _, event := range events {
			ch <- &etcdserverpb.WatchResponse{Header: &etcdserverpb.ResponseHeader{}, WatchId: req.WatchId, Events: []*mvccpb.Event{event}}
			if event.Kv.ModRevision > startingRev {
				startingRev = event.Kv.ModRevision
			}
		}
		if upperBound >= chanStartRev {
			break
		}
		time.Sleep(time.Millisecond * 100)
	}

	go func() {
		<-ctx.Done()
		m.tree.Remove(i, eventCh)
		close(eventCh)
	}()

	// Map the event channel into the watch response channel
	done := make(chan struct{})
	go func() {
		defer close(done)
		for event := range eventCh {
			if event.Kv.ModRevision < startingRev {
				continue
			}
			ch <- &etcdserverpb.WatchResponse{Header: &etcdserverpb.ResponseHeader{}, WatchId: req.WatchId, Events: []*mvccpb.Event{event}}
		}
	}()
	return func() { <-done }, 0
}

type Status struct {
	cancel context.CancelFunc
	done   chan struct{}
}

func (s *Status) Close() {
	s.cancel()
	<-s.done
}
