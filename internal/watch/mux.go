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

	"github.com/Azure/metaetcd/internal/util"
)

type EventTransformer interface {
	MungeEvents([]*clientv3.Event) (metaRev int64, events []*mvccpb.Event, ok bool)
}

// Mux bridges between incoming watch connections from clients and outgoing watch connections to member clusters.
type Mux struct {
	buffer      *util.TimeBuffer[adt.Interval, *eventWrapper]
	ch          chan *eventWrapper
	tree        *util.GroupTree[*mvccpb.Event]
	transformer EventTransformer
}

func NewMux(gapTimeout time.Duration, bufferLen int, et EventTransformer) *Mux {
	ch := make(chan *eventWrapper)
	m := &Mux{
		buffer:      util.NewTimeBuffer[adt.Interval](gapTimeout, bufferLen, ch),
		ch:          ch,
		tree:        util.NewGroupTree[*mvccpb.Event](),
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

func (m *Mux) StartWatch(ctx context.Context, client *clientv3.Client) (*Status, error) {
	watchesDialing.Inc()
	resp, err := client.KV.Get(ctx, "a") // any key will do - doesn't need to exist
	if err != nil {
		return nil, fmt.Errorf("getting current revision: %w", err)
	}

	// Don't use the provided context. It's for establishing a connection - this one is for running it.
	ctx, cancel := context.WithCancel(context.Background())
	s := &Status{
		cancel: cancel,
		done:   make(chan struct{}),
	}

	// Warm the buffer by starting the watch at (current revision) - (buffer length)
	nextEvent := (resp.Header.Revision + 1)
	startRev := nextEvent - int64(m.buffer.Len())
	if startRev < 0 {
		startRev = 0
	}

	ctx = clientv3.WithRequireLeader(ctx)
	w := client.Watch(ctx, "", clientv3.WithPrefix(), clientv3.WithRev(startRev), clientv3.WithPrevKV())
	watchesDialing.Dec()

	go func() {
		watchesRunning.Inc()
		defer watchesRunning.Dec()
		defer close(s.done)
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
			watchEventCount.Inc()
			m.buffer.Push(&eventWrapper{
				Event:     event,
				Timestamp: time.Now(),
				Key:       adt.NewStringAffinePoint(string(event.Kv.Key)),
			})
		}

	}
}

func (m *Mux) Watch(ctx context.Context, req *etcdserverpb.WatchCreateRequest, ch chan<- *etcdserverpb.WatchResponse) (func(), int64) {
	eventCh := make(chan *mvccpb.Event, m.buffer.Len())
	i := adt.NewStringAffineInterval(string(req.Key), string(req.RangeEnd))

	// Start listening for new events
	m.tree.Add(i, eventCh)

	ch <- &etcdserverpb.WatchResponse{WatchId: req.WatchId, Created: true, Header: &etcdserverpb.ResponseHeader{}}

	// Backfill old events
	events, min, max := m.buffer.Range(req.StartRevision, i)
	if min > req.StartRevision {
		staleWatchCount.Inc()
		return nil, min
	}
	for _, event := range events {
		ch <- &etcdserverpb.WatchResponse{Header: &etcdserverpb.ResponseHeader{}, WatchId: req.WatchId, Events: []*mvccpb.Event{event.Event}}
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
			if event.Kv.ModRevision < max {
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

type eventWrapper struct {
	*mvccpb.Event
	Key       adt.Interval
	Timestamp time.Time
}

func (e *eventWrapper) GetAge() time.Duration         { return time.Since(e.Timestamp) }
func (e *eventWrapper) GetRevision() int64            { return e.Kv.ModRevision }
func (e *eventWrapper) Matches(ivl adt.Interval) bool { return ivl.Compare(&e.Key) == 0 }
