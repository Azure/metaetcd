package watch

import (
	"context"
	"fmt"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/etcdserver/etcdserverpb"
	"go.uber.org/zap"

	"github.com/Azure/metaetcd/internal/scheme"
)

type Mux struct {
	logger *zap.Logger
	buffer *buffer
	bcast  *broadcast
}

func NewMux(logger *zap.Logger, gapTimeout time.Duration) *Mux {
	bcast := newBroadcast()
	m := &Mux{
		logger: logger,
		buffer: newBuffer(gapTimeout, 200, bcast, logger), // TODO: Allow buffer length to be set as flag
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
		MetaToMemberMap: newMapRing(1000, m.logger),
		cancel:          cancel,
		done:            make(chan struct{}),
	}

	startRev := resp.Header.Revision + 1
	ctx = clientv3.WithRequireLeader(ctx)
	w := client.Watch(ctx, "", clientv3.WithPrefix(), clientv3.WithRev(startRev), clientv3.WithPrevKV())
	go func() {
		close(s.done)
		m.watchLoop(s.MetaToMemberMap, w)
		if ctx.Err() == nil {
			m.logger.Sugar().Panicf("watch of client with endpoints '%+s' closed unexpectedly", client.Endpoints())
		}
	}()

	// TODO: Block until caches have been warmed

	return s, nil
}

// TODO: Move this business logic to the scheme package
func (m *Mux) watchLoop(metaToMemberMap *mapRing, w clientv3.WatchChan) {
	memberToMetaMap := newMapRing(200, m.logger)
	for msg := range w {
		var meta int64 = -1
		for _, event := range msg.Events {
			if string(event.Kv.Key) == scheme.MetaKey {
				meta = scheme.MetaRevFromMetaKey(event.Kv.Value)
				event.Kv.ModRevision = meta
				memberToMetaMap.Push(msg.Header.Revision, meta)
				metaToMemberMap.Push(meta, msg.Header.Revision)
				break
			}
		}
		if meta == -1 {
			continue // not a metaetcd event
		}
		m.logger.Info("observed watch event", zap.Int64("metaRev", meta))

		for _, event := range msg.Events {
			if event.PrevKv != nil && len(event.PrevKv.Value) >= 8 {
				event.PrevKv.ModRevision = scheme.MetaRevFromValue(event.PrevKv.Value)
				event.PrevKv.Value = event.PrevKv.Value[:len(event.PrevKv.Value)-8]
			}

			if event.Type == clientv3.EventTypeDelete {
				event.Kv.ModRevision, _ = memberToMetaMap.Get(event.Kv.ModRevision)
				continue
			}

			isCreate := event.Kv.CreateRevision == event.Kv.ModRevision
			event.Kv.ModRevision = scheme.MetaRevFromValue(event.Kv.Value)
			if isCreate {
				event.Kv.CreateRevision = event.Kv.ModRevision
			}
			event.Kv.Value = event.Kv.Value[:len(event.Kv.Value)-8]
		}

		m.buffer.Push(msg.Events)
	}
}

func (m *Mux) Watch(ctx context.Context, key, end []byte, rev int64, ch chan<- *etcdserverpb.WatchResponse) {
	broadcast := make(chan struct{}, 2)
	close := m.bcast.Watch(broadcast)
	go func() {
		<-ctx.Done()
		close()
	}()

	broadcast <- struct{}{}

	var startingRev int64
	var n int
	for range broadcast {
		resp := &etcdserverpb.WatchResponse{Header: &etcdserverpb.ResponseHeader{}}
		resp.Events, n, resp.Header.Revision = m.buffer.Range(startingRev, key, end)
		if n > 0 {
			ch <- resp
			startingRev = resp.Header.Revision
		}
	}
}

type Status struct {
	MetaToMemberMap *mapRing
	cancel          context.CancelFunc
	done            chan struct{}
}

func (s *Status) Close() {
	s.cancel()
	<-s.done
}
