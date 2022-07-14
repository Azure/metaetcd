package proxysvr

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/etcdserver/api/v3rpc/rpctypes"
	"github.com/coreos/etcd/etcdserver/etcdserverpb"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"github.com/Azure/metaetcd/internal/membership"
	"github.com/Azure/metaetcd/internal/scheme"
)

// TODO: Wire up compaction API and return watch events when compaction occurs

// TODO: Use per-member circuit breakers to avoid orphaned clock ticks and the watch latency they cause

type Server interface {
	etcdserverpb.KVServer
	etcdserverpb.WatchServer
	etcdserverpb.LeaseServer
}

type server struct {
	etcdserverpb.UnimplementedKVServer
	etcdserverpb.UnimplementedWatchServer
	etcdserverpb.UnimplementedLeaseServer

	coordinator *membership.CoordinatorClientSet
	members     *membership.Pool
}

func NewServer(coordinator *membership.CoordinatorClientSet, members *membership.Pool) Server {
	return &server{
		coordinator: coordinator,
		members:     members,
	}
}

func NewGRPCServer(maxIdle, interval, timeout time.Duration) *grpc.Server {
	return grpc.NewServer(
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle: maxIdle,
			Time:              interval,
			Timeout:           timeout,
		}),
		grpc.MaxRecvMsgSize(10*1024*1024),
		grpc.MaxSendMsgSize(10*1024*1024),
	)
}

func (s *server) Range(ctx context.Context, req *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	start := time.Now()
	if len(req.RangeEnd) == 0 {
		requestCount.WithLabelValues("Get").Inc()
	} else {
		requestCount.WithLabelValues("Range").Inc()
	}

	limit := req.Limit
	var metaRev int64
	if req.Revision != 0 {
		metaRev = req.Revision
	} else {
		var err error
		metaRev, err = s.now(ctx)
		if err != nil {
			return nil, err
		}
	}

	resp := &etcdserverpb.RangeResponse{Header: &etcdserverpb.ResponseHeader{Revision: metaRev}}
	if len(req.RangeEnd) == 0 {
		client := s.members.GetMemberForKey(string(req.Key))
		if err := s.rangeWithClient(ctx, req, resp, metaRev, client); err != nil {
			zap.L().Warn("completed single-key range with error", zap.String("key", string(req.Key)), zap.Int64("metaRev", metaRev), zap.Duration("latency", time.Since(start)), zap.Error(err))
			return nil, err
		}
		zap.L().Info("completed single-key range successfully", zap.String("key", string(req.Key)), zap.Int64("metaRev", metaRev), zap.Duration("latency", time.Since(start)))
		return resp, nil
	}

	// TODO: Concurrency?
	err := s.members.IterateMembers(func(client *membership.ClientSet) (bool, error) {
		err := s.rangeWithClient(ctx, req, resp, metaRev, client)
		return true, err
	})
	if err != nil {
		zap.L().Info("completed range with error", zap.String("start", string(req.Key)), zap.String("end", string(req.RangeEnd)), zap.Int64("metaRev", metaRev), zap.Int64("count", resp.Count), zap.Duration("latency", time.Since(start)), zap.Error(err))
		return nil, err
	}
	zap.L().Info("completed range successfully", zap.String("start", string(req.Key)), zap.String("end", string(req.RangeEnd)), zap.Int64("metaRev", metaRev), zap.Int64("count", resp.Count), zap.Duration("latency", time.Since(start)))

	sort.Slice(resp.Kvs, func(i, j int) bool { return bytes.Compare(resp.Kvs[i].Key, resp.Kvs[j].Key) > 0 })
	if limit != 0 && int64(len(resp.Kvs)) > limit {
		resp.Kvs = resp.Kvs[:limit]
		resp.Count = limit
		resp.More = true
	}

	return resp, nil
}

func (s *server) rangeWithClient(ctx context.Context, req *etcdserverpb.RangeRequest, resp *etcdserverpb.RangeResponse, metaRev int64, client *membership.ClientSet) error {
	memberRev, err := s.getMemberRev(ctx, client.ClientV3, metaRev)
	if err != nil {
		return err
	}
	req.Revision = memberRev

	r, err := client.KV.Range(ctx, req)
	if err != nil {
		return err
	}

	resp.Count += r.Count
	if !req.CountOnly {
		for _, kv := range r.Kvs {
			scheme.ResolveModRev(kv)
		}
		resp.Kvs = append(resp.Kvs, r.Kvs...)
	}

	return nil
}

func (s *server) Watch(srv etcdserverpb.Watch_WatchServer) error {
	requestCount.WithLabelValues("Watch").Inc()
	wg, _ := errgroup.WithContext(srv.Context())
	ch := make(chan *etcdserverpb.WatchResponse)
	id := uuid.Must(uuid.NewRandom()).String()
	zap.L().Info("starting watch connection", zap.String("watchID", id))

	wg.Go(func() error {
		for {
			msg, err := srv.Recv()
			if err != nil {
				return err
			}
			if r := msg.GetCreateRequest(); r != nil {
				if r.StartRevision == 0 {
					r.StartRevision, err = s.now(srv.Context())
					if err != nil {
						return err
					}
				}
				wg.Go(func() error {
					zap.L().Info("adding keyspace to watch connection", zap.String("watchID", id), zap.String("start", string(r.Key)), zap.String("end", string(r.RangeEnd)), zap.Int64("metaRev", r.StartRevision))
					s.members.WatchMux.Watch(srv.Context(), r.Key, r.RangeEnd, r.StartRevision, ch)
					return nil
				})
				ch <- &etcdserverpb.WatchResponse{WatchId: r.WatchId, Created: true, Header: &etcdserverpb.ResponseHeader{}}
			}
			// TODO: Handle other types of incoming requests
		}
	})

	wg.Go(func() error {
		for msg := range ch {
			if err := srv.Send(msg); err != nil {
				return err
			}
		}
		return nil
	})

	if err := wg.Wait(); err != nil {
		zap.L().Warn("closing watch connection with error", zap.String("watchID", id), zap.Error(err))
	}
	zap.L().Info("closing watch connection", zap.String("watchID", id))
	return nil
}

func (s *server) Txn(ctx context.Context, req *etcdserverpb.TxnRequest) (*etcdserverpb.TxnResponse, error) {
	requestCount.WithLabelValues("Txn").Inc()
	key, err := scheme.ValidateTxComparisons(req.Compare)
	if err != nil {
		return nil, err
	}
	key, err = scheme.ValidateTxOps(key, req.Success)
	if err != nil {
		return nil, err
	}
	key, err = scheme.ValidateTxOps(key, req.Failure)
	if err != nil {
		return nil, err
	}

	client := s.members.GetMemberForKey(string(key))
	// TODO: Check if client is nil here and in other places too (only matters once clients can be added at runtime)
	for _, op := range req.Compare {
		r, ok := op.TargetUnion.(*etcdserverpb.Compare_ModRevision)
		if !ok {
			continue
		}
		if r.ModRevision == 0 {
			continue
		}
		memberRev, resp, err := s.resolveModComparison(ctx, client, key, r.ModRevision, req)
		if err != nil {
			return nil, err
		}
		if resp != nil {
			return resp, nil
		}
		r.ModRevision = memberRev
	}

	metaRev, err := s.tick(ctx)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(metaRev))
	scheme.AppendMetaRevToTxOps(buf, req.Success)
	scheme.AppendMetaRevToTxOps(buf, req.Failure)

	updateClockOp := &etcdserverpb.RequestOp{
		Request: &etcdserverpb.RequestOp_RequestPut{
			RequestPut: &etcdserverpb.PutRequest{
				Key:   []byte(scheme.MetaKey),
				Value: buf,
			},
		},
	}
	req.Success = append(req.Success, updateClockOp)
	req.Failure = append(req.Failure, updateClockOp)

	resp, err := client.KV.Txn(ctx, req)
	if err != nil {
		zap.L().Error("error sending tx", zap.String("key", string(key)), zap.Int64("metaRev", metaRev), zap.Error(err))
		return nil, err
	}
	for _, r := range resp.Responses {
		if p := r.GetResponsePut(); p != nil {
			scheme.ResolveModRev(p.PrevKv)
		}
		if p := r.GetResponseRange(); p != nil {
			for _, kv := range p.Kvs {
				scheme.ResolveModRev(kv)
			}
		}
		if p := r.GetResponseDeleteRange(); p != nil {
			for _, kv := range p.PrevKvs {
				scheme.ResolveModRev(kv)
			}
		}
	}
	resp.Header = &etcdserverpb.ResponseHeader{Revision: metaRev}
	if resp.Succeeded {
		zap.L().Info("tx applied successfully", zap.String("key", string(key)), zap.Int64("metaRev", metaRev))
	} else {
		revs := make([]int64, len(req.Compare))
		for i, cmp := range req.Compare {
			revs[i] = cmp.GetModRevision()
		}
		zap.L().Error("tx failed", zap.String("key", string(key)), zap.Int64("metaRev", metaRev), zap.Int64s("cmpModRevs", revs))
	}
	return resp, nil
}

func (s *server) tick(ctx context.Context) (int64, error) {
	resp, err := s.coordinator.ClientV3.KV.Txn(ctx).Then(
		clientv3.OpPut(scheme.MetaKey, "", clientv3.WithIgnoreValue()),
		clientv3.OpGet(scheme.MetaKey),
	).Commit()
	if errors.Is(err, rpctypes.ErrKeyNotFound) {
		return s.reconstituteClock(ctx, 1)
	}
	if err != nil {
		return 0, fmt.Errorf("ticking clock: %w", err)
	}
	return scheme.ResolveMetaRev(resp.Responses[1].GetResponseRange().Kvs[0]), nil
}

func (s *server) now(ctx context.Context) (int64, error) {
	resp, err := s.coordinator.ClientV3.Get(ctx, scheme.MetaKey)
	if err != nil {
		return 0, fmt.Errorf("getting clock: %w", err)
	}
	if len(resp.Kvs) == 0 {
		return s.reconstituteClock(ctx, 0)
	}
	return scheme.ResolveMetaRev(resp.Kvs[0]), nil
}

func (s *server) reconstituteClock(ctx context.Context, delta int64) (int64, error) {
	s.coordinator.ClockReconstitutionLock.Lock(ctx)
	defer s.coordinator.ClockReconstitutionLock.Unlock(context.Background())

	resp, err := s.coordinator.ClientV3.Get(ctx, scheme.MetaKey)
	if err != nil {
		return 0, fmt.Errorf("getting clock: %w", err)
	}
	if len(resp.Kvs) > 0 {
		return scheme.ResolveMetaRev(resp.Kvs[0]), nil
	}

	zap.L().Error("clock was lost - reconstituting from member clusters")

	var latestMetaRev int64
	s.members.IterateMembers(func(client *membership.ClientSet) (bool, error) {
		r, err := client.ClientV3.KV.Get(ctx, scheme.MetaKey)
		if err != nil {
			return false, err
		}
		if len(r.Kvs) == 0 || len(r.Kvs[0].Value) < 8 {
			return true, nil
		}
		rev := int64(binary.LittleEndian.Uint64(r.Kvs[0].Value))
		if rev > latestMetaRev {
			latestMetaRev = rev
		}
		return true, nil
	})
	latestMetaRev += delta

	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(latestMetaRev)-1)

	_, err = s.coordinator.ClientV3.KV.Put(ctx, scheme.MetaKey, string(buf))
	if err != nil {
		return 0, err
	}

	zap.L().Info("reconstituted meta cluster logic clock", zap.Int64("metaRev", latestMetaRev))
	return latestMetaRev, nil
}

func (s *server) getMemberRev(ctx context.Context, client *clientv3.Client, metaRev int64) (int64, error) {
	var zeroKeyRev int64
	i := 0
	for {
		i++
		var opts []clientv3.OpOption
		if zeroKeyRev > 0 {
			opts = append(opts, clientv3.WithRev(zeroKeyRev))
		}
		resp, err := client.KV.Get(ctx, scheme.MetaKey, opts...)
		if err != nil {
			return 0, err
		}

		if len(resp.Kvs) == 0 {
			return resp.Header.Revision, nil
		}

		lastMetaRev := int64(binary.LittleEndian.Uint64(resp.Kvs[0].Value))
		if lastMetaRev > metaRev {
			zeroKeyRev = resp.Kvs[0].ModRevision - 1
			continue
		}

		zap.L().Info("resolved member rev", zap.Int("attempts", i))
		getMemberRevDepth.Observe(float64(i))
		return resp.Kvs[0].ModRevision, nil
	}
}

func (s *server) LeaseGrant(ctx context.Context, req *etcdserverpb.LeaseGrantRequest) (*etcdserverpb.LeaseGrantResponse, error) {
	requestCount.WithLabelValues("LeaseGrant").Inc()
	if req.ID == 0 {
		req.ID = rand.Int63()
	}
	err := s.members.IterateMembers(func(cs *membership.ClientSet) (bool, error) {
		resp, err := cs.Lease.LeaseGrant(ctx, req)
		if err != nil {
			return true, err
		}
		if resp.Error != "" {
			return false, fmt.Errorf("lease error: %s", resp.Error)
		}
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	zap.L().Info("granted lease successfully", zap.Int64("id", req.ID), zap.Duration("ttl", time.Duration(req.TTL)*time.Second))
	return &etcdserverpb.LeaseGrantResponse{
		Header: &etcdserverpb.ResponseHeader{},
		ID:     req.ID,
		TTL:    req.TTL,
	}, nil
}

func (s *server) resolveModComparison(ctx context.Context, client *membership.ClientSet, key []byte, metaRev int64, req *etcdserverpb.TxnRequest) (int64, *etcdserverpb.TxnResponse, error) {
	resp, err := client.ClientV3.Get(ctx, string(key))
	if err != nil {
		return 0, nil, err
	}
	if len(resp.Kvs) == 0 {
		return 0, nil, nil
	}

	modMetaRev, failureResp := scheme.PreflightTxn(metaRev, req, resp)
	if failureResp != nil {
		zap.L().Error("tx failed pre-check", zap.String("key", string(key)), zap.Int64("metaRev", metaRev), zap.Int64("actualModMetaRev", modMetaRev))
		return 0, failureResp, nil
	}

	return resp.Kvs[0].ModRevision, nil, nil
}
