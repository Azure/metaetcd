package proxysvr

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/coreos/etcd/etcdserver/api/v3rpc/rpctypes"
	"github.com/coreos/etcd/etcdserver/etcdserverpb"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"

	"github.com/Azure/metaetcd/internal/clock"
	"github.com/Azure/metaetcd/internal/membership"
)

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
	clock       *clock.Clock
}

func NewServer(coord *membership.CoordinatorClientSet, members *membership.Pool, clock *clock.Clock) Server {
	return &server{
		coordinator: coord,
		members:     members,
		clock:       clock,
	}
}

func NewGRPCServer(ca, cert, key string, maxIdle, interval, timeout time.Duration) (*grpc.Server, error) {
	parsedCert, err := tls.LoadX509KeyPair(cert, key)
	if err != nil {
		return nil, err
	}
	cas := x509.NewCertPool()
	caPem, err := os.ReadFile(ca)
	if err != nil {
		return nil, err
	}
	if !cas.AppendCertsFromPEM(caPem) {
		return nil, fmt.Errorf("invalid ca pem")
	}
	tlsc := &tls.Config{
		Certificates: []tls.Certificate{parsedCert},
		RootCAs:      cas,
		ClientCAs:    cas,
		ClientAuth:   tls.RequireAndVerifyClientCert,
	}
	return grpc.NewServer(
		grpc.Creds(credentials.NewTLS(tlsc)),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle: maxIdle,
			Time:              interval,
			Timeout:           timeout,
		}),
		grpc.MaxRecvMsgSize(math.MaxInt32),
		grpc.MaxSendMsgSize(math.MaxInt32),
	), nil
}

func (s *server) Range(ctx context.Context, req *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	start := time.Now()
	if len(req.RangeEnd) == 0 {
		requestCount.WithLabelValues("Get").Inc()
	} else {
		requestCount.WithLabelValues("Range").Inc()
	}

	var metaRev int64
	if req.Revision != 0 {
		metaRev = req.Revision
	} else {
		var err error
		metaRev, err = s.clock.Now(ctx)
		if err != nil {
			return nil, err
		}
	}

	resp := &etcdserverpb.RangeResponse{Header: &etcdserverpb.ResponseHeader{Revision: metaRev}}
	if len(req.RangeEnd) == 0 {
		client := s.members.GetMemberForKey(string(req.Key))
		if err := s.rangeWithClient(ctx, req, resp, metaRev, client, nil); err != nil {
			zap.L().Warn("completed single-key range with error", zap.String("key", string(req.Key)), zap.Int64("metaRev", metaRev), zap.Duration("latency", time.Since(start)), zap.Error(err))
			return nil, err
		}
		zap.L().Info("completed single-key range successfully", zap.String("key", string(req.Key)), zap.Int64("metaRev", metaRev), zap.Duration("latency", time.Since(start)))
		return resp, nil
	}

	var mut sync.Mutex
	err := s.members.IterateMembers(ctx, func(ctx context.Context, client *membership.ClientSet) error {
		return s.rangeWithClient(ctx, req, resp, metaRev, client, &mut)
	})
	if req.Limit != 0 && int64(len(resp.Kvs)) > req.Limit {
		// TODO: Make sure to add test coverage for the sorting below
		sort.Slice(resp.Kvs, func(i, j int) bool { return bytes.Compare(resp.Kvs[i].Key, resp.Kvs[j].Key) < 0 })
		resp.Kvs = resp.Kvs[:req.Limit]
		resp.More = true
	}
	if err != nil {
		zap.L().Info("completed range with error", zap.String("start", string(req.Key)), zap.String("end", string(req.RangeEnd)), zap.Int64("metaRev", metaRev), zap.Int64("count", resp.Count), zap.Duration("latency", time.Since(start)), zap.Error(err))
		return nil, err
	}
	zap.L().Info("completed range successfully", zap.String("start", string(req.Key)), zap.String("end", string(req.RangeEnd)), zap.Int64("metaRev", metaRev), zap.Int64("count", resp.Count), zap.Int64("limit", req.Limit), zap.Duration("latency", time.Since(start)))

	return resp, nil
}

func (s *server) rangeWithClient(ctx context.Context, req *etcdserverpb.RangeRequest, resp *etcdserverpb.RangeResponse, metaRev int64, client *membership.ClientSet, mut *sync.Mutex) error {
	memberRev, err := s.clock.ResolveMetaToMember(ctx, client, metaRev)
	if err != nil {
		return err
	}

	reqCopy := *req
	reqCopy.Revision = memberRev
	r, err := client.KV.Range(ctx, &reqCopy)
	if err != nil {
		return fmt.Errorf("ranging at member rev %d: %w", memberRev, err)
	}

	resp.Count += r.Count
	if !req.CountOnly {
		s.clock.MungeRangeResp(r)

		if mut != nil {
			mut.Lock()
		}
		resp.Kvs = append(resp.Kvs, r.Kvs...)
		if mut != nil {
			mut.Unlock()
		}
	}

	return nil
}

func (s *server) Watch(srv etcdserverpb.Watch_WatchServer) error {
	requestCount.WithLabelValues("Watch").Inc()
	activeWatchCount.Inc()
	defer activeWatchCount.Dec()

	wg, ctx := errgroup.WithContext(srv.Context())
	id := uuid.Must(uuid.NewRandom()).String()
	zap.L().Info("starting watch connection", zap.String("watchID", id))

	ch := make(chan *etcdserverpb.WatchResponse)
	wg.Go(func() error {
		defer close(ch)
		for {
			msg, err := srv.Recv()
			if err != nil {
				return err
			}
			if r := msg.GetCreateRequest(); r != nil {
				if r.StartRevision == 0 {
					r.StartRevision, err = s.clock.Now(ctx)
					if err != nil {
						return err
					}
				}
				future, lowerBound := s.members.WatchMux.Watch(ctx, r, ch)
				if future == nil {
					zap.L().Warn("attempted to start watch before buffer", zap.String("watchID", id), zap.Int64("currentLowerBound", lowerBound), zap.Int64("metaRev", r.StartRevision))
					return rpctypes.ErrGRPCCompacted
				}
				zap.L().Info("added keyspace to watch connection", zap.String("watchID", id), zap.String("start", string(r.Key)), zap.String("end", string(r.RangeEnd)), zap.Int64("metaRev", r.StartRevision))
				wg.Go(func() error {
					future()
					return nil
				})
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
		return err
	}
	zap.L().Info("closing watch connection", zap.String("watchID", id))
	return nil
}

func (s *server) Txn(ctx context.Context, req *etcdserverpb.TxnRequest) (*etcdserverpb.TxnResponse, error) {
	requestCount.WithLabelValues("Txn").Inc()

	key, err := s.clock.ValidateTxn(req)
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
		memberRev, resp, err := s.clock.ResolveMetaToMemberTxn(ctx, client, key, r.ModRevision, req)
		if err != nil {
			return nil, err
		}
		if resp != nil {
			return resp, nil
		}
		r.ModRevision = memberRev
	}

	metaRev, err := s.clock.Tick(ctx)
	if err != nil {
		return nil, err
	}
	s.clock.MungeTxn(metaRev, req)

	resp, err := client.KV.Txn(ctx, req)
	if err != nil {
		zap.L().Error("error sending tx", zap.String("key", string(key)), zap.Int64("metaRev", metaRev), zap.Error(err))
		return nil, err
	}
	s.clock.MungeTxnResp(metaRev, resp)

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

func (s *server) LeaseGrant(ctx context.Context, req *etcdserverpb.LeaseGrantRequest) (*etcdserverpb.LeaseGrantResponse, error) {
	requestCount.WithLabelValues("LeaseGrant").Inc()
	if req.ID == 0 {
		req.ID = rand.Int63()
	}
	err := s.members.IterateMembers(ctx, func(ctx context.Context, cs *membership.ClientSet) error {
		resp, err := cs.Lease.LeaseGrant(ctx, req)
		if err != nil {
			return err
		}
		if resp.Error != "" {
			return fmt.Errorf("lease error: %s", resp.Error)
		}
		return nil
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

func (s *server) Compact(ctx context.Context, req *etcdserverpb.CompactionRequest) (*etcdserverpb.CompactionResponse, error) {
	err := s.members.IterateMembers(ctx, func(ctx context.Context, cs *membership.ClientSet) (err error) {
		reqCopy := *req
		reqCopy.Revision, err = s.clock.ResolveMetaToMember(ctx, cs, req.Revision)
		if err != nil {
			return err
		}

		_, err = cs.KV.Compact(ctx, &reqCopy)
		return err
	})
	if err != nil {
		return nil, err
	}

	reqCopy := *req
	reqCopy.Revision, err = s.clock.ResolveMetaToMember(ctx, s.coordinator.ClientSet, req.Revision)
	if err != nil {
		return nil, err
	}

	if _, err = s.coordinator.KV.Compact(ctx, &reqCopy); err != nil {
		return nil, err
	}

	return &etcdserverpb.CompactionResponse{}, nil
}
