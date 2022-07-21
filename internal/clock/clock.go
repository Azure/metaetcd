package clock

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/etcdserver/api/v3rpc/rpctypes"
	"github.com/coreos/etcd/etcdserver/etcdserverpb"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"go.uber.org/zap"

	"github.com/Azure/metaetcd/internal/membership"
	"github.com/Azure/metaetcd/internal/scheme"
)

// Clock implements the meta cluster's logic clock.
type Clock struct {
	Coordinator *membership.CoordinatorClientSet
	Members     *membership.Pool
}

func (c *Clock) Init() error {
	ctx, done := context.WithTimeout(context.Background(), time.Second*15)
	defer done()

	_, err := c.Coordinator.ClientV3.KV.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(scheme.MetaKey), "=", 0)).
		Then(clientv3.OpPut(scheme.MetaKey, string(make([]byte, 8)))).
		Commit()
	return err
}

func (c *Clock) MungeRangeResp(resp *etcdserverpb.RangeResponse) {
	for _, kv := range resp.Kvs {
		scheme.ResolveModRev(kv)
	}
}

func (c *Clock) MungeTxnPreflight(req *etcdserverpb.TxnRequest) ([]byte, error) {
	key, err := scheme.ValidateTxComparisons(req.Compare)
	if err != nil {
		return nil, err
	}
	key, err = scheme.ValidateTxOps(key, req.Success)
	if err != nil {
		return nil, err
	}
	return scheme.ValidateTxOps(key, req.Failure)
}

func (c *Clock) MungeTxn(metaRev int64, req *etcdserverpb.TxnRequest) {
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
}

func (c *Clock) MungeTxnResp(metaRev int64, resp *etcdserverpb.TxnResponse) {
	for _, r := range resp.Responses {
		if p := r.GetResponsePut(); p != nil {
			scheme.ResolveModRev(p.PrevKv)
			p.Header.Revision = metaRev
		}
		if p := r.GetResponseRange(); p != nil {
			for _, kv := range p.Kvs {
				scheme.ResolveModRev(kv)
			}
		}
		if p := r.GetResponseDeleteRange(); p != nil {
			for _, kv := range p.PrevKvs {
				scheme.ResolveModRev(kv)
				p.Header.Revision = metaRev
			}
		}
	}

	resp.Header = &etcdserverpb.ResponseHeader{Revision: metaRev}
}

func (c *Clock) MungeEvents(events []*clientv3.Event) (int64, []*mvccpb.Event, bool) {
	meta, ok := scheme.FindMetaEvent(events)
	if !ok {
		return meta, nil, false
	}

	scheme.TransformEvents(meta, events) // TODO: Merge loop with below

	if len(events) == 1 { // only the meta event
		e := mvccpb.Event(*events[0])
		return meta, []*mvccpb.Event{&e}, true
	}

	out := []*mvccpb.Event{}
	for _, event := range events {
		if string(event.Kv.Key) == scheme.MetaKey {
			continue
		}
		e := mvccpb.Event(*event)
		out = append(out, &e)
	}

	return meta, out, true
}

func (c *Clock) Reset(ctx context.Context) error {
	_, err := c.Coordinator.ClientV3.KV.Delete(ctx, scheme.MetaKey)
	return err
}

func (c *Clock) Now(ctx context.Context) (int64, error) {
	resp, err := c.Coordinator.ClientV3.Get(ctx, scheme.MetaKey)
	if err != nil {
		return 0, fmt.Errorf("getting clock: %w", err)
	}
	if len(resp.Kvs) == 0 {
		return c.reconstituteClock(ctx, 0)
	}
	return scheme.ResolveMetaRev(resp.Kvs[0]), nil
}

func (c *Clock) Tick(ctx context.Context) (int64, error) {
	resp, err := c.Coordinator.ClientV3.KV.Txn(ctx).Then(
		clientv3.OpPut(scheme.MetaKey, "", clientv3.WithIgnoreValue()),
		clientv3.OpGet(scheme.MetaKey),
	).Commit()
	if errors.Is(err, rpctypes.ErrKeyNotFound) {
		return c.reconstituteClock(ctx, 1)
	}
	if err != nil {
		return 0, fmt.Errorf("ticking clock: %w", err)
	}
	return scheme.ResolveMetaRev(resp.Responses[1].GetResponseRange().Kvs[0]), nil
}

func (c *Clock) reconstituteClock(ctx context.Context, delta int64) (int64, error) {
	c.Coordinator.ClockReconstitutionLock.Lock(ctx)
	defer c.Coordinator.ClockReconstitutionLock.Unlock(context.Background())

	// TODO: Counter

	resp, err := c.Coordinator.ClientV3.Get(ctx, scheme.MetaKey)
	if err != nil {
		return 0, fmt.Errorf("getting clock: %w", err)
	}
	if len(resp.Kvs) > 0 {
		return scheme.ResolveMetaRev(resp.Kvs[0]), nil
	}

	zap.L().Error("clock was lost - reconstituting from member clusters")

	var mut sync.Mutex
	var latestMetaRev int64
	c.Members.IterateMembers(ctx, func(ctx context.Context, client *membership.ClientSet) error {
		r, err := client.ClientV3.KV.Get(ctx, scheme.MetaKey)
		if err != nil {
			return err
		}
		if len(r.Kvs) == 0 || len(r.Kvs[0].Value) < 8 {
			return nil
		}
		rev := int64(binary.LittleEndian.Uint64(r.Kvs[0].Value))
		mut.Lock()
		defer mut.Unlock()
		if rev > latestMetaRev {
			latestMetaRev = rev
		}
		return nil
	})
	latestMetaRev += delta

	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(latestMetaRev)-1)

	_, err = c.Coordinator.ClientV3.KV.Put(ctx, scheme.MetaKey, string(buf))
	if err != nil {
		return 0, err
	}

	zap.L().Info("reconstituted meta cluster logic clock", zap.Int64("metaRev", latestMetaRev))
	return latestMetaRev, nil
}

func (c *Clock) ResolveMetaToMember(ctx context.Context, client *membership.ClientSet, metaRev int64) (int64, error) {
	var zeroKeyRev int64
	i := 0
	for {
		i++
		var opts []clientv3.OpOption
		if zeroKeyRev > 0 {
			opts = append(opts, clientv3.WithRev(zeroKeyRev))
		}
		resp, err := client.ClientV3.KV.Get(ctx, scheme.MetaKey, opts...)
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

func (c *Clock) ResolveMetaToMemberTxn(ctx context.Context, client *membership.ClientSet, key []byte, metaRev int64, req *etcdserverpb.TxnRequest) (int64, *etcdserverpb.TxnResponse, error) {
	resp, err := client.ClientV3.Get(ctx, string(key))
	if err != nil {
		return 0, nil, err
	}
	if len(resp.Kvs) == 0 {
		return 0, nil, nil
	}

	modMetaRev, failureResp := scheme.PreflightTxn(metaRev, req, resp)
	if failureResp != nil {
		zap.L().Warn("tx failed pre-check", zap.String("key", string(key)), zap.Int64("metaRev", metaRev), zap.Int64("actualModMetaRev", modMetaRev))
		return 0, failureResp, nil
	}

	return resp.Kvs[0].ModRevision, nil, nil
}
