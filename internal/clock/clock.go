package clock

import (
	"bytes"
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
)

const metaKey = "/meta"

var (
	errMultipleKeysInTx = errors.New("transactions can only involve a single key")
	errCreateRevCompare = errors.New("create revision comparisons are not supported")
	errPrevKv           = errors.New("previous kv is not supported in transactions")
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
		If(clientv3.Compare(clientv3.Version(metaKey), "=", 0)).
		Then(clientv3.OpPut(metaKey, string(make([]byte, 8)))).
		Commit()
	return err
}

func (c *Clock) MungeRangeResp(resp *etcdserverpb.RangeResponse) {
	for _, kv := range resp.Kvs {
		resolveModRev(kv)
	}
}

func (c *Clock) MungeTxnPreflight(req *etcdserverpb.TxnRequest) ([]byte, error) {
	key, err := validateTxComparisons(req.Compare)
	if err != nil {
		return nil, err
	}
	key, err = validateTxOps(key, req.Success)
	if err != nil {
		return nil, err
	}
	return validateTxOps(key, req.Failure)
}

func (c *Clock) MungeTxn(metaRev int64, req *etcdserverpb.TxnRequest) {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(metaRev))
	appendMetaRevToTxOps(buf, req.Success)
	appendMetaRevToTxOps(buf, req.Failure)

	updateClockOp := &etcdserverpb.RequestOp{
		Request: &etcdserverpb.RequestOp_RequestPut{
			RequestPut: &etcdserverpb.PutRequest{
				Key:   []byte(metaKey),
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
			resolveModRev(p.PrevKv)
			p.Header.Revision = metaRev
		}
		if p := r.GetResponseRange(); p != nil {
			for _, kv := range p.Kvs {
				resolveModRev(kv)
			}
		}
		if p := r.GetResponseDeleteRange(); p != nil {
			for _, kv := range p.PrevKvs {
				resolveModRev(kv)
				p.Header.Revision = metaRev
			}
		}
	}

	resp.Header = &etcdserverpb.ResponseHeader{Revision: metaRev}
}

func (c *Clock) MungeEvents(events []*clientv3.Event) (int64, []*mvccpb.Event, bool) {
	meta, ok := findMetaEvent(events)
	if !ok {
		return meta, nil, false
	}

	for _, event := range events {
		if event.PrevKv != nil && len(event.PrevKv.Value) >= 8 {
			event.PrevKv.ModRevision = getMetaRevFromValue(event.PrevKv.Value)
			event.PrevKv.Value = event.PrevKv.Value[:len(event.PrevKv.Value)-8]
		}
		if event.Type == clientv3.EventTypeDelete {
			event.Kv.ModRevision = meta
			continue
		}

		isCreate := event.Kv.CreateRevision == event.Kv.ModRevision
		event.Kv.ModRevision = getMetaRevFromValue(event.Kv.Value)
		if isCreate {
			event.Kv.CreateRevision = event.Kv.ModRevision
		}
		event.Kv.Value = event.Kv.Value[:len(event.Kv.Value)-8]
	}

	if len(events) == 1 { // only the meta event
		e := mvccpb.Event(*events[0])
		return meta, []*mvccpb.Event{&e}, true
	}

	out := []*mvccpb.Event{}
	for _, event := range events { // TODO: Merge with above
		if string(event.Kv.Key) == metaKey {
			continue
		}
		e := mvccpb.Event(*event)
		out = append(out, &e)
	}

	return meta, out, true
}

func (c *Clock) Reset(ctx context.Context) error {
	_, err := c.Coordinator.ClientV3.KV.Delete(ctx, metaKey)
	return err
}

func (c *Clock) Now(ctx context.Context) (int64, error) {
	resp, err := c.Coordinator.ClientV3.Get(ctx, metaKey)
	if err != nil {
		return 0, fmt.Errorf("getting clock: %w", err)
	}
	if len(resp.Kvs) == 0 {
		return c.reconstituteClock(ctx, 0)
	}
	return resolveMetaRev(resp.Kvs[0]), nil
}

func (c *Clock) Tick(ctx context.Context) (int64, error) {
	resp, err := c.Coordinator.ClientV3.KV.Txn(ctx).Then(
		clientv3.OpPut(metaKey, "", clientv3.WithIgnoreValue()),
		clientv3.OpGet(metaKey),
	).Commit()
	if errors.Is(err, rpctypes.ErrKeyNotFound) {
		return c.reconstituteClock(ctx, 1)
	}
	if err != nil {
		return 0, fmt.Errorf("ticking clock: %w", err)
	}
	return resolveMetaRev(resp.Responses[1].GetResponseRange().Kvs[0]), nil
}

func (c *Clock) reconstituteClock(ctx context.Context, delta int64) (int64, error) {
	c.Coordinator.ClockReconstitutionLock.Lock(ctx)
	defer c.Coordinator.ClockReconstitutionLock.Unlock(context.Background())

	// TODO: Counter

	resp, err := c.Coordinator.ClientV3.Get(ctx, metaKey)
	if err != nil {
		return 0, fmt.Errorf("getting clock: %w", err)
	}
	if len(resp.Kvs) > 0 {
		return resolveMetaRev(resp.Kvs[0]), nil
	}

	zap.L().Error("clock was lost - reconstituting from member clusters")

	var mut sync.Mutex
	var latestMetaRev int64
	c.Members.IterateMembers(ctx, func(ctx context.Context, client *membership.ClientSet) error {
		r, err := client.ClientV3.KV.Get(ctx, metaKey)
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

	_, err = c.Coordinator.ClientV3.KV.Put(ctx, metaKey, string(buf))
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
		resp, err := client.ClientV3.KV.Get(ctx, metaKey, opts...)
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

	modMetaRev, failureResp := c.resolveMetaToMemberTxn(metaRev, req, resp)
	if failureResp != nil {
		zap.L().Warn("failed to resolve meta rev to member for tx", zap.String("key", string(key)), zap.Int64("metaRev", metaRev), zap.Int64("actualModMetaRev", modMetaRev))
		return 0, failureResp, nil
	}

	return resp.Kvs[0].ModRevision, nil, nil
}

func (c *Clock) resolveMetaToMemberTxn(metaRev int64, req *etcdserverpb.TxnRequest, current *clientv3.GetResponse) (int64, *etcdserverpb.TxnResponse) {
	modMetaRev := getMetaRevFromValue(current.Kvs[0].Value)
	if modMetaRev == metaRev {
		return current.Kvs[0].ModRevision, nil
	}

	returnVal := &etcdserverpb.TxnResponse{Header: &etcdserverpb.ResponseHeader{}}
	for _, kv := range current.Kvs {
		resolveModRev(kv)
	}

	for _, op := range req.Failure {
		if r := op.GetRequest(); r != nil {
			returnVal.Responses = append(returnVal.Responses, &etcdserverpb.ResponseOp{
				Response: &etcdserverpb.ResponseOp_ResponseRange{
					ResponseRange: &etcdserverpb.RangeResponse{
						Header: &etcdserverpb.ResponseHeader{},
						Kvs:    current.Kvs,
					},
				},
			})
		}
	}

	return modMetaRev, returnVal
}

func findMetaEvent(events []*clientv3.Event) (int64, bool) {
	for _, event := range events {
		if string(event.Kv.Key) == metaKey {
			meta := int64(binary.LittleEndian.Uint64(event.Kv.Value))
			event.Kv.ModRevision = meta
			return meta, true
		}
	}
	return 0, false
}

func validateTxComparisons(ops []*etcdserverpb.Compare) ([]byte, error) {
	var key []byte
	for _, op := range ops {
		if key == nil {
			key = op.Key
		} else if !bytes.Equal(key, op.Key) {
			return nil, errMultipleKeysInTx
		}
		if r := op.GetCreateRevision(); r != 0 {
			return nil, errCreateRevCompare
		}
	}
	return key, nil
}

func validateTxOps(key []byte, ops []*etcdserverpb.RequestOp) ([]byte, error) {
	for _, op := range ops {
		if put := op.GetRequestPut(); put != nil {
			if key == nil {
				key = put.Key
			}
			if !bytes.Equal(key, put.Key) {
				return key, errMultipleKeysInTx
			}
			if put.PrevKv {
				return key, errPrevKv
			}
			continue
		}

		if delete := op.GetRequestDeleteRange(); delete != nil {
			if key == nil {
				key = delete.Key
			}
			if len(delete.RangeEnd) > 0 {
				return key, errMultipleKeysInTx
			}
			if !bytes.Equal(key, delete.Key) {
				return key, errMultipleKeysInTx
			}
			continue
		}

		if rangeOp := op.GetRequestRange(); rangeOp != nil {
			if key == nil {
				key = rangeOp.Key
			}
			if len(rangeOp.RangeEnd) > 0 {
				return key, errMultipleKeysInTx
			}
			if !bytes.Equal(key, rangeOp.Key) {
				return key, errMultipleKeysInTx
			}
			continue
		}
	}
	return key, nil
}

func resolveModRev(kv *mvccpb.KeyValue) {
	if kv == nil {
		return
	}
	if len(kv.Value) < 9 {
		kv.ModRevision = 0
		kv.CreateRevision = 0
		return
	}
	buf := kv.Value[len(kv.Value)-8 : len(kv.Value)]
	kv.ModRevision = int64(binary.LittleEndian.Uint64(buf))
	kv.CreateRevision = 0
	kv.Value = kv.Value[:len(kv.Value)-8]
}

func resolveMetaRev(kv *mvccpb.KeyValue) int64 {
	if len(kv.Value) < 8 {
		return kv.Version
	}
	offset := int64(binary.LittleEndian.Uint64(kv.Value))
	return offset + kv.Version
}

func appendMetaRevToTxOps(metaRevBytes []byte, ops []*etcdserverpb.RequestOp) {
	for _, op := range ops {
		if put := op.GetRequestPut(); put != nil {
			put.Value = append(put.Value, 0, 0, 0, 0, 0, 0, 0, 0)
			copy(put.Value[len(put.Value)-8:], metaRevBytes)
			continue
		}
	}
}

func getMetaRevFromValue(val []byte) int64 {
	if len(val) < 8 {
		return 0
	}
	return int64(binary.LittleEndian.Uint64(val[len(val)-8:]))
}
