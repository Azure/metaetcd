package scheme

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/etcdserver/etcdserverpb"
	"github.com/coreos/etcd/mvcc/mvccpb"
)

var (
	errMultipleKeysInTx = errors.New("transactions can only involve a single key")
	errCreateRevCompare = errors.New("create revision comparisons are not supported")
	errPrevKv           = errors.New("previous kv is not supported in transactions")
)

const MetaKey = "/meta"

func GetMetaRevFromValue(val []byte) int64 {
	if len(val) < 8 {
		return 0
	}
	return int64(binary.LittleEndian.Uint64(val[len(val)-8:]))
}

func GetMetaRevFromMetaKey(val []byte) int64 {
	return int64(binary.LittleEndian.Uint64(val))
}

func AppendMetaRevToTxOps(metaRevBytes []byte, ops []*etcdserverpb.RequestOp) {
	for _, op := range ops {
		if put := op.GetRequestPut(); put != nil {
			put.Value = append(put.Value, 0, 0, 0, 0, 0, 0, 0, 0)
			copy(put.Value[len(put.Value)-8:], metaRevBytes)
			continue
		}
	}
}

func ResolveMetaRev(kv *mvccpb.KeyValue) int64 {
	if len(kv.Value) < 8 {
		return kv.Version
	}
	offset := int64(binary.LittleEndian.Uint64(kv.Value))
	return offset + kv.Version
}

func ResolveModRev(kv *mvccpb.KeyValue) {
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

func ValidateTxComparisons(ops []*etcdserverpb.Compare) ([]byte, error) {
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

func ValidateTxOps(key []byte, ops []*etcdserverpb.RequestOp) ([]byte, error) {
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

func PreflightTxn(metaRev int64, req *etcdserverpb.TxnRequest, current *clientv3.GetResponse) (int64, *etcdserverpb.TxnResponse) {
	modMetaRev := GetMetaRevFromValue(current.Kvs[0].Value)
	if modMetaRev == metaRev {
		return current.Kvs[0].ModRevision, nil
	}

	returnVal := &etcdserverpb.TxnResponse{Header: &etcdserverpb.ResponseHeader{}}
	for _, kv := range current.Kvs {
		ResolveModRev(kv)
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

func FindMetaEvent(events []*clientv3.Event) (int64, bool) {
	for _, event := range events {
		if string(event.Kv.Key) == MetaKey {
			meta := GetMetaRevFromMetaKey(event.Kv.Value)
			event.Kv.ModRevision = meta
			return meta, true
		}
	}
	return 0, false
}

func TransformEvents(meta int64, events []*clientv3.Event) {
	for _, event := range events {
		if event.PrevKv != nil && len(event.PrevKv.Value) >= 8 {
			event.PrevKv.ModRevision = GetMetaRevFromValue(event.PrevKv.Value)
			event.PrevKv.Value = event.PrevKv.Value[:len(event.PrevKv.Value)-8]
		}
		if event.Type == clientv3.EventTypeDelete {
			event.Kv.ModRevision = meta
			continue
		}

		isCreate := event.Kv.CreateRevision == event.Kv.ModRevision
		event.Kv.ModRevision = GetMetaRevFromValue(event.Kv.Value)
		if isCreate {
			event.Kv.CreateRevision = event.Kv.ModRevision
		}
		event.Kv.Value = event.Kv.Value[:len(event.Kv.Value)-8]
	}
}
