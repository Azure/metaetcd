package scheme

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/coreos/etcd/etcdserver/etcdserverpb"
	"github.com/coreos/etcd/mvcc/mvccpb"
)

var (
	errMultipleKeys = errors.New("transactions can only involve a single key")
	errCreateRev    = errors.New("create revisions are not supported")
	errPrevKv       = errors.New("previous kv is not supported in transactions")
)

const MetaKey = "/meta"

func MetaRevFromValue(val []byte) int64 {
	if len(val) < 8 {
		return 0
	}
	return int64(binary.LittleEndian.Uint64(val[len(val)-8:]))
}

func MetaRevFromMetaKey(val []byte) int64 {
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

func ValidateTxComparisons(ops []*etcdserverpb.Compare) ([]byte, error) {
	var key []byte
	for _, op := range ops {
		if key == nil {
			key = op.Key
		} else if !bytes.Equal(key, op.Key) {
			return nil, errMultipleKeys
		}
		if r := op.GetCreateRevision(); r != 0 {
			return nil, errCreateRev
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
				return key, errMultipleKeys
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
				return key, errMultipleKeys
			}
			if !bytes.Equal(key, delete.Key) {
				return key, errMultipleKeys
			}
			continue
		}

		if rangeOp := op.GetRequestRange(); rangeOp != nil {
			if key == nil {
				key = rangeOp.Key
			}
			if len(rangeOp.RangeEnd) > 0 {
				return key, errMultipleKeys
			}
			if !bytes.Equal(key, rangeOp.Key) {
				return key, errMultipleKeys
			}
			continue
		}
	}
	return key, nil
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
