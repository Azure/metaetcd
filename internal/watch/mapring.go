package watch

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"
)

type mapRing struct {
	mut          sync.Mutex
	bcast        *broadcast
	key, value   []int64
	len, pointer int
	logger       *zap.Logger
}

func newMapRing(n int, logger *zap.Logger) *mapRing {
	return &mapRing{bcast: newBroadcast(), key: make([]int64, n), value: make([]int64, n), len: n, logger: logger}
}

func (r *mapRing) Push(key, value int64) {
	r.mut.Lock()
	defer r.mut.Unlock()
	r.key[r.pointer] = key
	r.value[r.pointer] = value
	r.pointer = (r.pointer + 1) % r.len
	r.bcast.Send()
}

func (r *mapRing) Get(key int64) (int64, bool) {
	r.mut.Lock()
	defer r.mut.Unlock()
	p := r.pointer
	for i := 0; i < r.len; i++ {
		k := (i + p) % r.len
		if r.key[k] == key {
			return r.value[k], true
		}
	}
	return 0, false
}

func (r *mapRing) WaitGet(ctx context.Context, key int64, timeout time.Duration) (int64, bool) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ch := make(chan struct{}, 2)
	defer r.bcast.Watch(ch)()

	if val, ok := r.Get(key); ok {
		return val, ok
	}

	start := time.Now()
	for {
		select {
		case <-ch:
			if val, ok := r.Get(key); ok {
				r.logger.Info("got mapring result after wait period", zap.Int64("key", key), zap.Int64("value", val), zap.Duration("latency", time.Since(start)))
				return val, ok
			}
		case <-ctx.Done():
			max := r.latestKey()
			r.logger.Info("timeout while waiting for mapring result", zap.Int64("key", key), zap.Duration("latency", time.Since(start)), zap.Int64("maxCacheKey", max))
			return -1, false
		}
	}
}

func (r *mapRing) latestKey() int64 {
	r.mut.Lock()
	defer r.mut.Unlock()
	i := (r.pointer - 1) % r.len
	if i < 0 {
		return 0
	}
	return r.key[i]
}
