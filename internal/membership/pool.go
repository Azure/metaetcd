package membership

import (
	"context"
	"fmt"
	"hash/fnv"
	"io"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/Azure/metaetcd/internal/watch"
)

// partitionCount is the number of partitions in meta cluster's keyspace.
// It's hardcoded to avoid mismatched values across instances of this process.
const partitionCount = 16

// PartitionID references one of the partitions implied by partitionCount.
type PartitionID int8

// MemberID is a monotonic ID. As members are added, this can only go up.
type MemberID int64

// Pool is a dynamic set of etcd clients, responsible for their entire lifecycle.
type Pool struct {
	WatchMux    *watch.Mux
	grpcContext *GrpcContext

	mut           sync.RWMutex
	clients       []*ClientSet
	byMemberID    map[MemberID]*ClientSet
	byPartitionID map[PartitionID]*ClientSet
}

func NewPool(gc *GrpcContext, wm *watch.Mux) *Pool {
	return &Pool{
		WatchMux:      wm,
		grpcContext:   gc,
		byMemberID:    make(map[MemberID]*ClientSet),
		byPartitionID: make(map[PartitionID]*ClientSet),
	}
}

func (p *Pool) AddMember(ctx context.Context, id MemberID, endpointURL string, partitions []PartitionID) error {
	clientset, err := NewClientSet(p.grpcContext, endpointURL)
	if err != nil {
		return fmt.Errorf("constructing clientset: %w", err)
	}

	clientset.WatchStatus, err = p.WatchMux.StartWatch(ctx, clientset.ClientV3)
	if err != nil {
		return fmt.Errorf("starting watch connection: %w", err)
	}

	p.mut.Lock()
	defer p.mut.Unlock()

	p.clients = append(p.clients, clientset)
	p.byMemberID[id] = clientset
	for _, pid := range partitions {
		p.byPartitionID[pid] = clientset
	}

	return nil
}

func (p *Pool) IterateMembers(ctx context.Context, fn func(context.Context, *ClientSet) error) error {
	p.mut.RLock()
	defer p.mut.RUnlock()
	wg, ctx := errgroup.WithContext(ctx)
	for _, cs := range p.clients {
		cs := cs
		wg.Go(func() error { return fn(ctx, cs) })
	}
	return wg.Wait()
}

func (p *Pool) GetMemberForKey(key string) *ClientSet {
	h := fnv.New64()
	if _, err := io.WriteString(h, key); err != nil {
		panic(err) // impossible
	}
	keyInt := h.Sum64()

	// Adopted from github.com/lithammer/go-jump-consistent-hash
	var b, j int64
	for j < int64(partitionCount) {
		b = j
		keyInt = keyInt*2862933555777941757 + 1
		j = int64(float64(b+1) * (float64(int64(1)<<31) / float64((keyInt>>33)+1)))
	}

	p.mut.RLock()
	defer p.mut.RUnlock()

	if len(p.clients) == 0 {
		return nil
	}

	return p.byPartitionID[PartitionID(b)]
}

// NewStaticPartitions naively assigns partitions to a static number of members.
// Useful when not using a dynamic means of assignment (testing, static clusters, etc.)
func NewStaticPartitions(memberCount int) [][]PartitionID {
	ids := make([][]PartitionID, memberCount)
	cursor := 0
	for i := 0; i < partitionCount; i++ {
		ids[cursor] = append(ids[cursor], PartitionID(i))
		cursor++
		if cursor >= memberCount {
			cursor = 0
		}
	}
	return ids
}
