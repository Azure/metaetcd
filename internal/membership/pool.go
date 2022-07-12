package membership

import (
	"fmt"
	"hash/fnv"
	"io"
	"sync"

	"github.com/Azure/metaetcd/internal/watch"
)

const partitionCount = 16

type PartitionID int8

type ClientID int64

type Pool struct {
	WatchMux *watch.Mux

	mut                  sync.RWMutex
	clients              []*ClientSet
	clientsByClientID    map[ClientID]*ClientSet
	clientsByPartitionID map[PartitionID]*ClientSet
	scc                  *SharedClientContext
}

func NewPool(scc *SharedClientContext, WatchMux *watch.Mux) *Pool {
	return &Pool{
		clientsByClientID:    make(map[ClientID]*ClientSet),
		clientsByPartitionID: make(map[PartitionID]*ClientSet),
		scc:                  scc,
		WatchMux:             WatchMux,
	}
}

func (p *Pool) AddMember(id ClientID, endpointURL string, partitions []PartitionID) error {
	clientset, err := NewClientSet(p.scc, endpointURL)
	if err != nil {
		return fmt.Errorf("constructing clientset: %w", err)
	}

	clientset.WatchStatus, err = p.WatchMux.StartWatch(clientset.ClientV3)
	if err != nil {
		return fmt.Errorf("starting watch connection: %w", err)
	}

	p.mut.Lock()
	defer p.mut.Unlock()

	p.clients = append(p.clients, clientset)
	p.clientsByClientID[id] = clientset
	for _, pid := range partitions {
		p.clientsByPartitionID[pid] = clientset
	}

	return nil
}

func (p *Pool) IterateMembers(fn func(*ClientSet) (bool, error)) error {
	p.mut.RLock()
	defer p.mut.RUnlock()
	for _, cs := range p.clients {
		if ok, err := fn(cs); !ok || err != nil {
			return err
		}
	}
	return nil
}

func (p *Pool) GetMemberForKey(key string) *ClientSet {
	// TODO: Return nil if no clients (only matters once clients can be registered at runtime)

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

	client, ok := p.clientsByPartitionID[PartitionID(b)]
	if !ok {
		panic(fmt.Sprintf("client not found for partition ID %d", b)) // unlikely
	}
	return client
}

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
