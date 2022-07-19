package watch

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/etcdserver/etcdserverpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Azure/metaetcd/internal/scheme"
	"github.com/Azure/metaetcd/internal/testutil"
)

func TestMuxIntegration(t *testing.T) {
	clients := testutil.StartEtcds(t, 2)
	m := NewMux(time.Millisecond, 100)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go m.Run(ctx)

	// Start watches
	for _, client := range clients {
		m.StartWatch(client)
	}

	// Create a few events
	createEvent(t, clients[0], "test-2", 2)
	createEvent(t, clients[1], "test-3", 3)

	// Start a watch
	ch := make(chan *etcdserverpb.WatchResponse, 100)
	go func() {
		ok := m.Watch(ctx, []byte("test"), []byte("test0"), 2, ch)
		assert.True(t, ok)
	}()

	time.Sleep(time.Millisecond * 10) // TODO: Avoid

	// Create some events now that the watch is running
	createEvent(t, clients[0], "test-4", 4)
	createEvent(t, clients[1], "test-5", 5)

	// We should eventually receive every event
	for i := 0; i < 4; i++ {
		event := <-ch
		assert.Equal(t, int64(i+2), event.Events[0].Kv.ModRevision)
	}

	// Negative case for stale watches
	ok := m.Watch(ctx, []byte("test"), []byte("test0"), 1, ch)
	assert.False(t, ok)
}

func createEvent(t *testing.T, client *clientv3.Client, key string, metaRev int64) {
	buf := make([]byte, 8) // TODO: Move to scheme package
	binary.LittleEndian.PutUint64(buf, uint64(metaRev))

	_, err := client.KV.Txn(context.Background()).
		Then(clientv3.OpPut("test-1", string(buf)), clientv3.OpPut(scheme.MetaKey, string(buf))).
		Commit()
	require.NoError(t, err)
}
