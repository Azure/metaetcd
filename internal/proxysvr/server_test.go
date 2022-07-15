package proxysvr

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/etcdserver/etcdserverpb"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/Azure/metaetcd/internal/membership"
	"github.com/Azure/metaetcd/internal/scheme"
	"github.com/Azure/metaetcd/internal/testutil"
	"github.com/Azure/metaetcd/internal/watch"
)

var ctx = context.Background()

func TestIntegrationBulk(t *testing.T) {
	n := 100
	var lastSeenMetaRev int64
	client, coord := startServer(t)

	// Start watches to observe the test operations
	watchCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	watch := client.Watch(watchCtx, "key-", clientv3.WithRange(clientv3.GetPrefixRangeEnd("key-")), clientv3.WithPrevKV())

	creationRevs := map[string]int64{}
	t.Run("create", func(t *testing.T) {
		for i := 0; i < n+1; i++ {
			key := fmt.Sprintf("key-%d", i)
			value := fmt.Sprintf("value-%d", i)

			resp, err := client.Txn(ctx).Then(clientv3.OpPut(key, value)).Commit()
			require.NoError(t, err)
			lastSeenMetaRev = resp.Header.Revision
			creationRevs[key] = lastSeenMetaRev // used to assert on mod rev later
			assert.Equal(t, int64(i+2), lastSeenMetaRev, "n is zero-indexed, meta rev initializes to one, first write is two")

			// Delete the last value
			if i == n {
				resp, err := client.Txn(ctx).Then(clientv3.OpDelete(fmt.Sprintf("key-%d", i-1))).Commit()
				require.NoError(t, err)
				lastSeenMetaRev = resp.Header.Revision
			}
		}
	})

	t.Run("watch", func(t *testing.T) {
		collectEvents(t, watch, 100)
	})

	t.Run("watch from rev", func(t *testing.T) {
		watch := client.Watch(watchCtx, "key-", clientv3.WithRange(clientv3.GetPrefixRangeEnd("key-")), clientv3.WithPrevKV(), clientv3.WithRev(lastSeenMetaRev-15))
		collectEvents(t, watch, 16)
	})
	cancel()

	t.Run("get", func(t *testing.T) {
		for i := 0; i < n-1; i++ {
			key := fmt.Sprintf("key-%d", i)
			value := fmt.Sprintf("value-%d", i)

			getResp, err := client.Get(ctx, key)
			require.NoError(t, err)
			require.Len(t, getResp.Kvs, 1, "a single KV response is expected for key %s", key)
			assert.Equal(t, key, string(getResp.Kvs[0].Key), "key matches")
			assert.Equal(t, value, string(getResp.Kvs[0].Value), "value matches")
			assert.Equal(t, creationRevs[key], getResp.Kvs[0].ModRevision, "mod rev matches meta cluster rev at the time the key was last written")
			assert.Equal(t, lastSeenMetaRev, getResp.Header.Revision, "meta cluster rev at time of read matches the rev of the last observed write")
		}
	})

	rangeAll := func() {
		t.Run("range", func(t *testing.T) {
			resp, err := client.Get(ctx, "key-", clientv3.WithRange(clientv3.GetPrefixRangeEnd("key-")))
			require.NoError(t, err)
			require.Len(t, resp.Kvs, n)
			assert.Equal(t, int64(n), resp.Count)
			assert.Equal(t, lastSeenMetaRev, resp.Header.Revision, "meta cluster rev at time of read matches the rev of the last observed write")

			for i := 0; i < n; i++ {
				assert.Equal(t, creationRevs[string(resp.Kvs[i].Key)], resp.Kvs[i].ModRevision, "mod rev matches meta cluster rev at the time the key was last written")
			}
		})
	}
	rangeAll()

	t.Run("range with limit", func(t *testing.T) {
		resp, err := client.Get(ctx, "key-", clientv3.WithRange(clientv3.GetPrefixRangeEnd("key-")), clientv3.WithLimit(11))
		require.NoError(t, err)
		assert.Equal(t, lastSeenMetaRev, resp.Header.Revision, "meta cluster rev at time of read matches the rev of the last observed write")
		assert.Len(t, resp.Kvs, 11)
		assert.Equal(t, int64(11), resp.Count)
	})

	t.Run("range page over the keyspace", func(t *testing.T) {
		var keys []*mvccpb.KeyValue
		startKey := "key-"
		for {
			resp, err := client.Get(ctx, startKey, clientv3.WithRange(clientv3.GetPrefixRangeEnd("key-")), clientv3.WithLimit(9))
			require.NoError(t, err)
			// TODO: Assert on page length (I think it's incorrect currently)

			keys = append(keys, resp.Kvs...)
			if len(keys) >= n {
				break
			}

			require.NotEmpty(t, resp.Kvs)
			startKey = string(resp.Kvs[len(resp.Kvs)-1].Key) + "\x00"
		}

		for i := 0; i < n; i++ {
			assert.Equal(t, creationRevs[string(keys[i].Key)], keys[i].ModRevision)
		}
	})

	t.Run("range with count", func(t *testing.T) {
		resp, err := client.Get(ctx, "key-", clientv3.WithRange(clientv3.GetPrefixRangeEnd("key-")), clientv3.WithCountOnly())
		require.NoError(t, err)
		assert.Equal(t, lastSeenMetaRev, resp.Header.Revision, "meta cluster rev at time of read matches the rev of the last observed write")
		assert.Len(t, resp.Kvs, 0)
		assert.Equal(t, int64(n), resp.Count)
	})

	t.Run("range with revision", func(t *testing.T) {
		resp, err := client.Get(ctx, "key-", clientv3.WithRange(clientv3.GetPrefixRangeEnd("key-")), clientv3.WithRev(20))
		require.NoError(t, err)
		require.Len(t, resp.Kvs, 19)
		assert.Equal(t, int64(19), resp.Count)
	})

	// Delete the clock state from the coordinator before sending a range to test reconstitution on reads
	t.Run("delete clock state", func(t *testing.T) {
		_, err := coord.KV.Delete(ctx, scheme.MetaKey)
		require.NoError(t, err)
	})
	rangeAll()

	// Delete clock state again to test reconstitution on writes (next step in test)
	t.Run("delete clock state again", func(t *testing.T) {
		_, err := coord.KV.Delete(ctx, scheme.MetaKey)
		require.NoError(t, err)
	})

	// Update a single key and prove the mod rev reflects the meta cluster rev
	t.Run("update single key", func(t *testing.T) {
		txnResp, err := client.Txn(ctx).Then(clientv3.OpPut("key-1", "new-value")).Commit()
		require.NoError(t, err)
		lastSeenMetaRev = txnResp.Header.Revision

		resp, err := client.Get(ctx, "key-1")
		require.NoError(t, err)
		assert.Equal(t, txnResp.Header.Revision, resp.Kvs[0].ModRevision, "mod revision matches the number of writes we've made (init, n creates, one update)")
	})

	t.Run("update single key using mod rev constraint", func(t *testing.T) {
		resp, err := client.Get(ctx, "key-1")
		require.NoError(t, err)

		txnResp, err := client.Txn(ctx).
			If(clientv3.Compare(clientv3.ModRevision("key-1"), "=", resp.Kvs[0].ModRevision)).
			Then(clientv3.OpPut("key-1", "new-value-2")).Commit()
		require.NoError(t, err)
		lastSeenMetaRev = txnResp.Header.Revision

		resp, err = client.Get(ctx, "key-1")
		require.NoError(t, err)
		assert.Equal(t, "new-value-2", string(resp.Kvs[0].Value))
		assert.Equal(t, txnResp.Header.Revision, resp.Kvs[0].ModRevision, "mod revision matches the number of writes we've made (init, n creates, one update)")
	})

	t.Run("update single key using incorrect mod rev constraint", func(t *testing.T) {
		txnResp, err := client.Txn(ctx).
			If(clientv3.Compare(clientv3.ModRevision("key-1"), "=", lastSeenMetaRev-10)).
			Then(clientv3.OpPut("key-1", "new-value-3")).Commit()
		require.NoError(t, err)
		assert.False(t, txnResp.Succeeded)
		lastSeenMetaRev = txnResp.Header.Revision

		resp, err := client.Get(ctx, "key-1")
		require.NoError(t, err)
		assert.Equal(t, "new-value-2", string(resp.Kvs[0].Value))
	})

	t.Run("compaction", func(t *testing.T) {
		_, err := client.Compact(ctx, lastSeenMetaRev-1)
		require.NoError(t, err)

		resp, err := client.Get(ctx, "key-", clientv3.WithRange(clientv3.GetPrefixRangeEnd("key-")))
		require.NoError(t, err)
		require.Len(t, resp.Kvs, n)
	})
}

func startServer(t testing.TB) (proxy, coord *clientv3.Client) {
	coordinatoorURL := testutil.StartEtcd(t, "COORDINATOR")
	member1URL := testutil.StartEtcd(t, "MEMBER-1")
	member2URL := testutil.StartEtcd(t, "MEMBER-2")

	lis, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	t.Cleanup(func() {
		lis.Close()
	})

	svr := newServer(t, coordinatoorURL, []string{member1URL, member2URL}, &membership.SharedClientContext{}, time.Second*5)
	grpcServer := grpc.NewServer()
	etcdserverpb.RegisterKVServer(grpcServer, svr)
	etcdserverpb.RegisterWatchServer(grpcServer, svr)
	go grpcServer.Serve(lis)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"http://" + lis.Addr().String()},
		DialTimeout: 2 * time.Second,
	})
	require.NoError(t, err)
	return client, svr.(*server).coordinator.ClientV3
}

func newServer(t testing.TB, coordinatorURL string, memberURLs []string, scc *membership.SharedClientContext, watchTimeout time.Duration) Server {
	coordinator, err := membership.InitCoordinator(scc, coordinatorURL)
	require.NoError(t, err)

	watchMux := watch.NewMux(time.Second, 200)
	members := membership.NewPool(scc, watchMux)

	partitions := membership.NewStaticPartitions(len(memberURLs))
	for i, memberURL := range memberURLs {
		err = members.AddMember(membership.ClientID(i), memberURL, partitions[i])
		require.NoError(t, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		watchMux.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		t.Log("waiting for graceful shutdown...")
		<-done
		t.Log("gracefully shut down")
	})
	return NewServer(coordinator, members)
}

func collectEvents(t *testing.T, watch clientv3.WatchChan, n int) []*clientv3.Event {
	i := 0
	slice := make([]*clientv3.Event, n)
	for msg := range watch {
		for _, event := range msg.Events {
			t.Logf("got event %d (rev %d) from watch", i, event.Kv.ModRevision)
			slice[i] = event
			i++
			if i >= n {
				return slice
			}
		}
	}
	return nil
}
