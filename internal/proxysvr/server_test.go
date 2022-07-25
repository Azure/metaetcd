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

	"github.com/Azure/metaetcd/internal/clock"
	"github.com/Azure/metaetcd/internal/membership"
	"github.com/Azure/metaetcd/internal/testutil"
	"github.com/Azure/metaetcd/internal/watch"
)

var ctx = context.Background()

func TestIntegrationBulk(t *testing.T) {
	n := 100
	var lastSeenMetaRev int64
	client, s := startServer(t)

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
		testutil.CollectEvents(t, watch, 100)
	})

	t.Run("watch from rev", func(t *testing.T) {
		watch := client.Watch(watchCtx, "key-", clientv3.WithRange(clientv3.GetPrefixRangeEnd("key-")), clientv3.WithPrevKV(), clientv3.WithRev(lastSeenMetaRev-15))
		testutil.CollectEvents(t, watch, 16)
	})

	t.Run("watch from too old of rev", func(t *testing.T) {
		watch := client.Watch(watchCtx, "key-", clientv3.WithRange(clientv3.GetPrefixRangeEnd("key-")), clientv3.WithPrevKV(), clientv3.WithRev(-2))
		event := <-watch
		assert.EqualError(t, event.Err(), "etcdserver: mvcc: required revision has been compacted")
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
		assert.Equal(t, int64(n), resp.Count)
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

			// TODO: This smells sus
			if !assert.NotEmpty(t, resp.Kvs) {
				startKey = string(resp.Kvs[len(resp.Kvs)-1].Key) + "\x00"
			}
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
		require.NoError(t, s.clock.Reset(ctx))
	})
	rangeAll()

	// Delete clock state again to test reconstitution on writes (next step in test)
	t.Run("delete clock state again", func(t *testing.T) {
		require.NoError(t, s.clock.Reset(ctx))
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
		assert.Equal(t, txnResp.Header.Revision, txnResp.Responses[0].GetResponsePut().Header.Revision)
		lastSeenMetaRev = txnResp.Header.Revision
		creationRevs["key-1"] = txnResp.Header.Revision

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

		resp, err := client.Get(ctx, "key-1")
		require.NoError(t, err)
		assert.Equal(t, "new-value-2", string(resp.Kvs[0].Value))
	})

	t.Run("update single key using incorrect mod rev constraint with get", func(t *testing.T) {
		txnResp, err := client.Txn(ctx).
			If(clientv3.Compare(clientv3.ModRevision("key-1"), "=", lastSeenMetaRev-10)).
			Then(clientv3.OpPut("key-1", "new-value-3")).
			Else(clientv3.OpGet("key-1")).Commit()
		require.NoError(t, err)
		assert.False(t, txnResp.Succeeded)
		assert.Equal(t, int64(0), txnResp.Header.Revision)
		assert.Equal(t, creationRevs["key-1"], txnResp.Responses[0].GetResponseRange().Kvs[0].ModRevision)

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

func startServer(t testing.TB) (*clientv3.Client, *server) {
	coordinatoorURL := testutil.StartEtcd(t)
	member1URL := testutil.StartEtcd(t)
	member2URL := testutil.StartEtcd(t)

	lis, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	t.Cleanup(func() {
		lis.Close()
	})

	svr := newServer(t, coordinatoorURL, []string{member1URL, member2URL}, time.Second*5)
	grpcServer := grpc.NewServer()
	etcdserverpb.RegisterKVServer(grpcServer, svr)
	etcdserverpb.RegisterWatchServer(grpcServer, svr)
	go grpcServer.Serve(lis)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"http://" + lis.Addr().String()},
		DialTimeout: 2 * time.Second,
	})
	require.NoError(t, err)
	return client, svr.(*server)
}

func newServer(t testing.TB, coordinatorURL string, memberURLs []string, watchTimeout time.Duration) Server {
	gc := &membership.GrpcContext{}
	coordinator, err := membership.InitCoordinator(gc, coordinatorURL)
	require.NoError(t, err)

	clk := &clock.Clock{Coordinator: coordinator}
	watchMux := watch.NewMux(time.Second, 200, clk)
	members := membership.NewPool(gc, watchMux)
	clk.Members = members

	partitions := membership.NewStaticPartitions(len(memberURLs))
	for i, memberURL := range memberURLs {
		err = members.AddMember(membership.MemberID(i), memberURL, partitions[i])
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

	require.NoError(t, clk.Init())
	return NewServer(coordinator, members, clk)
}
