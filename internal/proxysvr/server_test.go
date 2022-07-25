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

func TestCRUD(t *testing.T) {
	var creationRev, updateRev, deleteRev int64
	const key = "key"
	client, _ := startServer(t)

	t.Run("create", func(t *testing.T) {
		resp, err := client.Txn(ctx).Then(clientv3.OpPut(key, "value-1")).Commit()
		require.NoError(t, err)
		creationRev = resp.Header.Revision
	})

	t.Run("get creation", func(t *testing.T) {
		getResp, err := client.Get(ctx, key)
		require.NoError(t, err)
		require.Len(t, getResp.Kvs, 1)
		assert.Equal(t, key, string(getResp.Kvs[0].Key))
		assert.Equal(t, "value-1", string(getResp.Kvs[0].Value))
		assert.Equal(t, creationRev, getResp.Kvs[0].ModRevision)
		assert.Equal(t, creationRev, getResp.Header.Revision)
	})

	t.Run("update", func(t *testing.T) {
		resp, err := client.Txn(ctx).Then(clientv3.OpPut(key, "value-2")).Commit()
		require.NoError(t, err)
		assert.Equal(t, creationRev+1, resp.Header.Revision)
		updateRev = resp.Header.Revision
	})

	t.Run("get update", func(t *testing.T) {
		getResp, err := client.Get(ctx, key)
		require.NoError(t, err)
		require.Len(t, getResp.Kvs, 1)
		assert.Equal(t, key, string(getResp.Kvs[0].Key))
		assert.Equal(t, "value-2", string(getResp.Kvs[0].Value))
		assert.Equal(t, updateRev, getResp.Kvs[0].ModRevision)
		assert.Equal(t, updateRev, getResp.Header.Revision)
	})

	t.Run("delete", func(t *testing.T) {
		resp, err := client.Txn(ctx).Then(clientv3.OpDelete(key)).Commit()
		require.NoError(t, err)
		assert.Equal(t, updateRev+1, resp.Header.Revision)
		deleteRev = resp.Header.Revision
	})

	t.Run("get deletion", func(t *testing.T) {
		getResp, err := client.Get(ctx, key)
		require.NoError(t, err)
		require.Len(t, getResp.Kvs, 0)
		assert.Equal(t, deleteRev, getResp.Header.Revision)
	})
}

func TestWatchHappyPath(t *testing.T) {
	client, _ := startServer(t)

	// Create some events before starting the watch
	n := 10
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-before-%d", i)
		_, err := client.Txn(ctx).Then(clientv3.OpPut(key, "")).Commit()
		require.NoError(t, err)

		if i == n {
			_, err := client.Txn(ctx).Then(clientv3.OpDelete(fmt.Sprintf("key-before-%d", i-1))).Commit()
			require.NoError(t, err)
		}
	}

	// Start the watch
	watchCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	watch := client.Watch(watchCtx, "key-", clientv3.WithRange(clientv3.GetPrefixRangeEnd("key-")), clientv3.WithPrevKV())

	// Create some events now that the watch is running
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-after-%d", i)
		_, err := client.Txn(ctx).Then(clientv3.OpPut(key, "")).Commit()
		require.NoError(t, err)

		if i == n {
			_, err := client.Txn(ctx).Then(clientv3.OpDelete(fmt.Sprintf("key-after-%d", i-1))).Commit()
			require.NoError(t, err)
		}
	}

	// Prove all events are eventually receieved
	events := testutil.CollectEvents(t, watch, 20)
	assert.Equal(t, testutil.NewSeq(2, 22), testutil.GetRevisions(events))
}

func TestWatchFromRev(t *testing.T) {
	client, _ := startServer(t)

	// Create some events
	n := 10
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%d", i)
		_, err := client.Txn(ctx).Then(clientv3.OpPut(key, "")).Commit()
		require.NoError(t, err)
	}

	watchCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	watch := client.Watch(watchCtx, "key-", clientv3.WithRange(clientv3.GetPrefixRangeEnd("key-")), clientv3.WithPrevKV(), clientv3.WithRev(5))

	// Prove all events are eventually receieved
	events := testutil.CollectEvents(t, watch, 5)
	assert.Equal(t, testutil.NewSeq(2, 7), testutil.GetRevisions(events))
}

func TestWatchCompacted(t *testing.T) {
	client, _ := startServer(t)

	watchCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	watch := client.Watch(watchCtx, "key-", clientv3.WithRange(clientv3.GetPrefixRangeEnd("key-")), clientv3.WithPrevKV(), clientv3.WithRev(-2))
	event := <-watch
	assert.EqualError(t, event.Err(), "etcdserver: mvcc: required revision has been compacted")
}

func TestTxModRevisionComparisonHappyPath(t *testing.T) {
	const key = "key"
	client, _ := startServer(t)

	// Create
	createResp, err := client.Txn(ctx).Then(clientv3.OpPut(key, "value-1")).Commit()
	require.NoError(t, err)

	// Update
	txnResp, err := client.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", createResp.Header.Revision)).
		Then(clientv3.OpPut(key, "value-2")).Commit()
	require.NoError(t, err)
	assert.Equal(t, txnResp.Header.Revision, txnResp.Responses[0].GetResponsePut().Header.Revision)
	assert.NotEqual(t, createResp.Header.Revision, txnResp.Header.Revision)
}

func TestTxModRevisionComparisonIncorrectRev(t *testing.T) {
	const key = "key"
	client, _ := startServer(t)

	// Create
	createResp, err := client.Txn(ctx).Then(clientv3.OpPut(key, "value-1")).Commit()
	require.NoError(t, err)

	// Update
	txnResp, err := client.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", createResp.Header.Revision-1)).
		Then(clientv3.OpPut(key, "value-2")).Commit()
	require.NoError(t, err)
	assert.False(t, txnResp.Succeeded)
	assert.Zero(t, txnResp.Header.Revision)
}

func TestTxModRevisionComparisonIncorrectRevWithGet(t *testing.T) {
	const key = "key"
	client, _ := startServer(t)

	// Create
	createResp, err := client.Txn(ctx).Then(clientv3.OpPut(key, "value-1")).Commit()
	require.NoError(t, err)

	// Update
	txnResp, err := client.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", createResp.Header.Revision-1)).
		Then(clientv3.OpPut(key, "value-2")).
		Else(clientv3.OpGet(key)).Commit()
	require.NoError(t, err)
	assert.False(t, txnResp.Succeeded)
	assert.Zero(t, txnResp.Header.Revision)
	require.NotNil(t, txnResp.Responses[0].GetResponseRange())
	require.Equal(t, "value-1", string(txnResp.Responses[0].GetResponseRange().Kvs[0].Value))
}

func TestCompaction(t *testing.T) {
	const key = "key"
	client, _ := startServer(t)

	// Create and update a key
	createResp, err := client.Txn(ctx).Then(clientv3.OpPut(key, "value-1")).Commit()
	require.NoError(t, err)

	updateResp, err := client.Txn(ctx).Then(clientv3.OpPut(key, "value-2")).Commit()
	require.NoError(t, err)

	// Compact
	_, err = client.Compact(ctx, updateResp.Header.Revision)
	require.NoError(t, err)

	// Try to get older rev
	_, err = client.Get(ctx, key, clientv3.WithRev(createResp.Header.Revision))
	require.EqualError(t, err, "etcdserver: mvcc: required revision has been compacted")
}

func TestRange(t *testing.T) {
	client, _ := startServer(t)

	n := 10
	var creationRev int64
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%d", i+1)
		resp, err := client.Txn(ctx).Then(clientv3.OpPut(key, "")).Commit()
		require.NoError(t, err)
		creationRev = resp.Header.Revision
	}

	t.Run("all", func(t *testing.T) {
		resp, err := client.Get(ctx, "key-", clientv3.WithRange(clientv3.GetPrefixRangeEnd("key-")))
		require.NoError(t, err)
		assert.False(t, resp.More)
		assert.Equal(t, int64(n), resp.Count)
		assert.Equal(t, creationRev, resp.Header.Revision)
		assert.Equal(t, []string{"key-1", "key-10", "key-2", "key-3", "key-4", "key-5", "key-6", "key-7", "key-8", "key-9"}, testutil.GetKeys(testutil.NewItems(resp.Kvs)))
	})

	t.Run("at previous rev", func(t *testing.T) {
		resp, err := client.Get(ctx, "key-", clientv3.WithRange(clientv3.GetPrefixRangeEnd("key-")), clientv3.WithRev(creationRev-3))
		require.NoError(t, err)
		assert.False(t, resp.More)
		assert.Equal(t, int64(n-3), resp.Count)
		assert.Equal(t, creationRev-3, resp.Header.Revision)
		assert.Equal(t, []string{"key-1", "key-2", "key-3", "key-4", "key-5", "key-6", "key-7"}, testutil.GetKeys(testutil.NewItems(resp.Kvs)))
	})

	t.Run("paging over keys", func(t *testing.T) {
		var kvs [][]*mvccpb.KeyValue
		var counts []int64
		var mores []bool
		startKey := "key-"
		for i := 0; i < 5; i++ {
			resp, err := client.Get(ctx, startKey, clientv3.WithRange(clientv3.GetPrefixRangeEnd("key-")), clientv3.WithLimit(3))
			require.NoError(t, err)

			kvs = append(kvs, resp.Kvs)
			counts = append(counts, resp.Count)
			mores = append(mores, resp.More)
			if len(resp.Kvs) > 0 {
				startKey = string(resp.Kvs[len(resp.Kvs)-1].Key) + "\x00"
			}
		}
		assert.Equal(t, []string{"key-1", "key-10", "key-2"}, testutil.GetKeys(testutil.NewItems(kvs[0])))
		assert.Equal(t, []string{"key-3", "key-4", "key-5"}, testutil.GetKeys(testutil.NewItems(kvs[1])))
		assert.Equal(t, []string{"key-6", "key-7", "key-8"}, testutil.GetKeys(testutil.NewItems(kvs[2])))
		assert.Equal(t, []string{"key-9"}, testutil.GetKeys(testutil.NewItems(kvs[3])))
		assert.Len(t, kvs[4], 0)
		assert.Equal(t, []int64{10, 7, 4, 1, 0}, counts)
		assert.Equal(t, []bool{true, true, true, false, false}, mores)
	})
}

func TestReconstituteClockOnRead(t *testing.T) {
	key := "key"
	client, s := startServer(t)

	// Increment the clock
	createResp, err := client.Txn(ctx).Then(clientv3.OpPut(key, "")).Commit()
	require.NoError(t, err)

	// Lose the current time
	require.NoError(t, s.clock.Reset(ctx))

	// Get the value
	resp, err := client.Get(ctx, key)
	require.NoError(t, err)
	require.Len(t, resp.Kvs, 1)
	assert.Equal(t, createResp.Header.Revision, resp.Kvs[0].ModRevision)
}

func TestReconstituteClockOnWrite(t *testing.T) {
	client, s := startServer(t)

	// Increment the clock
	createResp, err := client.Txn(ctx).Then(clientv3.OpPut("key-1", "")).Commit()
	require.NoError(t, err)

	// Lose the current time
	require.NoError(t, s.clock.Reset(ctx))

	// Increment the clock again
	secondCreateResp, err := client.Txn(ctx).Then(clientv3.OpPut("key-2", "")).Commit()
	require.NoError(t, err)
	assert.Equal(t, createResp.Header.Revision+1, secondCreateResp.Header.Revision)
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
		err = members.AddMember(ctx, membership.MemberID(i), memberURL, partitions[i])
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
