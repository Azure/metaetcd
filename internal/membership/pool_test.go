package membership

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Azure/metaetcd/internal/testutil"
	"github.com/Azure/metaetcd/internal/watch"
)

func TestPoolIntegration(t *testing.T) {
	ctx := context.Background()
	gc := &GrpcContext{GrpcKeepaliveInterval: time.Second, GrpcKeepaliveTimeout: time.Second * 5}
	wm := watch.NewMux(time.Second, 100, nil)
	p := NewPool(gc, wm)

	t.Run("get member for key no members", func(t *testing.T) {
		assert.Nil(t, p.GetMemberForKey("anything"))
	})

	t.Run("add member happy path", func(t *testing.T) {
		partitions := NewStaticPartitions(2)
		p.AddMember(ctx, MemberID(0), testutil.StartEtcd(t), partitions[0])
		p.AddMember(ctx, MemberID(1), testutil.StartEtcd(t), partitions[1])
	})

	t.Run("iterate members happy path", func(t *testing.T) {
		err := p.IterateMembers(context.Background(), func(ctx context.Context, cs *ClientSet) error {
			assert.NotEmpty(t, cs.ClientV3.Endpoints())
			return nil
		})
		require.NoError(t, err)
	})

	t.Run("get member for key happy path", func(t *testing.T) {
		first := p.GetMemberForKey("anything")
		second := p.GetMemberForKey("anything1")
		assert.False(t, first == second)
	})
}

func TestNewStaticPartitions(t *testing.T) {
	partitions := NewStaticPartitions(3)
	assert.Equal(t, [][]PartitionID{
		{0, 3, 6, 9, 12, 15},
		{1, 4, 7, 10, 13},
		{2, 5, 8, 11, 14},
	}, partitions)
}
