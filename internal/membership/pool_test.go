package membership

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewStaticPartitions(t *testing.T) {
	partitions := NewStaticPartitions(3)
	assert.Equal(t, [][]PartitionID{
		{0, 3, 6, 9, 12, 15},
		{1, 4, 7, 10, 13},
		{2, 5, 8, 11, 14},
	}, partitions)
}
