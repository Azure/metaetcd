package testutil

import (
	"fmt"
	"net"
	"os/exec"
	"testing"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/stretchr/testify/require"
)

func StartEtcd(t testing.TB) string {
	peerPort := getAvailablePort(t)
	clientPort := getAvailablePort(t)

	cmd := exec.Command("etcd",
		"--initial-cluster", fmt.Sprintf("default=http://localhost:%d", peerPort),
		"--listen-peer-urls", fmt.Sprintf("http://localhost:%d", peerPort),
		"--initial-advertise-peer-urls", fmt.Sprintf("http://localhost:%d", peerPort),
		"--listen-client-urls", fmt.Sprintf("http://localhost:%d", clientPort),
		"--advertise-client-urls", fmt.Sprintf("http://localhost:%d", clientPort),
		"--debug",
	)
	cmd.Dir = t.TempDir()

	t.Cleanup(func() {
		if err := cmd.Process.Kill(); err != nil {
			panic(err)
		}
	})

	require.NoError(t, cmd.Start())
	return fmt.Sprintf("http://localhost:%d", clientPort)
}

func getAvailablePort(t testing.TB) int {
	listener, err := net.Listen("tcp", ":0")
	require.NoError(t, err)
	listener.Close()
	return listener.Addr().(*net.TCPAddr).Port
}

func CollectEvents(t *testing.T, watch clientv3.WatchChan, n int) []*Item {
	i := 0
	slice := make([]*Item, n)
	for msg := range watch {
		for _, event := range msg.Events {
			t.Logf("got event %d (rev %d) from watch", i, event.Kv.ModRevision)
			slice[i] = &Item{KeyValue: event.Kv}
			i++
			if i >= n {
				return slice
			}
		}
	}
	return nil
}

type Item struct {
	*mvccpb.KeyValue
}

func (e *Item) GetRevision() int64 { return e.ModRevision }
func (e *Item) GetKey() string     { return string(e.Key) }

func NewItems(kvs []*mvccpb.KeyValue) []*Item {
	slice := make([]*Item, len(kvs))
	for i, item := range kvs {
		slice[i] = &Item{KeyValue: item}
	}
	return slice
}

type HasRevision interface {
	GetRevision() int64
}

func GetRevisions[T HasRevision](items []T) []int64 {
	ret := make([]int64, len(items))
	for i, item := range items {
		ret[i] = item.GetRevision()
	}
	return ret
}

type HasKey interface {
	GetKey() string
}

func GetKeys[T HasKey](items []T) []string {
	ret := make([]string, len(items))
	for i, item := range items {
		ret[i] = item.GetKey()
	}
	return ret
}

func NewSeq(start, end int64) []int64 {
	slice := make([]int64, end-start)
	for i := int64(0); i < end-start; i++ {
		slice[i] = start + i
	}
	return slice
}
