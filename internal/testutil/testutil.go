package testutil

import (
	"fmt"
	"net"
	"os/exec"
	"testing"

	"github.com/coreos/etcd/clientv3"
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

func CollectEvents(t *testing.T, watch clientv3.WatchChan, n int) []*Event {
	i := 0
	slice := make([]*Event, n)
	for msg := range watch {
		for _, event := range msg.Events {
			t.Logf("got event %d (rev %d) from watch", i, event.Kv.ModRevision)
			slice[i] = &Event{Event: event}
			i++
			if i >= n {
				return slice
			}
		}
	}
	return nil
}

type Event struct {
	*clientv3.Event
}

func (e *Event) GetRevision() int64 { return e.Kv.ModRevision }

type HasRevision interface {
	GetRevision() int64
}

func GetEventRevisions[T HasRevision](events []T) []int64 {
	ret := make([]int64, len(events))
	for i, event := range events {
		ret[i] = event.GetRevision()
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
