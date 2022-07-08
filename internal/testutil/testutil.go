package testutil

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/etcdserver/etcdserverpb"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/stretchr/testify/require"
)

func StartEtcds(t testing.TB, n int) []*clientv3.Client {
	clients := make([]*clientv3.Client, n)
	for i := 0; i < n; i++ {
		client, err := clientv3.New(clientv3.Config{
			Endpoints:   []string{StartEtcd(t, fmt.Sprintf("cluster-%d", i))},
			DialTimeout: 2 * time.Second,
		})
		require.NoError(t, err)
		clients[i] = client
	}
	return clients
}

func StartEtcd(t testing.TB, name string) string {
	peerPort := GetPort(t)
	clientPort := GetPort(t)

	cmd := exec.Command("etcd",
		"--initial-cluster", fmt.Sprintf("default=http://localhost:%d", peerPort),
		"--listen-peer-urls", fmt.Sprintf("http://localhost:%d", peerPort),
		"--initial-advertise-peer-urls", fmt.Sprintf("http://localhost:%d", peerPort),
		"--listen-client-urls", fmt.Sprintf("http://localhost:%d", clientPort),
		"--advertise-client-urls", fmt.Sprintf("http://localhost:%d", clientPort),
		"--debug",
	)
	cmd.Dir = t.TempDir()

	if os.Getenv("ETCD_DEBUG") != "" {
		cmd.Stderr = addPrefixToWriter(name)
		cmd.Stdout = addPrefixToWriter(name)
	}

	t.Cleanup(func() {
		if err := cmd.Process.Kill(); err != nil {
			panic(err)
		}
	})

	require.NoError(t, cmd.Start())
	return fmt.Sprintf("http://localhost:%d", clientPort)
}

func GetPort(t testing.TB) int {
	listener, err := net.Listen("tcp", ":0")
	require.NoError(t, err)
	listener.Close()
	return listener.Addr().(*net.TCPAddr).Port
}

func addPrefixToWriter(prefix string) io.Writer {
	r, w := io.Pipe()
	go func() {
		s := bufio.NewScanner(r)
		for s.Scan() {
			fmt.Fprintf(os.Stdout, "%s - %s\n", prefix, s.Text())
		}
	}()
	return w
}

func EventKeys(events []*mvccpb.Event) []string {
	ret := make([]string, len(events))
	for i, event := range events {
		ret[i] = string(event.Kv.Key)
	}
	return ret
}

func EventModRevs(events []*mvccpb.Event) []int64 {
	ret := make([]int64, len(events))
	for i, event := range events {
		ret[i] = event.Kv.ModRevision
	}
	return ret
}

func ReadWatch(ch chan *etcdserverpb.WatchResponse, n int) []*mvccpb.Event {
	msgs := make([]*mvccpb.Event, n)
	i := 0
	for {
		msg := <-ch
		for _, event := range msg.Events {
			msgs[i] = event
			if i >= n-1 {
				return msgs
			}
			i++
		}
	}
}
