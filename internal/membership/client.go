package membership

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"math"
	"net/url"
	"os"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/credentials"
	"github.com/coreos/etcd/etcdserver/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"github.com/Azure/metaetcd/internal/scheme"
)

type ClientSet struct {
	ClientV3 *clientv3.Client
	KV       etcdserverpb.KVClient
	Lease    etcdserverpb.LeaseClient
	GRPC     *grpc.ClientConn
}

func NewClientSet(scc *SharedClientContext, endpointURL string) (*ClientSet, error) {
	cs := &ClientSet{}
	var err error
	cs.ClientV3, err = clientv3.New(clientv3.Config{
		Endpoints:   []string{endpointURL},
		DialTimeout: 5 * time.Second,
		TLS:         scc.TLS,
	})
	if err != nil {
		return nil, fmt.Errorf("constructing etcd client: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	var authOption grpc.DialOption
	if scc.TLS == nil {
		authOption = grpc.WithInsecure()
	} else {
		authOption = grpc.WithTransportCredentials(credentials.NewBundle(credentials.Config{TLSConfig: scc.TLS}).TransportCredentials())
	}

	u, err := url.Parse(endpointURL)
	if err != nil {
		return nil, fmt.Errorf("parsing endpoint url: %w", err)
	}

	// TODO: Pin connections to cluster leader like clientv3 does
	cs.GRPC, err = grpc.DialContext(ctx, u.Host,
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:    time.Second * 5,
			Timeout: time.Second * 20, // TODO: Expose flags for these values
		}),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(math.MaxInt32)),
		authOption)
	if err != nil {
		return nil, fmt.Errorf("dialing grpc connection: %w", err)
	}
	cs.KV = etcdserverpb.NewKVClient(cs.GRPC)
	cs.Lease = etcdserverpb.NewLeaseClient(cs.GRPC)

	return cs, nil
}

func InitCoordinator(scc *SharedClientContext, endpointURL string) (*ClientSet, error) {
	cs, err := NewClientSet(scc, endpointURL)
	if err != nil {
		return nil, err
	}

	ctx, done := context.WithTimeout(context.Background(), time.Second*5)
	defer done()

	_, err = cs.ClientV3.KV.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(scheme.MetaKey), "=", 0)).
		Then(clientv3.OpPut(scheme.MetaKey, string(make([]byte, 8)))).
		Commit()
	if err != nil {
		return nil, fmt.Errorf("initializing clock: %w", err)
	}

	return cs, nil
}

type SharedClientContext struct {
	TLS *tls.Config
}

func NewSharedClientContext(clientCert, clientKey, caCert string) (*SharedClientContext, error) {
	if clientCert == "" {
		return &SharedClientContext{}, nil
	}
	cert, err := tls.LoadX509KeyPair(clientCert, clientKey)
	if err != nil {
		return nil, fmt.Errorf("loading client cert: %w", err)
	}

	cas := x509.NewCertPool()
	caCertPem, err := os.ReadFile(caCert)
	if err != nil {
		return nil, fmt.Errorf("reading ca cert: %w", err)
	}
	if !cas.AppendCertsFromPEM(caCertPem) {
		return nil, fmt.Errorf("ca cert is invalid")
	}

	return &SharedClientContext{
		TLS: &tls.Config{
			Certificates: []tls.Certificate{cert},
			RootCAs:      cas,
		},
	}, nil
}
