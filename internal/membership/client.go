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
	"github.com/coreos/etcd/clientv3/balancer"
	"github.com/coreos/etcd/clientv3/balancer/picker"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/coreos/etcd/clientv3/credentials"
	"github.com/coreos/etcd/etcdserver/etcdserverpb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"github.com/Azure/metaetcd/internal/scheme"
	"github.com/Azure/metaetcd/internal/watch"
)

const etcdRoundRobinBalancerName = "etcd-round-robin-lb"

func init() {
	balancer.RegisterBuilder(balancer.Config{
		Policy: picker.RoundrobinBalanced,
		Name:   etcdRoundRobinBalancerName,
		Logger: zap.L(),
	})
}

// ClientSet holds various clients used to access etcd.
type ClientSet struct {
	ClientV3    *clientv3.Client
	KV          etcdserverpb.KVClient
	Lease       etcdserverpb.LeaseClient
	GRPC        *grpc.ClientConn
	WatchStatus *watch.Status
}

func NewClientSet(gc *GrpcContext, endpointURL string) (*ClientSet, error) {
	cs := &ClientSet{}
	var err error
	cs.ClientV3, err = clientv3.New(clientv3.Config{
		Endpoints:   []string{endpointURL},
		DialTimeout: 5 * time.Second,
		TLS:         gc.TLS,
	})
	if err != nil {
		return nil, fmt.Errorf("constructing etcd client: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	var authOption grpc.DialOption
	if gc.TLS == nil {
		authOption = grpc.WithInsecure()
	} else {
		authOption = grpc.WithTransportCredentials(credentials.NewBundle(credentials.Config{TLSConfig: gc.TLS}).TransportCredentials())
	}

	u, err := url.Parse(endpointURL)
	if err != nil {
		return nil, fmt.Errorf("parsing endpoint url: %w", err)
	}

	cs.GRPC, err = grpc.DialContext(ctx, u.Host,
		grpc.WithBalancerName(etcdRoundRobinBalancerName),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:    gc.GrpcKeepaliveInterval,
			Timeout: gc.GrpcKeepaliveTimeout,
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

// CoordinatorClientSet is ClientSet plus extra fields that only pertain to coordinator clusters.
type CoordinatorClientSet struct {
	*ClientSet
	ClockReconstitutionLock *concurrency.Mutex
}

// InitCoordinator creates a CoordinatorClientSet and initializes the clock if it hasn't been already.
// This is important to avoid attempting to reconstitute the clock during bootstrapping of new clusters.
func InitCoordinator(gc *GrpcContext, endpointURL string) (*CoordinatorClientSet, error) {
	cs, err := NewClientSet(gc, endpointURL)
	if err != nil {
		return nil, err
	}

	ctx, done := context.WithTimeout(context.Background(), time.Second*15)
	defer done()

	_, err = cs.ClientV3.KV.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(scheme.MetaKey), "=", 0)).
		Then(clientv3.OpPut(scheme.MetaKey, string(make([]byte, 8)))).
		Commit()
	if err != nil {
		return nil, fmt.Errorf("initializing clock: %w", err)
	}

	sess, err := concurrency.NewSession(cs.ClientV3)
	if err != nil {
		return nil, fmt.Errorf("initializing etcd concurrency session: %w", err)
	}

	return &CoordinatorClientSet{
		ClientSet:               cs,
		ClockReconstitutionLock: concurrency.NewMutex(sess, "/locks/clock-reconstitution"),
	}, nil
}

// GrpcContext contains common values used to set up gRPC connections across the fleet of clusters.
type GrpcContext struct {
	GrpcKeepaliveInterval time.Duration
	GrpcKeepaliveTimeout  time.Duration
	TLS                   *tls.Config
}

func (g *GrpcContext) LoadPKI(clientCert, clientKey, caCert string) error {
	if clientCert == "" {
		return nil
	}
	cert, err := tls.LoadX509KeyPair(clientCert, clientKey)
	if err != nil {
		return fmt.Errorf("loading client cert: %w", err)
	}

	cas := x509.NewCertPool()
	caCertPem, err := os.ReadFile(caCert)
	if err != nil {
		return fmt.Errorf("reading ca cert: %w", err)
	}
	if !cas.AppendCertsFromPEM(caCertPem) {
		return fmt.Errorf("ca cert is invalid")
	}

	g.TLS = &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      cas,
	}
	return nil
}
