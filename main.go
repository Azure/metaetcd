package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/coreos/etcd/etcdserver/etcdserverpb"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"

	"github.com/Azure/metaetcd/internal/clock"
	"github.com/Azure/metaetcd/internal/membership"
	"github.com/Azure/metaetcd/internal/proxysvr"
	"github.com/Azure/metaetcd/internal/watch"
)

func main() {
	var (
		listenAddr               string
		coordinator              string
		membersStr               string
		clientCertPath           string
		clientCertKeyPath        string
		serverCertPath           string
		serverCertKeyPath        string
		caPath                   string
		watchTimeout             time.Duration
		pprofPort                int
		metricsPort              int
		watchBufferLen           int
		grpcSvrKeepaliveMaxIdle  time.Duration
		grpcSvrKeepaliveInterval time.Duration
		grpcSvrKeepaliveTimeout  time.Duration
		grpcContext              membership.GrpcContext
	)
	flag.StringVar(&listenAddr, "listen-addr", "127.0.0.1:2379", "address to serve the etcd proxy server on")
	flag.StringVar(&coordinator, "coordinator", "", "URL of the coordinator cluster")
	flag.StringVar(&membersStr, "members", "", "comma-separated list of member clusters")
	flag.StringVar(&clientCertPath, "client-cert", "", "cert used when connecting to the coordinator and member clusters")
	flag.StringVar(&clientCertKeyPath, "client-cert-key", "", "key of --client-cert")
	flag.StringVar(&serverCertPath, "server-cert", "", "cert presented to etcd proxy clients (optional)")
	flag.StringVar(&serverCertKeyPath, "server-cert-key", "", "key of --server-cert (optional)")
	flag.StringVar(&caPath, "ca-cert", "", "cert used to verify incoming and outgoing identities")
	flag.DurationVar(&watchTimeout, "watch-timeout", time.Second*10, "how long to wait before a watch message is considered missing")
	flag.IntVar(&watchBufferLen, "watch-buffer-len", 1000, "how many watch events to buffer")
	logLevel := zap.LevelFlag("v", zap.WarnLevel, "log level")
	flag.IntVar(&pprofPort, "pprof-port", 0, "port to serve pprof on. disabled if 0")
	flag.IntVar(&metricsPort, "metrics-port", 9090, "port to serve Prometheus metrics on. disabled if 0")
	flag.DurationVar(&grpcSvrKeepaliveMaxIdle, "grpc-server-keepalive-max-idle", time.Second*5, "")
	flag.DurationVar(&grpcSvrKeepaliveInterval, "grpc-server-keepalive-interval", time.Second*10, "")
	flag.DurationVar(&grpcSvrKeepaliveTimeout, "grpc-server-keepalive-timeout", time.Second*20, "")
	flag.DurationVar(&grpcContext.GrpcKeepaliveInterval, "grpc-client-keepalive-interval", time.Second*5, "")
	flag.DurationVar(&grpcContext.GrpcKeepaliveTimeout, "grpc-client-keepalive-timeout", time.Second*20, "")
	flag.Parse()

	logCfg := zap.NewProductionConfig()
	logCfg.Level.SetLevel(*logLevel)
	logger, err := logCfg.Build()
	if err != nil {
		panic(err)
	}
	zap.ReplaceGlobals(logger)

	rand.Seed(time.Now().Unix())

	members := strings.Split(membersStr, ",")

	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		zap.L().Sugar().Panicf("failed to start listener: %s", err)
	}

	if err := grpcContext.LoadPKI(clientCertPath, clientCertKeyPath, caPath); err != nil {
		zap.L().Sugar().Panicf("failed to load shared client context: %s", err)
	}

	if pprofPort > 0 {
		go func() {
			panic(http.ListenAndServe(fmt.Sprintf("127.0.0.1:%d", pprofPort), nil))
		}()
	}

	if metricsPort > 0 {
		go func() {
			mux := http.NewServeMux()
			mux.Handle("/metrics", promhttp.Handler())
			panic(http.ListenAndServe(fmt.Sprintf(":%d", metricsPort), mux))
		}()
	}

	coordClient, err := membership.InitCoordinator(&grpcContext, coordinator)
	if err != nil {
		zap.L().Sugar().Panicf("failed to create client for coordinator cluster: %s", err)
	}

	clk := &clock.Clock{Coordinator: coordClient}
	watchMux := watch.NewMux(watchTimeout, watchBufferLen, clk)
	pool := membership.NewPool(&grpcContext, watchMux)
	clk.Members = pool

	if err := clk.Init(); err != nil {
		zap.L().Sugar().Panicf("failed to initialize clock: %s", err)
	}

	partitions := membership.NewStaticPartitions(len(members))
	for i, memberURL := range members {
		err = pool.AddMember(context.Background(), membership.MemberID(i), memberURL, partitions[i])
		if err != nil {
			zap.L().Sugar().Panicf("failed to add member %q to the pool: %s", memberURL, err)
		}
	}

	grpcServer, err := proxysvr.NewGRPCServer(caPath, serverCertPath, serverCertKeyPath, grpcSvrKeepaliveMaxIdle, grpcSvrKeepaliveInterval, grpcSvrKeepaliveTimeout)
	if err != nil {
		zap.L().Sugar().Panicf("failed to construct grpc server: %s", err)
	}

	shutdownSig := make(chan os.Signal, 1)
	signal.Notify(shutdownSig, os.Interrupt, syscall.SIGTERM, syscall.SIGQUIT)
	go func() {
		<-shutdownSig
		zap.L().Warn("gracefully shutting down...")
		grpcServer.GracefulStop()
	}()

	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	wg.Add(1)
	go func() {
		defer wg.Add(-1)
		watchMux.Run(ctx)
		zap.L().Warn("watch mux gracefully shutdown")
	}()

	wg.Add(1)
	go func() {
		defer wg.Add(-1)
		svr := proxysvr.NewServer(coordClient, pool, clk)
		etcdserverpb.RegisterKVServer(grpcServer, svr)
		etcdserverpb.RegisterWatchServer(grpcServer, svr)
		etcdserverpb.RegisterLeaseServer(grpcServer, svr)
		zap.L().Info("initialized - ready to proxy requests")
		grpcServer.Serve(lis)
		zap.L().Warn("grpc server gracefully shut down")
		cancel() // now we can safely stop the watch mux
	}()

	wg.Wait()
}
