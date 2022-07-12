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
	"go.uber.org/zap"

	"github.com/Azure/metaetcd/internal/membership"
	"github.com/Azure/metaetcd/internal/proxysvr"
	"github.com/Azure/metaetcd/internal/watch"
)

func main() {
	var (
		coordinator              string
		membersStr               string
		clientCertPath           string
		clientCertKeyPath        string
		caPath                   string
		watchTimeout             time.Duration
		pprofPort                int
		watchBufferLen           int
		grpcSvrKeepaliveMaxIdle  time.Duration
		grpcSvrKeepaliveInterval time.Duration
		grpcSvrKeepaliveTimeout  time.Duration
		scc                      membership.SharedClientContext
	)
	flag.StringVar(&coordinator, "coordinator", "", "")
	flag.StringVar(&membersStr, "members", "", "")
	flag.StringVar(&clientCertPath, "client-cert", "", "")
	flag.StringVar(&clientCertKeyPath, "client-cert-key", "", "")
	flag.StringVar(&caPath, "ca-cert", "", "")
	flag.DurationVar(&watchTimeout, "watch-timeout", time.Second*10, "")
	flag.IntVar(&watchBufferLen, "watch-buffer-len", 3000, "")
	zap.LevelFlag("v", zap.WarnLevel, "log level (default is warn)")
	flag.IntVar(&pprofPort, "pprof-port", 0, "port to serve pprof on. disabled if 0")
	flag.DurationVar(&grpcSvrKeepaliveMaxIdle, "grpc-server-keepalive-max-idle", time.Second*5, "")
	flag.DurationVar(&grpcSvrKeepaliveInterval, "grpc-server-keepalive-interval", time.Second*10, "")
	flag.DurationVar(&grpcSvrKeepaliveTimeout, "grpc-server-keepalive-timeout", time.Second*20, "")
	flag.DurationVar(&scc.GrpcKeepaliveInterval, "grpc-client-keepalive-interval", time.Second*5, "")
	flag.DurationVar(&scc.GrpcKeepaliveTimeout, "grpc-client-keepalive-timeout", time.Second*20, "")
	flag.Parse()

	rand.Seed(time.Now().Unix())

	logger, err := zap.NewProduction()
	if err != nil {
		panic(err)
	}

	members := strings.Split(membersStr, ",")

	lis, err := net.Listen("tcp", "localhost:2379")
	if err != nil {
		logger.Sugar().Panicf("failed to start listener: %s", err)
	}

	if err := scc.LoadPKI(clientCertPath, clientCertKeyPath, caPath); err != nil {
		logger.Sugar().Panicf("failed to load shared client context: %s", err)
	}

	if pprofPort > 0 {
		go func() {
			panic(http.ListenAndServe(fmt.Sprintf("127.0.0.1:%d", pprofPort), nil))
		}()
	}

	coordClient, err := membership.InitCoordinator(&scc, coordinator)
	if err != nil {
		logger.Sugar().Panicf("failed to create client for coordinator cluster: %s", err)
	}

	watchMux := watch.NewMux(logger, watchTimeout, watchBufferLen)
	pool := membership.NewPool(&scc, watchMux)
	partitions := membership.NewStaticPartitions(len(members))
	for i, memberURL := range members {
		err = pool.AddMember(membership.ClientID(i), memberURL, partitions[i])
		if err != nil {
			logger.Sugar().Panicf("failed to add member %q to the pool: %s", memberURL, err)
		}
	}

	grpcServer := proxysvr.NewGRPCServer(grpcSvrKeepaliveMaxIdle, grpcSvrKeepaliveInterval, grpcSvrKeepaliveTimeout)
	shutdownSig := make(chan os.Signal, 1)
	signal.Notify(shutdownSig, os.Interrupt, syscall.SIGTERM, syscall.SIGQUIT)
	go func() {
		<-shutdownSig
		logger.Warn("gracefully shutting down...")
		grpcServer.GracefulStop()
	}()

	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	wg.Add(1)
	go func() {
		defer wg.Add(-1)
		watchMux.Run(ctx)
		logger.Warn("watch mux gracefully shutdown")
	}()

	wg.Add(1)
	go func() {
		defer wg.Add(-1)
		svr := proxysvr.NewServer(coordClient, pool, logger)
		etcdserverpb.RegisterKVServer(grpcServer, svr)
		etcdserverpb.RegisterWatchServer(grpcServer, svr)
		etcdserverpb.RegisterLeaseServer(grpcServer, svr)
		logger.Info("initialized - ready to proxy requests")
		grpcServer.Serve(lis)
		logger.Warn("grpc server gracefully shut down")
		cancel() // now we can safely stop the watch mux
	}()

	wg.Wait()
}
