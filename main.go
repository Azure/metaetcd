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
		coordinator       string
		membersStr        string
		clientCertPath    string
		clientCertKeyPath string
		caPath            string
		watchTimeout      time.Duration
		pprofPort         int
		watchBufferLen    int
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

	scc, err := membership.NewSharedClientContext(clientCertPath, clientCertKeyPath, caPath)
	if err != nil {
		logger.Sugar().Panicf("failed to load shared client context: %s", err)
	}

	if pprofPort > 0 {
		go func() {
			panic(http.ListenAndServe(fmt.Sprintf("127.0.0.1:%d", pprofPort), nil))
		}()
	}

	coordClient, err := membership.InitCoordinator(scc, coordinator)
	if err != nil {
		logger.Sugar().Panicf("failed to create client for coordinator cluster: %s", err)
	}

	watchMux := watch.NewMux(logger, watchTimeout, watchBufferLen)
	pool := membership.NewPool(scc, watchMux)
	partitions := membership.NewStaticPartitions(len(members))
	for i, memberURL := range members {
		err = pool.AddMember(membership.ClientID(i), memberURL, partitions[i])
		if err != nil {
			logger.Sugar().Panicf("failed to add member %q to the pool: %s", memberURL, err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	grpcServer := proxysvr.NewGRPCServer()

	shutdownSig := make(chan os.Signal, 1)
	signal.Notify(shutdownSig, os.Interrupt, syscall.SIGTERM, syscall.SIGQUIT)
	go func() {
		<-shutdownSig
		grpcServer.GracefulStop()
		cancel()
	}()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Add(-1)
		watchMux.Run(ctx)
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
	}()

	wg.Wait()
}
