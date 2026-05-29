// olric-sidecar drains the dirty-data shm ring into MySQL and serves a small
// control RPC surface (Notify, LoadFromMySQL, DrainPartition,
// PurgeBelowGeneration, Shutdown) over a unix socket shared with olric-node.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/zhixiongdu/olricstack/internal/ring"
	"github.com/zhixiongdu/olricstack/internal/sidecar"
	"google.golang.org/grpc"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	ringPath := envString("RING_PATH", "/var/lib/olricstack/shared/oplog.ring")
	ringCapacity := uint64(envInt("RING_CAPACITY_BYTES", 16*1024*1024))
	socketPath := envString("CONTROL_SOCKET", "/var/lib/olricstack/shared/oplog.sock")

	if err := os.MkdirAll(filepath.Dir(ringPath), 0o755); err != nil {
		log.Fatalf("create ring dir: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(socketPath), 0o755); err != nil {
		log.Fatalf("create socket dir: %v", err)
	}

	if _, err := os.Stat(ringPath); os.IsNotExist(err) {
		if err := ring.Create(ringPath, ringCapacity); err != nil {
			log.Fatalf("create ring: %v", err)
		}
	} else if err != nil {
		log.Fatalf("stat ring: %v", err)
	}

	consumer, err := ring.OpenConsumer(ringPath)
	if err != nil {
		log.Fatalf("open ring consumer: %v", err)
	}
	defer consumer.Close()

	backend, closeBackend, err := buildBackend()
	if err != nil {
		log.Fatalf("build backend: %v", err)
	}
	defer func() {
		if err := closeBackend(context.Background()); err != nil {
			log.Printf("close backend: %v", err)
		}
	}()

	srv, err := sidecar.NewServer(consumer, backend, sidecar.Config{
		BatchSize:         envInt("CONSUMER_BATCH_SIZE", 256),
		IdlePoll:          envDuration("CONSUMER_IDLE_POLL", 200*time.Millisecond),
		FlushTimeout:      envDuration("CONSUMER_FLUSH_TIMEOUT", 30*time.Second),
		MaxPendingRecords: envIntAllowZero("MAX_PENDING_RECORDS", 0),
		MaxPendingBytes:   uint64(envIntAllowZero("MAX_PENDING_BYTES", 0)),
	})
	if err != nil {
		log.Fatalf("build sidecar server: %v", err)
	}

	_ = os.Remove(socketPath)
	listener, err := sidecar.Listen(socketPath)
	if err != nil {
		log.Fatalf("listen unix socket %s: %v", socketPath, err)
	}
	defer func() {
		_ = listener.Close()
		_ = os.Remove(socketPath)
	}()

	grpcServer := grpc.NewServer()
	sidecar.RegisterOplogControlServer(grpcServer, srv)

	consumerErr := make(chan error, 1)
	go func() {
		consumerErr <- srv.Run(ctx)
	}()

	serveErr := make(chan error, 1)
	go func() {
		log.Printf("sidecar bootstrap complete socket=%s ring=%s", socketPath, ringPath)
		serveErr <- grpcServer.Serve(listener)
	}()

	select {
	case <-ctx.Done():
		grpcServer.GracefulStop()
		<-serveErr
		<-consumerErr
	case err := <-serveErr:
		if err != nil {
			log.Fatalf("serve sidecar: %v", err)
		}
	case err := <-consumerErr:
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Fatalf("consumer loop: %v", err)
		}
	}
}

func buildBackend() (sidecar.Backend, func(context.Context) error, error) {
	dsn := os.Getenv("MYSQL_DSN")
	if dsn == "" {
		return nil, nil, fmt.Errorf("MYSQL_DSN is required")
	}
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil, nil, fmt.Errorf("open mysql: %w", err)
	}
	backend, err := sidecar.NewMySQLBackend(db, envInt("FLUSH_BATCH_SIZE", 256))
	if err != nil {
		return nil, nil, err
	}
	return backend, backend.Close, nil
}

func envInt(name string, fallback int) int {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		log.Printf("invalid %s=%q, using %d", name, value, fallback)
		return fallback
	}
	return parsed
}

func envIntAllowZero(name string, fallback int) int {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < 0 {
		log.Printf("invalid %s=%q, using %d", name, value, fallback)
		return fallback
	}
	return parsed
}

func envDuration(name string, fallback time.Duration) time.Duration {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed <= 0 {
		log.Printf("invalid %s=%q, using %s", name, value, fallback)
		return fallback
	}
	return parsed
}

func envString(name, fallback string) string {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	return strings.TrimSpace(value)
}
