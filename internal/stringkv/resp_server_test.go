package stringkv

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestRESPServerStringCommandsUseDefaultDMap(t *testing.T) {
	dmap := newFakeDMap()
	server, addr := startTestRESPServer(t, dmap)
	defer server.Shutdown()

	client := redis.NewClient(&redis.Options{Addr: addr, Protocol: 3})
	defer client.Close()

	ctx := context.Background()
	if err := client.Set(ctx, "alice", "A", 0).Err(); err != nil {
		t.Fatalf("set: %v", err)
	}
	if got := dmap.values["alice"]; got != "A" {
		t.Fatalf("expected default dmap write, got %q", got)
	}
	got, err := client.Get(ctx, "alice").Result()
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got != "A" {
		t.Fatalf("expected A, got %q", got)
	}
}

func TestRESPServerSetPXAndExpire(t *testing.T) {
	dmap := newFakeDMap()
	server, addr := startTestRESPServer(t, dmap)
	defer server.Shutdown()

	client := redis.NewClient(&redis.Options{Addr: addr, Protocol: 3})
	defer client.Close()

	ctx := context.Background()
	if err := client.Set(ctx, "alice", "A", 1500*time.Millisecond).Err(); err != nil {
		t.Fatalf("set px: %v", err)
	}
	if got := dmap.ttls["alice"]; got != 1500*time.Millisecond {
		t.Fatalf("expected 1500ms ttl, got %s", got)
	}
	ok, err := client.Expire(ctx, "alice", 2*time.Second).Result()
	if err != nil {
		t.Fatalf("expire: %v", err)
	}
	if !ok || dmap.ttls["alice"] != 2*time.Second {
		t.Fatalf("expected expire to set 2s ttl, ok=%v ttl=%s", ok, dmap.ttls["alice"])
	}
}

func TestRESPServerMissingGetReturnsNil(t *testing.T) {
	dmap := newFakeDMap()
	server, addr := startTestRESPServer(t, dmap)
	defer server.Shutdown()

	client := redis.NewClient(&redis.Options{Addr: addr, Protocol: 3})
	defer client.Close()

	err := client.Get(context.Background(), "missing").Err()
	if !errors.Is(err, redis.Nil) {
		t.Fatalf("expected redis nil, got %v", err)
	}
}

func TestRESPServerRejectsDMCommands(t *testing.T) {
	dmap := newFakeDMap()
	server, addr := startTestRESPServer(t, dmap)
	defer server.Shutdown()

	client := redis.NewClient(&redis.Options{Addr: addr, Protocol: 3})
	defer client.Close()

	err := client.Do(context.Background(), "dm.put", "users", "alice", "A").Err()
	if err == nil {
		t.Fatal("expected DM command to be rejected")
	}
}

func startTestRESPServer(t *testing.T, dmap *fakeDMap) (*RESPServer, string) {
	return startTestRESPServerWithConfig(t, dmap, RESPServerConfig{CommandTimeout: time.Second})
}

func startTestRESPServerWithConfig(t *testing.T, dmap *fakeDMap, cfg RESPServerConfig) (*RESPServer, string) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := listener.Addr().String()
	_ = listener.Close()
	if cfg.Addr == "" {
		cfg.Addr = addr
	}
	if cfg.CommandTimeout <= 0 {
		cfg.CommandTimeout = time.Second
	}

	service := newTestService(t, newFakeProviderForDMap(DefaultDMap, dmap), nil).Service
	server, err := NewRESPServer(service, cfg)
	if err != nil {
		t.Fatalf("new resp server: %v", err)
	}
	errCh := make(chan error, 1)
	go func() {
		errCh <- server.ListenAndServe()
	}()
	t.Cleanup(func() {
		_ = server.Shutdown()
		select {
		case <-errCh:
		case <-time.After(time.Second):
			t.Fatal("resp server did not stop")
		}
	})
	waitForTCP(t, cfg.Addr)
	return server, cfg.Addr
}

func newFakeProviderForDMap(name string, dmap *fakeDMap) *fakeProvider {
	return &fakeProvider{dmaps: map[string]*fakeDMap{name: dmap}}
}

func waitForTCP(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 20*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("tcp server %s did not become ready", addr)
}

// TestRESPServerShutdownDrainsInflightCommand pins F6: Shutdown must wait for
// an in-flight command to finish (within ShutdownGrace) instead of force-
// closing the client mid-write.
func TestRESPServerShutdownDrainsInflightCommand(t *testing.T) {
	dmap := newFakeDMap()
	release := make(chan struct{})
	entered := make(chan struct{})
	var once sync.Once
	dmap.setHook = func(ctx context.Context, key, value string, ttl time.Duration) error {
		once.Do(func() { close(entered) })
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	server, addr := startTestRESPServerWithConfig(t, dmap, RESPServerConfig{
		CommandTimeout: 5 * time.Second,
		ShutdownGrace:  2 * time.Second,
	})

	client := redis.NewClient(&redis.Options{Addr: addr, Protocol: 3})
	defer client.Close()

	setErr := make(chan error, 1)
	go func() {
		setErr <- client.Set(context.Background(), "alice", "A", 0).Err()
	}()

	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("set never reached the dmap hook")
	}

	shutdownErr := make(chan error, 1)
	go func() { shutdownErr <- server.Shutdown() }()

	// Shutdown must not return before the in-flight command unblocks.
	select {
	case err := <-shutdownErr:
		t.Fatalf("Shutdown returned before drain finished: %v", err)
	case <-time.After(150 * time.Millisecond):
	}

	close(release)

	select {
	case err := <-shutdownErr:
		if err != nil {
			t.Fatalf("shutdown after drain: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("shutdown did not return after command released")
	}

	if err := <-setErr; err != nil {
		t.Fatalf("set should have completed during drain: %v", err)
	}
	if dmap.values["alice"] != "A" {
		t.Fatalf("expected alice=A after drain, got %q", dmap.values["alice"])
	}
}

// TestRESPServerShutdownForceClosesAfterGrace pins the second half of F6: if a
// command will not finish within ShutdownGrace, Shutdown must return after the
// grace window and force-close the underlying connection so the surrounding
// shutdown sequence can proceed.
func TestRESPServerShutdownForceClosesAfterGrace(t *testing.T) {
	dmap := newFakeDMap()
	stuck := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-stuck:
		default:
			close(stuck)
		}
	})
	dmap.setHook = func(ctx context.Context, _, _ string, _ time.Duration) error {
		select {
		case <-stuck:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// CommandTimeout is intentionally larger than ShutdownGrace: the in-flight
	// command will not unblock within grace, forcing Shutdown to fall through
	// the grace branch and close the redcon server.
	server, addr := startTestRESPServerWithConfig(t, dmap, RESPServerConfig{
		CommandTimeout: 2 * time.Second,
		ShutdownGrace:  100 * time.Millisecond,
	})

	client := redis.NewClient(&redis.Options{Addr: addr, Protocol: 3})
	defer client.Close()

	setDone := make(chan error, 1)
	go func() {
		setDone <- client.Set(context.Background(), "alice", "A", 0).Err()
	}()

	// Give the command time to enter the hook before triggering shutdown.
	time.Sleep(50 * time.Millisecond)

	start := time.Now()
	if err := server.Shutdown(); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	elapsed := time.Since(start)

	// Shutdown should return shortly after the grace window — bound it so a
	// regression that drops the force-close path fails loudly.
	if elapsed > time.Second {
		t.Fatalf("shutdown took %s, expected close-after-grace", elapsed)
	}

	// The forced connection close should surface as an error on the client.
	select {
	case err := <-setDone:
		if err == nil {
			t.Fatal("expected forced shutdown to fail the in-flight command")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("client SET never returned after forced shutdown")
	}
}

// TestRESPServerShutdownIsIdempotent guards against the previous "not serving"
// string-match path: calling Shutdown twice must not return an error.
func TestRESPServerShutdownIsIdempotent(t *testing.T) {
	dmap := newFakeDMap()
	server, _ := startTestRESPServer(t, dmap)
	if err := server.Shutdown(); err != nil {
		t.Fatalf("first shutdown: %v", err)
	}
	if err := server.Shutdown(); err != nil {
		t.Fatalf("second shutdown should be a no-op, got %v", err)
	}
}
