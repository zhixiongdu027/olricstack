package stringkv

import (
	"context"
	"errors"
	"net"
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
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := listener.Addr().String()
	_ = listener.Close()

	service := newTestService(t, newFakeProviderForDMap(DefaultDMap, dmap), nil).Service
	server, err := NewRESPServer(service, RESPServerConfig{
		Addr:           addr,
		CommandTimeout: time.Second,
	})
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
	waitForTCP(t, addr)
	return server, addr
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
