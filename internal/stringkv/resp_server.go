package stringkv

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tidwall/redcon"
)

const DefaultDMap = "__default__"

// DefaultRESPShutdownGrace is the default time RESPServer.Shutdown waits for
// in-flight commands to finish before force-closing client connections.
const DefaultRESPShutdownGrace = 2 * time.Second

type RESPServerConfig struct {
	Addr           string
	DefaultDMap    string
	CommandTimeout time.Duration
	// ShutdownGrace bounds how long Shutdown waits for in-flight commands to
	// complete before force-closing remaining client connections. Zero falls
	// back to DefaultRESPShutdownGrace.
	ShutdownGrace time.Duration
}

type RESPServer struct {
	service        *Service
	addr           string
	defaultDMap    string
	commandTimeout time.Duration
	shutdownGrace  time.Duration
	server         *redcon.Server

	running  atomic.Bool
	stopping atomic.Bool
	inflight sync.WaitGroup
}

func NewRESPServer(service *Service, cfg RESPServerConfig) (*RESPServer, error) {
	if service == nil {
		return nil, errors.New("string kv service is nil")
	}
	if cfg.Addr == "" {
		return nil, errors.New("resp listen address is empty")
	}
	defaultDMap := cfg.DefaultDMap
	if defaultDMap == "" {
		defaultDMap = DefaultDMap
	}
	timeout := cfg.CommandTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	grace := cfg.ShutdownGrace
	if grace <= 0 {
		grace = DefaultRESPShutdownGrace
	}
	s := &RESPServer{
		service:        service,
		addr:           cfg.Addr,
		defaultDMap:    defaultDMap,
		commandTimeout: timeout,
		shutdownGrace:  grace,
	}
	s.server = redcon.NewServer(cfg.Addr, s.handle, s.onAccept, nil)
	return s, nil
}

func (s *RESPServer) ListenAndServe() error {
	s.running.Store(true)
	defer s.running.Store(false)
	return s.server.ListenAndServe()
}

// Shutdown stops accepting new connections, waits up to ShutdownGrace for
// in-flight commands to complete, then closes the underlying redcon server
// (which force-closes any remaining client connections). It is safe to call
// multiple times; subsequent calls return nil.
//
// Implementation note: redcon.Server.Close is itself forceful — it closes the
// listener AND every accepted connection in one shot. To get a real drain we
// must NOT call Close until inflight reaches zero (or the grace window
// expires). Until that happens, new connections are rejected by onAccept and
// new commands on existing connections are rejected by handle.
func (s *RESPServer) Shutdown() error {
	if !s.stopping.CompareAndSwap(false, true) {
		return nil
	}
	if !s.running.Load() {
		return nil
	}

	done := make(chan struct{})
	go func() {
		s.inflight.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Clean drain: every handler returned and flushed its response.
	case <-time.After(s.shutdownGrace):
		// Grace exceeded. Falling through to Close will force every
		// remaining connection shut, which is what production callers want
		// when a misbehaving client is holding the server hostage.
	}
	return s.server.Close()
}

func (s *RESPServer) onAccept(_ redcon.Conn) bool {
	return !s.stopping.Load()
}

func (s *RESPServer) handle(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) == 0 {
		conn.WriteError("ERR empty command")
		return
	}
	if s.stopping.Load() {
		conn.WriteError("ERR server is shutting down")
		_ = conn.Close()
		return
	}
	s.inflight.Add(1)
	defer s.inflight.Done()

	name := strings.ToUpper(argString(cmd.Args[0]))
	switch name {
	case "HELLO":
		s.handleHello(conn, cmd)
	case "PING":
		s.handlePing(conn, cmd)
	case "GET":
		s.handleGet(conn, cmd)
	case "SET":
		s.handleSet(conn, cmd)
	case "DEL":
		s.handleDel(conn, cmd)
	case "EXPIRE":
		s.handleExpire(conn, cmd)
	default:
		conn.WriteError("ERR unsupported command")
	}
}

func (s *RESPServer) handleHello(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 1 && len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments for 'hello' command")
		return
	}
	if len(cmd.Args) == 2 && argString(cmd.Args[1]) != "3" {
		conn.WriteError("NOPROTO unsupported protocol version")
		return
	}
	conn.WriteRaw([]byte("%4\r\n$6\r\nserver\r\n$10\r\nolricstack\r\n$7\r\nversion\r\n$3\r\n0.1\r\n$5\r\nproto\r\n:3\r\n$4\r\nmode\r\n$10\r\nstandalone\r\n"))
}

func (s *RESPServer) handlePing(conn redcon.Conn, cmd redcon.Command) {
	switch len(cmd.Args) {
	case 1:
		conn.WriteString("PONG")
	case 2:
		conn.WriteBulk(cmd.Args[1])
	default:
		conn.WriteError("ERR wrong number of arguments for 'ping' command")
	}
}

func (s *RESPServer) handleGet(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments for 'get' command")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.commandTimeout)
	defer cancel()
	value, err := s.service.Get(ctx, s.defaultDMap, argString(cmd.Args[1]))
	if err != nil {
		writeServiceError(conn, err)
		return
	}
	conn.WriteBulkString(value)
}

func (s *RESPServer) handleSet(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) < 3 {
		conn.WriteError("ERR wrong number of arguments for 'set' command")
		return
	}
	ttl, err := parseSetTTL(cmd.Args[3:])
	if err != nil {
		conn.WriteError("ERR " + err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.commandTimeout)
	defer cancel()
	if err := s.service.Set(ctx, s.defaultDMap, argString(cmd.Args[1]), argString(cmd.Args[2]), ttl); err != nil {
		writeServiceError(conn, err)
		return
	}
	conn.WriteString("OK")
}

func (s *RESPServer) handleDel(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) < 2 {
		conn.WriteError("ERR wrong number of arguments for 'del' command")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.commandTimeout)
	defer cancel()
	var deleted int
	for _, key := range cmd.Args[1:] {
		if err := s.service.Delete(ctx, s.defaultDMap, argString(key)); err != nil {
			writeServiceError(conn, err)
			return
		}
		deleted++
	}
	conn.WriteInt(deleted)
}

func (s *RESPServer) handleExpire(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 3 {
		conn.WriteError("ERR wrong number of arguments for 'expire' command")
		return
	}
	seconds, err := strconv.ParseInt(argString(cmd.Args[2]), 10, 64)
	if err != nil || seconds <= 0 {
		conn.WriteError("ERR invalid expire time")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.commandTimeout)
	defer cancel()
	if err := s.service.Expire(ctx, s.defaultDMap, argString(cmd.Args[1]), time.Duration(seconds)*time.Second); err != nil {
		if errors.Is(err, ErrNotFound) {
			conn.WriteInt(0)
			return
		}
		writeServiceError(conn, err)
		return
	}
	conn.WriteInt(1)
}

func parseSetTTL(args [][]byte) (time.Duration, error) {
	var ttl time.Duration
	for i := 0; i < len(args); {
		option := strings.ToUpper(argString(args[i]))
		switch option {
		case "EX", "PX":
			if i+1 >= len(args) {
				return 0, fmt.Errorf("missing value for %s", option)
			}
			value, err := strconv.ParseInt(argString(args[i+1]), 10, 64)
			if err != nil || value <= 0 {
				return 0, fmt.Errorf("invalid %s value", option)
			}
			if option == "EX" {
				ttl = time.Duration(value) * time.Second
			} else {
				ttl = time.Duration(value) * time.Millisecond
			}
			i += 2
		default:
			return 0, fmt.Errorf("unsupported SET option %s", option)
		}
	}
	return ttl, nil
}

func writeServiceError(conn redcon.Conn, err error) {
	switch {
	case errors.Is(err, ErrNotFound):
		conn.WriteRaw([]byte("_\r\n"))
	case errors.Is(err, ErrLeaseExpired):
		conn.WriteError("TRYAGAIN serving lease is expired")
	case errors.Is(err, context.DeadlineExceeded):
		conn.WriteError("TIMEOUT command timed out")
	case errors.Is(err, context.Canceled):
		conn.WriteError("ERR command canceled")
	default:
		conn.WriteError("ERR " + err.Error())
	}
}

func argString(arg []byte) string {
	return string(arg)
}

func ServeRESP(ctx context.Context, service *Service, cfg RESPServerConfig) error {
	server, err := NewRESPServer(service, cfg)
	if err != nil {
		return err
	}
	errCh := make(chan error, 1)
	go func() {
		log.Printf("string kv RESP service listening on %s", cfg.Addr)
		errCh <- server.ListenAndServe()
	}()
	select {
	case <-ctx.Done():
		// Bound the time we spend draining so a hung client cannot block the
		// surrounding shutdown sequence indefinitely.
		shutdownCh := make(chan error, 1)
		go func() { shutdownCh <- server.Shutdown() }()
		select {
		case shutdownErr := <-shutdownCh:
			if shutdownErr != nil && !isListenerClosed(shutdownErr) {
				return shutdownErr
			}
			return nil
		case <-time.After(server.shutdownGrace + time.Second):
			return fmt.Errorf("resp server shutdown exceeded grace window")
		}
	case err := <-errCh:
		if err != nil && !isListenerClosed(err) {
			return err
		}
		return nil
	}
}

// isListenerClosed reports whether err is the benign "listener already closed"
// signal raised by redcon.Server.Close or by an underlying net.Listener that
// has been shut down. We accept it as a clean stop instead of propagating it.
func isListenerClosed(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, net.ErrClosed) {
		return true
	}
	// redcon.Server.Close returns errors.New("not serving") when the server
	// has not started or has already stopped. There is no exported sentinel
	// to compare against, so we fall back to a tightly-scoped string match
	// that only succeeds for that specific message. Keep this disjunction
	// narrow so dependency upgrades that change the wording surface as a
	// real error rather than a silent miss.
	return err.Error() == "not serving"
}
