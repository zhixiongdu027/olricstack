package stringkv

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/tidwall/redcon"
)

const DefaultDMap = "__default__"

type RESPServerConfig struct {
	Addr           string
	DefaultDMap    string
	CommandTimeout time.Duration
}

type RESPServer struct {
	service        *Service
	addr           string
	defaultDMap    string
	commandTimeout time.Duration
	server         *redcon.Server
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
	s := &RESPServer{
		service:        service,
		addr:           cfg.Addr,
		defaultDMap:    defaultDMap,
		commandTimeout: timeout,
	}
	s.server = redcon.NewServer(cfg.Addr, s.handle, nil, nil)
	return s, nil
}

func (s *RESPServer) ListenAndServe() error {
	return s.server.ListenAndServe()
}

func (s *RESPServer) Shutdown() error {
	return s.server.Close()
}

func (s *RESPServer) handle(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) == 0 {
		conn.WriteError("ERR empty command")
		return
	}
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
		if err := server.Shutdown(); err != nil && !strings.Contains(err.Error(), "not serving") {
			return err
		}
		return nil
	case err := <-errCh:
		if err != nil && !errors.Is(err, net.ErrClosed) {
			return err
		}
		return nil
	}
}
