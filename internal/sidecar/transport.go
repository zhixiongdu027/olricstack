package sidecar

import (
	"context"
	"errors"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Listen returns a unix-socket listener bound to socketPath. The caller is
// responsible for cleaning up the path on shutdown.
func Listen(socketPath string) (net.Listener, error) {
	if socketPath == "" {
		return nil, errors.New("socket path is empty")
	}
	return net.Listen("unix", socketPath)
}

// Client is a thin convenience wrapper around the generated gRPC client. It
// exists so node-side callers don't have to know about codec selection.
type Client struct {
	conn *grpc.ClientConn
	rpc  OplogControlClient
}

// Dial connects to the sidecar over a unix socket.
func Dial(ctx context.Context, socketPath string) (*Client, error) {
	if socketPath == "" {
		return nil, errors.New("socket path is empty")
	}
	conn, err := grpc.DialContext(
		ctx,
		"unix:"+socketPath,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(jsonCodec{})),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn, rpc: NewOplogControlClient(conn)}, nil
}

func (c *Client) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

func (c *Client) Notify(ctx context.Context, pending uint64) error {
	_, err := c.rpc.Notify(ctx, &NotifyRequest{Pending: pending}, grpc.ForceCodec(jsonCodec{}))
	return err
}

func (c *Client) LoadFromMySQL(ctx context.Context, req *LoadRequest) (*LoadResponse, error) {
	return c.rpc.LoadFromMySQL(ctx, req, grpc.ForceCodec(jsonCodec{}))
}

func (c *Client) DrainPartition(ctx context.Context, req *DrainPartitionRequest) error {
	_, err := c.rpc.DrainPartition(ctx, req, grpc.ForceCodec(jsonCodec{}))
	return err
}

func (c *Client) PurgeBelowGeneration(ctx context.Context, minGeneration int64) (int, error) {
	resp, err := c.rpc.PurgeBelowGeneration(ctx, &PurgeRequest{MinGeneration: minGeneration}, grpc.ForceCodec(jsonCodec{}))
	if err != nil {
		return 0, err
	}
	return resp.Purged, nil
}

func (c *Client) Shutdown(ctx context.Context) error {
	_, err := c.rpc.Shutdown(ctx, &Empty{}, grpc.ForceCodec(jsonCodec{}))
	return err
}
