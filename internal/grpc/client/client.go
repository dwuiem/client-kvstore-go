package client

import (
	"context"
	kvstore "github.com/HSE-RDBMS-course-work/kvstore-proto/gen/go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Client struct {
	api  kvstore.KVStoreClient
	conn *grpc.ClientConn
}

func NewClient(address string, opts []grpc.DialOption) (*Client, error) {
	conn, err := grpc.NewClient(address, opts...)
	if err != nil {
		return nil, err
	}
	return &Client{
		api:  kvstore.NewKVStoreClient(conn),
		conn: conn,
	}, nil
}

func (c *Client) Put(ctx context.Context, key, value string, ttl int64) error {
	_, err := c.api.Put(ctx, &kvstore.PutIn{Key: key, Value: value, Ttl: ttl})
	if err != nil {
		st, ok := status.FromError(err)
		if !ok {
			panic(err) // todo
		}
		switch st.Code() {
		case codes.Unauthenticated:
			return ErrUnauthenticated
		default:
			return ErrIsNotLeader
		}
	}
	return nil
}

func (c *Client) Delete(ctx context.Context, key string) error {
	_, err := c.api.Delete(ctx, &kvstore.DeleteIn{Key: key})
	if err != nil {
		st, ok := status.FromError(err)
		if !ok {
			panic(err)
		}
		switch st.Code() {
		case codes.Unauthenticated:
			return ErrUnauthenticated
		default:
			return ErrIsNotLeader
		}
	}
	return nil
}

func (c *Client) Get(ctx context.Context, key string) (string, error) {
	out, err := c.api.Get(ctx, &kvstore.GetIn{Key: key})
	if err != nil {
		st, ok := status.FromError(err)
		if !ok {
			panic(err) // todo
		}
		switch st.Code() {
		case codes.Unauthenticated:
			return "", ErrUnauthenticated
		case codes.NotFound:
			return "", ErrNotFound
		default:
			return "", ErrIsNotLeader
		}
	}
	return out.Value, nil
}

func (c *Client) ConsistentGet(ctx context.Context, key string) (string, error) {
	out, err := c.api.ConsistentGet(ctx, &kvstore.GetIn{Key: key})
	if err != nil {
		st, ok := status.FromError(err)
		if !ok {
			panic(err) // todo
		}
		switch st.Code() {
		case codes.Unauthenticated:
			return "", ErrUnauthenticated
		case codes.NotFound:
			return "", ErrNotFound
		default:
			return "", ErrIsNotLeader
		}
	}
	return out.Value, nil
}

func (c *Client) CloseConn() error {
	return c.conn.Close()
}
