package internal

import (
	"client-kvstore-go/internal/interceptors"
	"errors"
	kvstore "github.com/HSE-RDBMS-course-work/kvstore-proto/gen/go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	store kvstore.KVStoreClient
	conn  *grpc.ClientConn
}

func New(username, password string, addresses []string) (*Client, error) {
	authInterceptor := interceptors.NewAuth(username, password)
	for _, address := range addresses {
		opts := []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithUnaryInterceptor(authInterceptor),
		}
		conn, err := grpc.NewClient(address, opts...)
		if err != nil {
			continue
		}
		return &Client{
			store: kvstore.NewKVStoreClient(conn),
			conn:  conn,
		}, nil
	}
	return nil, errors.New("error connecting to store")
}

func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
