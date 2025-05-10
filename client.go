package client_kvstore_go

import (
	"context"
	kvstore "github.com/HSE-RDBMS-course-work/kvstore-proto/gen/go"
	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"time"
)

type client struct {
	api  kvstore.KVStoreClient
	conn *grpc.ClientConn
}

func newClient(address string, opts []grpc.DialOption) (*client, error) {
	conn, err := grpc.NewClient(address, opts...)
	if err != nil {
		return nil, err
	}
	return &client{
		api:  kvstore.NewKVStoreClient(conn),
		conn: conn,
	}, nil
}

func (n *client) closeConn() error {
	return n.conn.Close()
}

func (n *client) healthy() bool {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	healthClient := healthpb.NewHealthClient(n.conn)
	resp, err := healthClient.Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		return false
	}
	return resp.GetStatus() == healthpb.HealthCheckResponse_SERVING
}
