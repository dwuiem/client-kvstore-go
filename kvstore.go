package client_kvstore_go

import (
	"context"
	"errors"
	kvstore "github.com/HSE-RDBMS-course-work/kvstore-proto/gen/go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"sync/atomic"
	"time"
)

type KVStoreClient struct {
	clients []*client
	leader  atomic.Value
}

func NewKVStoreClient(username, password string, addresses []string) (*KVStoreClient, error) {
	if len(addresses) == 0 {
		return nil, errors.New("no addresses provided")
	}

	authInterceptor := newAuthInterceptor(username, password)
	c := &KVStoreClient{
		clients: make([]*client, len(addresses)),
		leader:  atomic.Value{},
	}
	for i, address := range addresses {
		opts := []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithUnaryInterceptor(authInterceptor),
		}
		newClient, err := newClient(address, opts)
		if err != nil {
			return nil, err
		}
		c.clients[i] = newClient
	}
	ldr, err := c.findNewLeader()
	if err != nil {
		return nil, err
	}
	c.leader.Store(ldr)
	return c, nil
}

// todo clear unavailable nodes, retries
// findNewLeader return a new leader of store
func (c *KVStoreClient) findNewLeader() (*client, error) {
	for _, client := range c.clients {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := client.api.Put(ctx, &kvstore.PutIn{
			Key:   "Key",
			Value: "Value",
			Ttl:   1,
		})
		cancel()

		if err != nil {
			st, ok := status.FromError(err)
			if !ok {
				panic(err) // TODO
			}
			switch st.Code() {
			case codes.Unavailable:
				_ = client.closeConn()
			}
		} else {
			return client, nil
		}
	}
	return nil, errors.New("no leader found")
}

func (c *KVStoreClient) updateLeader() error {
	newLeader, err := c.findNewLeader()
	if err != nil {
		return errors.New("error getting leader")
	}
	c.leader.Store(newLeader)
	return nil
}

func (c *KVStoreClient) getLeader() (*client, error) {
	ldr := c.leader.Load()
	if ldr == nil {
		err := c.updateLeader()
		if err != nil {
			return nil, err
		}
	}
	return ldr.(*client), nil
}

func (c *KVStoreClient) Close() error {
	for _, client := range c.clients {
		if err := client.closeConn(); err != nil {
			return err
		}
		client.api = nil
	}
	c.clients = nil
	return nil
}
