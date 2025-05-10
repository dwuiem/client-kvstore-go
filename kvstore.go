package client_kvstore_go

import (
	"context"
	"errors"
	"fmt"
	kvstore "github.com/HSE-RDBMS-course-work/kvstore-proto/gen/go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"sync/atomic"
)

var errIsNotLeader = errors.New("not leader")

type KVStoreClient struct {
	clients     []*client
	leaderIndex atomic.Int32
}

func NewKVStoreClient(username, password string, addresses []string, opts []grpc.DialOption) (*KVStoreClient, error) {
	if len(username) == 0 {
		return nil, ErrUsernameNotValid
	}
	if len(password) == 0 {
		return nil, ErrPasswordNotValid
	}
	if len(addresses) == 0 {
		return nil, ErrNoAddressProvided
	}
	opts = append(opts, grpc.WithUnaryInterceptor(
		newAuthInterceptor(username, password),
	))
	c := &KVStoreClient{
		clients:     make([]*client, len(addresses)),
		leaderIndex: atomic.Int32{},
	}
	for i, address := range addresses {
		newClient, err := newClient(address, opts)
		if err != nil {
			return nil, fmt.Errorf("failed to connect to %s: %w", address, err)
		}
		c.clients[i] = newClient
	}
	c.leaderIndex.Store(0)
	return c, nil
}

func (c *KVStoreClient) execute(req func() error) error {
	err := req()
	if err != nil {
		except := c.leaderIndex.Load()
		for i := range c.clients {
			if i == int(except) {
				continue
			}
			c.setLeader(i)
			err := req()
			if err != nil {
				continue
			}
			return nil
		}
		return ErrNoAvailableLeader
	}
	return nil
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

func (c *KVStoreClient) setLeader(new int) {
	old := c.leaderIndex.Load()
	c.leaderIndex.CompareAndSwap(old, int32(new))
}

// todo handle errors

func (c *KVStoreClient) Put(ctx context.Context, key, value string, ttl int64) error {
	var err error
	executeErr := c.execute(func() error {
		err = c.tryPut(ctx, key, value, ttl)
		if errors.Is(err, errIsNotLeader) {
			return err
		}
		return nil
	})
	if executeErr != nil {
		return ErrNoAvailableLeader
	}
	return err
}

func (c *KVStoreClient) Get(ctx context.Context, key string) (string, error) {
	var value string
	var err error

	executeErr := c.execute(func() error {
		value, err = c.tryGet(ctx, key)
		if errors.Is(err, errIsNotLeader) {
			return err
		}
		return nil
	})

	if executeErr != nil {
		return "", ErrNoAvailableLeader
	}
	return value, err
}

func (c *KVStoreClient) ConsistentGet(ctx context.Context, key string) (string, error) {
	var value string
	var err error

	executeErr := c.execute(func() error {
		value, err = c.tryConsistentGet(ctx, key)
		if errors.Is(err, errIsNotLeader) {
			return err
		}
		return nil
	})

	if executeErr != nil {
		return "", ErrNoAvailableLeader
	}

	return value, err
}

func (c *KVStoreClient) Delete(ctx context.Context, key string) error {
	var err error
	executeErr := c.execute(func() error {
		err = c.tryDelete(ctx, key)
		if errors.Is(err, errIsNotLeader) {
			return err
		}
		return nil
	})

	if executeErr != nil {
		return ErrNoAvailableLeader
	}
	return err
}

func (c *KVStoreClient) tryPut(ctx context.Context, key, value string, ttl int64) error {
	client := c.clients[c.leaderIndex.Load()]
	_, err := client.api.Put(ctx, &kvstore.PutIn{
		Key:   key,
		Value: value,
		Ttl:   ttl,
	})
	if err != nil {
		_, ok := status.FromError(err)
		if !ok {
			panic(err) // todo
		}
		return errIsNotLeader
	}
	return nil
}

func (c *KVStoreClient) tryGet(ctx context.Context, key string) (string, error) {
	client := c.clients[c.leaderIndex.Load()]
	out, err := client.api.Get(ctx, &kvstore.GetIn{Key: key})
	if err != nil {
		st, ok := status.FromError(err)
		if !ok {
			panic(err) // todo
		}
		if st.Code() == codes.NotFound {
			return "", ErrNotFound
		}
		return "", errIsNotLeader
	}
	return out.Value, nil

}

func (c *KVStoreClient) tryConsistentGet(ctx context.Context, key string) (string, error) {
	client := c.clients[c.leaderIndex.Load()]
	out, err := client.api.ConsistentGet(ctx, &kvstore.GetIn{Key: key})
	if err != nil {
		st, ok := status.FromError(err)
		if !ok {
			panic(err) // todo
		}
		if st.Code() == codes.NotFound {
			return "", ErrNotFound
		}
		return "", errIsNotLeader
	}
	return out.Value, nil
}

func (c *KVStoreClient) tryDelete(ctx context.Context, key string) error {
	client := c.clients[c.leaderIndex.Load()]
	_, err := client.api.Delete(ctx, &kvstore.DeleteIn{Key: key})
	if err != nil {
		_, ok := status.FromError(err)
		if !ok {
			panic(err) // todo
		}
		return errIsNotLeader
	}
	return nil
}
