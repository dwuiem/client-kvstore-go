package kvstore

import (
	"client-kvstore-go/internal/grpc/client"
	"client-kvstore-go/internal/grpc/interceptors"
	"context"
	"errors"
	"fmt"
	"google.golang.org/grpc"
	"sync/atomic"
)

type KVStore struct {
	clients     []*client.Client
	leaderIndex atomic.Int32
}

func NewKVStoreClient(username, password string, addresses []string, opts []grpc.DialOption) (*KVStore, error) {
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
		interceptors.NewAuthInterceptor(username, password),
	))
	c := &KVStore{
		clients:     make([]*client.Client, len(addresses)),
		leaderIndex: atomic.Int32{},
	}
	for i, address := range addresses {
		newClient, err := client.NewClient(address, opts)
		if err != nil {
			return nil, fmt.Errorf("failed to connect to %s: %w", address, err)
		}
		c.clients[i] = newClient
	}
	c.leaderIndex.Store(0)
	return c, nil
}

func (store *KVStore) Close() error {
	for _, c := range store.clients {
		if err := c.CloseConn(); err != nil {
			return err
		}
	}
	return nil
}

func (store *KVStore) Put(ctx context.Context, key, value string, ttl int64) error {
	var err error
	executeErr := store.execute(func() error {
		leader := store.clients[store.leaderIndex.Load()]
		err = leader.Put(ctx, key, value, ttl)
		if errors.Is(err, client.ErrIsNotLeader) {
			return err
		}
		return nil
	})
	if executeErr != nil {
		return ErrNoAvailableLeader
	}
	return err
}

func (store *KVStore) Get(ctx context.Context, key string) (string, error) {
	var value string
	var err error

	executeErr := store.execute(func() error {
		leader := store.clients[store.leaderIndex.Load()]
		value, err = leader.Get(ctx, key)
		if errors.Is(err, client.ErrIsNotLeader) {
			return err
		}
		return nil
	})

	if executeErr != nil {
		return "", ErrNoAvailableLeader
	}
	return value, err
}

func (store *KVStore) ConsistentGet(ctx context.Context, key string) (string, error) {
	var value string
	var err error

	executeErr := store.execute(func() error {
		leader := store.clients[store.leaderIndex.Load()]
		value, err = leader.ConsistentGet(ctx, key)
		if errors.Is(err, client.ErrIsNotLeader) {
			return err
		}
		return nil
	})

	if executeErr != nil {
		return "", ErrNoAvailableLeader
	}

	return value, err
}

func (store *KVStore) Delete(ctx context.Context, key string) error {
	var err error
	executeErr := store.execute(func() error {
		leader := store.clients[store.leaderIndex.Load()]
		err = leader.Delete(ctx, key)
		if errors.Is(err, client.ErrIsNotLeader) {
			return err
		}
		return nil
	})

	if executeErr != nil {
		return ErrNoAvailableLeader
	}
	return err
}

func (store *KVStore) setLeader(new int) {
	old := store.leaderIndex.Load()
	store.leaderIndex.CompareAndSwap(old, int32(new))
}

func (store *KVStore) execute(action func() error) error {
	err := action()
	if err != nil {
		exLeader := store.leaderIndex.Load()
		for i := range store.clients {
			if i == int(exLeader) {
				continue
			}
			store.setLeader(i)
			err := action()
			if err != nil {
				continue
			}
			return nil
		}
		return ErrNoAvailableLeader
	}
	return nil
}
