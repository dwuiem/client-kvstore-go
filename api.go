package client_kvstore_go

import (
	"context"
	kvstore "github.com/HSE-RDBMS-course-work/kvstore-proto/gen/go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// todo handle errors, avoid recursion

func (c *KVStoreClient) Put(ctx context.Context, key, value string, ttl int64) error {
	client, err := c.getLeader()
	if err != nil {
		return err
	}
	_, err = client.api.Put(ctx, &kvstore.PutIn{
		Key:   key,
		Value: value,
		Ttl:   ttl,
	})
	if err != nil {
		st, ok := status.FromError(err)
		if !ok {
			panic(err) // todo
		}
		switch st.Code() {
		case codes.FailedPrecondition:
			err := c.updateLeader()
			if err != nil {
				return err
			}
			return c.Put(ctx, key, value, ttl)
		case codes.Unavailable:
			_ = client.closeConn() // todo
		case codes.Internal:
			panic("internal error in client")
		}
		return err
	}
	return nil
}

func (c *KVStoreClient) Get(ctx context.Context, key string) (string, error) {
	client, err := c.getLeader()
	if err != nil {
		return "", err
	}
	out, err := client.api.Get(ctx, &kvstore.GetIn{Key: key})
	if err != nil {
		st, ok := status.FromError(err)
		if !ok {
			panic(err) // todo
		}
		switch st.Code() {
		case codes.FailedPrecondition:
			err := c.updateLeader()
			if err != nil {
				return "", err
			}
			return c.Get(ctx, key)
		case codes.Unavailable:
			_ = client.closeConn() // todo
		case codes.Internal:
			panic("internal error in client") // todo
		case codes.NotFound:
			return "", ErrNotFound
		}
		return "", err
	}
	return out.Value, nil
}

func (c *KVStoreClient) ConsistentGet(ctx context.Context, key string) (string, error) {
	client, err := c.getLeader()
	if err != nil {
		return "", err
	}
	out, err := client.api.ConsistentGet(ctx, &kvstore.GetIn{Key: key})
	if err != nil {
		st, ok := status.FromError(err)
		if !ok {
			panic(err) // todo
		}
		switch st.Code() {
		case codes.FailedPrecondition:
			err := c.updateLeader()
			if err != nil {
				return "", err
			}
			return c.Get(ctx, key)
		case codes.Unavailable:
			_ = client.closeConn() // todo
		case codes.Internal:
			panic("internal error in client") // todo

		case codes.NotFound:
			return "", ErrNotFound
		}
		return "", err
	}
	return out.Value, nil
}

func (c *KVStoreClient) Delete(ctx context.Context, key string) error {
	client, err := c.getLeader()
	if err != nil {
		return err
	}
	_, err = client.api.Get(ctx, &kvstore.GetIn{Key: key})
	if err != nil {
		st, ok := status.FromError(err)
		if !ok {
			panic(err) // todo
		}
		switch st.Code() {
		case codes.FailedPrecondition:
			err := c.updateLeader()
			if err != nil {
				return err
			}
			return c.Delete(ctx, key)
		case codes.Unavailable:
			_ = client.closeConn() // todo
		case codes.Internal:
			panic("internal error in client")
		}
		return err
	}
	return nil
}
