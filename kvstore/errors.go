package kvstore

import (
	"client-kvstore-go/internal/grpc/client"
	"errors"
)

var (
	ErrUsernameNotValid  = errors.New("username is empty")
	ErrPasswordNotValid  = errors.New("password is empty")
	ErrNoAddressProvided = errors.New("addresses are empty")
	ErrNoAvailableLeader = errors.New("no leader available")
	ErrValueNotFound     = client.ErrNotFound
	ErrUnauthenticated   = client.ErrUnauthenticated
)
