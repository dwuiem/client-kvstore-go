package client

import "errors"

var (
	ErrNotFound        = errors.New("value not found")
	ErrUnauthenticated = errors.New("not authenticated")
	ErrIsNotLeader     = errors.New("client is not leader")
)
