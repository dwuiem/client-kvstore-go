package client_kvstore_go

import "errors"

var (
	ErrNotFound          = errors.New("value not found")
	ErrUsernameNotValid  = errors.New("username is empty")
	ErrPasswordNotValid  = errors.New("password is empty")
	ErrNoAddressProvided = errors.New("addresses are empty")
	ErrNoAvailableLeader = errors.New("no leader available")
)
