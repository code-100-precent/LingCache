package storage

import "errors"

var (
	ErrKeyNotFound    = errors.New("key not found")
	ErrWrongType      = errors.New("wrong type")
	ErrKeyExists      = errors.New("key already exists")
	ErrInvalidDbIndex = errors.New("invalid database index")
)
