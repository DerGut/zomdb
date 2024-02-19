package zomdb

import (
	"context"
	"errors"
)

type DB struct{}

func New() (*DB, error) {
	return &DB{}, nil
}

func (d *DB) Close() error {
	return errors.New("not implemented")
}

func (d *DB) Get(context.Context, string) (string, error) {
	return "", errors.New("not implemented")
}

func (d *DB) Set(context.Context, string, string) error {
	return errors.New("not implemented")
}
