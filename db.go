package sql

import (
	"context"
	"database/sql"
)

type Result sql.Result

type Scanner interface {
	Scan(...interface{}) error
}

type Queryer interface {
	Exec(context.Context, string, ...interface{}) (Result, error)
	QueryRow(context.Context, string, ...interface{}) Scanner
	Query(context.Context, string, ...interface{}) (Cursor, error)
}

type Tx interface {
	Queryer

	Commit() error
	Rollback() error
}

type DB interface {
	Queryer

	BeginTx(context.Context) (Tx, error)
	Driver() string
}

type Returning struct {
	Field string
}

type MiddlewareFactory interface {
	Wrap(DB) DB
}
