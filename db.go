package sql

import (
	"context"
	"database/sql"
)

type (
	Result = sql.Result

	NullInt64  = sql.NullInt64
	NullString = sql.NullString
	NullBool   = sql.NullBool
)

var (
	ErrConnDone = sql.ErrConnDone
	ErrNoRows   = sql.ErrNoRows
	ErrTxDone   = sql.ErrTxDone
)

type Option interface {
	IsSQLOption()
}

type Scanner interface {
	Scan(...any) error
}

type Queryer interface {
	Exec(context.Context, string, ...any) (Result, error)
	QueryRow(context.Context, string, ...any) Scanner
	Query(context.Context, string, ...any) (Cursor, error)
}

type DB interface {
	Queryer

	BeginTx(context.Context, TxOptions) (Tx, error)
	Driver() string
}

// DriverOf returns the driver of q, the DBs and the transactions they
// begin expose it. It returns an empty string when q does not.
func DriverOf(q Queryer) string {
	if d, ok := q.(interface{ Driver() string }); ok {
		return d.Driver()
	}

	return ""
}

type TxOptions struct {
	Isolation IsolationLevel
}

type Returning struct {
	Field string
}

func (Returning) IsSQLOption() {}

type Consistency uint8

func (Consistency) IsSQLOption() {}

const (
	EventuallyConsistent Consistency = iota
	StronglyConsistent
)

func StripOptions(vs []any) []any {
	var res []any

	for _, v := range vs {
		if _, ok := v.(Option); !ok {
			res = append(res, v)
		}
	}

	return res
}

type MiddlewareFactory interface {
	Wrap(DB) DB
}
