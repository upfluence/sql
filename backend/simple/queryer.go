package simple

import (
	"context"
	stdsql "database/sql"

	"github.com/upfluence/sql"
)

type stdQueryer interface {
	ExecContext(context.Context, string, ...any) (stdsql.Result, error)
	QueryRowContext(context.Context, string, ...any) *stdsql.Row
	QueryContext(context.Context, string, ...any) (*stdsql.Rows, error)
}

type queryer struct {
	q stdQueryer
}

func (q *queryer) Exec(ctx context.Context, qry string, vs ...any) (sql.Result, error) {
	return q.q.ExecContext(ctx, qry, sql.StripOptions(vs)...)
}

func (q *queryer) QueryRow(ctx context.Context, qry string, vs ...any) sql.Scanner {
	return q.q.QueryRowContext(ctx, qry, sql.StripOptions(vs)...)
}

func (q *queryer) Query(ctx context.Context, qry string, vs ...any) (sql.Cursor, error) {
	return q.q.QueryContext(ctx, qry, sql.StripOptions(vs)...)
}
