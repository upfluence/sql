package simple

import (
	"context"
	stdsql "database/sql"

	"github.com/upfluence/sql"
)

type stdQueryer interface {
	ExecContext(context.Context, string, ...interface{}) (stdsql.Result, error)
	QueryRowContext(context.Context, string, ...interface{}) *stdsql.Row
	QueryContext(context.Context, string, ...interface{}) (*stdsql.Rows, error)
}

type queryer struct {
	stdQueryer
}

func (q *queryer) Exec(ctx context.Context, qry string, vs ...interface{}) (sql.Result, error) {
	return q.ExecContext(ctx, qry, sql.StripOptions(vs)...)
}

func (q *queryer) QueryRow(ctx context.Context, qry string, vs ...interface{}) sql.Scanner {
	return q.QueryRowContext(ctx, qry, sql.StripOptions(vs)...)
}

func (q *queryer) Query(ctx context.Context, qry string, vs ...interface{}) (sql.Cursor, error) {
	return q.QueryContext(ctx, qry, sql.StripOptions(vs)...)
}
