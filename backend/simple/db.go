package simple

import (
	"context"
	stdsql "database/sql"

	"github.com/upfluence/sql"
)

func NewDB(driver, uri string) (sql.DB, error) {
	var plainDB, err = stdsql.Open(driver, uri)

	if err != nil {
		return nil, err
	}

	return &db{plainDB}, nil
}

type db struct {
	*stdsql.DB
}

func stripReturningFields(vs []interface{}) []interface{} {
	var res []interface{}

	for _, v := range vs {
		if _, ok := v.(*sql.Returning); !ok {
			res = append(res, v)
		}
	}

	return res
}

func (d *db) Exec(ctx context.Context, q string, vs ...interface{}) (sql.Result, error) {
	return d.ExecContext(ctx, q, stripReturningFields(vs)...)
}

func (d *db) QueryRow(ctx context.Context, q string, vs ...interface{}) sql.Scanner {
	return d.QueryRowContext(ctx, q, vs...)
}

func (d *db) Query(ctx context.Context, q string, vs ...interface{}) (sql.Cursor, error) {
	return d.QueryContext(ctx, q, vs...)
}
