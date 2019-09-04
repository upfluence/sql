package postgres

import (
	"context"
	"fmt"

	"github.com/upfluence/sql"
	"github.com/upfluence/sql/sqlparser"
)

type fakeResult int64

func (r fakeResult) LastInsertId() (int64, error) { return int64(r), nil }
func (fakeResult) RowsAffected() (int64, error)   { return 1, nil }

func NewDB(d sql.DB, p sqlparser.SQLParser) sql.DB {
	return &db{DB: d, e: &execer{q: d, p: p}}
}

type db struct {
	sql.DB

	e *execer
}

func (db *db) BeginTx(ctx context.Context) (sql.Tx, error) {
	cur, err := db.DB.BeginTx(ctx)

	if err != nil {
		return nil, err
	}

	return &tx{Tx: cur, e: &execer{q: cur, p: db.e.p}}, nil
}

func (db *db) Exec(ctx context.Context, q string, vs ...interface{}) (sql.Result, error) {
	return db.e.Exec(ctx, q, vs...)
}

type tx struct {
	sql.Tx

	e *execer
}

func (tx *tx) Exec(ctx context.Context, q string, vs ...interface{}) (sql.Result, error) {
	return tx.e.Exec(ctx, q, vs...)
}

type execer struct {
	q sql.Queryer
	p sqlparser.SQLParser
}

func (e *execer) Exec(ctx context.Context, q string, vs ...interface{}) (sql.Result, error) {
	if e.p.GetStatementType(q) != sqlparser.StmtInsert {
		return e.q.Exec(ctx, q, vs...)
	}

	var (
		args []interface{}
		ret  *sql.Returning
	)

	for _, v := range vs {
		if r, ok := v.(*sql.Returning); ok {
			ret = r
		} else {
			args = append(args, v)
		}
	}

	if ret != nil {
		var id int64

		if err := e.q.QueryRow(
			ctx,
			fmt.Sprintf("%s RETURNING %s", q, ret.Field),
			args...,
		).Scan(&id); err != nil {
			return nil, err
		}

		return fakeResult(id), nil
	}

	return e.q.Exec(ctx, q, vs...)
}
