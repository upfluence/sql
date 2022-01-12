package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/lib/pq"

	"github.com/upfluence/sql"
	"github.com/upfluence/sql/sqlparser"
)

type db struct {
	*queryer

	db sql.DB
}

func NewDB(d sql.DB, p sqlparser.SQLParser) sql.DB {
	return &db{queryer: &queryer{q: d, p: p}, db: d}
}

func (db *db) Driver() string { return db.db.Driver() }

func (db *db) BeginTx(ctx context.Context, opts sql.TxOptions) (sql.Tx, error) {
	cur, err := db.db.BeginTx(ctx, opts)

	if err != nil {
		return nil, err
	}

	return &tx{queryer: &queryer{q: cur, p: db.p}, tx: cur}, nil
}

type tx struct {
	*queryer

	tx sql.Tx
}

func (tx *tx) Commit() error   { return tx.tx.Commit() }
func (tx *tx) Rollback() error { return tx.tx.Rollback() }

type queryer struct {
	q sql.Queryer
	p sqlparser.SQLParser
}

func (q *queryer) QueryRow(ctx context.Context, stmt string, vs ...interface{}) sql.Scanner {
	return &scanner{sc: q.q.QueryRow(ctx, stmt, vs...)}
}

func (q *queryer) Query(ctx context.Context, stmt string, vs ...interface{}) (sql.Cursor, error) {
	cur, err := q.q.Query(ctx, stmt, vs...)

	if err != nil {
		return nil, wrapErr(err)
	}

	return &cursor{Cursor: cur}, nil
}

func (q *queryer) Exec(ctx context.Context, stmt string, vs ...interface{}) (sql.Result, error) {
	if q.p.GetStatementType(stmt) != sqlparser.StmtInsert {
		res, err := q.q.Exec(ctx, stmt, vs...)
		return res, wrapErr(err)
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

		if err := q.q.QueryRow(
			ctx,
			fmt.Sprintf("%s RETURNING %s", stmt, ret.Field),
			args...,
		).Scan(&id); err != nil {
			return nil, wrapErr(err)
		}

		return sql.StaticResult(id), nil
	}

	res, err := q.q.Exec(ctx, stmt, vs...)
	return res, wrapErr(err)
}

type scanner struct {
	sc sql.Scanner
}

func (sc *scanner) Scan(vs ...interface{}) error {
	return wrapErr(sc.sc.Scan(vs...))
}

type cursor struct {
	sql.Cursor
}

func (c *cursor) Scan(vs ...interface{}) error {
	return wrapErr(c.Cursor.Scan(vs...))
}

func IsPostgresDB(d sql.DB) bool {
	_, ok := d.(*db)
	return ok
}

const (
	constraintClass = pq.ErrorClass("23")
	rollbackClass   = pq.ErrorClass("40")
)

func wrapErr(err error) error {
	if err == nil {
		return err
	}

	var pqErr *pq.Error

	if !errors.As(err, &pqErr) {
		return err
	}

	switch pqErr.Code.Class() {
	case constraintClass:
		return wrapConstraintErr(pqErr)
	case rollbackClass:
		return wrapRollbackError(pqErr)
	default:
		return err
	}
}

func wrapRollbackError(pqErr *pq.Error) error {
	var err = sql.RollbackError{Cause: pqErr}

	if pqErr.Code == pq.ErrorCode("40001") {
		err.Type = sql.SerializationFailure
	}

	return err
}

func wrapConstraintErr(pqErr *pq.Error) error {
	var err = sql.ConstraintError{Cause: pqErr}

	switch pqErr.Code {
	case pq.ErrorCode("23503"):
		err.Type = sql.ForeignKey
	case pq.ErrorCode("23502"):
		err.Type = sql.NotNull
	case pq.ErrorCode("23505"):
		if strings.HasSuffix(pqErr.Constraint, "_pkey") {
			err.Type = sql.PrimaryKey
		} else {
			err.Type = sql.Unique
		}
	}

	return err
}
