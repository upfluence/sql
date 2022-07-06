package sqlite3

import (
	"context"
	"regexp"
	"strconv"
	"strings"

	"github.com/mattn/go-sqlite3"
	"github.com/upfluence/errors"

	"github.com/upfluence/sql"
)

var (
	argRegexp = regexp.MustCompile(`\$\d+`)

	ErrInvalidArgsNumber = errors.New("invalid arg number")
)

type db struct {
	*queryer

	db sql.DB
}

func NewDB(d sql.DB) sql.DB {
	return &db{queryer: &queryer{q: d}, db: d}
}

func (db *db) BeginTx(ctx context.Context, opts sql.TxOptions) (sql.Tx, error) {
	dtx, err := db.db.BeginTx(ctx, opts)

	if err != nil {
		return nil, wrapErr(err)
	}

	return &tx{queryer: &queryer{q: dtx}, tx: dtx}, nil
}

type tx struct {
	*queryer

	tx sql.Tx
}

func (tx *tx) Commit() error   { return wrapErr(tx.tx.Commit()) }
func (tx *tx) Rollback() error { return wrapErr(tx.tx.Rollback()) }

func (db *db) Driver() string { return db.db.Driver() }

type queryer struct {
	q sql.Queryer
}

func (q *queryer) Exec(ctx context.Context, stmt string, vs ...interface{}) (sql.Result, error) {
	stmt, vs, err := q.rewrite(stmt, vs)

	if err != nil {
		return nil, err
	}

	res, err := q.q.Exec(ctx, stmt, vs...)

	return res, wrapErr(err)
}

func (q *queryer) QueryRow(ctx context.Context, stmt string, vs ...interface{}) sql.Scanner {
	stmt, vs, err := q.rewrite(stmt, vs)

	if err != nil {
		return errScanner{err}
	}

	return &scanner{sc: q.q.QueryRow(ctx, stmt, vs...)}
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

type errScanner struct {
	error
}

func (es errScanner) Scan(...interface{}) error { return es.error }

func (q *queryer) Query(ctx context.Context, stmt string, vs ...interface{}) (sql.Cursor, error) {
	stmt, vs, err := q.rewrite(stmt, vs)

	if err != nil {
		return nil, err
	}

	cur, err := q.q.Query(ctx, stmt, vs...)

	return &cursor{Cursor: cur}, wrapErr(err)
}

func (q *queryer) rewrite(stmt string, vs []interface{}) (string, []interface{}, error) {
	var (
		args = make(map[int]int)

		i int
	)

	vs = sql.StripOptions(vs)

	rstmt := argRegexp.ReplaceAllStringFunc(stmt, func(frag string) string {
		v, err := strconv.Atoi(strings.TrimPrefix(frag, "$"))

		if err != nil {
			panic(err)
		}

		args[v] = i
		i++

		return "?"
	})

	if len(vs) != len(args) {
		return "", nil, ErrInvalidArgsNumber
	}

	rvs := make([]interface{}, len(vs))

	for k, i := range args {
		if k > len(rvs) {
			return "", nil, ErrInvalidArgsNumber
		}

		rvs[i] = vs[k-1]
	}

	return rstmt, rvs, nil
}

func wrapErr(err error) error {
	if err == nil {
		return nil
	}

	var sqlErr sqlite3.Error

	if !errors.As(err, &sqlErr) {
		return err
	}

	switch sqlErr.Code {
	case sqlite3.ErrConstraint:
		return wrapConstraintError(sqlErr)
	case sqlite3.ErrLocked:
		return sql.RollbackError{Cause: err, Type: sql.Locked}
	default:
		return err
	}
}

func parseConstraintName(msg string) string {
	vs := strings.Split(msg, "constraint failed: ")

	if len(vs) != 2 {
		return ""
	}

	vs = strings.Split(vs[1], ".")

	if len(vs) != 2 {
		return ""
	}

	return vs[1]
}

func wrapConstraintError(sqlErr sqlite3.Error) sql.ConstraintError {
	err := sql.ConstraintError{
		Cause:      sqlErr,
		Constraint: parseConstraintName(sqlErr.Error()),
	}

	switch sqlErr.ExtendedCode {
	case sqlite3.ErrConstraintPrimaryKey:
		err.Type = sql.PrimaryKey
	case sqlite3.ErrConstraintForeignKey:
		err.Type = sql.ForeignKey
	case sqlite3.ErrConstraintNotNull:
		err.Type = sql.NotNull
	case sqlite3.ErrConstraintUnique:
		err.Type = sql.Unique
	}

	return err
}

func IsSQLite3DB(d sql.DB) bool {
	_, ok := d.(*db)
	return ok
}
