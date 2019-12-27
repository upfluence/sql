package sqlbuilder

import (
	"context"

	"github.com/upfluence/sql"
)

type QueryBuilder struct {
	sql.Queryer
}

func (qb *QueryBuilder) PrepareSelect(ss SelectStatement) Queryer {
	return &selectQueryer{qb: qb, ss: ss}
}

func (qb *QueryBuilder) PrepareInsert(is InsertStatement) Execer {
	return &execer{qb: qb, stmt: is}
}

func (qb *QueryBuilder) PrepareUpdate(us UpdateStatement) Execer {
	return &execer{qb: qb, stmt: us}
}

func (qb *QueryBuilder) PrepareDelete(ds DeleteStatement) Execer {
	return &execer{qb: qb, stmt: ds}
}

type Execer interface {
	Exec(context.Context, map[string]interface{}) (sql.Result, error)
}

type statement interface {
	buildQuery(map[string]interface{}) (string, []interface{}, error)
}

type execer struct {
	qb   *QueryBuilder
	stmt statement
}

func (e *execer) Exec(ctx context.Context, qvs map[string]interface{}) (sql.Result, error) {
	stmt, vs, err := e.stmt.buildQuery(qvs)

	if err != nil {
		return nil, err
	}

	return e.qb.Exec(ctx, stmt, vs...)
}

type Scanner interface {
	Scan(map[string]interface{}) error
}

type Cursor interface {
	Scanner

	Close() error
	Err() error
	Next() bool
}

type Queryer interface {
	Query(context.Context, map[string]interface{}) (Cursor, error)
	QueryRow(context.Context, map[string]interface{}) Scanner
}

type selectQueryer struct {
	qb *QueryBuilder
	ss SelectStatement
}

func (sq *selectQueryer) Query(ctx context.Context, qvs map[string]interface{}) (Cursor, error) {
	stmt, vs, ks, err := sq.ss.buildQuery(qvs)

	if err != nil {
		return nil, err
	}

	cur, err := sq.qb.Query(ctx, stmt, vs...)

	if err != nil {
		return nil, err
	}

	return &cursor{sc: &scanner{sc: cur, ks: ks}, Cursor: cur}, nil
}

func (sq *selectQueryer) QueryRow(ctx context.Context, qvs map[string]interface{}) Scanner {
	stmt, vs, ks, err := sq.ss.buildQuery(qvs)

	if err != nil {
		return errScanner{err}
	}

	return &scanner{sc: sq.qb.QueryRow(ctx, stmt, vs...), ks: ks}
}

type cursor struct {
	sql.Cursor

	sc Scanner
}

func (c *cursor) Scan(vs map[string]interface{}) error {
	return c.sc.Scan(vs)
}

type scanner struct {
	sc sql.Scanner
	ks []string
}

type errScanner struct{ error }

func (es errScanner) Scan(map[string]interface{}) error { return es.error }

func (sc *scanner) Scan(vs map[string]interface{}) error {
	var svs = make([]interface{}, len(sc.ks))

	for i, k := range sc.ks {
		v, ok := vs[k]

		if !ok {
			return ErrMissingKey{Key: k}
		}

		svs[i] = v
	}

	return sc.sc.Scan(svs...)
}
