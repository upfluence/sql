package sqlbuilder

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/upfluence/errors"

	"github.com/upfluence/sql"
)

type QueryBuilder struct {
	sql.Queryer
}

func (qb *QueryBuilder) driver() string { return sql.DriverOf(qb.Queryer) }

func (qb *QueryBuilder) PrepareSelect(ss SelectStatement) *SelectQueryer {
	return &SelectQueryer{QueryBuilder: qb, Statement: ss}
}

func (qb *QueryBuilder) PrepareInsert(is InsertStatement) *InsertExecer {
	return &InsertExecer{
		execer:       execer{qb: qb, stmt: is},
		QueryBuilder: qb,
		Statement:    is,
	}
}

func (qb *QueryBuilder) PrepareUpdate(us UpdateStatement) *UpdateExecer {
	return &UpdateExecer{
		execer:       execer{qb: qb, stmt: us},
		QueryBuilder: qb,
		Statement:    us,
	}
}

func (qb *QueryBuilder) PrepareDelete(ds DeleteStatement) *DeleteExecer {
	return &DeleteExecer{
		execer:       execer{qb: qb, stmt: ds},
		QueryBuilder: qb,
		Statement:    ds,
	}
}

type statement interface {
	buildQuery(string, map[string]any) (string, []any, error)
}

type InsertExecer struct {
	execer

	QueryBuilder *QueryBuilder
	Statement    InsertStatement
}

type errScanner struct {
	error
}

func (es errScanner) Scan(...any) error {
	return es.error
}

func (ie *InsertExecer) QueryRow(ctx context.Context, qvs map[string]any) sql.Scanner {
	stmt := ie.Statement
	stmt.isQuery = true

	sstmt, vs, err := stmt.buildQuery(ie.qb.driver(), qvs)

	if err != nil {
		return errScanner{error: err}
	}

	return ie.qb.QueryRow(ctx, sstmt, vs...)
}

func (ie *InsertExecer) MultiExec(ctx context.Context, vvs []map[string]any, qvs map[string]any) (sql.Result, error) {
	stmt, vs, err := ie.Statement.buildQueries(ie.qb.driver(), vvs, qvs)

	if err != nil {
		return nil, err
	}

	return ie.qb.Exec(ctx, stmt, vs...)
}

// MultiQuery inserts all the given rows in a single statement. The returned
// cursor yields one row per inserted row, holding the Returning fields of the
// statement, in the order they are declared. Rows skipped by an ON CONFLICT
// clause are not part of it. The cursor must be drained or closed by the
// caller; without any Returning field it yields no row.
func (ie *InsertExecer) MultiQuery(ctx context.Context, vvs []map[string]any, qvs map[string]any) (Cursor, error) {
	stmt := ie.Statement
	stmt.isQuery = true

	sstmt, vs, err := stmt.buildQueries(ie.qb.driver(), vvs, qvs)

	if err != nil {
		return nil, err
	}

	rs := stmt.returnings()
	ks := make([]string, len(rs))

	for i, r := range rs {
		ks[i] = r.Field
	}

	cur, err := ie.qb.Query(ctx, sstmt, vs...)

	if err != nil {
		return nil, errors.WithStack(err)
	}

	return &cursor{sc: &scanner{sc: cur, ks: ks}, Cursor: cur}, nil
}

type UpdateExecer struct {
	execer

	QueryBuilder *QueryBuilder
	Statement    UpdateStatement
}

type DeleteExecer struct {
	execer

	QueryBuilder *QueryBuilder
	Statement    DeleteStatement
}

type SelectQueryer struct {
	QueryBuilder *QueryBuilder
	Statement    SelectStatement
}

func (sq *SelectQueryer) Query(ctx context.Context, qvs map[string]any) (Cursor, error) {
	stmt, vs, ks, err := sq.Statement.buildQuery(sq.QueryBuilder.driver(), qvs)

	if err != nil {
		return nil, err
	}

	cur, err := sq.QueryBuilder.Query(ctx, stmt, vs...)

	if err != nil {
		return nil, err
	}

	return &cursor{sc: &scanner{sc: cur, ks: ks}, Cursor: cur}, nil
}

func (sq *SelectQueryer) QueryRow(ctx context.Context, qvs map[string]any) Scanner {
	stmt, vs, ks, err := sq.Statement.buildQuery(sq.QueryBuilder.driver(), qvs)

	if err != nil {
		return ErrScanner{Err: err}
	}

	return &scanner{sc: sq.QueryBuilder.QueryRow(ctx, stmt, vs...), ks: ks}
}

type QueryWriter interface {
	io.Writer

	RedeemVariable(any) string

	// Driver returns the driver the query is built for, or an empty string
	// when the queryer of the QueryBuilder does not expose it.
	Driver() string
}

type queryWriter struct {
	strings.Builder

	i  int
	vs []any

	driver string
}

func (qw *queryWriter) Driver() string { return qw.driver }

func (qw *queryWriter) RedeemVariable(v any) string {
	qw.i++
	qw.vs = append(qw.vs, v)
	return fmt.Sprintf("$%d", qw.i)
}
