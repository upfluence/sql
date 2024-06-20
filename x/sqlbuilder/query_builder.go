package sqlbuilder

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/upfluence/sql"
)

type QueryBuilder struct {
	sql.Queryer
}

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
	buildQuery(map[string]interface{}) (string, []interface{}, error)
}

type InsertExecer struct {
	execer

	QueryBuilder *QueryBuilder
	Statement    InsertStatement
}

func (ie *InsertExecer) MultiExec(ctx context.Context, vvs []map[string]interface{}, qvs map[string]interface{}) (sql.Result, error) {
	stmt, vs, err := ie.Statement.buildQueries(vvs, qvs)

	if err != nil {
		return nil, err
	}

	return ie.qb.Exec(ctx, stmt, vs...)
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

func (sq *SelectQueryer) Query(ctx context.Context, qvs map[string]interface{}) (Cursor, error) {
	stmt, vs, ks, err := sq.Statement.buildQuery(qvs)

	if err != nil {
		return nil, err
	}

	cur, err := sq.QueryBuilder.Query(ctx, stmt, vs...)

	if err != nil {
		return nil, err
	}

	return &cursor{sc: &scanner{sc: cur, ks: ks}, Cursor: cur}, nil
}

func (sq *SelectQueryer) QueryRow(ctx context.Context, qvs map[string]interface{}) Scanner {
	stmt, vs, ks, err := sq.Statement.buildQuery(qvs)

	if err != nil {
		return ErrScanner{Err: err}
	}

	return &scanner{sc: sq.QueryBuilder.QueryRow(ctx, stmt, vs...), ks: ks}
}

type QueryWriter interface {
	io.Writer

	RedeemVariable(interface{}) string
}

type queryWriter struct {
	strings.Builder

	i  int
	vs []interface{}
}

func (qw *queryWriter) RedeemVariable(v interface{}) string {
	qw.i++
	qw.vs = append(qw.vs, v)
	return fmt.Sprintf("$%d", qw.i)
}
