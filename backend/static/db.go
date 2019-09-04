package static

import (
	"context"
	"reflect"
	"testing"

	"github.com/upfluence/sql"
)

type Query struct {
	Query string
	Args  []interface{}
}

func (q Query) Assert(t *testing.T, stmt string, args ...interface{}) {
	if q.Query != stmt {
		t.Errorf("q.Query = %v, want %v", q.Query, stmt)
	}

	if len(q.Args) != len(args) {
		t.Errorf("len(q.Query) = %v, want %v", len(q.Args), len(args))
	}

	for i, got := range q.Args {
		if want := args[i]; !reflect.DeepEqual(got, want) {
			t.Errorf("q.Query[%d] = %v, want %v", i, got, want)
		}
	}
}

type Queryer struct {
	ExecQueries []Query
	ExecResult  sql.Result
	ExecErr     error

	QueryRowQueries []Query
	QueryRowScanner sql.Scanner

	QueryQueries []Query
	QueryScanner sql.Cursor
	QueryErr     error
}

type DB struct {
	Queryer

	Tx    sql.Tx
	TxErr error
}

type Tx struct {
	Queryer

	CommitErr, RollbackErr error
}

func (tx *Tx) Commit() error   { return tx.CommitErr }
func (tx *Tx) Rollback() error { return tx.RollbackErr }

func (db *DB) Driver() string                          { return "sqltest" }
func (db *DB) BeginTx(context.Context) (sql.Tx, error) { return db.Tx, db.TxErr }

func (q *Queryer) Exec(_ context.Context, stmt string, args ...interface{}) (sql.Result, error) {
	q.ExecQueries = append(q.ExecQueries, Query{stmt, args})
	return q.ExecResult, q.ExecErr
}

func (q *Queryer) QueryRow(_ context.Context, stmt string, args ...interface{}) sql.Scanner {
	q.QueryRowQueries = append(q.QueryRowQueries, Query{stmt, args})
	return q.QueryRowScanner
}

func (q *Queryer) Query(_ context.Context, stmt string, args ...interface{}) (sql.Cursor, error) {
	q.QueryQueries = append(q.QueryQueries, Query{stmt, args})
	return q.QueryScanner, q.QueryErr
}

type StaticResult struct {
	LastInsertIDRes, RowsAffectedRes   int64
	LastInsertedIDErr, RowsAffectedErr error
}

func (s *StaticResult) LastInsertId() (int64, error) {
	return s.LastInsertIDRes, s.LastInsertedIDErr
}

func (s *StaticResult) RowsAffected() (int64, error) {
	return s.RowsAffectedRes, s.RowsAffectedErr
}
