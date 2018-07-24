package sqltest

import (
	"context"

	"github.com/upfluence/sql"
)

type Query struct {
	Query string
	Args  []interface{}
}

type StaticDB struct {
	ExecQueries []Query
	ExecResult  sql.Result
	ExecErr     error

	QueryRowQueries []Query
	QueryRowScanner sql.Scanner

	QueryQueries []Query
	QueryScanner sql.Cursor
	QueryErr     error
}

func (db *StaticDB) Exec(_ context.Context, q string, args ...interface{}) (sql.Result, error) {
	db.ExecQueries = append(db.ExecQueries, Query{q, args})
	return db.ExecResult, db.ExecErr
}

func (db *StaticDB) QueryRow(_ context.Context, q string, args ...interface{}) sql.Scanner {
	db.QueryRowQueries = append(db.QueryRowQueries, Query{q, args})
	return db.QueryRowScanner
}

func (db *StaticDB) Query(_ context.Context, q string, args ...interface{}) (sql.Cursor, error) {
	db.QueryQueries = append(db.QueryQueries, Query{q, args})
	return db.QueryScanner, db.QueryErr
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
