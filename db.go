// Package sql provides database abstraction for PostgreSQL and SQLite3.
//
// The package abstracts away backend-specific differences by providing unified
// interfaces for querying and transaction management. It supports advanced
// deployment topologies like master-slave replication and load balancing while
// maintaining a simple, familiar API similar to database/sql.
//
// # Backend Support
//
// The package is designed to work transparently with PostgreSQL and SQLite3.
// All database-specific behaviors (error handling, parameter placeholders,
// constraint mapping) are handled internally. Users write queries using the
// target database's native SQL dialect.
//
// # Databases with Replication
//
// When using master-slave setups, the package automatically routes write
// operations (INSERT, UPDATE, DELETE) to the master and read operations (SELECT)
// to slave replicas. Use the Consistency option to override this behavior:
//
//	// Force reads from master
//	row := db.QueryRow(ctx, query, arg, sql.StronglyConsistent)
//
// # Transaction Retry
//
// ExecuteTx automatically retries transactions that fail due to transient
// errors like serialization conflicts. This is particularly useful in high-
// concurrency environments where conflicts are expected and retries are safe.
//
// # Related Packages
//
// The x/sqlbuilder package provides type-safe query construction.
// The x/migration package handles database schema versioning.
package sql

import (
	"context"
	"database/sql"
)

type (
	// Result is the result of an Exec operation. It is a re-export of
	// database/sql.Result for convenience.
	Result = sql.Result

	// NullInt64 represents an int64 that may be null. It is a re-export of
	// database/sql.NullInt64 for convenience.
	NullInt64 = sql.NullInt64

	// NullString represents a string that may be null. It is a re-export of
	// database/sql.NullString for convenience.
	NullString = sql.NullString

	// NullBool represents a bool that may be null. It is a re-export of
	// database/sql.NullBool for convenience.
	NullBool = sql.NullBool
)

var (
	// ErrConnDone is exported from database/sql for convenience.
	ErrConnDone = sql.ErrConnDone

	// ErrNoRows is exported from database/sql for convenience.
	ErrNoRows = sql.ErrNoRows

	// ErrTxDone is exported from database/sql for convenience.
	ErrTxDone = sql.ErrTxDone
)

// Option configures the behavior of database operations.
// Options are used to specify non-standard behavior such as consistency levels
// or desired result columns (RETURNING clause).
type Option interface {
	IsSQLOption()
}

// Scanner is an interface for scanning values from result rows.
// It is a re-export of database/sql Scanner for convenience.
type Scanner interface {
	Scan(...interface{}) error
}

// Queryer is the interface that provides methods for executing queries.
type Queryer interface {
	// Exec executes a statement that doesn't return rows (INSERT, UPDATE, DELETE, etc).
	//
	//	result, err := db.Exec(ctx, "UPDATE users SET active = $1 WHERE id = $2", true, userID)
	Exec(context.Context, string, ...interface{}) (Result, error)

	// QueryRow executes a query expected to return a single row.
	// Errors are deferred until Scan is called.
	//
	//	var name string
	//	err := db.QueryRow(ctx, "SELECT name FROM users WHERE id = $1", userID).Scan(&name)
	QueryRow(context.Context, string, ...interface{}) Scanner

	// Query executes a query that may return multiple rows.
	//
	//	rows, err := db.Query(ctx, "SELECT id, name FROM users WHERE active = $1", true)
	//	if err != nil {
	//		return err
	//	}
	//	for rows.Next() {
	//		var id int
	//		var name string
	//		if err := rows.Scan(&id, &name); err != nil {
	//			return err
	//		}
	//		// process row
	//	}
	Query(context.Context, string, ...interface{}) (Cursor, error)
}

// DB is a database connection handle.
// It is safe for concurrent use by multiple goroutines.
type DB interface {
	Queryer

	// BeginTx starts a new transaction with the given options.
	BeginTx(context.Context, TxOptions) (Tx, error)

	// Driver returns the driver name (e.g., "postgres" or "sqlite3").
	Driver() string
}

// TxOptions configures a transaction.
type TxOptions struct {
	// Isolation sets the transaction isolation level.
	// If unset, the database default is used.
	Isolation IsolationLevel
}

// Returning is an Option that specifies columns to return from a write operation
// (INSERT or UPDATE). This is only supported by PostgreSQL.
type Returning struct {
	Field string
}

func (Returning) IsSQLOption() {}

// Consistency is an Option that specifies the consistency level for queries.
//
// In master-slave replication setups, this controls whether a query may be
// served from a slave replica or must use the master database.
type Consistency uint8

func (Consistency) IsSQLOption() {}

const (
	// EventuallyConsistent permits queries to be served from replicas.
	EventuallyConsistent Consistency = iota

	// StronglyConsistent requires queries to be served from the master.
	StronglyConsistent
)

// StripOptions returns a new slice with all Option values removed from args.
//
//	args := []interface{}{42, sql.StronglyConsistent, "text"}
//	clean := sql.StripOptions(args) // Returns []interface{}{42, "text"}
func StripOptions(vs []interface{}) []interface{} {
	var res []interface{}

	for _, v := range vs {
		if _, ok := v.(Option); !ok {
			res = append(res, v)
		}
	}

	return res
}

// MiddlewareFactory creates a middleware that wraps a DB.
// Middleware may be used for query logging, metrics collection, or tracing.
type MiddlewareFactory interface {
	// Wrap returns a wrapped DB that applies the middleware to all operations.
	Wrap(DB) DB
}
