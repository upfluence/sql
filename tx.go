package sql

import (
	"context"
	"database/sql"

	"github.com/upfluence/errors"
)

var (
	// ErrRollback is a sentinel error returned by a transaction function to cause
	// the transaction to be rolled back without treating it as an error.
	ErrRollback = errors.New("rollback sentinel")

	// InfiniteRetry disables the retry limit for ExecuteTx.
	InfiniteRetry = -1

	defaultExecuteTxOptions = executeTxOptions{
		retryCount: InfiniteRetry,
		retryCheck: isRetryableError,
	}
)

// IsolationLevel is a transaction isolation level.
// It is exported from database/sql for convenience.
type IsolationLevel = sql.IsolationLevel

const (
	// LevelDefault is the database default isolation level.
	LevelDefault IsolationLevel = iota

	// LevelReadUncommitted is the weakest isolation level.
	LevelReadUncommitted

	// LevelReadCommitted prevents dirty reads.
	LevelReadCommitted

	// LevelWriteCommitted is between LevelReadCommitted and LevelRepeatableRead.
	LevelWriteCommitted

	// LevelRepeatableRead prevents dirty and non-repeatable reads.
	LevelRepeatableRead

	// LevelSnapshot is similar to LevelSerializable on some databases.
	LevelSnapshot

	// LevelSerializable is the strongest isolation level.
	LevelSerializable

	// LevelLinearizable is even stronger than LevelSerializable on some databases.
	LevelLinearizable
)

// Tx is a transaction handle.
// It provides the same query methods as DB and additionally supports committing
// and rolling back the transaction.
type Tx interface {
	Queryer

	// Commit commits the transaction.
	Commit() error

	// Rollback rolls back the transaction.
	// It is safe to call Rollback on a closed or already committed transaction.
	Rollback() error
}

// QueryerFunc is a function that executes transactional logic.
// If it returns a non-nil error, the transaction is rolled back.
// If it returns ErrRollback, the transaction is rolled back without error propagation.
type QueryerFunc func(Queryer) error

type executeTxOptions struct {
	retryCount int
	retryCheck func(error) bool
}

// ExecuteTxOption configures ExecuteTx behavior.
type ExecuteTxOption func(*executeTxOptions)

func (opts executeTxOptions) shouldRetry(i int) bool {
	if opts.retryCount == InfiniteRetry {
		return true
	}
	return i < opts.retryCount
}

func isRetryableError(err error) bool {
	var re RollbackError
	if !errors.As(err, &re) {
		return false
	}
	return re.Type == SerializationFailure || re.Type == Locked
}

// WithRetryCount sets the maximum number of retries for ExecuteTx.
// The default is InfiniteRetry (no limit).
//
//	err := sql.ExecuteTx(ctx, db, opts, fn, sql.WithRetryCount(3))
func WithRetryCount(i int) ExecuteTxOption {
	return func(opts *executeTxOptions) { opts.retryCount = i }
}

// WithCustomRetryCheck sets the error filter function for determining whether
// an error is retryable. It overrides the default filter.
//
//	err := sql.ExecuteTx(ctx, db, opts, fn,
//		sql.WithCustomRetryCheck(func(err error) bool {
//			return isTemporaryNetworkError(err)
//		}))
func WithCustomRetryCheck(fn func(error) bool) ExecuteTxOption {
	return func(opts *executeTxOptions) { opts.retryCheck = fn }
}

// ExecuteTx executes fn in a new transaction, automatically retrying if
// a retryable error occurs (serialization failure or lock timeout by default).
//
// If fn returns ErrRollback, the transaction is rolled back and nil is returned.
// Otherwise, if fn returns no error, the transaction is committed.
//
//	err := sql.ExecuteTx(ctx, db, sql.TxOptions{
//		Isolation: sql.LevelSerializable,
//	}, func(tx sql.Queryer) error {
//		result, err := tx.Exec(ctx, "INSERT INTO logs (msg) VALUES ($1)", msg)
//		if err != nil {
//			return err
//		}
//		_, err = result.RowsAffected()
//		return err
//	})
func ExecuteTx(ctx context.Context, db DB, txOpts TxOptions, fn QueryerFunc, exOpts ...ExecuteTxOption) error {
	var (
		i int

		opts = defaultExecuteTxOptions
	)

	for _, fn := range exOpts {
		fn(&opts)
	}

	for {
		tx, err := db.BeginTx(ctx, txOpts)

		if err != nil {
			return errors.Wrap(err, "cant begin the tx")
		}

		switch err := fn(tx); {
		case err == nil:
			err := tx.Commit()

			if !opts.retryCheck(err) || !opts.shouldRetry(i) {
				return errors.Wrap(err, "cant commit the tx")
			}

			i++
		case errors.Is(err, ErrRollback):
			tx.Rollback()
			return nil
		case opts.retryCheck(err):
			tx.Rollback()

			if !opts.shouldRetry(i) {
				return err
			}

			i++
		default:
			tx.Rollback()
			return err
		}
	}
}
