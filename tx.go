package sql

import (
	"context"
	"database/sql"

	"github.com/upfluence/errors"
)

var (
	ErrRollback = errors.New("rollback sentinel")

	InfiniteRetry = -1

	defaultExecuteTxOptions = executeTxOptions{
		retryCount: InfiniteRetry,
		retryCheck: isRetryableError,
	}
)

type IsolationLevel = sql.IsolationLevel

const (
	LevelDefault IsolationLevel = iota
	LevelReadUncommitted
	LevelReadCommitted
	LevelWriteCommitted
	LevelRepeatableRead
	LevelSnapshot
	LevelSerializable
	LevelLinearizable
)

type Tx interface {
	Queryer

	Commit() error
	Rollback() error
}

type QueryerFunc func(Queryer) error

type executeTxOptions struct {
	retryCount int
	retryCheck func(error) bool
}

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

func WithCustomRetryCheck(fn func(error) bool) ExecuteTxOption {
	return func(opts *executeTxOptions) { opts.retryCheck = fn }
}

func WithRetryCount(i int) ExecuteTxOption {
	return func(opts *executeTxOptions) { opts.retryCount = i }
}

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
			return errors.Wrap(tx.Commit(), "cant commit the tx")
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
