package sql

import (
	"context"
	"database/sql"
	"errors"
)

type IsolationLevel = sql.IsolationLevel

var ErrRollback = errors.New("sql: rollback sentinel")

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

func ExecuteTx(ctx context.Context, db DB, opts TxOptions, fn QueryerFunc) error {
	tx, err := db.BeginTx(ctx, opts)

	if err != nil {
		return err
	}

	switch err := fn(tx); err {
	case nil:
		return tx.Commit()
	case ErrRollback:
		tx.Rollback()
		return nil
	default:
		tx.Rollback()
		return err
	}
}
