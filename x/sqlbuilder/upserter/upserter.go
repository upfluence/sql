package upserter

import (
	"context"
	"errors"

	"github.com/upfluence/sql"
	"github.com/upfluence/sql/x/sqlbuilder"
)

var (
	errNoQueryValues = errors.New("x/sqlbuilder: No QueryValue marker given")

	oneMarker = sqlbuilder.SQLExpression("one", "1")
)

type Statement struct {
	Table string

	QueryValues  []sqlbuilder.Marker
	InsertValues []sqlbuilder.Marker
	SetValues    []sqlbuilder.Marker

	Returning *sql.Returning
}

type UpsertStatement = Statement

type Upserter struct {
	sql.DB
}

func (u *Upserter) executeTx(ctx context.Context, fn func(sql.Queryer) error) error {
	return sql.ExecuteTx(
		ctx,
		u,
		// In order to avoid concurrent insert for the same "query values",
		// isolation level "serializable" is needed
		sql.TxOptions{Isolation: sql.LevelSerializable},
		fn,
	)
}

func (u *Upserter) PrepareUpsert(stmt Statement) sqlbuilder.Execer {
	return newExecer(u, stmt)
}

type queryerTxExecutor struct {
	sql.Queryer
}

func (qte *queryerTxExecutor) executeTx(ctx context.Context, fn func(sql.Queryer) error) error {
	switch err := fn(qte); err {
	case nil, sql.ErrRollback:
		return nil
	default:
		return err
	}
}

func InTxUpserter(q sql.Queryer, stmt Statement) sqlbuilder.Execer {
	return newExecer(&queryerTxExecutor{Queryer: q}, stmt)
}
