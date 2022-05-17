package upserter

import (
	"context"

	"github.com/upfluence/errors"

	"github.com/upfluence/sql"
	"github.com/upfluence/sql/x/sqlbuilder"
)

var (
	errNoQueryValues = errors.New("No QueryValue marker given")

	oneMarker = sqlbuilder.SQLExpression("one", "1")
)

const (
	Insert Mode = 1 << iota
	Update

	Upsert = Insert | Update
)

type Mode uint8

type Statement struct {
	Table string

	QueryValues  []sqlbuilder.Marker
	InsertValues []sqlbuilder.Marker
	SetValues    []sqlbuilder.Marker

	Returning *sql.Returning

	Mode Mode
}

func (s Statement) mode() Mode {
	if s.Mode == 0 {
		return Upsert
	}

	return s.Mode
}

type UpsertStatement = Statement

type Upserter struct {
	sql.DB

	ExecuteTxOptions []sql.ExecuteTxOption
}

func (u *Upserter) executeTx(ctx context.Context, fn func(sql.Queryer) error) error {
	return sql.ExecuteTx(
		ctx,
		u,
		// In order to avoid concurrent insert for the same "query values",
		// isolation level "serializable" is needed
		sql.TxOptions{Isolation: sql.LevelSerializable},
		fn,
		u.ExecuteTxOptions...,
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
