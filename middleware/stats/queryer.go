package stats

import (
	"context"

	"github.com/upfluence/sql"
)

type queryer struct {
	sql.Queryer

	instruments instruments
}

func (q *queryer) Exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	operation := q.instruments.exec.Begin()
	res, err := q.Queryer.Exec(ctx, query, args...)
	operation.Finish(err)

	if err == nil && res != nil {
		rowsAffected, rowsAffectedErr := res.RowsAffected()

		if rowsAffectedErr == nil {
			q.instruments.rowsAffected.Add(rowsAffected)
		}
	}

	return res, err //nolint:wrapcheck
}

func (q *queryer) QueryRow(ctx context.Context, query string, args ...any) sql.Scanner {
	operation := q.instruments.queryRow.Begin()

	return &scanner{
		Scanner:   q.Queryer.QueryRow(ctx, query, args...),
		operation: operation,
	}
}

func (q *queryer) Query(ctx context.Context, query string, args ...any) (sql.Cursor, error) {
	operation := q.instruments.query.Begin()
	rawCursor, err := q.Queryer.Query(ctx, query, args...)

	if err != nil {
		operation.Finish(err)

		return rawCursor, err //nolint:wrapcheck
	}

	return &cursor{
		Cursor:      rawCursor,
		operation:   operation,
		rowsScanned: q.instruments.rowsScanned,
	}, nil
}
