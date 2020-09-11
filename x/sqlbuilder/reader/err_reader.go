package reader

import (
	"context"

	"github.com/upfluence/sql/x/sqlbuilder"
)

type ErrReader struct {
	Err error
}

func (er ErrReader) WithPredicateClauses(...sqlbuilder.PredicateClause) Reader {
	return er
}

func (er ErrReader) WithPagination(Pagination) Reader {
	return er
}

func (er ErrReader) WithOrdering(sqlbuilder.OrderByClause) Reader {
	return er
}

func (er ErrReader) Read(context.Context, ReadOptions) (sqlbuilder.Cursor, error) {
	return nil, er.Err
}
