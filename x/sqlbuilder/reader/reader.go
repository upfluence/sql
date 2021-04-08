package reader

import (
	"context"

	"github.com/upfluence/sql"
	"github.com/upfluence/sql/x/sqlbuilder"
)

type PredicateClauseReducer func(...sqlbuilder.PredicateClause) sqlbuilder.PredicateClause

type Pagination struct {
	Offset int
	Limit  int
}

type ReadOptions struct {
	SelectClauses []sqlbuilder.Marker
	GroupByClause []sqlbuilder.Marker
	HavingClause  sqlbuilder.PredicateClause

	SkipPagination bool
	SkipOrdering   bool

	Consistency sql.Consistency
}

type Reader interface {
	// WithPredicateClauses: It will apply the given predicate clauses to the
	// SQL request in conjunction with the predicate clauses defined in the legacy
	// of the reader
	WithPredicateClauses(...sqlbuilder.PredicateClause) Reader

	// WithPagination: Overwrites the pagination setting with the attribute
	WithPagination(Pagination) Reader

	// WithOrdering: Overwrites the ordering setting with the attribute
	WithOrdering(sqlbuilder.OrderByClause) Reader

	Read(context.Context, ReadOptions) (sqlbuilder.Cursor, error)
}

func RootReader(q sql.Queryer, table string) Reader {
	return reader{
		pr: &rootReader{
			qb: &sqlbuilder.QueryBuilder{Queryer: q},
			t:  table,
			r:  sqlbuilder.And,
		},
	}
}

var (
	zeroPagination Pagination
	zeroOrdering   sqlbuilder.OrderByClause
)

type reader struct {
	pr parentReader
}

func (r reader) WithPredicateClauses(pcs ...sqlbuilder.PredicateClause) Reader {
	if len(pcs) == 0 {
		return r
	}

	return reader{pr: &withPredicatesReader{parentReader: r.pr, pcs: pcs}}
}

func (r reader) WithPagination(p Pagination) Reader {
	return reader{pr: &withPaginationReader{parentReader: r.pr, p: p}}
}

func (r reader) WithOrdering(obc sqlbuilder.OrderByClause) Reader {
	return reader{pr: &withOrderingReader{parentReader: r.pr, obc: obc}}
}

func (r reader) Read(ctx context.Context, opts ReadOptions) (sqlbuilder.Cursor, error) {
	stmt := sqlbuilder.SelectStatement{
		Table:         r.pr.table(),
		SelectClauses: opts.SelectClauses,
		GroupByClause: opts.GroupByClause,
		HavingClause:  opts.HavingClause,
		WhereClause:   r.pr.reducer()(r.pr.predicateClauses()...),
		Consistency:   opts.Consistency,
	}

	if p := r.pr.pagination(); !opts.SkipPagination && p != zeroPagination {
		stmt.Offset = sqlbuilder.NullableInt{Int: p.Offset, Valid: true}
		stmt.Limit = sqlbuilder.NullableInt{Int: p.Limit, Valid: true}
	}

	if o := r.pr.ordering(); !opts.SkipOrdering && o != zeroOrdering {
		stmt.OrderByClauses = []sqlbuilder.OrderByClause{o}
	}

	return r.pr.queryBuilder().PrepareSelect(stmt).Query(ctx, nil)
}

type parentReader interface {
	queryBuilder() *sqlbuilder.QueryBuilder
	table() string

	reducer() PredicateClauseReducer
	predicateClauses() []sqlbuilder.PredicateClause
	pagination() Pagination
	ordering() sqlbuilder.OrderByClause
}

type withPaginationReader struct {
	parentReader

	p Pagination
}

func (wpr *withPaginationReader) pagination() Pagination { return wpr.p }

type withPredicatesReader struct {
	parentReader

	pcs []sqlbuilder.PredicateClause
}

func (wpr *withPredicatesReader) predicateClauses() []sqlbuilder.PredicateClause {
	return append(
		append(
			[]sqlbuilder.PredicateClause{},
			wpr.parentReader.predicateClauses()...,
		),
		wpr.pcs...,
	)
}

type withOrderingReader struct {
	parentReader

	obc sqlbuilder.OrderByClause
}

func (wor *withOrderingReader) ordering() sqlbuilder.OrderByClause {
	return wor.obc
}

type rootReader struct {
	qb *sqlbuilder.QueryBuilder
	t  string
	r  PredicateClauseReducer
}

func (rr *rootReader) queryBuilder() *sqlbuilder.QueryBuilder { return rr.qb }

func (rr *rootReader) table() string                   { return rr.t }
func (rr *rootReader) reducer() PredicateClauseReducer { return rr.r }
func (rr *rootReader) pagination() Pagination          { return zeroPagination }

func (rr *rootReader) ordering() sqlbuilder.OrderByClause {
	return zeroOrdering
}

func (rr *rootReader) predicateClauses() []sqlbuilder.PredicateClause {
	return nil
}
