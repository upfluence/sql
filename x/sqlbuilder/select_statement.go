package sqlbuilder

import (
	"fmt"

	"github.com/upfluence/sql"
)

type NullableInt struct {
	Int   int
	Valid bool
}

type SelectStatement struct {
	Table string

	JoinClauses    []JoinClause
	OrderByClauses []OrderByClause
	SelectClauses  []Marker
	WhereClause    PredicateClause
	GroupByClause  []Marker
	HavingClause   PredicateClause

	Offset NullableInt
	Limit  NullableInt

	Consistency sql.Consistency
}

func (ss SelectStatement) Clone() SelectStatement {
	return SelectStatement{
		Table:          ss.Table,
		JoinClauses:    cloneJoinClauses(ss.JoinClauses),
		OrderByClauses: cloneOrderByClauses(ss.OrderByClauses),
		SelectClauses:  cloneMarkers(ss.SelectClauses),
		WhereClause:    clonePredicateClause(ss.WhereClause),
		GroupByClause:  cloneMarkers(ss.GroupByClause),
		HavingClause:   clonePredicateClause(ss.HavingClause),
		Offset:         ss.Offset,
		Limit:          ss.Limit,
		Consistency:	ss.Consistency,
	}
}

func (ss SelectStatement) buildQuery(vs map[string]interface{}) (string, []interface{}, []string, error) {
	var (
		qw       queryWriter
		bindings []string
	)

	if len(ss.SelectClauses) == 0 {
		return "", nil, nil, errNoMarkers
	}

	qw.WriteString("SELECT ")

	for i, c := range ss.SelectClauses {
		qw.WriteString(c.ToSQL())

		if i < len(ss.SelectClauses)-1 {
			qw.WriteString(", ")
		}

		bindings = append(bindings, c.Binding())
	}

	qw.WriteString(" FROM ")
	qw.WriteString(ss.Table)

	for _, jc := range ss.JoinClauses {
		jc.WriteTo(&qw, vs)
	}

	if wc := ss.WhereClause; wc != nil {
		qw.WriteString(" WHERE ")

		if err := wc.WriteTo(&qw, vs); err != nil {
			return "", nil, nil, err
		}
	}

	if len(ss.GroupByClause) > 0 {
		qw.WriteString(" GROUP BY ")

		for i, c := range ss.GroupByClause {
			qw.WriteString(c.ToSQL())

			if i < len(ss.GroupByClause)-1 {
				qw.WriteString(", ")
			}
		}
	}

	if hc := ss.HavingClause; hc != nil {
		qw.WriteString(" HAVING ")

		if err := hc.WriteTo(&qw, vs); err != nil {
			return "", nil, nil, err
		}
	}

	if len(ss.OrderByClauses) > 0 {
		qw.WriteString(" ORDER BY ")

		for i, c := range ss.OrderByClauses {
			qw.WriteString(c.ToSQL())

			if i < len(ss.OrderByClauses)-1 {
				qw.WriteString(", ")
			}
		}
	}

	if ss.Limit.Valid {
		fmt.Fprintf(&qw, " LIMIT %d", ss.Limit.Int)
	}

	if ss.Offset.Valid {
		fmt.Fprintf(&qw, " OFFSET %d", ss.Offset.Int)
	}

	if ss.Consistency != sql.EventuallyConsistent {
		qw.vs = append(qw.vs, ss.Consistency)
	}

	return qw.String(), qw.vs, bindings, nil
}
