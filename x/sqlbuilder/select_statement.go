package sqlbuilder

import (
	"fmt"
	"io"
	"strings"
)

type SelectStatement struct {
	Table string

	JoinClauses   []JoinClause
	SelectClauses []Marker
	WhereClause   PredicateClause
	GroupByClause []Marker
	HavingClause  PredicateClause
}

type JoinType string

const (
	DefaultJoin JoinType = ""
	InnerJoin   JoinType = "INNER"
	OuterJoin   JoinType = "OUTER"
)

type JoinClause struct {
	Table string
	Type  JoinType

	WhereClause PredicateClause
}

func (jc JoinClause) WriteTo(w QueryWriter, vs map[string]interface{}) error {
	fmt.Fprintf(w, " %s JOIN %s", strings.ToUpper(string(jc.Type)), jc.Table)

	if jc.WhereClause == nil {
		return nil
	}

	io.WriteString(w, " ON ")
	return jc.WhereClause.WriteTo(w, vs)
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

	return qw.String(), qw.vs, bindings, nil
}
