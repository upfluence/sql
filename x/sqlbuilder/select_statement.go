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

type QueryWriter interface {
	io.Writer

	RedeemVariable(interface{}) string
}

type selectQueryWriter struct {
	strings.Builder

	i  int
	vs []interface{}
}

func (sqw *selectQueryWriter) RedeemVariable(v interface{}) string {
	sqw.i++
	sqw.vs = append(sqw.vs, v)
	return fmt.Sprintf("$%d", sqw.i)
}

func (ss SelectStatement) buildQuery(vs map[string]interface{}) (string, []interface{}, []string, error) {
	var (
		sqw      selectQueryWriter
		bindings []string
	)

	sqw.WriteString("SELECT ")

	for i, c := range ss.SelectClauses {
		sqw.WriteString(c.ToSQL())

		if i < len(ss.SelectClauses)-1 {
			sqw.WriteString(", ")
		}

		bindings = append(bindings, c.Binding())
	}

	sqw.WriteString(" FROM ")
	sqw.WriteString(ss.Table)

	for _, jc := range ss.JoinClauses {
		jc.WriteTo(&sqw, vs)
	}

	if wc := ss.WhereClause; wc != nil {
		sqw.WriteString(" WHERE ")

		if err := wc.WriteTo(&sqw, vs); err != nil {
			return "", nil, nil, err
		}
	}

	if len(ss.GroupByClause) > 0 {
		sqw.WriteString(" GROUP BY ")

		for i, c := range ss.GroupByClause {
			sqw.WriteString(c.ToSQL())

			if i < len(ss.GroupByClause)-1 {
				sqw.WriteString(", ")
			}
		}
	}

	if hc := ss.HavingClause; hc != nil {
		sqw.WriteString(" HAVING ")

		if err := hc.WriteTo(&sqw, vs); err != nil {
			return "", nil, nil, err
		}
	}

	return sqw.String(), sqw.vs, bindings, nil
}
