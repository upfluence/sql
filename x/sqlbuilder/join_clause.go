package sqlbuilder

import (
	"errors"
	"fmt"
	"io"
	"strings"
)

var errEmptyWhereClause = errors.New("x/sqlbuilder: where clause on join is empty")

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
		return errEmptyWhereClause
	}

	io.WriteString(w, " ON ")
	return jc.WhereClause.WriteTo(w, vs)
}

func cloneJoinClauses(jcs []JoinClause) []JoinClause {
	if len(jcs) == 0 {
		return nil
	}

	res := make([]JoinClause, len(jcs))

	for i, jc := range jcs {
		res[i] = JoinClause{
			Table:       jc.Table,
			Type:        jc.Type,
			WhereClause: jc.WhereClause.Clone(),
		}
	}

	return res
}
