package sqlbuilder

import "fmt"

type DeleteStatement struct {
	Table string

	WhereClause PredicateClause
}

func (ds DeleteStatement) Clone() DeleteStatement {
	return DeleteStatement{
		Table:       ds.Table,
		WhereClause: clonePredicateClause(ds.WhereClause),
	}
}

func (ds DeleteStatement) buildQuery(vs map[string]interface{}) (string, []interface{}, error) {
	var qw queryWriter

	fmt.Fprintf(&qw, "DELETE FROM %s WHERE ", ds.Table)

	if ds.WhereClause == nil {
		return "", nil, ErrMissingPredicate
	}

	if err := ds.WhereClause.WriteTo(&qw, vs); err != nil {
		return "", nil, err
	}

	return qw.String(), qw.vs, nil
}
