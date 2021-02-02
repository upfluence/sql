package sqlbuilder

import "fmt"

type UpdateStatement struct {
	Table string

	Fields      []Marker
	WhereClause PredicateClause
}

func (us UpdateStatement) Clone() UpdateStatement {
	return UpdateStatement{
		Table:       us.Table,
		Fields:      cloneMarkers(us.Fields),
		WhereClause: clonePredicateClause(us.WhereClause),
	}
}

func (us UpdateStatement) buildQuery(vs map[string]interface{}) (string, []interface{}, error) {
	var qw queryWriter

	if len(us.Fields) == 0 {
		return "", nil, errNoMarkers
	}

	fmt.Fprintf(&qw, "UPDATE %s SET ", us.Table)

	for i, f := range us.Fields {
		k := f.Binding()
		v, ok := vs[k]

		if !ok {
			return "", nil, ErrMissingKey{Key: k}
		}

		fmt.Fprintf(&qw, "%s = %s", columnName(f), qw.RedeemVariable(v))

		if i < len(us.Fields)-1 {
			qw.WriteString(", ")
		}
	}

	if us.WhereClause == nil {
		return "", nil, ErrMissingPredicate
	}

	qw.WriteString(" WHERE ")

	if err := us.WhereClause.WriteTo(&qw, vs); err != nil {
		return "", nil, err
	}

	return qw.String(), qw.vs, nil
}
