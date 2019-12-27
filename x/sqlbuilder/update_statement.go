package sqlbuilder

import "fmt"

type UpdateStatement struct {
	Table string

	Fields      []Marker
	WhereClause PredicateClause
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

		fmt.Fprintf(&qw, "%s = %s", f.ToSQL(), qw.RedeemVariable(v))

		if i < len(us.Fields)-1 {
			qw.WriteString(", ")
		}
	}

	if wc := us.WhereClause; wc != nil {
		qw.WriteString(" WHERE ")

		if err := wc.WriteTo(&qw, vs); err != nil {
			return "", nil, err
		}
	}

	return qw.String(), qw.vs, nil
}
