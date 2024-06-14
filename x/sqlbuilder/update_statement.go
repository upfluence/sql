package sqlbuilder

import (
	"fmt"
	"io"
)

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

func writeUpdateClause(f Marker, qw QueryWriter, vs map[string]interface{}) error {
	if qs, ok := f.(QuerySegment); ok {
		return qs.WriteTo(qw, vs)
	}

	k := f.Binding()
	v, ok := vs[k]

	if !ok {
		return ErrMissingKey{Key: k}
	}

	_, err := io.WriteString(qw, qw.RedeemVariable(v))
	return err
}

func writeUpdateClauses(fs []Marker, qw QueryWriter, vs map[string]interface{}) error {
	for i, f := range fs {
		fmt.Fprintf(qw, "%s = ", columnName(f))

		if err := writeUpdateClause(f, qw, vs); err != nil {
			return err
		}

		if i < len(fs)-1 {
			io.WriteString(qw, ", ")
		}
	}

	return nil
}

func (us UpdateStatement) buildQuery(vs map[string]interface{}) (string, []interface{}, error) {
	var qw queryWriter

	if len(us.Fields) == 0 {
		return "", nil, errNoMarkers
	}

	fmt.Fprintf(&qw, "UPDATE %s SET ", us.Table)

	if err := writeUpdateClauses(us.Fields, &qw, vs); err != nil {
		return "", nil, err
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
