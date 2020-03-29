package sqlbuilder

import (
	"fmt"
	"strings"
)

type InsertStatement struct {
	Table string

	Fields []Marker
}

func (is InsertStatement) Clone() InsertStatement {
	return InsertStatement{Table: is.Table, Fields: cloneMarkers(is.Fields)}
}

func (is InsertStatement) buildQuery(qvs map[string]interface{}) (string, []interface{}, error) {
	var (
		b strings.Builder

		ks = make([]string, len(is.Fields))
		vs = make([]interface{}, len(is.Fields))
	)

	if len(is.Fields) == 0 {
		return "", nil, errNoMarkers
	}

	fmt.Fprintf(&b, "INSERT INTO %s(", is.Table)

	for i, f := range is.Fields {
		b.WriteString(f.ToSQL())

		if i < len(is.Fields)-1 {
			b.WriteString(", ")
		}

		ks[i] = f.Binding()
	}

	b.WriteString(") VALUES (")

	for i := range is.Fields {
		fmt.Fprintf(&b, "$%d", i+1)

		if i < len(is.Fields)-1 {
			b.WriteString(", ")
		}
	}

	b.WriteRune(')')

	for i, k := range ks {
		v, ok := qvs[k]

		if !ok {
			return "", nil, ErrMissingKey{Key: k}
		}

		vs[i] = v
	}

	return b.String(), vs, nil
}
