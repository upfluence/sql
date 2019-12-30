package sqlbuilder

import (
	"errors"
	"fmt"
)

var errNoMarkers = errors.New("x/sqlbuilder: No marker given to the statement")

type Marker interface {
	Binding() string
	ToSQL() string
}

func Column(k string) Marker { return column(k) }

type column string

func (c column) Binding() string { return string(c) }
func (c column) ToSQL() string   { return string(c) }

func SQLExpression(m, exp string) Marker { return sqlMarker{m: m, sql: exp} }

type sqlMarker struct {
	m   string
	sql string
}

func (sm sqlMarker) Binding() string { return sm.m }
func (sm sqlMarker) ToSQL() string   { return sm.sql }

func ColumnWithTable(b, t, c string) Marker {
	return sqlMarker{m: b, sql: fmt.Sprintf("%q.%q", t, c)}
}
