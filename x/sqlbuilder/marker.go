package sqlbuilder

import (
	"errors"
	"fmt"
	"strings"
)

var errNoMarkers = errors.New("x/sqlbuilder: No marker given to the statement")

type Marker interface {
	Binding() string
	ToSQL() string
	Clone() Marker
}

func Column(k string) Marker { return column(k) }

type column string

func (c column) ColumnName() string { return string(c) }
func (c column) Binding() string    { return string(c) }
func (c column) ToSQL() string      { return string(c) }
func (c column) Clone() Marker      { return c }

func SQLExpression(m, exp string) Marker { return sqlMarker{m: m, sql: exp} }

type sqlMarker struct {
	m   string
	sql string
}

func (sm sqlMarker) Binding() string { return sm.m }
func (sm sqlMarker) ToSQL() string   { return sm.sql }
func (sm sqlMarker) Clone() Marker   { return sm }

type columnWithTable struct {
	table   string
	column  string
	binding string
}

func (cwt columnWithTable) ColumnName() string { return cwt.column }
func (cwt columnWithTable) Binding() string    { return cwt.binding }
func (cwt columnWithTable) Clone() Marker      { return cwt }

func (cwt columnWithTable) ToSQL() string {
	return fmt.Sprintf("%q.%q", cwt.table, cwt.column)
}

func ColumnWithTable(b, t, c string) Marker {
	return columnWithTable{binding: b, table: t, column: c}
}

func cloneMarkers(ms []Marker) []Marker {
	if len(ms) == 0 {
		return nil
	}

	res := make([]Marker, len(ms))

	for i, m := range ms {
		res[i] = m.Clone()
	}

	return res
}

func columnName(m Marker) string {
	if cn, ok := m.(interface{ ColumnName() string }); ok {
		return cn.ColumnName()
	}

	return m.ToSQL()
}

func SQLFunction(m Marker, fn string, args ...string) Marker {
	return SQLExpression(
		m.Binding(),
		fmt.Sprintf(
			"%s(%s)",
			fn,
			strings.Join(append([]string{m.ToSQL()}, args...), ","),
		),
	)
}
