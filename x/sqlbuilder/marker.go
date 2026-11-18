package sqlbuilder

import (
	"fmt"
	"io"
	"strings"

	"github.com/upfluence/errors"
)

var errNoMarkers = errors.New("No marker given to the statement")

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

// Excluded makes the DO UPDATE clause of an ON CONFLICT reference the value the
// row was proposed for insertion with, instead of binding a variable, so that
// every row of a multi row insert updates with its own value:
//
//	ON CONFLICT (id) DO UPDATE SET bar = EXCLUDED.bar
//
// Markers writing their own SQL are returned as they are.
func Excluded(m Marker) Marker {
	if _, ok := m.(QuerySegment); ok {
		return m
	}

	return excluded{Marker: m}
}

type excluded struct{ Marker }

func (e excluded) ColumnName() string { return ColumnName(e.Marker) }
func (e excluded) Clone() Marker      { return excluded{Marker: e.Marker.Clone()} }

func (e excluded) WriteTo(qw QueryWriter, _ map[string]any) error {
	_, err := fmt.Fprintf(qw, "EXCLUDED.%s", ColumnName(e.Marker))

	return errors.Wrap(err, "cant write the excluded column")
}

// ValueExpression makes the marker write the given SQL expression as its value
// instead of binding a variable, in the values of an INSERT as in the SET clause
// of an UPDATE or of an ON CONFLICT DO UPDATE:
//
//	INSERT INTO foo(bar) VALUES (CURRENT_TIMESTAMP)
//	UPDATE foo SET bar = CURRENT_TIMESTAMP
//
// No value has to be given for its binding.
func ValueExpression(m Marker, exp string) Marker {
	return valueExpression{Marker: m, exp: exp}
}

type valueExpression struct {
	Marker

	exp string
}

func (ve valueExpression) ColumnName() string { return ColumnName(ve.Marker) }

func (ve valueExpression) Clone() Marker {
	return valueExpression{Marker: ve.Marker.Clone(), exp: ve.exp}
}

func (ve valueExpression) WriteTo(qw QueryWriter, _ map[string]any) error {
	_, err := io.WriteString(qw, ve.exp)

	return errors.Wrap(err, "cant write the value expression")
}

// IsValueExpression reports whether the marker writes its value as a SQL
// expression, built with ValueExpression, instead of binding a variable.
func IsValueExpression(m Marker) bool {
	_, ok := m.(valueExpression)

	return ok
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

func ColumnName(m Marker) string {
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
