package sqlbuilder

import (
	"fmt"
	"strings"

	"github.com/upfluence/errors"
)

var errNoMarkers = errors.New("No marker given to the statement")

// Marker represents a SQL column or expression reference.
type Marker interface {
	Binding() string
	ToSQL() string
	Clone() Marker
}

// Column returns a marker for a column name.
//
//	stmt := sqlbuilder.PrepareSelect("users").Columns(
//		sqlbuilder.Column("id"),
//		sqlbuilder.Column("name"),
//	)
func Column(k string) Marker { return column(k) }

type column string

func (c column) ColumnName() string { return string(c) }
func (c column) Binding() string    { return string(c) }
func (c column) ToSQL() string      { return string(c) }
func (c column) Clone() Marker      { return c }

// SQLExpression returns a marker for a custom SQL expression.
//
//	stmt := sqlbuilder.PrepareSelect("users").Columns(
//		sqlbuilder.SQLExpression("user_id", "users.id"),
//		sqlbuilder.SQLExpression("full_name", "CONCAT(first_name, ' ', last_name)"),
//	)
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

// ColumnWithTable returns a marker for a qualified column reference (table.column).
//
//	stmt := sqlbuilder.PrepareSelect("users u").
//		Columns(
//			sqlbuilder.ColumnWithTable("user_id", "u", "id"),
//			sqlbuilder.ColumnWithTable("user_name", "u", "name"),
//		)
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

// SQLFunction returns a marker for a SQL function call.
//
//	stmt := sqlbuilder.PrepareSelect("users").Columns(
//		sqlbuilder.Column("id"),
//		sqlbuilder.SQLFunction(sqlbuilder.Column("created_at"), "DATE"),
//	)
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
