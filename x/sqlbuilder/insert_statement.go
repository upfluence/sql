package sqlbuilder

import (
	"fmt"
	"io"

	"github.com/upfluence/sql"
)

type OnConflictTarget struct {
	Fields []Marker
	Where  PredicateClause
}

func (oct *OnConflictTarget) Clone() *OnConflictTarget {
	if oct == nil {
		return nil
	}

	var w PredicateClause

	if oct.Where != nil {
		w = oct.Where.Clone()
	}

	return &OnConflictTarget{
		Fields: cloneMarkers(oct.Fields),
		Where:  w,
	}
}

func (oct *OnConflictTarget) WriteTo(qw QueryWriter, vs map[string]interface{}) error {
	io.WriteString(qw, "(")

	for i, f := range oct.Fields {
		io.WriteString(qw, columnName(f))

		if i < len(oct.Fields)-1 {
			io.WriteString(qw, ", ")
		}
	}

	io.WriteString(qw, ")")

	if oct.Where != nil {
		io.WriteString(qw, " WHERE ")

		return oct.Where.WriteTo(qw, vs)
	}

	return nil
}

type OnConflictAction interface {
	isOnConflictAction()
	Clone() OnConflictAction

	QuerySegment
}

type Update []Marker

func (ms Update) Clone() OnConflictAction {
	return Update(cloneMarkers([]Marker(ms)))
}

func (Update) isOnConflictAction() {}
func (ms Update) WriteTo(qw QueryWriter, vs map[string]interface{}) error {
	io.WriteString(qw, "UPDATE SET ")

	return writeUpdateClauses(ms, qw, vs)
}

type nothing struct{}

func (nothing) Clone() OnConflictAction { return Nothing }
func (nothing) isOnConflictAction()     {}
func (nothing) WriteTo(qw QueryWriter, _ map[string]interface{}) error {
	_, err := io.WriteString(qw, "NOTHING")

	return err
}

var Nothing nothing

type OnConflictClause struct {
	Target *OnConflictTarget
	Action OnConflictAction
}

func (occ *OnConflictClause) Clone() *OnConflictClause {
	if occ == nil {
		return nil
	}

	return &OnConflictClause{
		Target: occ.Target.Clone(),
		Action: occ.Action.Clone(),
	}
}

type InsertStatement struct {
	Table string

	Fields []Marker

	Returning *sql.Returning
	OnConfict *OnConflictClause
}

func (is InsertStatement) Clone() InsertStatement {
	var r *sql.Returning

	if is.Returning != nil {
		rr := *is.Returning

		r = &rr
	}

	return InsertStatement{
		Table:     is.Table,
		Fields:    cloneMarkers(is.Fields),
		Returning: r,
		OnConfict: is.OnConfict.Clone(),
	}
}

func (is InsertStatement) buildQuery(qvs map[string]interface{}) (string, []interface{}, error) {
	var qw queryWriter

	if len(is.Fields) == 0 {
		return "", nil, errNoMarkers
	}

	fmt.Fprintf(&qw, "INSERT INTO %s(", is.Table)

	for i, f := range is.Fields {
		qw.WriteString(columnName(f))

		if i < len(is.Fields)-1 {
			qw.WriteString(", ")
		}
	}

	qw.WriteString(") VALUES (")

	for i, f := range is.Fields {
		v, ok := qvs[f.Binding()]

		if !ok {
			return "", nil, ErrMissingKey{Key: f.Binding()}
		}

		qw.WriteString(qw.RedeemVariable(v))

		if i < len(is.Fields)-1 {
			qw.WriteString(", ")
		}
	}

	qw.WriteRune(')')

	if oc := is.OnConfict; oc != nil {
		qw.WriteString(" ON CONFLICT ")

		if t := oc.Target; t != nil {
			if err := t.WriteTo(&qw, qvs); err != nil {
				return "", nil, err
			}

			qw.WriteString(" ")
		}

		qw.WriteString("DO ")

		if err := oc.Action.WriteTo(&qw, qvs); err != nil {
			return "", nil, err
		}
	}

	if is.Returning != nil {
		qw.vs = append(qw.vs, is.Returning)
	}

	return qw.String(), qw.vs, nil
}
