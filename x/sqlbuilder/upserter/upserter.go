package upserter

import (
	"context"
	"database/sql/driver"
	"errors"
	"reflect"

	"github.com/upfluence/sql"
	"github.com/upfluence/sql/x/sqlbuilder"
)

var (
	errNoQueryValues = errors.New("x/sqlbuilder: No QueryValue marker given")
	errNoSetValues   = errors.New("x/sqlbuilder: No SetValue marker given")
)

type UpsertStatement struct {
	Table string

	QueryValues  []sqlbuilder.Marker
	InsertValues []sqlbuilder.Marker
	SetValues    []sqlbuilder.Marker

	Returning *sql.Returning
}

type Upserter struct {
	sql.DB
}

type errExecer struct{ error }

func (ee errExecer) Exec(context.Context, map[string]interface{}) (sql.Result, error) {
	return nil, ee.error
}

func (u *Upserter) PrepareUpsert(us UpsertStatement) sqlbuilder.Execer {
	if len(us.QueryValues) == 0 {
		return errExecer{errNoQueryValues}
	}

	if len(us.SetValues) == 0 {
		return errExecer{errNoSetValues}
	}

	var (
		clauses = make([]sqlbuilder.PredicateClause, len(us.QueryValues))

		ue = upsertExecer{
			u:   u,
			qfs: make([]string, len(us.QueryValues)),
			sfs: make([]string, len(us.SetValues)),
			ss: sqlbuilder.SelectStatement{
				Table:         us.Table,
				SelectClauses: append([]sqlbuilder.Marker{}, us.SetValues...),
			},
			us: sqlbuilder.UpdateStatement{
				Table:  us.Table,
				Fields: us.SetValues,
			},
			is: sqlbuilder.InsertStatement{
				Table: us.Table,
				Fields: make(
					[]sqlbuilder.Marker,
					len(us.QueryValues)+len(us.SetValues)+len(us.InsertValues),
				),
				Returning: us.Returning,
			},
		}
	)

	if r := us.Returning; r != nil {
		var (
			found bool
			m     sqlbuilder.Marker
		)

		for _, v := range ue.ss.SelectClauses {
			if v.ToSQL() == r.Field {
				found = true
				m = v
				break
			}
		}

		if !found {
			m = sqlbuilder.Column(r.Field)
			ue.ss.SelectClauses = append(ue.ss.SelectClauses, m)
		}

		ue.returningMarker = m
	}

	for i, qv := range us.QueryValues {
		clauses[i] = sqlbuilder.Eq(qv)
		ue.qfs[i] = qv.Binding()
		ue.is.Fields[i] = qv
	}

	for i, sv := range us.SetValues {
		ue.sfs[i] = sv.Binding()
		ue.is.Fields[len(us.QueryValues)+i] = sv
	}

	for i, iv := range us.InsertValues {
		ue.is.Fields[len(us.QueryValues)+len(us.SetValues)+i] = iv
	}

	clause := sqlbuilder.And(clauses...)

	ue.ss.WhereClause = clause
	ue.us.WhereClause = clause

	return &ue
}

type upsertExecer struct {
	u *Upserter

	returningMarker sqlbuilder.Marker

	qfs []string
	sfs []string

	ss sqlbuilder.SelectStatement
	us sqlbuilder.UpdateStatement
	is sqlbuilder.InsertStatement
}

func (ue *upsertExecer) Exec(ctx context.Context, vs map[string]interface{}) (sql.Result, error) {
	var (
		res    sql.Result
		lastID int64

		existing = make(map[string]interface{})
		qvs      = make(map[string]interface{})
	)

	for _, f := range ue.qfs {
		v, ok := vs[f]

		if !ok {
			return nil, sqlbuilder.ErrMissingKey{Key: f}
		}

		qvs[f] = v
	}

	for _, f := range ue.sfs {
		v, ok := vs[f]

		if !ok {
			return nil, sqlbuilder.ErrMissingKey{Key: f}
		}

		existing[f] = reflect.New(reflect.TypeOf(v)).Interface()
	}

	if m := ue.returningMarker; m != nil {
		existing[m.Binding()] = &lastID
	}

	return res, sql.ExecuteTx(ctx, ue.u, func(q sql.Queryer) error {
		var (
			err error

			qb = sqlbuilder.QueryBuilder{Queryer: q}
		)

		switch err = qb.PrepareSelect(ue.ss).QueryRow(ctx, qvs).Scan(existing); err {
		case nil:
			pristine := true

			for _, sf := range ue.sfs {
				if !reflect.DeepEqual(
					reflect.ValueOf(existing[sf]).Elem().Interface(),
					vs[sf],
				) {
					pristine = false
					break
				}
			}

			if pristine {
				res = driver.RowsAffected(0)

				if ue.returningMarker != nil {
					res = lastIDResult{Result: res, id: lastID}
				}

				return sql.ErrRollback
			}

			res, err = qb.PrepareUpdate(ue.us).Exec(ctx, vs)

			if err == nil && ue.returningMarker != nil {
				res = lastIDResult{Result: res, id: lastID}
			}
		case sql.ErrNoRows:
			res, err = qb.PrepareInsert(ue.is).Exec(ctx, vs)
		default:
		}

		return err
	})
}

type lastIDResult struct {
	sql.Result

	id int64
}

func (lir lastIDResult) LastInsertId() (int64, error) { return lir.id, nil }
