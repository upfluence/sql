package upserter

import (
	"context"
	"database/sql/driver"
	"reflect"

	"github.com/upfluence/sql"
	"github.com/upfluence/sql/x/sqlbuilder"
)

type errExecer struct{ error }

func (ee errExecer) Exec(context.Context, map[string]interface{}) (sql.Result, error) {
	return nil, ee.error
}

type txExecutor interface {
	executeTx(context.Context, func(sql.Queryer) error) error
}

type execer struct {
	te txExecutor

	returningMarker sqlbuilder.Marker

	qfs []string
	sfs []string

	ss sqlbuilder.SelectStatement
	us sqlbuilder.UpdateStatement
	is sqlbuilder.InsertStatement

	mode Mode
}

func newExecer(te txExecutor, stmt Statement) sqlbuilder.Execer {
	if len(stmt.QueryValues) == 0 {
		return errExecer{errNoQueryValues}
	}

	var (
		clauses = make([]sqlbuilder.PredicateClause, len(stmt.QueryValues))

		e = execer{
			te:  te,
			qfs: make([]string, len(stmt.QueryValues)),
			sfs: make([]string, len(stmt.SetValues)),
			ss: sqlbuilder.SelectStatement{
				Table:         stmt.Table,
				SelectClauses: append([]sqlbuilder.Marker{oneMarker}, stmt.SetValues...),
			},
			us: sqlbuilder.UpdateStatement{
				Table:  stmt.Table,
				Fields: stmt.SetValues,
			},
			is: sqlbuilder.InsertStatement{
				Table: stmt.Table,
				Fields: make(
					[]sqlbuilder.Marker,
					len(stmt.QueryValues)+len(stmt.SetValues)+len(stmt.InsertValues),
				),
				Returning: stmt.Returning,
			},
			mode: stmt.mode(),
		}
	)

	if r := stmt.Returning; r != nil {
		var (
			found bool
			m     sqlbuilder.Marker
		)

		for _, v := range e.ss.SelectClauses {
			if v.ToSQL() == r.Field {
				found = true
				m = v
				break
			}
		}

		if !found {
			m = sqlbuilder.Column(r.Field)
			e.ss.SelectClauses = append(e.ss.SelectClauses, m)
		}

		e.returningMarker = m
	}

	for i, qv := range stmt.QueryValues {
		clauses[i] = sqlbuilder.Eq(qv)
		e.qfs[i] = qv.Binding()
		e.is.Fields[i] = qv
	}

	for i, sv := range stmt.SetValues {
		e.sfs[i] = sv.Binding()
		e.is.Fields[len(stmt.QueryValues)+i] = sv
	}

	for i, iv := range stmt.InsertValues {
		e.is.Fields[len(stmt.QueryValues)+len(stmt.SetValues)+i] = iv
	}

	clause := sqlbuilder.And(clauses...)

	e.ss.WhereClause = clause
	e.us.WhereClause = clause

	return &e
}

func (e *execer) Exec(ctx context.Context, vs map[string]interface{}) (sql.Result, error) {
	var (
		res    sql.Result
		lastID int64
		one    int64

		existing = map[string]interface{}{"one": &one}
		qvs      = make(map[string]interface{})
	)

	for _, f := range e.qfs {
		v, ok := vs[f]

		if !ok {
			return nil, sqlbuilder.ErrMissingKey{Key: f}
		}

		qvs[f] = v
	}

	for _, f := range e.sfs {
		v, ok := vs[f]

		if !ok {
			return nil, sqlbuilder.ErrMissingKey{Key: f}
		}

		existing[f] = reflect.New(reflect.TypeOf(v)).Interface()
	}

	if m := e.returningMarker; m != nil {
		existing[m.Binding()] = &lastID
	}

	return res, e.te.executeTx(ctx, func(q sql.Queryer) error {
		var (
			err error

			qb = sqlbuilder.QueryBuilder{Queryer: q}
		)

		switch err = qb.PrepareSelect(e.ss).QueryRow(ctx, qvs).Scan(existing); err {
		case nil:
			pristine := true

			for _, sf := range e.sfs {
				if !reflect.DeepEqual(
					reflect.ValueOf(existing[sf]).Elem().Interface(),
					vs[sf],
				) {
					pristine = false
					break
				}
			}

			if pristine || e.mode&Update == 0 {
				res = driver.RowsAffected(0)

				if e.returningMarker != nil {
					res = lastIDResult{Result: res, id: lastID}
				}

				return sql.ErrRollback
			}

			res, err = qb.PrepareUpdate(e.us).Exec(ctx, vs)

			if err == nil && e.returningMarker != nil {
				res = lastIDResult{Result: res, id: lastID}
			}
		case sql.ErrNoRows:
			if e.mode&Insert == 0 {
				res = driver.RowsAffected(0)
				return sql.ErrRollback
			}

			res, err = qb.PrepareInsert(e.is).Exec(ctx, vs)
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
