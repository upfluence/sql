package upserter

import (
	"context"
	"database/sql/driver"
	"fmt"

	"github.com/upfluence/errors"
	"github.com/upfluence/sql"
	"github.com/upfluence/sql/x/sqlbuilder"
)

type errExecer struct{ error }

func (ee errExecer) Exec(context.Context, map[string]any) (sql.Result, error) {
	return nil, ee.error
}

type txExecutor interface {
	queryer() sql.Queryer
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

// MultiExecer executes the statement for several rows at once, all or none of
// them, each row holding the values of all the markers of the statement as Exec
// does. The result holds the sum of the rows affected. The rows must not be
// empty and the query values unique.
//
// The key constrained Insert and Upsert modes do it with a single INSERT, which
// is bound by the parameter limit of the database as the rows are not chunked.
// Without the key constraint, the statements of every row run one after the
// other in a single transaction, which saves the transaction of each row but
// not its round trips.
//
// The key constrained Update mode, the key constrained Upsert mode with a
// PredicateClause and the statements with Returning do not implement it, so
// that the capability can be discovered with a type assertion.
type MultiExecer interface {
	sqlbuilder.Execer

	MultiExec(context.Context, []map[string]any) (sql.Result, error)
}

type multiExecer struct {
	sqlbuilder.Execer

	ie *sqlbuilder.InsertExecer
}

func (me *multiExecer) MultiExec(ctx context.Context, vvs []map[string]any) (sql.Result, error) {
	res, err := me.ie.MultiExec(ctx, vvs, nil)

	return res, errors.WithStack(err)
}

func insertFields(stmt Statement) []sqlbuilder.Marker {
	return append(
		append(
			append([]sqlbuilder.Marker{}, stmt.QueryValues...),
			stmt.InsertValues...,
		),
		stmt.SetValues...,
	)
}

func excludedUpdate(ms []sqlbuilder.Marker) sqlbuilder.Update {
	res := make(sqlbuilder.Update, len(ms))

	for i, m := range ms {
		res[i] = sqlbuilder.Excluded(m)
	}

	return res
}

func newExecer(te txExecutor, stmt Statement) sqlbuilder.Execer {
	if len(stmt.QueryValues) == 0 {
		return errExecer{errNoQueryValues}
	}

	if stmt.QueryConstrained {
		qb := sqlbuilder.QueryBuilder{Queryer: te.queryer()}

		switch stmt.mode() {
		case Upsert:
			var action sqlbuilder.OnConflictAction = sqlbuilder.Nothing

			if len(stmt.SetValues) > 0 {
				action = sqlbuilder.Update(stmt.SetValues)
			}

			e := qb.PrepareInsert(
				sqlbuilder.InsertStatement{
					Table:  stmt.Table,
					Fields: insertFields(stmt),
					OnConfict: &sqlbuilder.OnConflictClause{
						Target: &sqlbuilder.OnConflictTarget{
							Fields: stmt.QueryValues,
						},
						Action: action,
						Where:  stmt.PredicateClause,
					},
					Returning: stmt.Returning,
				},
			)

			var re sqlbuilder.Execer = &sqlbuilder.RetryExecer{
				Execer: e,
				ShouldRetry: func(err error) bool {
					var ce sql.ConstraintError

					if !errors.As(err, &ce) {
						return false
					}

					return ce.Type == sql.PrimaryKey || ce.Type == sql.Unique
				},
				RetryCount: -1,
			}

			// the predicate is bound to the values of a single row, which the
			// ON CONFLICT clause of a multi rows INSERT cannot hold
			if stmt.Returning != nil || stmt.PredicateClause != nil {
				return re
			}

			var multiAction sqlbuilder.OnConflictAction = sqlbuilder.Nothing

			if len(stmt.SetValues) > 0 {
				multiAction = excludedUpdate(stmt.SetValues)
			}

			return &multiExecer{
				Execer: re,
				ie: qb.PrepareInsert(
					sqlbuilder.InsertStatement{
						Table:  stmt.Table,
						Fields: insertFields(stmt),
						OnConfict: &sqlbuilder.OnConflictClause{
							Target: &sqlbuilder.OnConflictTarget{
								Fields: stmt.QueryValues,
							},
							Action: multiAction,
						},
					},
				),
			}
		case Insert:
			ie := qb.PrepareInsert(
				sqlbuilder.InsertStatement{
					Table:  stmt.Table,
					Fields: insertFields(stmt),
					OnConfict: &sqlbuilder.OnConflictClause{
						Target: &sqlbuilder.OnConflictTarget{
							Fields: stmt.QueryValues,
						},
						Action: sqlbuilder.Nothing,
					},
					Returning: stmt.Returning,
				},
			)

			if stmt.Returning != nil {
				return ie
			}

			return &multiExecer{Execer: ie, ie: ie}
		case Update:
			clauses := make([]sqlbuilder.PredicateClause, len(stmt.QueryValues))

			for i, v := range stmt.QueryValues {
				clauses[i] = sqlbuilder.Eq(v)
			}

			if stmt.PredicateClause != nil {
				clauses = append(clauses, stmt.PredicateClause)
			}

			return qb.PrepareUpdate(
				sqlbuilder.UpdateStatement{
					Table:       stmt.Table,
					Fields:      stmt.SetValues,
					WhereClause: sqlbuilder.And(clauses...),
				},
			)
		default:
			return errExecer{fmt.Errorf("invalid mode: %d", stmt.Mode)}
		}
	}

	var selectClauses = []sqlbuilder.Marker{oneMarker}

	for _, m := range stmt.SetValues {
		selectClauses = append(selectClauses, &assertMarker{Marker: m})
	}

	var (
		clauses = make([]sqlbuilder.PredicateClause, len(stmt.QueryValues))

		e = execer{
			te:  te,
			qfs: make([]string, len(stmt.QueryValues)),
			sfs: make([]string, len(stmt.SetValues)),
			ss: sqlbuilder.SelectStatement{
				Table:         stmt.Table,
				SelectClauses: selectClauses,
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

	e.ss.WhereClause = sqlbuilder.And(clauses...)

	if stmt.PredicateClause != nil {
		clauses = append(clauses, stmt.PredicateClause)
	}

	e.us.WhereClause = sqlbuilder.And(clauses...)

	if stmt.Returning != nil {
		return singleExecer{Execer: &e}
	}

	return &e
}

type assertMarker struct {
	sqlbuilder.Marker
}

func (am *assertMarker) WriteTo(qw sqlbuilder.QueryWriter, vs map[string]any) error {
	_, err := fmt.Fprintf(
		qw,
		"%s = %s",
		am.ToSQL(),
		qw.RedeemVariable(vs["assert_"+am.Binding()]),
	)

	return err
}

// rowArgs holds the arguments of the statements of a row, the variables they
// are scanned into included.
type rowArgs struct {
	qvs      map[string]any
	existing map[string]any
	lastID   *int64
}

func (e *execer) prepareRow(vs map[string]any) (rowArgs, error) {
	var (
		lastID int64
		one    int64

		existing = map[string]any{"one": &one}
		qvs      = make(map[string]any)
	)

	for _, f := range e.qfs {
		v, ok := vs[f]

		if !ok {
			return rowArgs{}, sqlbuilder.ErrMissingKey{Key: f}
		}

		qvs[f] = v
	}

	for _, f := range e.sfs {
		v, ok := vs[f]

		if !ok {
			return rowArgs{}, sqlbuilder.ErrMissingKey{Key: f}
		}

		var val sql.NullBool

		qvs["assert_"+f] = v

		existing[f] = &val
	}

	if m := e.returningMarker; m != nil {
		existing[m.Binding()] = &lastID
	}

	return rowArgs{qvs: qvs, existing: existing, lastID: &lastID}, nil
}

// execRow runs the statements of a row. It returns sql.ErrRollback when
// nothing has to be written.
func (e *execer) execRow(ctx context.Context, q sql.Queryer, r rowArgs, vs map[string]any) (sql.Result, error) {
	var (
		res sql.Result
		err error

		qb = sqlbuilder.QueryBuilder{Queryer: q}
	)

	err = qb.PrepareSelect(e.ss).QueryRow(ctx, r.qvs).Scan(r.existing)

	switch {
	case err == nil:
		pristine := true

		for _, sf := range e.sfs {
			if val := r.existing[sf].(*sql.NullBool); !val.Bool {
				pristine = false

				break
			}
		}

		if pristine || e.mode&Update == 0 {
			res = driver.RowsAffected(0)

			if e.returningMarker != nil {
				res = lastIDResult{Result: res, id: *r.lastID}
			}

			return res, sql.ErrRollback
		}

		res, err = qb.PrepareUpdate(e.us).Exec(ctx, vs)

		if err == nil && e.returningMarker != nil {
			res = lastIDResult{Result: res, id: *r.lastID}
		}
	case errors.Is(err, sql.ErrNoRows):
		if e.mode&Insert == 0 {
			return driver.RowsAffected(0), sql.ErrRollback
		}

		res, err = qb.PrepareInsert(e.is).Exec(ctx, vs)
	default:
	}

	// the error is forwarded as it is, as Exec always did
	return res, err //nolint:wrapcheck
}

func (e *execer) Exec(ctx context.Context, vs map[string]any) (sql.Result, error) {
	r, err := e.prepareRow(vs)

	if err != nil {
		return nil, err
	}

	var res sql.Result

	err = e.te.executeTx(ctx, func(q sql.Queryer) error {
		var err error

		res, err = e.execRow(ctx, q, r, vs)

		return err
	})

	return res, err
}

// MultiExec runs the statements of every row in a single transaction, one row
// after the other as each of them depends on the content of the table. The
// result holds the sum of the rows affected.
func (e *execer) MultiExec(ctx context.Context, vvs []map[string]any) (sql.Result, error) {
	rs := make([]rowArgs, len(vvs))

	for i, vs := range vvs {
		r, err := e.prepareRow(vs)

		if err != nil {
			return nil, err
		}

		rs[i] = r
	}

	var total int64

	err := e.te.executeTx(ctx, func(q sql.Queryer) error {
		total = 0

		for i, r := range rs {
			res, err := e.execRow(ctx, q, r, vvs[i])

			// a row with nothing to write must not roll back the others
			if err != nil && !errors.Is(err, sql.ErrRollback) {
				return err
			}

			n, err := res.RowsAffected()

			if err != nil {
				return errors.Wrap(err, "cant read the affected rows")
			}

			total += n
		}

		return nil
	})

	return driver.RowsAffected(total), err
}

// singleExecer hides the MultiExec method of the executor it wraps.
type singleExecer struct{ sqlbuilder.Execer }

type lastIDResult struct {
	sql.Result

	id int64
}

func (lir lastIDResult) LastInsertId() (int64, error) { return lir.id, nil }
