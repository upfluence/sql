package sqlbuilder

import (
	"fmt"
	"io"
	"reflect"

	"github.com/upfluence/errors"
)

// ErrMissingPredicate is returned when a predicate is required but not provided.
var ErrMissingPredicate = errors.New("Missing predicate")

type plainSQLPredicate string

func (psp plainSQLPredicate) ToSQL() string { return string(psp) }

func (psp plainSQLPredicate) Clone() StaticStmtPredicateClause { return psp }

// PlainSQLPredicate returns a predicate from raw SQL text.
func PlainSQLPredicate(exp string) PredicateClause {
	return &staticStmtPredicateClauseWrapper{sspc: plainSQLPredicate(exp)}
}

// EqMarkers returns a predicate that compares two markers for equality.
func EqMarkers(l, r Marker) PredicateClause {
	return PlainSQLPredicate(fmt.Sprintf("%s = %s", l.ToSQL(), r.ToSQL()))
}

type StaticStmtPredicateClause interface {
	Clone() StaticStmtPredicateClause
	ToSQL() string
}

type staticStmtPredicateClauseWrapper struct {
	sspc StaticStmtPredicateClause
}

func (sspcw *staticStmtPredicateClauseWrapper) Clone() PredicateClause {
	return &staticStmtPredicateClauseWrapper{sspc: sspcw.sspc.Clone()}
}

func (sspcw *staticStmtPredicateClauseWrapper) WriteTo(w QueryWriter, _ map[string]interface{}) error {
	io.WriteString(w, sspcw.sspc.ToSQL())

	return nil
}

type StaticValuePredicateClause interface {
	WriteTo(QueryWriter) error
	Clone() StaticValuePredicateClause
}

type staticClause struct {
	pc PredicateClause
	vs map[string]interface{}
}

func (sc *staticClause) Clone() StaticValuePredicateClause {
	vs := make(map[string]interface{}, len(sc.vs))

	for k, v := range sc.vs {
		vs[k] = v
	}

	return &staticClause{pc: sc.pc.Clone(), vs: vs}
}

func (sc *staticClause) WriteTo(w QueryWriter) error {
	return sc.pc.WriteTo(w, sc.vs)
}

func Static(pc PredicateClause, vs map[string]interface{}) PredicateClause {
	return &staticValuePredicateClauseWrapper{
		svpc: &staticClause{pc: pc, vs: vs},
	}
}

// StaticIn creates a PredicateClause for an IN predicate with a static value.
// This is a convenience that combines In() with Static().
//
// Example:
//
//	sqlbuilder.StaticIn(sqlbuilder.Column("status"), []string{"active", "pending"})
func StaticIn(m Marker, v interface{}) PredicateClause {
	return Static(In(m), map[string]interface{}{m.Binding(): v})
}

// StaticEq creates a PredicateClause for equality with a static value.
//
// Example:
//
//	sqlbuilder.StaticEq(sqlbuilder.Column("status"), "active")
func StaticEq(m Marker, v interface{}) PredicateClause {
	return Static(Eq(m), map[string]interface{}{m.Binding(): v})
}

// StaticNe creates a PredicateClause for inequality with a static value.
//
// Example:
//
//	sqlbuilder.StaticNe(sqlbuilder.Column("status"), "deleted")
func StaticNe(m Marker, v interface{}) PredicateClause {
	return Static(Ne(m), map[string]interface{}{m.Binding(): v})
}

// StaticGt creates a PredicateClause for greater-than comparison with a static value.
//
// Example:
//
//	sqlbuilder.StaticGt(sqlbuilder.Column("age"), 18)
func StaticGt(m Marker, v interface{}) PredicateClause {
	return Static(Gt(m), map[string]interface{}{m.Binding(): v})
}

// StaticGte creates a PredicateClause for greater-than-or-equal comparison with a static value.
//
// Example:
//
//	sqlbuilder.StaticGte(sqlbuilder.Column("age"), 18)
func StaticGte(m Marker, v interface{}) PredicateClause {
	return Static(Gte(m), map[string]interface{}{m.Binding(): v})
}

// StaticLt creates a PredicateClause for less-than comparison with a static value.
//
// Example:
//
//	sqlbuilder.StaticLt(sqlbuilder.Column("age"), 65)
func StaticLt(m Marker, v interface{}) PredicateClause {
	return Static(Lt(m), map[string]interface{}{m.Binding(): v})
}

// StaticLte creates a PredicateClause for less-than-or-equal comparison with a static value.
//
// Example:
//
//	sqlbuilder.StaticLte(sqlbuilder.Column("created_at"), time.Now())
func StaticLte(m Marker, v interface{}) PredicateClause {
	return Static(Lte(m), map[string]interface{}{m.Binding(): v})
}

// StaticLike creates a PredicateClause for LIKE pattern matching with a static value.
//
// Example:
//
//	sqlbuilder.StaticLike(sqlbuilder.Column("email"), "%@example.com")
func StaticLike(m Marker, v string) PredicateClause {
	return Static(Like(m), map[string]interface{}{m.Binding(): v})
}

// IsNull creates a PredicateClause that tests for NULL values.
//
// Example:
//
//	sqlbuilder.IsNull(sqlbuilder.Column("deleted_at"))
func IsNull(m Marker) PredicateClause {
	return PlainSQLPredicate(fmt.Sprintf("%s IS NULL", m.ToSQL()))
}

// IsNotNull creates a PredicateClause that tests for non-NULL values.
//
// Example:
//
//	sqlbuilder.IsNotNull(sqlbuilder.Column("verified_at"))
func IsNotNull(m Marker) PredicateClause {
	return PlainSQLPredicate(fmt.Sprintf("%s IS NOT NULL", m.ToSQL()))
}

type notPredicateClause struct {
	pc PredicateClause
}

// Not creates a PredicateClause that negates another predicate.
//
// Example:
//
//	sqlbuilder.Not(sqlbuilder.Eq(sqlbuilder.Column("status")))
func Not(pc PredicateClause) PredicateClause {
	if npc, ok := pc.(interface{ Not() PredicateClause }); ok {
		return npc.Not()
	}

	return &notPredicateClause{pc: pc}
}

func (npc *notPredicateClause) Not() PredicateClause {
	return npc.pc.Clone()
}

func (npc *notPredicateClause) Clone() PredicateClause {
	return &notPredicateClause{pc: npc.pc.Clone()}
}

func (npc *notPredicateClause) WriteTo(w QueryWriter, vs map[string]interface{}) error {
	if _, err := io.WriteString(w, "NOT ("); err != nil {
		return err
	}

	if err := npc.pc.WriteTo(w, vs); err != nil {
		return err
	}

	_, err := io.WriteString(w, ")")
	return err
}

type staticValuePredicateClauseWrapper struct {
	svpc StaticValuePredicateClause
}

func (svpcw *staticValuePredicateClauseWrapper) Clone() PredicateClause {
	return &staticValuePredicateClauseWrapper{
		svpc: svpcw.svpc.Clone(),
	}
}

func (svpcw *staticValuePredicateClauseWrapper) WriteTo(w QueryWriter, _ map[string]interface{}) error {
	return svpcw.svpc.WriteTo(w)
}

type QuerySegment interface {
	WriteTo(QueryWriter, map[string]interface{}) error
}

type PredicateClause interface {
	QuerySegment
	Clone() PredicateClause
}

type ErrMissingKey struct{ Key string }

func (emk ErrMissingKey) Error() string {
	return fmt.Sprintf("%q key missing", emk.Key)
}

var errInvalidType = errors.New("invalid type")

// Eq creates a PredicateClause for equality comparison.
// The marker binding is used as the key for value substitution.
//
//	stmt := sqlbuilder.PrepareSelect("users").
//		Columns(sqlbuilder.Column("id")).
//		Where(sqlbuilder.Eq(sqlbuilder.Column("status")))
func Eq(m Marker) PredicateClause { return signClause(m, "=") }

// Ne creates a PredicateClause for inequality comparison.
//
//	sqlbuilder.Ne(sqlbuilder.Column("status"))
func Ne(m Marker) PredicateClause { return signClause(m, "!=") }

// Lt creates a PredicateClause for less-than comparison.
//
//	sqlbuilder.Lt(sqlbuilder.Column("age"))
func Lt(m Marker) PredicateClause { return signClause(m, "<") }

// Lte creates a PredicateClause for less-than-or-equal comparison.
//
//	sqlbuilder.Lte(sqlbuilder.Column("age"))
func Lte(m Marker) PredicateClause { return signClause(m, "<=") }

// Gt creates a PredicateClause for greater-than comparison.
//
//	sqlbuilder.Gt(sqlbuilder.Column("age"))
func Gt(m Marker) PredicateClause { return signClause(m, ">") }

// Gte creates a PredicateClause for greater-than-or-equal comparison.
//
//	sqlbuilder.Gte(sqlbuilder.Column("age"))
func Gte(m Marker) PredicateClause { return signClause(m, ">=") }

// Like creates a PredicateClause for LIKE pattern matching.
//
//	sqlbuilder.Like(sqlbuilder.Column("email"))
func Like(m Marker) PredicateClause { return signClause(m, "LIKE") }

func signClause(m Marker, s string) *basicClause {
	return &basicClause{m: m, fn: writeSignClause(s)}
}

func writeSignClause(s string) func(QueryWriter, interface{}, string) error {
	return func(w QueryWriter, vv interface{}, k string) error {
		fmt.Fprintf(w, "%s %s %s", k, s, w.RedeemVariable(vv))
		return nil
	}
}

type multiClause struct {
	wcs []PredicateClause

	op string
}

func wrapMultiClause(wcs []PredicateClause, op string) PredicateClause {
	var cs []PredicateClause

	for _, wc := range wcs {
		if wc == nil {
			continue
		}

		if mc, ok := wc.(multiClause); ok && mc.op == op {
			cs = append(cs, mc.wcs...)
			continue
		}

		cs = append(cs, wc)
	}

	switch len(cs) {
	case 0:
		return nil
	case 1:
		return cs[0]
	default:
		return multiClause{wcs: cs, op: op}
	}
}

// And combines multiple PredicateClauses with AND logic.
// Nil predicates are filtered out, and single predicates are returned unwrapped.
//
//	stmt := sqlbuilder.PrepareSelect("users").
//		Columns(sqlbuilder.Column("id")).
//		Where(sqlbuilder.And(
//			sqlbuilder.Eq(sqlbuilder.Column("status")),
//			sqlbuilder.Gt(sqlbuilder.Column("age")),
//		))
func And(wcs ...PredicateClause) PredicateClause {
	return wrapMultiClause(wcs, "AND")
}

// Or combines multiple PredicateClauses with OR logic.
// Nil predicates are filtered out, and single predicates are returned unwrapped.
//
//	stmt := sqlbuilder.PrepareSelect("users").
//		Columns(sqlbuilder.Column("id")).
//		Where(sqlbuilder.Or(
//			sqlbuilder.Eq(sqlbuilder.Column("status")),
//			sqlbuilder.IsNull(sqlbuilder.Column("deleted_at")),
//		))
func Or(wcs ...PredicateClause) PredicateClause {
	return wrapMultiClause(wcs, "OR")
}

func (mc multiClause) Clone() PredicateClause {
	var wcs []PredicateClause

	if len(mc.wcs) > 0 {
		wcs = make([]PredicateClause, len(mc.wcs))

		for i, pc := range mc.wcs {
			wcs[i] = pc.Clone()
		}
	}

	return multiClause{wcs: wcs, op: mc.op}
}

func (mc multiClause) WriteTo(w QueryWriter, vs map[string]interface{}) error {
	if len(mc.wcs) == 0 {
		io.WriteString(w, "1=0")
		return nil
	}

	io.WriteString(w, "(")

	for i, wc := range mc.wcs {
		if err := wc.WriteTo(w, vs); err != nil {
			return err
		}

		if i < len(mc.wcs)-1 {
			fmt.Fprintf(w, ") %s (", mc.op)
		}
	}

	io.WriteString(w, ")")

	return nil
}

// In creates a PredicateClause for IN list membership testing.
//
//	stmt := sqlbuilder.PrepareSelect("users").
//		Columns(sqlbuilder.Column("id")).
//		Where(sqlbuilder.In(sqlbuilder.Column("status")))
func In(m Marker) PredicateClause {
	return &basicClause{m: m, fn: writeInClause}
}

// Exists creates a PredicateClause for EXISTS (subquery) testing.
//
// Example:
//
//	sqlbuilder.Exists("SELECT 1 FROM orders WHERE user_id = users.id")
type Exists struct {
	// Table specifies the table to check for existence.
	Table string
	// WhereClause specifies the WHERE conditions for the EXISTS subquery.
	WhereClause PredicateClause
}

func (e *Exists) Clone() PredicateClause {
	return &Exists{Table: e.Table, WhereClause: e.WhereClause}
}

func (e *Exists) WriteTo(w QueryWriter, vs map[string]interface{}) error {
	io.WriteString(w, "EXISTS(SELECT 1 FROM ")
	io.WriteString(w, e.Table)
	io.WriteString(w, " WHERE ")
	if err := e.WhereClause.WriteTo(w, vs); err != nil {
		return err
	}
	io.WriteString(w, ")")

	return nil
}

type basicClause struct {
	m  Marker
	fn func(QueryWriter, interface{}, string) error
}

func (bc *basicClause) Clone() PredicateClause {
	return &basicClause{m: bc.m.Clone(), fn: bc.fn}
}

func (bc *basicClause) WriteTo(w QueryWriter, vs map[string]interface{}) error {
	b := bc.m.Binding()
	vv, ok := vs[b]

	if !ok {
		return ErrMissingKey{b}
	}

	return bc.fn(w, vv, bc.m.ToSQL())
}

func writeInClauseBasic(w QueryWriter, vv interface{}, k string) error {
	v := reflect.ValueOf(vv)

	if k := v.Kind(); k != reflect.Slice && k != reflect.Array {
		return errInvalidType
	}

	if v.Len() == 0 {
		io.WriteString(w, "1=0")
		return nil
	}

	fmt.Fprintf(w, "%s IN (", k)

	for i := 0; i < v.Len(); i++ {
		io.WriteString(w, w.RedeemVariable(v.Index(i).Interface()))

		if i < v.Len()-1 {
			io.WriteString(w, ", ")
		}
	}

	io.WriteString(w, ")")
	return nil
}

func clonePredicateClause(pc PredicateClause) PredicateClause {
	if pc == nil {
		return nil
	}

	return pc.Clone()
}
