package sqlbuilder

import (
	"errors"
	"fmt"
	"io"
	"reflect"
)

type plainSQLPredicate string

func (psp plainSQLPredicate) ToSQL() string { return string(psp) }

func (psp plainSQLPredicate) Clone() StaticStmtPredicateClause { return psp }

func PlainSQLPredicate(exp string) PredicateClause {
	return &staticStmtPredicateClauseWrapper{sspc: plainSQLPredicate(exp)}
}

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

func StaticIn(m Marker, v interface{}) PredicateClause {
	return Static(In(m), map[string]interface{}{m.Binding(): v})
}

func StaticEq(m Marker, v interface{}) PredicateClause {
	return Static(Eq(m), map[string]interface{}{m.Binding(): v})
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

type PredicateClause interface {
	WriteTo(QueryWriter, map[string]interface{}) error
	Clone() PredicateClause
}

type ErrMissingKey struct{ Key string }

func (emk ErrMissingKey) Error() string {
	return fmt.Sprintf("%q key missing", emk.Key)
}

var errInvalidType = errors.New("sqlbuilder: invalid type")

func Eq(m Marker) PredicateClause  { return signClause(m, "=") }
func Ne(m Marker) PredicateClause  { return signClause(m, "!=") }
func Lt(m Marker) PredicateClause  { return signClause(m, "<") }
func Lte(m Marker) PredicateClause { return signClause(m, "<=") }
func Gt(m Marker) PredicateClause  { return signClause(m, ">") }
func Gte(m Marker) PredicateClause { return signClause(m, ">=") }

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

func And(wcs ...PredicateClause) PredicateClause {
	return wrapMultiClause(wcs, "AND")
}

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

func In(m Marker) PredicateClause {
	return &basicClause{m: m, fn: writeInClause}
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

func writeInClause(w QueryWriter, vv interface{}, k string) error {
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
