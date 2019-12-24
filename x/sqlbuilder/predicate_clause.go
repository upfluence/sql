package sqlbuilder

import (
	"errors"
	"fmt"
	"io"
	"reflect"
)

type plainSQLPredicate string

func (psp plainSQLPredicate) ToSQL() string { return string(psp) }

func PlainSQLPredicate(exp string) PredicateClause {
	return &staticStmtPredicateClauseWrapper{sspc: plainSQLPredicate(exp)}
}

func EqMarkers(l, r Marker) PredicateClause {
	return &staticStmtPredicateClauseWrapper{
		sspc: plainSQLPredicate(fmt.Sprintf("%s = %s", l.ToSQL(), r.ToSQL())),
	}
}

type StaticStmtPredicateClause interface {
	ToSQL() string
}

type staticStmtPredicateClauseWrapper struct {
	sspc StaticStmtPredicateClause
}

func (sspcw *staticStmtPredicateClauseWrapper) WriteTo(w QueryWriter, _ map[string]interface{}) error {
	io.WriteString(w, sspcw.sspc.ToSQL())

	return nil
}

type StaticValuePredicateClause interface {
	WriteTo(w QueryWriter) error
}

type staticValuePredicateClauseWrapper struct {
	svpc StaticValuePredicateClause
}

func (svpcw *staticValuePredicateClauseWrapper) WriteTo(w QueryWriter, _ map[string]interface{}) error {
	return svpcw.svpc.WriteTo(w)
}

type PredicateClause interface {
	WriteTo(QueryWriter, map[string]interface{}) error
}

type ErrMissingKey struct{ Key string }

func (emk ErrMissingKey) Error() string {
	return fmt.Sprintf("%q key missing", emk.Key)
}

var errInvalidType = errors.New("sqlbuilder: invalid type")

type singleClause struct {
	sign string
	m    Marker
}

func Eq(m Marker) PredicateClause  { return singleClause{sign: "=", m: m} }
func Ne(m Marker) PredicateClause  { return singleClause{sign: "!=", m: m} }
func Lt(m Marker) PredicateClause  { return singleClause{sign: "<", m: m} }
func Lte(m Marker) PredicateClause { return singleClause{sign: "<=", m: m} }
func Gt(m Marker) PredicateClause  { return singleClause{sign: ">", m: m} }
func Gte(m Marker) PredicateClause { return singleClause{sign: ">=", m: m} }

func (sc singleClause) WriteTo(w QueryWriter, vs map[string]interface{}) error {
	b := sc.m.Binding()
	v, ok := vs[b]

	if !ok {
		return ErrMissingKey{b}
	}

	fmt.Fprintf(w, "%s %s %s", sc.m.ToSQL(), sc.sign, w.RedeemVariable(v))
	return nil
}

type multiClause struct {
	wcs []PredicateClause

	op string
}

func And(wcs ...PredicateClause) PredicateClause {
	return multiClause{wcs: wcs, op: "AND"}
}

func Or(wcs ...PredicateClause) PredicateClause {
	return multiClause{wcs: wcs, op: "OR"}
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

type inClause struct {
	m Marker
}

func In(m Marker) PredicateClause { return inClause{m: m} }

func (ic inClause) WriteTo(w QueryWriter, vs map[string]interface{}) error {
	b := ic.m.Binding()
	vv, ok := vs[b]

	if !ok {
		return ErrMissingKey{b}
	}

	return writeInClause(w, vv, ic.m.ToSQL())
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
