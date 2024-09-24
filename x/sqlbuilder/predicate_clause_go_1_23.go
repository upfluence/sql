//go:build go1.23

package sqlbuilder

import "reflect"

func writeInClause(w QueryWriter, vv interface{}, k string) error {
	v := reflect.ValueOf(vv)
	t := v.Type()

	if t.CanSeq() && t.Kind() != reflect.Slice && t.Kind() != reflect.Array {
		var vs []interface{}

		for v := range v.Seq() {
			vs = append(vs, v.Interface())
		}

		return writeInClauseBasic(w, vs, k)
	}

	return writeInClauseBasic(w, vv, k)
}
