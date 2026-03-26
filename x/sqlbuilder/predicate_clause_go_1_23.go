//go:build go1.23

package sqlbuilder

import "reflect"

func reflectSlice(vv any) (reflect.Value, error) {
	v := reflect.ValueOf(vv)
	t := v.Type()

	if t.CanSeq() && t.Kind() != reflect.Slice && t.Kind() != reflect.Array {
		var vs []any

		for v := range v.Seq() {
			vs = append(vs, v.Interface())
		}

		return reflect.ValueOf(vs), nil
	}

	return reflectSliceBasic(vv)
}
