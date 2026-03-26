//go:build !go1.23

package sqlbuilder

import "reflect"

func reflectSlice(vv any) (reflect.Value, error) {
	return reflectSliceBasic(vv)
}
