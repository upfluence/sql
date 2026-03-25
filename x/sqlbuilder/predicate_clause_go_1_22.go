//go:build !go1.23

package sqlbuilder

import "reflect"

func reflectSlice(vv interface{}) (reflect.Value, error) {
	return reflectSliceBasic(vv)
}
