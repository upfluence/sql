//go:build !go1.23

package sqlbuilder

func writeInClause(w QueryWriter, vv interface{}, k string) error {
	return writeInClauseBasic(w, vv, k)
}
