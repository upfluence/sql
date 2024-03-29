package sqlbuilder

import (
	"context"

	"github.com/upfluence/sql"
)

type Queryer interface {
	Query(context.Context, map[string]interface{}) (Cursor, error)
	QueryRow(context.Context, map[string]interface{}) Scanner
}

type Scanner interface {
	Scan(map[string]interface{}) error
}

type scanner struct {
	sc sql.Scanner
	ks []string
}

func (sc *scanner) Scan(vs map[string]interface{}) error {
	var svs = make([]interface{}, len(sc.ks))

	for i, k := range sc.ks {
		v, ok := vs[k]

		if !ok {
			return ErrMissingKey{Key: k}
		}

		svs[i] = v
	}

	return sc.sc.Scan(svs...)
}

type ErrScanner struct{ Err error }

func (es ErrScanner) Scan(map[string]interface{}) error { return es.Err }

type Cursor interface {
	Scanner

	Close() error
	Err() error
	Next() bool
}

type ScanFunc func(Scanner) error

type cursor struct {
	sql.Cursor

	sc Scanner
}

func (c *cursor) Scan(vs map[string]interface{}) error {
	return c.sc.Scan(vs)
}

func ScrollCursor(c Cursor, fn ScanFunc) error {
	defer c.Close()

	for c.Next() {
		if err := fn(c); err != nil {
			return err
		}
	}

	return c.Err()
}
