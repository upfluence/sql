package sqltest

import (
	"database/sql"
)

func StringArg(s string) ScanArg {
	return func(v interface{}) {
		target := v.(*string)
		*target = s
	}
}

func Int64Arg(s int64) ScanArg {
	return func(v interface{}) {
		target := v.(*int64)
		*target = s
	}
}

type ScanArg func(interface{})

type Scanner struct {
	Err  error
	Args []ScanArg
}

func (s Scanner) Scan(vs ...interface{}) error {
	for i, v := range vs {
		if i < len(s.Args) {
			s.Args[i](v)
		}
	}

	return s.Err
}

type SingleCursor struct {
	Scanner

	ReturnedErr error
	CloseErr    error

	seen bool
}

func (c *SingleCursor) Err() error   { return c.ReturnedErr }
func (c *SingleCursor) Close() error { return c.CloseErr }
func (c *SingleCursor) Next() bool   { return !c.seen }
func (c *SingleCursor) Scan(vs ...interface{}) error {
	if c.seen {
		return sql.ErrNoRows
	}

	c.seen = true

	return c.Scanner.Scan(vs...)
}

type MultipleCursor struct {
	Scanners []Scanner

	i int

	ReturnedErr error
	CloseErr    error
}

func (c *MultipleCursor) Err() error   { return c.ReturnedErr }
func (c *MultipleCursor) Close() error { return c.CloseErr }
func (c *MultipleCursor) Next() bool   { return c.i < len(c.Scanners) }

func (c *MultipleCursor) Scan(vs ...interface{}) error {
	if c.i < len(c.Scanners) {
		defer func() { c.i++ }()
		return c.Scanners[c.i].Scan(vs...)
	}

	return sql.ErrNoRows
}
