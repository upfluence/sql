package stats

import (
	"sync"

	"github.com/upfluence/stats"

	"github.com/upfluence/sql"
)

type scanner struct {
	sql.Scanner

	once      sync.Once
	operation stats.Operation
}

func (s *scanner) Scan(vs ...any) error {
	err := s.Scanner.Scan(vs...)
	s.once.Do(func() { s.operation.Finish(err) })

	return err //nolint:wrapcheck
}
