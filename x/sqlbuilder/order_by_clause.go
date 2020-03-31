package sqlbuilder

import "fmt"

type Direction string

const (
	Asc  Direction = "ASC"
	Desc Direction = "DESC"
)

type OrderByClause struct {
	Field     Marker
	Direction Direction
}

func (obc OrderByClause) ToSQL() string {
	if obc.Direction == "" {
		return obc.Field.ToSQL()
	}

	return fmt.Sprintf("%s %s", obc.Field.ToSQL(), obc.Direction)
}

func cloneOrderByClauses(obcs []OrderByClause) []OrderByClause {
	if len(obcs) == 0 {
		return nil
	}

	res := make([]OrderByClause, len(obcs))

	for i, obc := range obcs {
		res[i] = OrderByClause{Field: obc.Field.Clone(), Direction: obc.Direction}
	}

	return res
}
