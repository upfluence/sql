package sql

type StaticResult int64

func (r StaticResult) LastInsertId() (int64, error) { return int64(r), nil }
func (StaticResult) RowsAffected() (int64, error)   { return 1, nil }
