package sqltypes

import (
	"database/sql"
	"database/sql/driver"
	"time"
)

type NullUTCTime struct {
	Time  time.Time
	Valid bool
}

func (nut *NullUTCTime) Scan(v interface{}) error {
	var nt sql.NullTime

	if err := nt.Scan(v); err != nil {
		return err
	}

	nut.Valid = nt.Valid
	nut.Time = time.Unix(nt.Time.UTC().Unix(), 0).UTC()

	return nil
}

func (nut NullUTCTime) Value() (driver.Value, error) {
	if !nut.Valid {
		return nil, nil
	}

	return nut.Time.UTC().Truncate(time.Second), nil
}
