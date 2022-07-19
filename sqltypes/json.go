package sqltypes

import (
	"database/sql/driver"
	"encoding/json"

	"github.com/upfluence/errors"
)

type JSONValue struct {
	Data  interface{}
	Valid bool
}

func (jv *JSONValue) Scan(v interface{}) error {
	var err error

	jv.Valid = false

	switch vv := v.(type) {
	case nil:
		return nil
	case []byte:
		err = json.Unmarshal(vv, &jv.Data)
	case string:
		err = json.Unmarshal([]byte(vv), &jv.Data)
	default:
		err = errors.Wrap(errInvalidType, "expecting a byte slice or string")
	}

	if err != nil {
		return err
	}

	jv.Valid = true
	return nil
}

func (jv JSONValue) Value() (driver.Value, error) {
	if !jv.Valid {
		return nil, nil
	}

	return json.Marshal(jv.Data)
}
