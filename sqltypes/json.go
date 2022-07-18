package sqltypes

import (
	"database/sql/driver"
	"encoding/json"

	"github.com/upfluence/errors"
)

type JSONValue[T any] struct {
	Data  T
	Valid bool
}

func (jv *JSONValue[T]) Scan(v interface{}) error {
	switch vv := v.(type) {
	case []byte:
		return json.Unmarshal(vv, &jv.Data)
	case string:
		return json.Unmarshal([]byte(vv), &jv.Data)
	case nil:
		jv.Data = *new(T)
		return nil
	default:
		return errors.Wrap(errInvalidType, "expecting a byte slice or string")
	}
}

func (jv JSONValue[T]) Value() (driver.Value, error) {
	if !jv.Valid {
		return nil, nil
	}

	return json.Marshal(jv.Data)
}
