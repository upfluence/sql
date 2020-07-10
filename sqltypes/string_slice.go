package sqltypes

import (
	"bytes"
	"database/sql/driver"
	"encoding/csv"
	"fmt"
	"io"
	"strings"
)

type StringSlice struct {
	Strings []string
}

func (ss *StringSlice) Scan(v interface{}) error {
	if v == nil {
		ss.Strings = nil
	}

	var r io.Reader

	switch vv := v.(type) {
	case string:
		r = strings.NewReader(vv)
	case []byte:
		r = bytes.NewReader(vv)
	case nil:
		ss.Strings = nil
		return nil
	default:
		return fmt.Errorf("Unsupported %T type to assign to a StringSlice", v)
	}

	var err error

	ss.Strings, err = csv.NewReader(r).Read()

	return err
}

func (ss StringSlice) Value() (driver.Value, error) {
	if len(ss.Strings) == 0 {
		return nil, nil
	}

	var (
		buf bytes.Buffer

		w  = csv.NewWriter(&buf)
	)

	if err := w.Write(ss.Strings); err != nil {
		return nil, err
	}

	w.Flush()

	if res := buf.String(); len(res) > 1 {
		return res[:len(res)-1], nil
	}

	return nil, nil
}
