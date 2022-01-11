package sqltypes

import (
	"bytes"
	"database/sql/driver"
	"encoding/csv"
	"fmt"
	"io"
	"strings"

	"github.com/upfluence/errors"
)

var errInvalidType = errors.New("unsupported type to assign to a StringSlice")

type StringSlice struct {
	Strings []string
	Valid   bool
}

func (ss *StringSlice) Scan(v interface{}) error {
	var r io.Reader

	switch vv := v.(type) {
	case string:
		if vv == "" {
			ss.Strings = []string{}
			return nil
		}

		r = strings.NewReader(vv)
	case []byte:
		r = bytes.NewReader(vv)
	case nil:
		ss.Strings = nil
		return nil
	default:
		return errors.Wrap(errInvalidType, fmt.Sprintf("%T", v))
	}

	var err error

	ss.Strings, err = csv.NewReader(r).Read()

	return err
}

func (ss StringSlice) Value() (driver.Value, error) {
	if !ss.Valid {
		return nil, nil
	}

	if len(ss.Strings) == 0 {
		return "", nil
	}

	var (
		buf bytes.Buffer

		w = csv.NewWriter(&buf)
	)

	if err := w.Write(ss.Strings); err != nil {
		return nil, err
	}

	w.Flush()

	if res := buf.String(); len(res) > 1 {
		return res[:len(res)-1], nil
	}

	return "", nil
}
