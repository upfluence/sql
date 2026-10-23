package sqlbuilder

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/upfluence/errors"
)

const (
	sqlite3Driver = "sqlite3"

	// sqliteJSONBElementMatch matches the elements of a JSON array iterated
	// as jsonb_elem with the ones of the array of wanted values iterated as
	// jsonb_want. Values of different storage classes are never equal in
	// SQLite, strings never match numbers, and 1 matches 1.0 as it does in
	// PostgreSQL.
	sqliteJSONBElementMatch = "jsonb_elem.type IN ('integer', 'real', 'text') " +
		"AND jsonb_elem.value = jsonb_want.value"
)

var (
	errInvalidJSONBValue = errors.New("JSONB predicates only support strings and numbers")
	errInvalidJSONBKey   = errors.New("SQLite does not support object keys holding a double quote")
)

// JSONBArrayContainsAny matches the rows whose column, a JSONB array, holds
// at least one of the elements of the slice bound to the marker. The
// elements have to be strings or numbers.
func JSONBArrayContainsAny(m Marker) PredicateClause {
	return &basicClause{m: m, fn: writeJSONBArrayContainsAny}
}

// JSONBArrayContainsAll matches the rows whose column, a JSONB array, holds
// every element of the slice bound to the marker. The elements have to be
// strings or numbers.
func JSONBArrayContainsAll(m Marker) PredicateClause {
	return &basicClause{m: m, fn: writeJSONBArrayContainsAll}
}

// JSONBObjectContains matches the rows whose column, a JSONB object,
// contains the object bound to the marker, as the @> operator of
// PostgreSQL does. The object can only hold strings, numbers, objects and
// arrays of strings and numbers.
func JSONBObjectContains(m Marker) PredicateClause {
	return &basicClause{m: m, fn: writeJSONBObjectContains}
}

func StaticJSONBArrayContainsAny(m Marker, v any) PredicateClause {
	return Static(JSONBArrayContainsAny(m), map[string]any{m.Binding(): v})
}

func StaticJSONBArrayContainsAll(m Marker, v any) PredicateClause {
	return Static(JSONBArrayContainsAll(m), map[string]any{m.Binding(): v})
}

func StaticJSONBObjectContains(m Marker, v any) PredicateClause {
	return Static(JSONBObjectContains(m), map[string]any{m.Binding(): v})
}

func writeJSONBArrayContainsAny(w QueryWriter, vv any, k string) error {
	es, err := decodeJSONBArray(vv)

	if err != nil {
		return err
	}

	if len(es) == 0 {
		_, err := io.WriteString(w, "1=0")

		return errors.WithStack(err)
	}

	if w.Driver() == sqlite3Driver {
		want, err := encodeJSONB(es)

		if err != nil {
			return err
		}

		_, err = fmt.Fprintf(
			w,
			"(json_type(%s) = 'array' AND EXISTS (SELECT 1 FROM json_each(%s) "+
				"AS jsonb_elem, json_each(%s) AS jsonb_want WHERE %s))",
			k,
			k,
			w.RedeemVariable(want),
			sqliteJSONBElementMatch,
		)

		return errors.WithStack(err)
	}

	// A disjunction of containments, unlike the ?| operator, matches
	// numbers and can use both the jsonb_ops and jsonb_path_ops GIN indexes.
	cs := make([]string, len(es))

	for i, e := range es {
		v, err := encodeJSONB([]any{e})

		if err != nil {
			return err
		}

		cs[i] = fmt.Sprintf("%s @> %s::jsonb", k, w.RedeemVariable(v))
	}

	_, err = fmt.Fprintf(w, "(%s)", strings.Join(cs, " OR "))

	return errors.WithStack(err)
}

func writeJSONBArrayContainsAll(w QueryWriter, vv any, k string) error {
	es, err := decodeJSONBArray(vv)

	if err != nil {
		return err
	}

	if w.Driver() == sqlite3Driver {
		c, err := sqliteArrayContainsAll(w, k, "", es)

		if err != nil {
			return err
		}

		_, err = fmt.Fprintf(w, "(%s)", c)

		return errors.WithStack(err)
	}

	return writePostgresContains(w, k, es)
}

func writeJSONBObjectContains(w QueryWriter, vv any, k string) error {
	d, err := decodeJSONB(vv)

	if err != nil {
		return err
	}

	obj, ok := d.(map[string]any)

	if !ok {
		return errors.Wrapf(errInvalidJSONBValue, "%T is not an object", vv)
	}

	if err := checkJSONBObject(obj); err != nil {
		return err
	}

	if w.Driver() != sqlite3Driver {
		return writePostgresContains(w, k, obj)
	}

	cs, err := sqliteObjectContains(
		w,
		k,
		"$",
		obj,
		[]string{fmt.Sprintf("json_type(%s) = 'object'", k)},
	)

	if err != nil {
		return err
	}

	_, err = fmt.Fprintf(w, "(%s)", strings.Join(cs, " AND "))

	return errors.WithStack(err)
}

func writePostgresContains(w QueryWriter, k string, v any) error {
	want, err := encodeJSONB(v)

	if err != nil {
		return err
	}

	_, err = fmt.Fprintf(w, "%s @> %s::jsonb", k, w.RedeemVariable(want))

	return errors.WithStack(err)
}

// sqliteObjectContains appends to cs the conditions matching when the JSON
// object at path in k contains obj: one per leaf of obj.
func sqliteObjectContains(w QueryWriter, k, path string, obj map[string]any, cs []string) ([]string, error) {
	keys := make([]string, 0, len(obj))

	for key := range obj {
		keys = append(keys, key)
	}

	slices.Sort(keys)

	for _, key := range keys {
		if strings.Contains(key, `"`) {
			return nil, errors.Wrapf(errInvalidJSONBKey, "%q", key)
		}

		p := fmt.Sprintf(`%s."%s"`, path, key)

		switch v := obj[key].(type) {
		case map[string]any:
			if len(v) > 0 {
				var err error

				cs, err = sqliteObjectContains(w, k, p, v, cs)

				if err != nil {
					return nil, err
				}

				continue
			}

			cs = append(
				cs,
				fmt.Sprintf("json_type(%s, %s) = 'object'", k, w.RedeemVariable(p)),
			)
		case []any:
			c, err := sqliteArrayContainsAll(w, k, p, v)

			if err != nil {
				return nil, err
			}

			cs = append(cs, c)
		default:
			var (
				typ   = w.RedeemVariable(p)
				value = w.RedeemVariable(p)
			)

			cs = append(
				cs,
				fmt.Sprintf(
					"json_type(%s, %s) IN ('integer', 'real', 'text') AND "+
						"json_extract(%s, %s) = %s",
					k,
					typ,
					k,
					value,
					w.RedeemVariable(sqliteScalar(v)),
				),
			)
		}
	}

	return cs, nil
}

// sqliteArrayContainsAll returns the condition matching when the JSON array
// at path in k, or k itself when path is empty, holds every element of es.
// The column is iterated first so that its name is never resolved to one
// of the columns of the iteration of the wanted values.
func sqliteArrayContainsAll(w QueryWriter, k, path string, es []any) (string, error) {
	want, err := encodeJSONB(es)

	if err != nil {
		return "", err
	}

	var (
		typ  = sqliteJSONArgs(w, k, path)
		elem = sqliteJSONArgs(w, k, path)
	)

	return fmt.Sprintf(
		"json_type(%s) = 'array' AND (SELECT count(DISTINCT jsonb_want.key) "+
			"FROM json_each(%s) AS jsonb_elem, json_each(%s) AS jsonb_want "+
			"WHERE %s) = %d",
		typ,
		elem,
		w.RedeemVariable(want),
		sqliteJSONBElementMatch,
		len(es),
	), nil
}

func sqliteJSONArgs(w QueryWriter, k, path string) string {
	if path == "" {
		return k
	}

	return k + ", " + w.RedeemVariable(path)
}

// sqliteScalar converts a string or a JSON number to the value json_extract
// returns for it.
func sqliteScalar(v any) any {
	n, ok := v.(json.Number)

	if !ok {
		return v
	}

	if i, err := n.Int64(); err == nil {
		return i
	}

	f, _ := n.Float64()

	return f
}

func decodeJSONB(v any) (any, error) {
	buf, err := json.Marshal(v)

	if err != nil {
		return nil, errors.Wrap(err, "cant encode the JSONB value")
	}

	var (
		res any

		dec = json.NewDecoder(bytes.NewReader(buf))
	)

	dec.UseNumber()

	if err := dec.Decode(&res); err != nil {
		return nil, errors.Wrap(err, "cant decode the JSONB value")
	}

	return res, nil
}

func decodeJSONBArray(v any) ([]any, error) {
	d, err := decodeJSONB(v)

	if err != nil {
		return nil, err
	}

	switch d := d.(type) {
	case nil:
		return []any{}, nil
	case []any:
		for _, e := range d {
			if err := checkJSONBScalar(e); err != nil {
				return nil, err
			}
		}

		return d, nil
	}

	return nil, errors.Wrapf(errInvalidJSONBValue, "%T is not an array", v)
}

func checkJSONBScalar(v any) error {
	switch v.(type) {
	case string, json.Number:
		return nil
	}

	return errors.Wrapf(errInvalidJSONBValue, "unexpected %T", v)
}

func checkJSONBObject(obj map[string]any) error {
	for _, v := range obj {
		var err error

		switch v := v.(type) {
		case map[string]any:
			err = checkJSONBObject(v)
		case []any:
			for _, e := range v {
				if err = checkJSONBScalar(e); err != nil {
					break
				}
			}
		default:
			err = checkJSONBScalar(v)
		}

		if err != nil {
			return err
		}
	}

	return nil
}

func encodeJSONB(v any) (string, error) {
	buf, err := json.Marshal(v)

	if err != nil {
		return "", errors.Wrap(err, "cant encode the JSONB value")
	}

	return string(buf), nil
}
