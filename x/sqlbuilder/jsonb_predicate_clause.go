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
	errInvalidJSONBValue = errors.New("JSONB predicates do not support booleans and nulls")
	errInvalidJSONBKey   = errors.New("SQLite does not support object keys holding a double quote")
)

// JSONBArrayContainsAny matches the rows whose column, a JSONB array, holds
// at least one of the elements of the slice bound to the marker. An object
// or array element matches the elements containing it, as the @> operator
// of PostgreSQL does. The elements can not hold booleans and nulls.
func JSONBArrayContainsAny(m Marker) PredicateClause {
	return &basicClause{m: m, fn: writeJSONBArrayContainsAny}
}

// JSONBArrayContainsAll matches the rows whose column, a JSONB array, holds
// every element of the slice bound to the marker. An object or array element
// matches the elements containing it, as the @> operator of PostgreSQL does.
// The elements can not hold booleans and nulls.
func JSONBArrayContainsAll(m Marker) PredicateClause {
	return &basicClause{m: m, fn: writeJSONBArrayContainsAll}
}

// JSONBObjectContains matches the rows whose column, a JSONB object,
// contains the object bound to the marker, as the @> operator of
// PostgreSQL does. The object can not hold booleans and nulls.
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
		sc := sqliteContains{w: w}

		cs, err := sc.containsAny(k, es)

		if err != nil {
			return err
		}

		_, err = fmt.Fprintf(w, "(json_type(%s) = 'array' AND %s)", k, cs)

		return errors.WithStack(err)
	}

	// A disjunction of containments, unlike the ?| operator, matches
	// numbers and objects and can use both the jsonb_ops and jsonb_path_ops
	// GIN indexes.
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

	return writeJSONBContains(w, k, es)
}

func writeJSONBObjectContains(w QueryWriter, vv any, k string) error {
	d, err := decodeJSONB(vv)

	if err != nil {
		return err
	}

	if _, ok := d.(map[string]any); !ok {
		return errors.Wrapf(errInvalidJSONBValue, "%T is not an object", vv)
	}

	return writeJSONBContains(w, k, d)
}

func writeJSONBContains(w QueryWriter, k string, want any) error {
	if w.Driver() != sqlite3Driver {
		v, err := encodeJSONB(want)

		if err != nil {
			return err
		}

		_, err = fmt.Fprintf(w, "%s @> %s::jsonb", k, w.RedeemVariable(v))

		return errors.WithStack(err)
	}

	sc := sqliteContains{w: w}

	cs, err := sc.contains(k, "", want)

	if err != nil {
		return err
	}

	_, err = fmt.Fprintf(w, "(%s)", strings.Join(cs, " AND "))

	return errors.WithStack(err)
}

// sqliteContains writes the SQLite conditions matching a JSON document
// containing a wanted value, as the @> operator of PostgreSQL does.
type sqliteContains struct {
	w QueryWriter

	// items is the number of aliases given to the iterations of the arrays
	// holding objects or arrays, each one is unique so that the nested
	// iterations never shadow each other.
	items int
}

// contains returns the conditions matching when the JSON value at path in
// doc, or doc itself when path is empty, contains want.
func (sc *sqliteContains) contains(doc, path string, want any) ([]string, error) {
	switch want := want.(type) {
	case map[string]any:
		return sc.containsObject(doc, path, want)
	case []any:
		return sc.containsArray(doc, path, want)
	}

	if path == "" {
		path = "$"
	}

	var (
		typ   = sc.args(doc, path)
		value = sc.args(doc, path)
	)

	return []string{
		fmt.Sprintf(
			"json_type(%s) IN ('integer', 'real', 'text') AND json_extract(%s) = %s",
			typ,
			value,
			sc.w.RedeemVariable(sqliteScalar(want)),
		),
	}, nil
}

func (sc *sqliteContains) containsObject(doc, path string, obj map[string]any) ([]string, error) {
	var cs []string

	// The type of a non empty object below the root is checked by the
	// conditions on its keys, which never match a value of another type.
	if path == "" || len(obj) == 0 {
		cs = append(cs, fmt.Sprintf("json_type(%s) = 'object'", sc.args(doc, path)))
	}

	keys := make([]string, 0, len(obj))

	for key := range obj {
		keys = append(keys, key)
	}

	slices.Sort(keys)

	for _, key := range keys {
		if strings.Contains(key, `"`) {
			return nil, errors.Wrapf(errInvalidJSONBKey, "%q", key)
		}

		parent := path

		if parent == "" {
			parent = "$"
		}

		kcs, err := sc.contains(doc, fmt.Sprintf(`%s."%s"`, parent, key), obj[key])

		if err != nil {
			return nil, err
		}

		cs = append(cs, kcs...)
	}

	return cs, nil
}

// containsArray matches the scalars of es with a single iteration of the
// array, counting the ones it holds, and every object or array of es with
// an iteration of its own.
func (sc *sqliteContains) containsArray(doc, path string, es []any) ([]string, error) {
	cs := []string{fmt.Sprintf("json_type(%s) = 'array'", sc.args(doc, path))}

	scalars, others := splitJSONBScalars(es)

	if len(scalars) > 0 {
		want, err := encodeJSONB(scalars)

		if err != nil {
			return nil, err
		}

		elem := sc.args(doc, path)

		cs = append(
			cs,
			fmt.Sprintf(
				"(SELECT count(DISTINCT jsonb_want.key) FROM json_each(%s) AS "+
					"jsonb_elem, json_each(%s) AS jsonb_want WHERE %s) = %d",
				elem,
				sc.w.RedeemVariable(want),
				sqliteJSONBElementMatch,
				len(scalars),
			),
		)
	}

	for _, e := range others {
		c, err := sc.existsItem(doc, path, e)

		if err != nil {
			return nil, err
		}

		cs = append(cs, c)
	}

	return cs, nil
}

// containsAny returns the condition matching when the JSON array doc holds
// one of the elements of es.
func (sc *sqliteContains) containsAny(doc string, es []any) (string, error) {
	var (
		cs []string

		scalars, others = splitJSONBScalars(es)
	)

	if len(scalars) > 0 {
		want, err := encodeJSONB(scalars)

		if err != nil {
			return "", err
		}

		cs = append(
			cs,
			fmt.Sprintf(
				"EXISTS (SELECT 1 FROM json_each(%s) AS jsonb_elem, json_each(%s) "+
					"AS jsonb_want WHERE %s)",
				doc,
				sc.w.RedeemVariable(want),
				sqliteJSONBElementMatch,
			),
		)
	}

	for _, e := range others {
		c, err := sc.existsItem(doc, "", e)

		if err != nil {
			return "", err
		}

		cs = append(cs, c)
	}

	if len(cs) == 1 {
		return cs[0], nil
	}

	return "(" + strings.Join(cs, " OR ") + ")", nil
}

// existsItem returns the condition matching when the JSON array at path in
// doc holds an element containing want, an object or an array.
func (sc *sqliteContains) existsItem(doc, path string, want any) (string, error) {
	sc.items++

	var (
		array = sc.args(doc, path)
		alias = fmt.Sprintf("jsonb_item_%d", sc.items)
		typ   = "array"
	)

	if _, ok := want.(map[string]any); ok {
		typ = "object"
	}

	// json_each returns the strings of an array without their quotes, which
	// the JSON functions reject as malformed: the other elements are only
	// read as NULL, as the order the terms of a WHERE clause are evaluated
	// in is not guaranteed.
	cs, err := sc.contains(
		fmt.Sprintf("(CASE WHEN %[1]s.type = '%[2]s' THEN %[1]s.value END)", alias, typ),
		"",
		want,
	)

	if err != nil {
		return "", err
	}

	return fmt.Sprintf(
		"EXISTS (SELECT 1 FROM json_each(%s) AS %s WHERE %s)",
		array,
		alias,
		strings.Join(cs, " AND "),
	), nil
}

// args returns the arguments of a JSON function reading the value at path
// in doc, or doc itself when path is empty.
func (sc *sqliteContains) args(doc, path string) string {
	if path == "" {
		return doc
	}

	return doc + ", " + sc.w.RedeemVariable(path)
}

func splitJSONBScalars(es []any) ([]any, []any) {
	var scalars, others []any

	for _, e := range es {
		switch e.(type) {
		case map[string]any, []any:
			others = append(others, e)
		default:
			scalars = append(scalars, e)
		}
	}

	return scalars, others
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

// decodeJSONB returns the JSON representation of v, decoded with its
// numbers as json.Number, after checking it holds no booleans and nulls.
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

	if res == nil {
		return nil, nil
	}

	return res, checkJSONB(res)
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
		return d, nil
	}

	return nil, errors.Wrapf(errInvalidJSONBValue, "%T is not an array", v)
}

func checkJSONB(v any) error {
	switch v := v.(type) {
	case string, json.Number:
		return nil
	case map[string]any:
		for _, e := range v {
			if err := checkJSONB(e); err != nil {
				return err
			}
		}

		return nil
	case []any:
		for _, e := range v {
			if err := checkJSONB(e); err != nil {
				return err
			}
		}

		return nil
	}

	return errors.Wrapf(errInvalidJSONBValue, "unexpected %T", v)
}

func encodeJSONB(v any) (string, error) {
	buf, err := json.Marshal(v)

	if err != nil {
		return "", errors.Wrap(err, "cant encode the JSONB value")
	}

	return string(buf), nil
}
