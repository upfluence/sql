package sqltypes

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/upfluence/errors/errtest"
)

type testPayload struct {
	Key string `json:"key"`
}

func TestScan(t *testing.T) {
	for _, tt := range []struct {
		name string

		value   interface{}
		want    JSONValue
		wantErr errtest.ErrorAssertion
	}{
		{
			name:    "wrong value type",
			value:   1,
			want:    JSONValue{Data: &testPayload{}},
			wantErr: errtest.ErrorCause(errInvalidType),
		},
		{
			name:  "invalid json",
			value: "test",
			want:  JSONValue{Data: &testPayload{}},
			wantErr: errtest.ErrorAssertionFunc(
				func(tb testing.TB, err error) {
					assert.IsType(t, &json.SyntaxError{}, err)
				},
			),
		},
		{
			name:    "nil",
			want:    JSONValue{Data: &testPayload{}},
			wantErr: errtest.NoError(),
		},
		{
			name:    "success",
			value:   `{"key":"foo"}`,
			want:    JSONValue{Data: &testPayload{Key: "foo"}, Valid: true},
			wantErr: errtest.NoError(),
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var jv = JSONValue{Data: &testPayload{}}

			err := jv.Scan(tt.value)
			tt.wantErr.Assert(t, err)
			assert.Equal(t, tt.want, jv)
		})
	}
}

func TestShadowScan(t *testing.T) {
	for _, tt := range []struct {
		name string

		scanner sql.Scanner
		value   interface{}
		want    interface{}
	}{
		{
			name: "override object",
			scanner: &JSONValue{
				Data: &testPayload{Key: "foo"},
			},
			value: `{"key":"bar"}`,
			want: &JSONValue{
				Data:  &testPayload{Key: "bar"},
				Valid: true,
			},
		},
		{
			name: "override object with nil becomes invalid",
			scanner: &JSONValue{
				Data:  &testPayload{Key: "foo"},
				Valid: true,
			},
			want: &JSONValue{
				Data: &testPayload{Key: "foo"},
			},
		},
		{
			name: "override map with nil becomes invalid",
			scanner: &JSONValue{
				Data: map[string]interface{}{"foo": 1},
			},
			want: &JSONValue{
				Data:  map[string]interface{}{"foo": 1},
				Valid: false,
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.scanner.Scan(tt.value)
			assert.NoError(t, err)
			assert.Equal(t, tt.want, tt.scanner)
		})
	}
}

func TestValue(t *testing.T) {
	for _, tt := range []struct {
		name string

		value   driver.Valuer
		want    interface{}
		wantErr error
	}{
		{
			name: "invalid data",
			value: JSONValue{
				Data: testPayload{},
			},
		},
		{
			name: "string",
			value: JSONValue{
				Data:  "test",
				Valid: true,
			},
			want: []byte(`"test"`),
		},
		{
			name: "number",
			value: JSONValue{
				Data:  1,
				Valid: true,
			},
			want: []byte(`1`),
		},
		{
			name: "object",
			value: JSONValue{
				Data: testPayload{
					Key: "foo",
				},
				Valid: true,
			},
			want: []byte(`{"key":"foo"}`),
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			v, err := tt.value.Value()
			assert.Equal(t, tt.wantErr, err)
			assert.Equal(t, tt.want, v)
		})
	}
}
