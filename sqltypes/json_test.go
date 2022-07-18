package sqltypes

import (
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

		value   any
		want    testPayload
		wantErr errtest.ErrorAssertion
	}{
		{
			name:    "wrong value type",
			value:   1,
			wantErr: errtest.ErrorCause(errInvalidType),
		},
		{
			name:  "invalid json",
			value: "test",
			wantErr: errtest.ErrorAssertionFunc(
				func(tb testing.TB, err error) {
					assert.IsType(t, &json.SyntaxError{}, err)
				},
			),
		},
		{
			name:    "nil",
			want:    testPayload{},
			wantErr: errtest.NoError(),
		},
		{
			name:    "success",
			value:   `{"key":"foo"}`,
			want:    testPayload{Key: "foo"},
			wantErr: errtest.NoError(),
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var jv JSONValue[testPayload]

			err := jv.Scan(tt.value)
			tt.wantErr.Assert(t, err)
			assert.Equal(t, tt.want, jv.Data)
		})
	}
}

func TestValue(t *testing.T) {
	for _, tt := range []struct {
		name string

		value   driver.Valuer
		want    any
		wantErr error
	}{
		{
			name: "invalid data",
			value: JSONValue[testPayload]{
				Data: testPayload{},
			},
		},
		{
			name: "string",
			value: JSONValue[string]{
				Data:  "test",
				Valid: true,
			},
			want: []byte(`"test"`),
		},
		{
			name: "number",
			value: JSONValue[int64]{
				Data:  1,
				Valid: true,
			},
			want: []byte(`1`),
		},
		{
			name: "object",
			value: JSONValue[testPayload]{
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
