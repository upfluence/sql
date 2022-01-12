package sqltypes

import (
	"database/sql/driver"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/upfluence/errors/errtest"
)

func TestStringSlice_Scan(t *testing.T) {
	for _, tt := range []struct {
		name    string
		value   interface{}
		want    []string
		wantErr errtest.ErrorAssertion
	}{
		{
			name:    "valid",
			value:   `foo,bar`,
			want:    []string{"foo", "bar"},
			wantErr: errtest.NoError(),
		},
		{
			name:    "escape coma",
			value:   `"foo,fuu","bar"`,
			want:    []string{"foo,fuu", "bar"},
			wantErr: errtest.NoError(),
		},
		{
			name:    "empty slice",
			value:   "",
			want:    []string{},
			wantErr: errtest.NoError(),
		},
		{
			name:    "nil slice",
			value:   nil,
			want:    nil,
			wantErr: errtest.NoError(),
		},
		{
			name:    "invalid type",
			value:   true,
			want:    nil,
			wantErr: errtest.ErrorCause(errInvalidType),
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var v = StringSlice{}

			tt.wantErr.Assert(t, v.Scan(tt.value))
			assert.Equal(t, tt.want, v.Strings)
		})
	}
}

func TestStringSlice_Value(t *testing.T) {
	for _, tt := range []struct {
		name  string
		value StringSlice
		want  driver.Value
	}{
		{
			name: "valid",
			value: StringSlice{
				Strings: []string{"foo", "bar"},
				Valid:   true,
			},
			want: `foo,bar`,
		},
		{
			name: "invalid flag",
			value: StringSlice{
				Strings: []string{"foo", "bar"},
				Valid:   false,
			},
			want: nil,
		},
		{
			name: "escape coma",
			value: StringSlice{
				Strings: []string{"foo,fuu", "bar"},
				Valid:   true,
			},
			want: `"foo,fuu",bar`,
		},
		{
			name: "empty slice",
			value: StringSlice{
				Strings: []string{},
				Valid:   true,
			},
			want: "",
		},
		{
			name: "nil slice",
			value: StringSlice{
				Strings: []string{},
				Valid:   false,
			},
			want: nil,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var v, err = tt.value.Value()

			require.NoError(t, err)
			assert.Equal(t, tt.want, v)
		})
	}
}
