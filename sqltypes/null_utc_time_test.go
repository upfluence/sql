package sqltypes

import (
	"database/sql/driver"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/upfluence/pkg/testutil"
)

func TestNullUTCTime_Scan(t *testing.T) {
	var gmtLocation, err = time.LoadLocation("Europe/Paris")

	require.NoError(t, err)

	for _, tt := range []struct{
		name string
		value interface{}
		want time.Time
		errFn testutil.ErrorAssertion
	} {
		{
			name: "valid with utc",
			value: time.Date(2000, 1, 1, 8, 9, 10, 11, time.UTC),
			want: time.Date(2000, 1, 1, 8, 9, 10, 00, time.UTC),
			errFn: testutil.NoError(),
		},
		{
			name: "valid with other zone",
			value: time.Date(2000, 1, 1, 8, 9, 10, 11, gmtLocation),
			want: time.Date(2000, 1, 1, 7, 9, 10, 00, time.UTC),
			errFn: testutil.NoError(),
		},
		{
			name: "invalid",
			value: "invalid",
			errFn: func(t testing.TB, err error) {
				assert.Contains(t, err.Error(), "unsupported Scan")
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var n = NullUTCTime{}

			tt.errFn(t, n.Scan(tt.value))
			assert.Equal(t, tt.want, n.Time)
		})
	}
}

func TestNullUTCTime_Value(t *testing.T) {
	var gmtLocation, err = time.LoadLocation("Europe/Paris")

	require.NoError(t, err)

	for _, tt := range []struct{
		name string
		value NullUTCTime
		want driver.Value
	} {
		{
			name: "valid with utc",
			value: NullUTCTime{
				Time: time.Date(2000, 1, 1, 8, 9, 10, 11, time.UTC),
				Valid: true,
			},
			want: time.Date(2000, 1, 1, 8, 9, 10, 00, time.UTC),
		},
		{
			name: "invalid with value",
			value: NullUTCTime{
				Time: time.Date(2000, 1, 1, 8, 9, 10, 11, time.UTC),
				Valid: false,
			},
			want: nil,
		},
		{
			name: "valid with other zone",
			value: NullUTCTime{
				Time: time.Date(2000, 1, 1, 8, 9, 10, 11, gmtLocation),
				Valid: true,
			},
			want: time.Date(2000, 1, 1, 7, 9, 10, 00, time.UTC),
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var v, err = tt.value.Value()

			require.NoError(t, err)
			assert.Equal(t, tt.want, v)
		})
	}
}
