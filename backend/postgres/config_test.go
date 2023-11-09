package postgres

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDSN(t *testing.T) {
	for _, tt := range []struct {
		c   *Config
		dsn string
	}{
		{
			c:   &Config{DBName: "foobar", SSLMode: VerifyFull, SSLSNI: true},
			dsn: "postgres://localhost:5432/foobar?sslmode=verify-full&sslsni=1",
		},
		{
			c:   &Config{DBName: "foobar", CACertFile: "foobar"},
			dsn: "postgres://localhost:5432/foobar?sslmode=verify-ca&sslrootcert=foobar&sslsni=0",
		},
	} {
		dsn, err := tt.c.DSN()

		assert.NoError(t, err)
		assert.Equal(t, tt.dsn, dsn)
	}
}
