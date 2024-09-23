//go:build go1.23

package sqlbuilder

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSeqIN(t *testing.T) {
	var w queryWriter

	err := StaticIn(
		Column("foo"),
		slices.Values([]int{4, 5, 6}),
	).WriteTo(&w, nil)

	require.NoError(t, err)
	assert.Equal(t, "foo IN ($1, $2, $3)", w.String())
	assert.Equal(t, []any{4, 5, 6}, w.vs)
}
