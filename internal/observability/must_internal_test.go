package observability

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMust_PanicsOnConstructorError(t *testing.T) {
	require.Panics(t, func() { must(0, errors.New("name rejected")) })
	require.Equal(t, 7, must(7, nil))
}
