package eventbus_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/eventbus"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func TestNewInbox_ConsumerList_IsRefusedWhenEmptyOrRepeated(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		consumers []string
		wantPart  string
	}{
		{"no consumers", nil, "no consumers"},
		{"a repeated consumer", []string{"vsx-first", "vsx-second", "vsx-first"}, `duplicate consumer "vsx-first"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ledger, err := eventbus.NewInbox(&pgcommon.Pool{}, tc.consumers...)

			require.Error(t, err)
			assert.Nil(t, ledger)
			assert.Contains(t, err.Error(), tc.wantPart)
		})
	}
}

func TestNewInbox_TheServiceConsumerList_IsAccepted(t *testing.T) {
	t.Parallel()

	ledger, err := eventbus.NewInbox(&pgcommon.Pool{}, port.EventConsumers...)

	require.NoError(t, err)
	assert.NotNil(t, ledger)
}
