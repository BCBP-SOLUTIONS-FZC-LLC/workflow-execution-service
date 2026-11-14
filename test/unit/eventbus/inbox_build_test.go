package eventbus_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/eventbus"
)

func TestNewInbox_StoreRefused_ErrorNamesTheConsumerAndNoInboxIsReturned(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		pool      *pgcommon.Pool
		consumers []string
		wantPart  string
		wantCause string
	}{
		{"nil pool", nil, []string{"vsx-first", "vsx-second"}, `inbox store "vsx-first": `, "non-nil pool"},
		{"empty consumer name after a valid one", &pgcommon.Pool{}, []string{"vsx-first", ""}, `inbox store "": `, "non-empty consumer name"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ledger, err := eventbus.NewInbox(tc.pool, tc.consumers...)

			require.Error(t, err)
			assert.Nil(t, ledger)
			assert.Contains(t, err.Error(), tc.wantPart)
			assert.Contains(t, err.Error(), tc.wantCause)
		})
	}
}
