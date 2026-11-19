package entitlementsclient_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	httpadapter "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/http"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability/obstest"
)

func TestFeatureFlags_TextAfterTheJSONValue_IsRefusedAsAResponseFailure(t *testing.T) {
	obstest.Init(t)
	tenantID := uuid.New()
	valid := `{"tenant_id":"` + tenantID.String() + `","feature_flags":["sso_enabled"]}`
	cases := []struct {
		name string
		body string
	}{
		{"garbage after the object", valid + ` garbage`},
		{"a second object", valid + `{"tenant_id":"` + tenantID.String() + `","feature_flags":[]}`},
		{"a stray bracket", valid + `]`},
		{"html after the object", valid + "\n<html></html>"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			failures := observability.EntitlementsCallErrorsTotal.WithLabelValues("response")
			before := testutil.ToFloat64(failures)
			srv := serve(t, http.StatusOK, tc.body)

			flags, err := httpadapter.NewEntitlementsClient(srv.URL, time.Second).FeatureFlags(context.Background(), tenantID)

			require.ErrorIs(t, err, port.ErrEntitlementsUnavailable)
			assert.Nil(t, flags, "no flags are taken from an answer that cannot be trusted")
			assert.Equal(t, before+1, testutil.ToFloat64(failures))
		})
	}

	t.Run("trailing whitespace after the object is accepted", func(t *testing.T) {
		srv := serve(t, http.StatusOK, valid+" \n")

		flags, err := httpadapter.NewEntitlementsClient(srv.URL, time.Second).FeatureFlags(context.Background(), tenantID)

		require.NoError(t, err)
		assert.Equal(t, []string{"sso_enabled"}, flags)
	})
}
