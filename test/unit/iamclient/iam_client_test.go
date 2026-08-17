// Package iamclient_test exercises the IAMClient stub — its
// GetUserStatus endpoint contract isn't confirmed with the IAM team yet
// (internal/adapter/outbound/http/iam_client.go's own doc comment), so this
// test only confirms the stub's documented behavior: construct cleanly,
// always return ErrIAMContractNotConfirmed, never a partial/misleading
// UserStatus.
package iamclient_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	httpadapter "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/http"
)

func TestIAMClient_GetUserStatus_NotYetConfirmed(t *testing.T) {
	c := httpadapter.NewIAMClient("http://iam.internal", 5*time.Second)

	status, err := c.GetUserStatus(context.Background(), uuid.New(), uuid.New())
	require.ErrorIs(t, err, httpadapter.ErrIAMContractNotConfirmed)
	assert.Zero(t, status)
}
