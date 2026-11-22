package port_test

import (
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func TestAssigneeIneligibleError_WrappedWithPercentW_StillMatchesSentinelAndKeepsNodes(t *testing.T) {
	cases := []struct {
		name  string
		nodes []string
	}{
		{name: "two nodes", nodes: []string{"finance/review", "legal/sign"}},
		{name: "one node", nodes: []string{"finance/review"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			inner := &port.AssigneeIneligibleError{Nodes: tc.nodes}
			wrapped := fmt.Errorf("start instance: %w", fmt.Errorf("check eligibility: %w", inner))

			require.ErrorIs(t, wrapped, port.ErrAssigneeIneligible)
			var got *port.AssigneeIneligibleError
			require.ErrorAs(t, wrapped, &got)
			assert.Equal(t, tc.nodes, got.Nodes)
			assert.Same(t, inner, got)
			assert.Equal(t, port.ErrAssigneeIneligible, got.Unwrap())
		})
	}
}

func TestAssigneeIneligibleError_Error_IsFixedAndNamesNoNode(t *testing.T) {
	cases := []struct {
		name  string
		nodes []string
	}{
		{name: "nil nodes", nodes: nil},
		{name: "empty nodes", nodes: []string{}},
		{name: "one node", nodes: []string{"finance/review"}},
		{name: "two nodes", nodes: []string{"finance/review", "legal/sign"}},
	}
	want := (&port.AssigneeIneligibleError{}).Error()
	require.NotEmpty(t, want)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := (&port.AssigneeIneligibleError{Nodes: tc.nodes}).Error()

			assert.Equal(t, want, got, "the text must not change with the node list")
			for _, n := range tc.nodes {
				assert.NotContains(t, got, n)
			}
			assert.NotEqual(t, port.ErrAssigneeIneligible.Error(), got, "the typed error has its own text, apart from the sentinel's")
		})
	}
}

func TestUserAvailabilityScope_BuildsKeyFromTenantThenUser(t *testing.T) {
	tenant := uuid.MustParse("11111111-1111-4111-8111-111111111111")
	user := uuid.MustParse("22222222-2222-4222-8222-222222222222")

	cases := []struct {
		name         string
		tenant, user uuid.UUID
		want         string
	}{
		{
			name:   "tenant then user",
			tenant: tenant, user: user,
			want: "user_availability:11111111-1111-4111-8111-111111111111:22222222-2222-4222-8222-222222222222",
		},
		{
			name:   "swapped ids give another key",
			tenant: user, user: tenant,
			want: "user_availability:22222222-2222-4222-8222-222222222222:11111111-1111-4111-8111-111111111111",
		},
		{
			name:   "nil uuids are not rejected",
			tenant: uuid.Nil, user: uuid.Nil,
			want: "user_availability:00000000-0000-0000-0000-000000000000:00000000-0000-0000-0000-000000000000",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, port.UserAvailabilityScope(tc.tenant, tc.user))
		})
	}

	assert.NotEqual(t, port.UserAvailabilityScope(tenant, user), port.UserAvailabilityScope(user, tenant))
}

func TestCursor_RoundTripKeepsNanosecondsAndOffset(t *testing.T) {
	id := uuid.MustParse("a3c7d1e2-9f45-4b6a-8c12-3e9b7d4f6a01")
	plusFive := time.FixedZone("plus5", 5*3600+30*60)
	cases := []struct {
		name string
		at   time.Time
	}{
		{name: "nanoseconds in UTC", at: time.Date(2026, 7, 20, 8, 52, 11, 123_456_789, time.UTC)},
		{name: "non UTC offset", at: time.Date(2026, 7, 20, 8, 52, 11, 987_654_321, plusFive)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			decoded, err := port.DecodeCursor(port.EncodeCursor(port.CursorPosition{CreatedAt: tc.at, ID: id}))

			require.NoError(t, err)
			assert.True(t, tc.at.Equal(decoded.CreatedAt), "want %s got %s", tc.at, decoded.CreatedAt)
			assert.Equal(t, tc.at.Nanosecond(), decoded.CreatedAt.Nanosecond())
			assert.Equal(t, id, decoded.ID)
			_, wantOffset := tc.at.Zone()
			_, gotOffset := decoded.CreatedAt.Zone()
			assert.Equal(t, wantOffset, gotOffset, "the offset survives the round trip")
		})
	}
}

func TestCursor_EncodedZeroCreatedAt_IsRefusedOnDecode(t *testing.T) {
	raw := port.EncodeCursor(port.CursorPosition{ID: uuid.MustParse("a3c7d1e2-9f45-4b6a-8c12-3e9b7d4f6a01")})

	got, err := port.DecodeCursor(raw)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "decode cursor: missing created_at")
	assert.Equal(t, port.CursorPosition{}, got)
}

func TestDecodeCursor_NotPaddedURLSafeBase64_IsRefusedWithDecodeCursorPrefix(t *testing.T) {
	valid := base64.URLEncoding.EncodeToString([]byte(`{"created_at":"2026-07-20T08:52:11Z","id":"a3c7d1e2-9f45-4b6a-8c12-3e9b7d4f6a01"} `))
	require.True(t, strings.HasSuffix(valid, "="), "the fixture must be padded for the unpadded case to differ")
	// Same bytes as `valid` in the standard alphabet: '-' becomes '+' and '_' becomes '/'. Pick a
	// payload whose URL-safe form uses those characters.
	stdBytes := []byte{0xfb, 0xff, 0xfe, 0xfb, 0xff}
	stdForm := base64.StdEncoding.EncodeToString(stdBytes)
	require.True(t, strings.ContainsAny(stdForm, "+/"))

	cases := []struct {
		name string
		raw  string
	}{
		{name: "standard alphabet", raw: stdForm},
		{name: "unpadded", raw: strings.TrimRight(valid, "=")},
		{name: "garbage characters", raw: "not-valid-base64!!!"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := port.DecodeCursor(tc.raw)

			require.Error(t, err)
			assert.True(t, strings.HasPrefix(err.Error(), "decode cursor: "), err.Error())
			var corrupt base64.CorruptInputError
			assert.True(t, errors.As(err, &corrupt), "the refusal is a base64 error, not a JSON one")
			assert.Equal(t, port.CursorPosition{}, got)
		})
	}
}

func TestDecodeCursor_Base64OfNonObjectJSON_IsRefusedAsBadContent(t *testing.T) {
	enc := func(s string) string { return base64.URLEncoding.EncodeToString([]byte(s)) }
	cases := []struct {
		name    string
		raw     string
		wantMsg string
	}{
		{name: "json array", raw: enc(`[1,2]`), wantMsg: "decode cursor: json: cannot unmarshal array"},
		{name: "plain text", raw: enc(`not-json`), wantMsg: "decode cursor: invalid character"},
		{name: "bad time", raw: enc(`{"created_at":"yesterday","id":"a3c7d1e2-9f45-4b6a-8c12-3e9b7d4f6a01"}`), wantMsg: "decode cursor: parsing time"},
		{name: "bad uuid", raw: enc(`{"created_at":"2026-07-20T08:52:11Z","id":"not-a-uuid"}`), wantMsg: "decode cursor: invalid UUID"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := port.DecodeCursor(tc.raw)

			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantMsg)
			assert.NotContains(t, err.Error(), "missing", "the content is unreadable, not merely incomplete")
			assert.Equal(t, port.CursorPosition{}, got)
		})
	}
}
