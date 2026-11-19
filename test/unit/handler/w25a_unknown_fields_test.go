package handler_test

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// w25aUnknownFieldBody builds a claim body with one valid field and the given extra field names.
func w25aUnknownFieldBody(names []string) string {
	parts := []string{`"record_version":1`}
	for _, n := range names {
		parts = append(parts, fmt.Sprintf("%q:1", n))
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func w25aNumberedNames(n int) []string {
	names := make([]string, n)
	for i := range names {
		names[i] = fmt.Sprintf("f%02d", i)
	}
	return names
}

// An unknown field is a 400 that names it; the list holds at most 20 names in ascending order and each name is
// cut to 64 runes (never in the middle of a multi-byte character).
func TestStrictBody_UnknownFields_ListIsCappedSortedAndEachNameIsCut(t *testing.T) {
	sixtyFour := strings.Repeat("a", 64)
	multiByte := strings.Repeat("é", 70)
	cases := []struct {
		name  string
		names []string
		want  []string
	}{
		{name: "exactly 20 names are all reported", names: w25aNumberedNames(20), want: w25aNumberedNames(20)},
		{name: "21 names report the first 20 in ascending order", names: w25aNumberedNames(21), want: w25aNumberedNames(20)},
		{name: "30 names listed in reverse still come back ascending", names: w25aReverse(w25aNumberedNames(30)), want: w25aNumberedNames(20)},
		{name: "a name of exactly 64 characters is kept whole", names: []string{sixtyFour}, want: []string{sixtyFour}},
		{name: "a name of 65 characters is cut to 64", names: []string{sixtyFour + "b"}, want: []string{sixtyFour}},
		{name: "a 70 character name is cut to 64", names: []string{strings.Repeat("a", 70)}, want: []string{sixtyFour}},
		{name: "a multi-byte name is cut to 64 runes", names: []string{multiByte}, want: []string{strings.Repeat("é", 64)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tasks := &fakeTaskService{
				claim: func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, int64) (*port.Task, error) {
					t.Error("the service must not be called for a body with unknown fields")
					return newTestTask(), nil
				},
			}
			router := newRouter(newHandler(tasks, &fakeEligibilityChecker{}))

			w := w25aServe(router, w25aRaw(http.MethodPost, w25aClaimPath(), "application/json", w25aUnknownFieldBody(tc.names)))

			p := w25aDecodeProblem(t, w, http.StatusBadRequest)
			assert.Equal(t, "BAD_REQUEST", p.Code)
			assert.Equal(t, "request body has fields this endpoint does not define", p.Detail)
			got := make([]string, len(p.InvalidParams))
			for i, ip := range p.InvalidParams {
				got[i] = ip.Name
				assert.Equal(t, "is not a known field", ip.Reason)
			}
			require.Equal(t, tc.want, got)
		})
	}
}

func w25aReverse(in []string) []string {
	out := make([]string, len(in))
	for i, s := range in {
		out[len(in)-1-i] = s
	}
	return out
}
