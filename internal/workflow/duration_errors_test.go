package workflow

import (
	"strings"
	"testing"
	"time"
)

func TestParseISODuration_NamesWhyAPeriodIsRefused(t *testing.T) {
	for _, tc := range []struct {
		in      string
		message string
	}{
		{"P", "has no length"},
		{"PT", "has no length"},
		{"pt5m", "is not an ISO 8601 period"},
		{"5m", "is not an ISO 8601 period"},
		{"P1D2H", "is not an ISO 8601 period"},
		{"-PT5M", "is not an ISO 8601 period"},
		{"P1Y", "is not an ISO 8601 period"},
		{"P1M", "is not an ISO 8601 period"},
		{"P99999999999999999999D", "is too long"},
	} {
		_, err := parseISODuration(tc.in)
		if err == nil || !strings.Contains(err.Error(), tc.message) {
			t.Errorf("parseISODuration(%q) error = %v, want one containing %q", tc.in, err, tc.message)
		}
	}
}

func TestParseISODuration_ADesignatorWithNothingAfterTheTIsStillAPeriod(t *testing.T) {
	got, err := parseISODuration("P1DT")
	if err != nil || got != 24*time.Hour {
		t.Errorf("parseISODuration(%q) = %v, %v; want %v", "P1DT", got, err, 24*time.Hour)
	}
}
