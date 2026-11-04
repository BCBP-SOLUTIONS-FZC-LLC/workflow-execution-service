package workflow

import (
	"testing"
	"time"
)

func TestParseISODuration(t *testing.T) {
	for in, want := range map[string]time.Duration{
		"P5D":      5 * 24 * time.Hour,
		"P2W":      14 * 24 * time.Hour,
		"PT1H30M":  90 * time.Minute,
		"P1DT12H":  36 * time.Hour,
		"PT45S":    45 * time.Second,
		"P0D":      0,
		"PT0H0M1S": time.Second,
	} {
		got, err := parseISODuration(in)
		if err != nil || got != want {
			t.Errorf("parseISODuration(%q) = %v, %v; want %v", in, got, err, want)
		}
	}
	for _, in := range []string{"", "P", "PT", "5D", "P1Y", "P1M", "P1W2D", "P-1D", "PT1.5H", "P99999999999999999999D", "P106752D"} {
		if got, err := parseISODuration(in); err == nil {
			t.Errorf("parseISODuration(%q) = %v, want an error", in, got)
		}
	}
}
