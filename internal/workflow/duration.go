package workflow

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"time"
)

var isoDurationRe = regexp.MustCompile(`^P(?:(\d+)W|(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?)?)$`)

func parseISODuration(s string) (time.Duration, error) {
	m := isoDurationRe.FindStringSubmatch(s)
	if m == nil {
		return 0, fmt.Errorf("workflow: %q is not an ISO 8601 period", s)
	}
	units := []time.Duration{7 * 24 * time.Hour, 24 * time.Hour, time.Hour, time.Minute, time.Second}
	var total time.Duration
	var seen bool
	for i, unit := range units {
		if m[i+1] == "" {
			continue
		}
		seen = true
		n, err := strconv.ParseInt(m[i+1], 10, 64)
		if err != nil || n > int64(math.MaxInt64/unit) || total > math.MaxInt64-time.Duration(n)*unit {
			return 0, fmt.Errorf("workflow: ISO 8601 period %q is too long", s)
		}
		total += time.Duration(n) * unit
	}
	if !seen {
		return 0, fmt.Errorf("workflow: ISO 8601 period %q has no length", s)
	}
	return total, nil
}
