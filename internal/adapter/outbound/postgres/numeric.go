package postgres

import "math"

func clampInt32[T ~int | ~int64](n T) int32 {
	switch {
	case int64(n) > math.MaxInt32:
		return math.MaxInt32
	case int64(n) < math.MinInt32:
		return math.MinInt32
	default:
		return int32(n)
	}
}

func pageLimit(limit int) int32 {
	return clampInt32(min(int64(limit), math.MaxInt32-1) + 1)
}
