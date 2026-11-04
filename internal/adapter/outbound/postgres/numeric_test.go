package postgres

import (
	"math"
	"testing"
)

func TestClampInt32(t *testing.T) {
	cases := []struct {
		name string
		in   int64
		want int32
	}{
		{"a value that fits is unchanged", 42, 42},
		{"a negative value that fits is unchanged", -7, -7},
		{"a value above the range is the largest int32", math.MaxInt32 + 1, math.MaxInt32},
		{"a value below the range is the smallest int32", math.MinInt32 - 1, math.MinInt32},
		{"the largest int64 is the largest int32", math.MaxInt64, math.MaxInt32},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := clampInt32(tc.in); got != tc.want {
				t.Errorf("clampInt32(%d) = %d, want %d", tc.in, got, tc.want)
			}
			if got := clampInt32(int(tc.in)); got != tc.want {
				t.Errorf("clampInt32(int(%d)) = %d, want %d", tc.in, got, tc.want)
			}
		})
	}
}

func TestPageLimit(t *testing.T) {
	cases := []struct {
		name string
		in   int
		want int32
	}{
		{"a page asks for one row more than its limit", 100, 101},
		{"the largest int still asks for a positive limit", math.MaxInt, math.MaxInt32},
		{"the largest int32 does not wrap", math.MaxInt32, math.MaxInt32},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := pageLimit(tc.in); got != tc.want {
				t.Errorf("pageLimit(%d) = %d, want %d", tc.in, got, tc.want)
			}
		})
	}
}
