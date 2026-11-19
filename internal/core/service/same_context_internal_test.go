package service

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSameContext_ComparesNumbersByValue(t *testing.T) {
	tests := []struct {
		name      string
		stored    string
		requested string
		want      bool
	}{
		{"exponent form of the stored integer", `{"a":100}`, `{"a":1e2}`, true},
		{"trailing zero fraction", `{"a":1}`, `{"a":1.0}`, true},
		{"nested and in a list", `{"o":[100,{"p":2.50}]}`, `{"o":[1e2,{"p":2.5}]}`, true},
		{"large integers one apart", `{"a":9007199254740993}`, `{"a":9007199254740992}`, false},
		{"large integer against its float rounding", `{"a":9007199254740993}`, `{"a":9.007199254740993e15}`, true},
		{"different fractions", `{"a":0.1}`, `{"a":0.10000000000000001}`, false},
		{"a number against a string", `{"a":1}`, `{"a":"1"}`, false},
		{"a different key", `{"a":1}`, `{"b":1}`, false},
		{"lists of another length", `{"a":[1]}`, `{"a":[1,2]}`, false},
		{"an exponent too large to expand", `{"a":1e999999999}`, `{"a":1e999999999}`, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, sameContext([]byte(tt.stored), []byte(tt.requested)))
		})
	}
}

func TestSameContext_ComparesStructureAndLargeIntegersExactly(t *testing.T) {
	tests := []struct {
		name      string
		stored    string
		requested string
		want      bool
	}{
		{"equal objects in another order", `{"a":1,"b":2}`, `{"b":2,"a":1}`, true},
		{"large integers one apart", `{"a":9007199254740993}`, `{"a":9007199254740992}`, false},
		{"same large integer", `{"a":9007199254740993}`, `{"a":9007199254740993}`, true},
		{"nested large integers one apart", `{"o":[9007199254740993]}`, `{"o":[9007199254740992]}`, false},
		{"null equals empty object", `null`, `{}`, true},
		{"empty equals empty object", ``, `{}`, true},
		{"unreadable stored", `{`, `{}`, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, sameContext([]byte(tt.stored), []byte(tt.requested)))
		})
	}
}
