package service

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSameContext_ComparesNumbersByTheirLiteralText(t *testing.T) {
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
