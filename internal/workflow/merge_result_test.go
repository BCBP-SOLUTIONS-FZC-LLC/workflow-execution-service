package workflow

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMergeResult_KeepsIntegersAbove2Pow53Verbatim(t *testing.T) {
	tests := []struct {
		name    string
		context string
		result  string
		want    string
	}{
		{"result number", `{}`, `{"id":9007199254740993}`, `{"id":9007199254740993}`},
		{"context number", `{"a":9007199254740993}`, `{"b":1}`, `{"a":9007199254740993,"b":1}`},
		{"nested number", `{}`, `{"o":{"id":9007199254740993}}`, `{"o":{"id":9007199254740993}}`},
		{"result overwrites", `{"a":1}`, `{"a":9007199254740995}`, `{"a":9007199254740995}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := &interpreter{contextJSON: tt.context}

			require.NoError(t, in.mergeResult(tt.result))

			assert.Equal(t, tt.want, in.contextJSON)
		})
	}
}
