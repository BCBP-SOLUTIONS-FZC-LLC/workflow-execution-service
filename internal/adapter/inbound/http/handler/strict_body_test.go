package handler

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type strictInner struct {
	Name string `json:"name"`
}

type strictEmbedded struct {
	Note string `json:"note"`
}

type strictBody struct {
	strictEmbedded
	ID       string                 `json:"id"`
	Inner    *strictInner           `json:"inner"`
	Items    []strictInner          `json:"items"`
	ByKey    map[string]strictInner `json:"by_key"`
	Raw      json.RawMessage        `json:"raw"`
	Anything any                    `json:"anything"`
	Skipped  string                 `json:"-"`
}

func TestCheckKnownFields(t *testing.T) {
	tests := []struct {
		name string
		body string
		want []string
	}{
		{name: "known fields at every depth", body: `{"id":"a","note":"n","inner":{"name":"x"},"items":[{"name":"y"}],"by_key":{"k":{"name":"z"}},"raw":{"free":1},"anything":{"free":2}}`},
		{name: "a field name in the wrong case", body: `{"ID":"a"}`, want: []string{"ID"}},
		{name: "a nested unknown key", body: `{"inner":{"name":"x","extra":1}}`, want: []string{"inner.extra"}},
		{name: "an unknown key inside a list", body: `{"items":[{"name":"y"},{"nam":"z"}]}`, want: []string{"items[1].nam"}},
		{name: "an unknown key inside a map value", body: `{"by_key":{"k":{"Name":"z"}}}`, want: []string{"by_key.k.Name"}},
		{name: "a field tagged out is unknown", body: `{"Skipped":"s","-":"t"}`, want: []string{"-", "Skipped"}},
		{name: "every unknown field, sorted", body: `{"zz":1,"aa":2,"inner":{"mm":3}}`, want: []string{"aa", "inner.mm", "zz"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := checkKnownFields([]byte(tt.body), &strictBody{})
			if tt.want == nil {
				require.NoError(t, err)
				return
			}
			var unknown *unknownFieldsError
			require.True(t, errors.As(err, &unknown), "error %v", err)
			assert.Equal(t, tt.want, unknown.names)
		})
	}
}

func TestCheckKnownFields_ReportsAtMostTwentyNames(t *testing.T) {
	fields := make([]string, 0, 30)
	for i := range 30 {
		fields = append(fields, fmt.Sprintf(`"f%02d":1`, i))
	}

	err := checkKnownFields([]byte("{"+strings.Join(fields, ",")+"}"), &strictBody{})

	var unknown *unknownFieldsError
	require.True(t, errors.As(err, &unknown))
	assert.Len(t, unknown.names, maxReportedUnknownFields)
}
