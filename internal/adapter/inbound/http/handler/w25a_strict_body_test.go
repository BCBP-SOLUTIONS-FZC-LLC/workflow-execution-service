package handler

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

type w25aUUIDInner struct {
	ID uuid.UUID `json:"id"`
}

type w25aUUIDBody struct {
	ID    uuid.UUID                `json:"id"`
	Other uuid.UUID                `json:"other"`
	Text  string                   `json:"text"`
	Ptr   *uuid.UUID               `json:"ptr"`
	IDs   []uuid.UUID              `json:"ids"`
	Inner w25aUUIDInner            `json:"inner"`
	Items []w25aUUIDInner          `json:"items"`
	ByKey map[string]uuid.UUID     `json:"by_key"`
	Maps  map[string]w25aUUIDInner `json:"maps"`
	Raw   json.RawMessage          `json:"raw"`
}

// A malformed uuid is named by its dotted path, with [i] for list positions; a raw JSON field, an array in a
// field that is not a list, and a value that is not a string where a uuid is expected add no name.
func TestInvalidUUIDFields_NamesTheDottedPathAndSkipsWhatIsNotAString(t *testing.T) {
	const good = "6f8e1c1e-3c7d-4e0a-9d52-0a6f2f1b7c11"
	cases := []struct {
		name string
		body string
		want []string
	}{
		{name: "top level field", body: `{"id":"nope"}`, want: []string{"id"}},
		{name: "pointer field", body: `{"ptr":"nope"}`, want: []string{"ptr"}},
		{name: "nested object", body: `{"inner":{"id":"nope"}}`, want: []string{"inner.id"}},
		{name: "list element", body: `{"ids":["` + good + `","nope"]}`, want: []string{"ids[1]"}},
		{name: "object inside a list", body: `{"items":[{"id":"` + good + `"},{"id":"nope"}]}`, want: []string{"items[1].id"}},
		{name: "map value", body: `{"by_key":{"k":"nope"}}`, want: []string{"by_key.k"}},
		{name: "object inside a map value", body: `{"maps":{"k":{"id":"nope"}}}`, want: []string{"maps.k.id"}},
		{name: "several names are sorted", body: `{"other":"x","id":"y","ids":["z"]}`, want: []string{"id", "ids[0]", "other"}},
		{name: "raw JSON field is skipped", body: `{"raw":{"id":"nope"},"id":"nope"}`, want: []string{"id"}},
		{name: "array in a field that is not a list", body: `{"text":["nope"],"id":"nope"}`, want: []string{"id"}},
		{name: "array of strings in an object field", body: `{"inner":["nope"],"id":"nope"}`, want: []string{"id"}},
		{name: "number where a uuid is expected", body: `{"id":12345,"other":"nope"}`, want: []string{"other"}},
		{name: "boolean and null where a uuid is expected", body: `{"id":true,"ptr":null,"other":"nope"}`, want: []string{"other"}},
		{name: "object where a uuid is expected", body: `{"id":{"x":"nope"},"other":"nope"}`, want: []string{"other"}},
		{name: "a string in a string field is not a uuid slot", body: `{"text":"nope","id":"nope"}`, want: []string{"id"}},
		{name: "unknown field is not named here", body: `{"extra":"nope","id":"nope"}`, want: []string{"id"}},
		{name: "valid uuids add nothing", body: `{"id":"` + good + `","ids":["` + good + `"]}`, want: nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, invalidUUIDFields([]byte(tc.body), &w25aUUIDBody{}))
		})
	}
}
