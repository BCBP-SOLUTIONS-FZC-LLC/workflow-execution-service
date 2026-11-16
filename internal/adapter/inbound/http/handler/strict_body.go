package handler

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/google/uuid"
)

const maxReportedUnknownFields = 20

var (
	rawMessageType = reflect.TypeFor[json.RawMessage]()
	uuidType       = reflect.TypeFor[uuid.UUID]()
)

type unknownFieldsError struct {
	names []string
}

func (e *unknownFieldsError) Error() string {
	return "request body has fields this endpoint does not define: " + strings.Join(e.names, ", ")
}

type keyPolicy func(fields map[string]reflect.Type, key string) bool

func unknownKey(fields map[string]reflect.Type, key string) bool {
	_, ok := fields[key]
	return !ok
}

func caseVariantKey(fields map[string]reflect.Type, key string) bool {
	if _, ok := fields[key]; ok {
		return false
	}
	for name := range fields {
		if strings.EqualFold(name, key) {
			return true
		}
	}
	return false
}

func checkKnownFields(body []byte, obj any) error {
	return checkFields(body, obj, unknownKey)
}

func checkFieldCase(body []byte, obj any) error {
	return checkFields(body, obj, caseVariantKey)
}

func checkFields(body []byte, obj any, reject keyPolicy) error {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	var doc any
	if err := dec.Decode(&doc); err != nil {
		return fmt.Errorf("decode request body: %w", err)
	}
	var names []string
	collectUnknown(doc, reflect.TypeOf(obj), "", reject, &names)
	if len(names) == 0 {
		return nil
	}
	sort.Strings(names)
	if len(names) > maxReportedUnknownFields {
		names = names[:maxReportedUnknownFields]
	}
	return &unknownFieldsError{names: names}
}

func collectUnknown(doc any, t reflect.Type, path string, reject keyPolicy, names *[]string) {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t == rawMessageType {
		return
	}
	switch value := doc.(type) {
	case map[string]any:
		collectObject(value, t, path, reject, names)
	case []any:
		if t.Kind() != reflect.Slice && t.Kind() != reflect.Array {
			return
		}
		for i, item := range value {
			collectUnknown(item, t.Elem(), fmt.Sprintf("%s[%d]", path, i), reject, names)
		}
	}
}

func collectObject(obj map[string]any, t reflect.Type, path string, reject keyPolicy, names *[]string) {
	if t.Kind() == reflect.Map {
		for key, item := range obj {
			collectUnknown(item, t.Elem(), joinPath(path, key), reject, names)
		}
		return
	}
	if t.Kind() != reflect.Struct {
		return
	}
	fields := jsonFields(t)
	for key, item := range obj {
		field, ok := fields[key]
		if !ok {
			if reject(fields, key) {
				*names = append(*names, truncateFieldName(joinPath(path, key)))
			}
			continue
		}
		collectUnknown(item, field, joinPath(path, key), reject, names)
	}
}

func jsonFields(t reflect.Type) map[string]reflect.Type {
	fields := map[string]reflect.Type{}
	for i := range t.NumField() {
		f := t.Field(i)
		name, _, _ := strings.Cut(f.Tag.Get("json"), ",")
		if name == "-" {
			continue
		}
		if f.Anonymous && name == "" {
			embedded := f.Type
			for embedded.Kind() == reflect.Pointer {
				embedded = embedded.Elem()
			}
			if embedded.Kind() == reflect.Struct {
				for k, v := range jsonFields(embedded) {
					fields[k] = v
				}
				continue
			}
		}
		if !f.IsExported() {
			continue
		}
		if name == "" {
			name = f.Name
		}
		fields[name] = f.Type
	}
	return fields
}

func joinPath(path, key string) string {
	if path == "" {
		return key
	}
	return path + "." + key
}

func truncateFieldName(name string) string {
	runes := []rune(name)
	if len(runes) > maxEchoedFieldName {
		return string(runes[:maxEchoedFieldName])
	}
	return name
}

// invalidUUIDFields names every string value that sits where a uuid.UUID is
// expected but does not parse as one, so a malformed id is reported as a bad
// field rather than as a body that is not JSON.
func invalidUUIDFields(body []byte, obj any) []string {
	var doc any
	if err := json.NewDecoder(bytes.NewReader(body)).Decode(&doc); err != nil {
		return nil
	}
	var names []string
	collectInvalidUUIDs(doc, reflect.TypeOf(obj), "", &names)
	sort.Strings(names)
	if len(names) > maxReportedUnknownFields {
		names = names[:maxReportedUnknownFields]
	}
	return names
}

func collectInvalidUUIDs(doc any, t reflect.Type, path string, names *[]string) {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t == rawMessageType {
		return
	}
	switch value := doc.(type) {
	case string:
		if t == uuidType {
			if _, err := uuid.Parse(value); err != nil {
				*names = append(*names, truncateFieldName(path))
			}
		}
	case map[string]any:
		collectInvalidUUIDsInObject(value, t, path, names)
	case []any:
		if t.Kind() != reflect.Slice && t.Kind() != reflect.Array {
			return
		}
		for i, item := range value {
			collectInvalidUUIDs(item, t.Elem(), fmt.Sprintf("%s[%d]", path, i), names)
		}
	}
}

func collectInvalidUUIDsInObject(obj map[string]any, t reflect.Type, path string, names *[]string) {
	if t.Kind() == reflect.Map {
		for key, item := range obj {
			collectInvalidUUIDs(item, t.Elem(), joinPath(path, key), names)
		}
		return
	}
	if t.Kind() != reflect.Struct {
		return
	}
	fields := jsonFields(t)
	for key, item := range obj {
		if field, ok := fields[key]; ok {
			collectInvalidUUIDs(item, field, joinPath(path, key), names)
		}
	}
}
