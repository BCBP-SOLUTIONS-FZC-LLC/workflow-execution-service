package handler

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
)

const maxReportedUnknownFields = 20

var rawMessageType = reflect.TypeFor[json.RawMessage]()

type unknownFieldsError struct {
	names []string
}

func (e *unknownFieldsError) Error() string {
	return "request body has fields this endpoint does not define: " + strings.Join(e.names, ", ")
}

func checkKnownFields(body []byte, obj any) error {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	var doc any
	if err := dec.Decode(&doc); err != nil {
		return fmt.Errorf("decode request body: %w", err)
	}
	var names []string
	collectUnknown(doc, reflect.TypeOf(obj), "", &names)
	if len(names) == 0 {
		return nil
	}
	sort.Strings(names)
	if len(names) > maxReportedUnknownFields {
		names = names[:maxReportedUnknownFields]
	}
	return &unknownFieldsError{names: names}
}

func collectUnknown(doc any, t reflect.Type, path string, names *[]string) {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t == rawMessageType {
		return
	}
	switch value := doc.(type) {
	case map[string]any:
		collectObject(value, t, path, names)
	case []any:
		if t.Kind() != reflect.Slice && t.Kind() != reflect.Array {
			return
		}
		for i, item := range value {
			collectUnknown(item, t.Elem(), fmt.Sprintf("%s[%d]", path, i), names)
		}
	}
}

func collectObject(obj map[string]any, t reflect.Type, path string, names *[]string) {
	if t.Kind() == reflect.Map {
		for key, item := range obj {
			collectUnknown(item, t.Elem(), joinPath(path, key), names)
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
			*names = append(*names, truncateFieldName(joinPath(path, key)))
			continue
		}
		collectUnknown(item, field, joinPath(path, key), names)
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
