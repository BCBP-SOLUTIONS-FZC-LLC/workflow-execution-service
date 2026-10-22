package contract_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestAsyncAPI_EveryMessageHasASchemaFile(t *testing.T) {
	raw, err := os.ReadFile("../../api/asyncapi.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Components struct {
			Messages map[string]struct {
				Name string `yaml:"name"`
			} `yaml:"messages"`
		} `yaml:"components"`
	}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	if len(doc.Components.Messages) == 0 {
		t.Fatal("no messages in the AsyncAPI document")
	}
	snake := regexp.MustCompile(`([a-z0-9])([A-Z])`)
	for key, msg := range doc.Components.Messages {
		file := strings.ToLower(snake.ReplaceAllString(msg.Name, "${1}_${2}"))
		if strings.HasSuffix(key, "Inbound") {
			file += "_inbound"
		}
		path := filepath.Join("../../internal/eventschema", file+".json")
		if _, err := os.Stat(path); err != nil {
			t.Errorf("message %s has no schema file %s", key, path)
		}
	}
}
