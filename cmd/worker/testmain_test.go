package main

import (
	"os"
	"path/filepath"
	"testing"
)

// TestMain removes the worker binary the built-binary tests compile once per run.
func TestMain(m *testing.M) {
	code := m.Run()
	if wkbootBinaryPath != "" {
		_ = os.RemoveAll(filepath.Dir(wkbootBinaryPath))
	}
	os.Exit(code)
}
