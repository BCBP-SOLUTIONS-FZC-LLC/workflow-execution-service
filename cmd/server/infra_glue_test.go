package main

import (
	"context"
	"testing"
	"time"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/config"
)

func TestNewGlueCodec_CarriesTheConfiguredStaleness(t *testing.T) {
	cfg := &config.Config{AWSUseStub: true, GlueSchemaMaxStaleness: 17 * time.Minute}

	codec, err := newGlueCodec(context.Background(), cfg, nil)

	if err != nil {
		t.Fatalf("newGlueCodec() error = %v", err)
	}
	if codec.MaxStaleness != 17*time.Minute {
		t.Errorf("MaxStaleness = %v, want 17m", codec.MaxStaleness)
	}
}
