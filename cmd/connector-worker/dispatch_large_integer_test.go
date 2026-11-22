package main

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseDispatchJob_ResolvedInputsKeepIntegersAbove2Pow53Verbatim(t *testing.T) {
	entry, _ := cwdepsEntry("1-0", "send-email", map[string]string{
		"resolved_inputs": `{"amount":9007199254740993,"nested":{"id":9007199254740995}}`,
	})

	job, err := parseDispatchJob(entry)

	require.NoError(t, err)
	got, err := json.Marshal(job.resolvedInputs)
	require.NoError(t, err)
	assert.Equal(t, `{"amount":9007199254740993,"nested":{"id":9007199254740995}}`, string(got))
}
