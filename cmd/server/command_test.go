package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseCommand_NoArgumentRunsTheServerAndMigrateRunsTheMigrations(t *testing.T) {
	got, err := parseCommand(nil)
	require.NoError(t, err)
	assert.Equal(t, serveCommand, got)

	got, err = parseCommand([]string{"migrate"})
	require.NoError(t, err)
	assert.Equal(t, migrateCommand, got)
}

func TestParseCommand_AnyOtherFirstArgumentIsRefusedWithUsage(t *testing.T) {
	for _, args := range [][]string{{"migrat"}, {"Migrate"}, {"serve"}, {"--help"}, {"migrate", "extra"}, {""}} {
		_, err := parseCommand(args)

		require.Error(t, err, "args %q", args)
		var usage *usageError
		require.True(t, errors.As(err, &usage), "args %q", args)
		assert.Contains(t, err.Error(), "usage:")
		assert.Contains(t, err.Error(), "migrate")
	}
}

func TestServerBinary_AMistypedFirstArgumentExitsTwoWithUsageAndStartsNothing(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping built-binary test: -short flag set")
	}
	bin := filepath.Join(t.TempDir(), "server")
	buildCtx, cancelBuild := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancelBuild()
	out, err := exec.CommandContext(buildCtx, "go", "build", "-o", bin, ".").CombinedOutput()
	require.NoError(t, err, string(out))

	runCtx, cancelRun := context.WithTimeout(context.Background(), time.Minute)
	defer cancelRun()
	cmd := exec.CommandContext(runCtx, bin, "migrat")
	cmd.Env = []string{"PATH=" + os.Getenv("PATH")}
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	runErr := cmd.Run()

	var exit *exec.ExitError
	require.True(t, errors.As(runErr, &exit), "run error = %v", runErr)
	assert.Equal(t, 2, exit.ExitCode())
	assert.Contains(t, stderr.String(), "usage:")
	assert.Contains(t, stderr.String(), `"migrat"`)
	assert.NotContains(t, stderr.String(), "config error")
	assert.Empty(t, stdout.String())
}
