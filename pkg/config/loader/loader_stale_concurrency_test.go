package loader

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStaleConcurrencyDeactivationThresholdEnv pins the env-to-duration path
// for the stale concurrency deactivation threshold: an unset variable keeps
// the 25h engine default, and a duration string from the environment overrides
// it. The value flows from ServerConfigFile through ServerConfig into the
// tasks controller's WithStaleConcurrencyThreshold option.
func TestStaleConcurrencyDeactivationThresholdEnv(t *testing.T) {
	cf, err := LoadServerConfigFile()
	require.NoError(t, err)
	assert.Equal(t, 25*time.Hour, cf.StaleConcurrencyDeactivationThreshold,
		"the unset threshold must default to 25h (24h idle window + 1h last_active_at refresh cache)")

	t.Setenv("SERVER_STALE_CONCURRENCY_DEACTIVATION_THRESHOLD", "2h")

	cf, err = LoadServerConfigFile()
	require.NoError(t, err)
	assert.Equal(t, 2*time.Hour, cf.StaleConcurrencyDeactivationThreshold,
		"SERVER_STALE_CONCURRENCY_DEACTIVATION_THRESHOLD=2h must parse into the threshold")
}
