package appconfig

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewFlushOptionsQueueDepthDefaultsToMax(t *testing.T) {
	opts, err := NewFlushOptions("", 0, 0, 1, "")

	require.NoError(t, err)
	assert.Equal(t, uint32(maxFlushQueueDepth), opts.QueueDepth)
}

func TestNewFlushOptionsQueueDepthUsesConfiguredValue(t *testing.T) {
	opts, err := NewFlushOptions("", 0, 7, 1, "")

	require.NoError(t, err)
	assert.Equal(t, uint32(7), opts.QueueDepth)
}

func TestNewFlushOptionsQueueDepthCapsAtMax(t *testing.T) {
	opts, err := NewFlushOptions("", 0, 99, 1, "")

	require.NoError(t, err)
	assert.Equal(t, uint32(maxFlushQueueDepth), opts.QueueDepth)
}
