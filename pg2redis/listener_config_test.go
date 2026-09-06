package pg2redis

import (
	"testing"

	"github.com/alikonhz/pglogrepl2json/pg2redis/redisconfig"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRedisListenerConfigDefaultsFlushBufferSize(t *testing.T) {
	listenerConfig, err := NewListenerOptions(&redisconfig.RedisAppConfig{})

	require.NoError(t, err)
	assert.Equal(t, uint32(defaultRedisFlushBufferSize), listenerConfig.WriterOpts.FlushOpts.BufferSize)
}

func TestRedisListenerConfigUsesConfiguredFlushBufferSize(t *testing.T) {
	listenerConfig, err := NewListenerOptions(&redisconfig.RedisAppConfig{
		RedisFlushBufferSize: 37,
	})

	require.NoError(t, err)
	assert.Equal(t, uint32(37), listenerConfig.WriterOpts.FlushOpts.BufferSize)
}
