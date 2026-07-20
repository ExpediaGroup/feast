package debuglogging

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestShouldSampleRequest_RateZero(t *testing.T) {
	for i := 0; i < 100; i++ {
		assert.False(t, ShouldSampleRequest(0))
	}
}

func TestShouldSampleRequest_RateOne(t *testing.T) {
	for i := 0; i < 100; i++ {
		assert.True(t, ShouldSampleRequest(1))
	}
}

func TestShouldSampleRequest_RateNegativeTreatedAsZero(t *testing.T) {
	assert.False(t, ShouldSampleRequest(-0.5))
}

func TestShouldEmit_RequestFlaggedAlwaysEmitsRegardlessOfConfig(t *testing.T) {
	assert.True(t, ShouldEmit(true, Config{Enabled: false, SampleRate: 0}))
	assert.True(t, ShouldEmit(true, Config{Enabled: true, SampleRate: 0}))
}

func TestShouldEmit_NotFlaggedAndDisabledNeverEmits(t *testing.T) {
	assert.False(t, ShouldEmit(false, Config{Enabled: false, SampleRate: 1}))
}

func TestShouldEmit_NotFlaggedButEnabledFollowsSampleRate(t *testing.T) {
	assert.True(t, ShouldEmit(false, Config{Enabled: true, SampleRate: 1}))
	assert.False(t, ShouldEmit(false, Config{Enabled: true, SampleRate: 0}))
}
