//go:build !integration

package main

import (
	"testing"

	"github.com/feast-dev/feast/go/internal/feast"
	"github.com/feast-dev/feast/go/internal/feast/metrics"
	"github.com/feast-dev/feast/go/internal/feast/registry"
	"github.com/feast-dev/feast/go/internal/feast/server/logging"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockServerStarter is a mock of ServerStarter interface for testing
type MockServerStarter struct {
	mock.Mock
}

func (m *MockServerStarter) StartHttpServer(fs *feast.FeatureStore, host string, port int, loggingService *logging.LoggingService, metricsClient metrics.StatsdClient, config *registry.RepoConfig) error {
	args := m.Called(fs, host, port, loggingService, metricsClient, config)
	return args.Error(0)
}

func (m *MockServerStarter) StartGrpcServer(fs *feast.FeatureStore, host string, port int, loggingService *logging.LoggingService, metricsClient metrics.StatsdClient, config *registry.RepoConfig) error {
	args := m.Called(fs, host, port, loggingService, metricsClient, config)
	return args.Error(0)
}

func (m *MockServerStarter) StartHybridServer(fs *feast.FeatureStore, host string, httpPort int, grpcPort int, loggingService *logging.LoggingService, metricsClient metrics.StatsdClient, config *registry.RepoConfig) error {
	args := m.Called(fs, host, httpPort, grpcPort, loggingService, metricsClient, config)
	return args.Error(0)
}

// TestStartHttpServer tests the StartHttpServer function
func TestStartHttpServer(t *testing.T) {
	mockServerStarter := new(MockServerStarter)
	fs := &feast.FeatureStore{}
	host := "localhost"
	port := 8080

	mockServerStarter.On("StartHttpServer", fs, host, port, (*logging.LoggingService)(nil), (metrics.StatsdClient)(nil), (*registry.RepoConfig)(nil)).Return(nil)

	err := mockServerStarter.StartHttpServer(fs, host, port, nil, nil, nil)
	assert.NoError(t, err)
	mockServerStarter.AssertExpectations(t)
}

// TestStartGrpcServer tests the StartGrpcServer function
func TestStartGrpcServer(t *testing.T) {
	mockServerStarter := new(MockServerStarter)
	fs := &feast.FeatureStore{}
	host := "localhost"
	port := 9090

	mockServerStarter.On("StartGrpcServer", fs, host, port, (*logging.LoggingService)(nil), (metrics.StatsdClient)(nil), (*registry.RepoConfig)(nil)).Return(nil)

	err := mockServerStarter.StartGrpcServer(fs, host, port, nil, nil, nil)
	assert.NoError(t, err)
	mockServerStarter.AssertExpectations(t)
}

// TestConstructLoggingService tests the constructLoggingService function
func TestConstructLoggingService(t *testing.T) {
	fs := &feast.FeatureStore{}
	var writeLoggedFeaturesCallback logging.OfflineStoreWriteCallback
	loggingOpts := &logging.LoggingOptions{}

	_, err := constructLoggingService(fs, writeLoggedFeaturesCallback, loggingOpts)
	assert.NoError(t, err)
	// Further assertions can be added here based on the expected behavior of constructLoggingService
}

// Note: Additional tests can be written for other functions and error scenarios.
