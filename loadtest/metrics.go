package loadtest

import (
	"context"
	"fmt"
	"math/rand"
	"os/exec"
	"sync"
	"sync/atomic"
	"time"
)

// Metrics tracks test metrics
type Metrics struct {
	mu               sync.RWMutex
	publishedCount   int64
	receivedCount    int64
	errorCount       int64
	commitCount      int64
	commitErrorCount int64
	latencies        []time.Duration
	startTime        time.Time
	errors           []string
}

// NewMetrics creates a new metrics instance
func NewMetrics() *Metrics {
	return &Metrics{
		latencies: make([]time.Duration, 0),
		startTime: time.Now(),
		errors:    make([]string, 0),
	}
}

// RecordPublish increments published count
func (m *Metrics) RecordPublish() {
	atomic.AddInt64(&m.publishedCount, 1)
}

// RecordReceive increments received count
func (m *Metrics) RecordReceive() {
	atomic.AddInt64(&m.receivedCount, 1)
}

// RecordError increments error count and stores error
func (m *Metrics) RecordError(err string) {
	atomic.AddInt64(&m.errorCount, 1)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.errors = append(m.errors, err)
}

// RecordLatency records message latency
func (m *Metrics) RecordLatency(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.latencies = append(m.latencies, d)
}

// GetSnapshot returns current metrics snapshot
func (m *Metrics) GetSnapshot() MetricsSnapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()

	published := atomic.LoadInt64(&m.publishedCount)
	received := atomic.LoadInt64(&m.receivedCount)
	errors := atomic.LoadInt64(&m.errorCount)

	elapsed := time.Since(m.startTime).Seconds()

	return MetricsSnapshot{
		Published:    published,
		Received:     received,
		Errors:       errors,
		Elapsed:      elapsed,
		PublishRate:  float64(published) / elapsed,
		ReceiveRate:  float64(received) / elapsed,
		ErrorRate:    float64(errors) / float64(published+1),
		LatencyCount: len(m.latencies),
	}
}

// MetricsSnapshot represents a point-in-time metrics view
type MetricsSnapshot struct {
	Published    int64
	Received     int64
	Errors       int64
	Elapsed      float64
	PublishRate  float64
	ReceiveRate  float64
	ErrorRate    float64
	LatencyCount int
}

// WorkerInstance represents a running test instance
type WorkerInstance struct {
	ID       int
	Type     string
	Scenario *Scenario
	Cmd      *exec.Cmd
	Cancel   context.CancelFunc
	Metrics  *Metrics
}

// generateMessage creates a message based on pattern
func generateMessage(pattern string, size int, counter int) string {
	switch pattern {
	case "fixed":
		return fmt.Sprintf("load-test-message-%s", randomString(size-20))
	case "incremental":
		base := fmt.Sprintf("msg-%d-", counter)
		remaining := size - len(base)
		if remaining > 0 {
			return base + randomString(remaining)
		}
		return base
	case "random":
		fallthrough
	default:
		return randomString(size)
	}
}

// randomString generates a random string of specified length
func randomString(length int) string {
	if length <= 0 {
		return ""
	}
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

// buildPublishCommand constructs CLI publish command
func buildPublishCommand(cliPath string, config *LoadTestConfig, scenario *Scenario, instanceID int, message string) *exec.Cmd {
	// Make group ID unique per instance to avoid ack channel conflicts
	uniqueGroupID := fmt.Sprintf("%s-instance-%d", scenario.GroupID, instanceID)

	args := []string{
		"publish",
		scenario.Topic,
		message,
		"--endpoint", config.EventBus.Endpoint,
		"--token", config.EventBus.Token,
		"--group-id", uniqueGroupID,
	}

	// Add metadata if specified
	if scenario.Metadata != nil {
		if scenario.Metadata.TraceID != "" {
			args = append(args, "--trace-id", scenario.Metadata.TraceID)
		}
		if scenario.Metadata.OrgID != "" {
			args = append(args, "--org-id", scenario.Metadata.OrgID)
		}

		// Test impact of target fields
		if scenario.PublishConfig.IncludeTargetFields {
			if scenario.Metadata.TargetType != "" {
				args = append(args, "--target-type", scenario.Metadata.TargetType)
			}
			if scenario.Metadata.TargetID != "" {
				args = append(args, "--target-id", scenario.Metadata.TargetID)
			}
		}
	}

	return exec.Command(cliPath, args...)
}

// buildSubscribeCommand constructs CLI subscribe command
func buildSubscribeCommand(cliPath string, config *LoadTestConfig, scenario *Scenario, instanceID int) *exec.Cmd {
	// Make group ID unique per instance
	uniqueGroupID := fmt.Sprintf("%s-instance-%d", scenario.GroupID, instanceID)

	args := []string{
		"subscribe",
		scenario.Topic,
		"--endpoint", config.EventBus.Endpoint,
		"--token", config.EventBus.Token,
		"--group-id", uniqueGroupID,
	}

	// Add target field filters if specified
	if scenario.TargetFilters != nil {
		if scenario.TargetFilters.TargetType != "" {
			args = append(args, "--target-type", scenario.TargetFilters.TargetType)
		}
		if scenario.TargetFilters.TargetID != "" {
			args = append(args, "--target-id", scenario.TargetFilters.TargetID)
		}
	}

	return exec.Command(cliPath, args...)
}
