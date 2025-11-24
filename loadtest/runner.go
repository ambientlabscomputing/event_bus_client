package loadtest

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
)

// Runner orchestrates the load test execution
type Runner struct {
	Config  *LoadTestConfig
	CLIPath string
	Metrics map[string]*Metrics
	Workers []*WorkerInstance
	mu      sync.Mutex
	ctx     context.Context
	cancel  context.CancelFunc
}

// NewRunner creates a new test runner
func NewRunner(config *LoadTestConfig, cliPath string) *Runner {
	ctx, cancel := context.WithCancel(context.Background())

	return &Runner{
		Config:  config,
		CLIPath: cliPath,
		Metrics: make(map[string]*Metrics),
		Workers: make([]*WorkerInstance, 0),
		ctx:     ctx,
		cancel:  cancel,
	}
}

// Run executes the load test
func (r *Runner) Run() error {
	// Validate config
	if err := r.Config.Validate(); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}

	duration, err := r.Config.GetDuration()
	if err != nil {
		return fmt.Errorf("invalid duration: %w", err)
	}

	rampUp, err := r.Config.GetRampUp()
	if err != nil {
		return fmt.Errorf("invalid ramp_up: %w", err)
	}

	fmt.Printf("🚀 Starting load test: %s\n", r.Config.TestName)
	fmt.Printf("Duration: %v | Ramp-up: %v\n", duration, rampUp)
	fmt.Printf("Scenarios: %d\n\n", len(r.Config.Scenarios))

	// Initialize metrics for each scenario
	for _, scenario := range r.Config.Scenarios {
		r.Metrics[scenario.Name] = NewMetrics()
	}

	// Start workers for each scenario
	for _, scenario := range r.Config.Scenarios {
		if err := r.startScenario(&scenario, rampUp); err != nil {
			return fmt.Errorf("failed to start scenario %s: %w", scenario.Name, err)
		}
	}

	// Start progress reporter
	go r.reportProgress()

	// Wait for test duration
	timer := time.NewTimer(duration)
	<-timer.C

	// Stop all workers
	r.Stop()

	// Print final report
	r.printFinalReport()

	return nil
}

// startScenario starts all instances for a scenario
func (r *Runner) startScenario(scenario *Scenario, rampUp time.Duration) error {
	fmt.Printf("📊 Starting scenario: %s (%s)\n", scenario.Name, scenario.Type)
	fmt.Printf("   Instances: %d | Topic: %s\n", scenario.Instances, scenario.Topic)

	if scenario.PublishConfig != nil {
		fmt.Printf("   Publish Rate: %d msg/s | Message Size: %d bytes\n",
			scenario.PublishConfig.MessagesPerSecond,
			scenario.PublishConfig.MessageSize)
		fmt.Printf("   Target Fields: %v\n", scenario.PublishConfig.IncludeTargetFields)
	}

	// Calculate stagger delay for ramp-up
	var staggerDelay time.Duration
	if rampUp > 0 && scenario.Instances > 1 {
		staggerDelay = rampUp / time.Duration(scenario.Instances)
	}

	for i := 0; i < scenario.Instances; i++ {
		if err := r.startWorker(scenario, i); err != nil {
			return fmt.Errorf("failed to start worker %d: %w", i, err)
		}

		// Stagger instance startup during ramp-up
		if staggerDelay > 0 && i < scenario.Instances-1 {
			time.Sleep(staggerDelay)
		}
	}

	fmt.Printf("✅ Scenario %s started\n\n", scenario.Name)
	return nil
}

// startWorker starts a single worker instance
func (r *Runner) startWorker(scenario *Scenario, instanceID int) error {
	metrics := r.Metrics[scenario.Name]

	switch scenario.Type {
	case "publisher":
		return r.startPublisher(scenario, instanceID, metrics)
	case "subscriber":
		return r.startSubscriber(scenario, instanceID, metrics)
	case "mixed":
		// Start both publisher and subscriber
		if err := r.startPublisher(scenario, instanceID, metrics); err != nil {
			return err
		}
		return r.startSubscriber(scenario, instanceID, metrics)
	default:
		return fmt.Errorf("unknown scenario type: %s", scenario.Type)
	}
}

// startPublisher starts a publisher worker
func (r *Runner) startPublisher(scenario *Scenario, instanceID int, metrics *Metrics) error {
	go func() {
		counter := 0
		interval := time.Second / time.Duration(scenario.PublishConfig.MessagesPerSecond)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-r.ctx.Done():
				return
			case <-ticker.C:
				// Generate message
				message := generateMessage(
					scenario.PublishConfig.MessagePattern,
					scenario.PublishConfig.MessageSize,
					counter,
				)

				// Build and execute command
				cmd := buildPublishCommand(r.CLIPath, r.Config, scenario, instanceID, message)
				var stderr bytes.Buffer
				cmd.Stderr = &stderr
				startTime := time.Now()

				if err := cmd.Run(); err != nil {
					errMsg := stderr.String()
					if errMsg == "" {
						errMsg = err.Error()
					}
					metrics.RecordError(fmt.Sprintf("publish error: %v", errMsg))
					// Log first few errors for debugging
					if counter < 3 {
						fmt.Printf("[%s instance %d] Error: %s\n", scenario.Name, instanceID, errMsg)
					}
				} else {
					metrics.RecordPublish()
					metrics.RecordLatency(time.Since(startTime))
					if counter < 3 {
						fmt.Printf("[%s instance %d] Success: message %d published\n", scenario.Name, instanceID, counter)
					}
				}

				counter++
			}
		}
	}()

	return nil
}

// startSubscriber starts a subscriber worker
func (r *Runner) startSubscriber(scenario *Scenario, instanceID int, metrics *Metrics) error {
	ctx, cancel := context.WithCancel(r.ctx)

	worker := &WorkerInstance{
		ID:       instanceID,
		Type:     "subscriber",
		Scenario: scenario,
		Cancel:   cancel,
		Metrics:  metrics,
	}

	r.mu.Lock()
	r.Workers = append(r.Workers, worker)
	r.mu.Unlock()

	go func() {
		cmd := buildSubscribeCommand(r.CLIPath, r.Config, scenario, instanceID)

		// Capture stdout to parse received messages
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			metrics.RecordError(fmt.Sprintf("failed to create stdout pipe: %v", err))
			return
		}

		// Capture stderr for debugging
		stderr, err := cmd.StderrPipe()
		if err != nil {
			metrics.RecordError(fmt.Sprintf("failed to create stderr pipe: %v", err))
			return
		}

		// Set up context cancellation
		go func() {
			<-ctx.Done()
			if cmd.Process != nil {
				cmd.Process.Kill()
			}
		}()

		// Start the command
		if err := cmd.Start(); err != nil {
			metrics.RecordError(fmt.Sprintf("failed to start subscriber: %v", err))
			return
		}

		// Parse stdout for received messages
		go func() {
			scanner := bufio.NewScanner(stdout)
			for scanner.Scan() {
				line := scanner.Text()
				// Look for "Received message:" in the output
				if strings.Contains(line, "Received message:") {
					metrics.RecordReceive()
				}
			}
		}()

		// Capture stderr for errors (non-blocking)
		go func() {
			scanner := bufio.NewScanner(stderr)
			for scanner.Scan() {
				line := scanner.Text()
				if strings.Contains(line, "Error:") || strings.Contains(line, "Failed") {
					metrics.RecordError(fmt.Sprintf("[%s instance %d] %s", scenario.Name, instanceID, line))
				}
			}
		}()

		// Wait for command to finish
		if err := cmd.Wait(); err != nil && ctx.Err() == nil {
			metrics.RecordError(fmt.Sprintf("subscribe error: %v", err))
		}
	}()

	return nil
}

// Stop stops all workers
func (r *Runner) Stop() {
	fmt.Println("\n🛑 Stopping load test...")
	r.cancel()
	time.Sleep(2 * time.Second) // Give workers time to clean up
}

// reportProgress prints periodic progress reports
func (r *Runner) reportProgress() {
	interval, _ := r.Config.GetReportInterval()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
			r.printProgress()
		}
	}
}

// printProgress prints current progress
func (r *Runner) printProgress() {
	fmt.Println("\n" + strings.Repeat("━", 60))
	fmt.Printf("📈 Progress Report - %s\n", time.Now().Format("15:04:05"))
	fmt.Println(strings.Repeat("━", 60))

	for name, metrics := range r.Metrics {
		snapshot := metrics.GetSnapshot()
		fmt.Printf("\n%s:\n", name)
		fmt.Printf("  Published: %d msgs (%.1f/s)\n", snapshot.Published, snapshot.PublishRate)
		fmt.Printf("  Received:  %d msgs (%.1f/s)\n", snapshot.Received, snapshot.ReceiveRate)
		fmt.Printf("  Errors:    %d (%.2f%%)\n", snapshot.Errors, snapshot.ErrorRate*100)
		fmt.Printf("  Elapsed:   %.1fs\n", snapshot.Elapsed)
	}
}

// printFinalReport prints the final test report
func (r *Runner) printFinalReport() {
	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Printf("📊 FINAL REPORT - %s\n", r.Config.TestName)
	fmt.Println(strings.Repeat("=", 60))

	var totalPublished, totalReceived, totalErrors int64

	for name, metrics := range r.Metrics {
		snapshot := metrics.GetSnapshot()
		totalPublished += snapshot.Published
		totalReceived += snapshot.Received
		totalErrors += snapshot.Errors

		fmt.Printf("\n%s:\n", name)
		fmt.Printf("  Total Published:  %d msgs\n", snapshot.Published)
		fmt.Printf("  Total Received:   %d msgs\n", snapshot.Received)
		fmt.Printf("  Errors:           %d\n", snapshot.Errors)
		fmt.Printf("  Publish Rate:     %.1f msg/s\n", snapshot.PublishRate)
		fmt.Printf("  Receive Rate:     %.1f msg/s\n", snapshot.ReceiveRate)
		fmt.Printf("  Error Rate:       %.2f%%\n", snapshot.ErrorRate*100)
	}

	fmt.Println("\n" + strings.Repeat("-", 60))
	fmt.Printf("TOTAL:\n")
	fmt.Printf("  Messages Published: %d\n", totalPublished)
	fmt.Printf("  Messages Received:  %d\n", totalReceived)
	fmt.Printf("  Total Errors:       %d\n", totalErrors)
	fmt.Println(strings.Repeat("=", 60) + "\n")
}
