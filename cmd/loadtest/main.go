package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/ambientlabscomputing/event_bus_client/loadtest"
)

func main() {
	// Parse flags
	configPath := flag.String("config", "", "Path to load test configuration file (JSON)")
	cliPath := flag.String("cli", "./eventbus_cli", "Path to eventbus_cli binary")
	dryRun := flag.Bool("dry-run", false, "Validate configuration without running test")
	flag.Parse()

	if *configPath == "" {
		fmt.Fprintln(os.Stderr, "Error: -config flag is required")
		flag.Usage()
		os.Exit(1)
	}

	// Load configuration
	config, err := loadtest.LoadFromFile(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading config: %v\n", err)
		os.Exit(1)
	}

	// Validate configuration
	if err := config.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "Configuration error: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("✅ Configuration loaded successfully: %s\n", *configPath)

	if *dryRun {
		fmt.Println("\n📋 Configuration Summary:")
		fmt.Printf("  Test Name: %s\n", config.TestName)
		fmt.Printf("  Duration: %s\n", config.Duration)
		fmt.Printf("  Scenarios: %d\n\n", len(config.Scenarios))

		for i, scenario := range config.Scenarios {
			fmt.Printf("  Scenario %d: %s\n", i+1, scenario.Name)
			fmt.Printf("    Type: %s\n", scenario.Type)
			fmt.Printf("    Instances: %d\n", scenario.Instances)
			fmt.Printf("    Topic: %s\n", scenario.Topic)

			if scenario.PublishConfig != nil {
				fmt.Printf("    Messages/sec: %d\n", scenario.PublishConfig.MessagesPerSecond)
				fmt.Printf("    Message size: %d bytes\n", scenario.PublishConfig.MessageSize)
				fmt.Printf("    Include target fields: %v\n", scenario.PublishConfig.IncludeTargetFields)
			}
			fmt.Println()
		}

		fmt.Println("✅ Dry run complete. Configuration is valid.")
		return
	}

	// Verify CLI exists
	absCliPath, err := filepath.Abs(*cliPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error resolving CLI path: %v\n", err)
		os.Exit(1)
	}

	if _, err := os.Stat(absCliPath); os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "Error: CLI binary not found at %s\n", absCliPath)
		fmt.Fprintf(os.Stderr, "Build it with: go build -o eventbus_cli ./cmd/eventbus_cli\n")
		os.Exit(1)
	}

	// Create and run test
	runner := loadtest.NewRunner(config, absCliPath)
	if err := runner.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "Load test failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("✅ Load test completed successfully")
}
