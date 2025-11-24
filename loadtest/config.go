package loadtest

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// LoadTestConfig defines the load test configuration
type LoadTestConfig struct {
	TestName       string         `json:"test_name"`
	EventBus       EventBusConfig `json:"event_bus"`
	Scenarios      []Scenario     `json:"scenarios"`
	Duration       string         `json:"duration"`
	ReportInterval string         `json:"report_interval"`
	RampUp         string         `json:"ramp_up,omitempty"`
}

// EventBusConfig contains the event bus connection details
type EventBusConfig struct {
	Endpoint string `json:"endpoint"`
	Token    string `json:"token"`
}

// Scenario defines a test scenario
type Scenario struct {
	Name          string           `json:"name"`
	Type          string           `json:"type"` // "publisher", "subscriber", "mixed"
	Instances     int              `json:"instances"`
	Topic         string           `json:"topic"`
	GroupID       string           `json:"group_id"`
	PublishConfig *PublishConfig   `json:"publish_config,omitempty"`
	Metadata      *MessageMetadata `json:"metadata,omitempty"`
}

// PublishConfig defines publishing behavior
type PublishConfig struct {
	MessagesPerSecond   int    `json:"messages_per_second"`
	MessageSize         int    `json:"message_size"` // in bytes
	BatchSize           int    `json:"batch_size"`
	MessagePattern      string `json:"message_pattern"`       // "fixed", "random", "incremental"
	IncludeTargetFields bool   `json:"include_target_fields"` // Test impact of target_type/target_id
}

// MessageMetadata defines optional message metadata
type MessageMetadata struct {
	TraceID    string `json:"trace_id,omitempty"`
	OrgID      string `json:"org_id,omitempty"`
	TargetType string `json:"target_type,omitempty"`
	TargetID   string `json:"target_id,omitempty"`
}

// LoadFromFile loads configuration from a JSON file
func LoadFromFile(path string) (*LoadTestConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var config LoadTestConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	// Set defaults
	if config.Duration == "" {
		config.Duration = "60s"
	}
	if config.ReportInterval == "" {
		config.ReportInterval = "5s"
	}
	if config.TestName == "" {
		config.TestName = "load_test"
	}

	return &config, nil
}

// GetDuration parses duration strings
func (c *LoadTestConfig) GetDuration() (time.Duration, error) {
	return time.ParseDuration(c.Duration)
}

// GetReportInterval parses report interval
func (c *LoadTestConfig) GetReportInterval() (time.Duration, error) {
	return time.ParseDuration(c.ReportInterval)
}

// GetRampUp parses ramp up duration
func (c *LoadTestConfig) GetRampUp() (time.Duration, error) {
	if c.RampUp == "" {
		return 0, nil
	}
	return time.ParseDuration(c.RampUp)
}

// Validate validates the configuration
func (c *LoadTestConfig) Validate() error {
	if c.EventBus.Endpoint == "" {
		return fmt.Errorf("event_bus.endpoint is required")
	}
	if c.EventBus.Token == "" {
		return fmt.Errorf("event_bus.token is required")
	}
	if len(c.Scenarios) == 0 {
		return fmt.Errorf("at least one scenario is required")
	}

	for i, scenario := range c.Scenarios {
		if scenario.Name == "" {
			return fmt.Errorf("scenario[%d].name is required", i)
		}
		if scenario.Type != "publisher" && scenario.Type != "subscriber" && scenario.Type != "mixed" {
			return fmt.Errorf("scenario[%d].type must be 'publisher', 'subscriber', or 'mixed'", i)
		}
		if scenario.Instances < 1 {
			return fmt.Errorf("scenario[%d].instances must be >= 1", i)
		}
		if scenario.Topic == "" {
			return fmt.Errorf("scenario[%d].topic is required", i)
		}
		if scenario.GroupID == "" {
			return fmt.Errorf("scenario[%d].group_id is required", i)
		}

		if scenario.Type == "publisher" || scenario.Type == "mixed" {
			if scenario.PublishConfig == nil {
				return fmt.Errorf("scenario[%d].publish_config is required for publisher/mixed scenarios", i)
			}
			if scenario.PublishConfig.MessagesPerSecond < 1 {
				return fmt.Errorf("scenario[%d].publish_config.messages_per_second must be >= 1", i)
			}
			if scenario.PublishConfig.MessageSize < 1 {
				c.Scenarios[i].PublishConfig.MessageSize = 256 // default
			}
			if scenario.PublishConfig.BatchSize < 1 {
				c.Scenarios[i].PublishConfig.BatchSize = 1 // default
			}
			if scenario.PublishConfig.MessagePattern == "" {
				c.Scenarios[i].PublishConfig.MessagePattern = "random"
			}
		}
	}

	return nil
}
