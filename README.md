# Event Bus Client

A high-performance Go client library and CLI for the Ambient Labs Event Bus, supporting publish/subscribe messaging with advanced filtering and load testing capabilities.

[![Go Version](https://img.shields.io/badge/Go-1.24.3-blue.svg)](https://golang.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

## Features

- 🚀 **High Performance**: HTTP POST publishing with WebSocket subscriptions achieving 596+ msg/s throughput
- 🎯 **Target Field Filtering**: Filter messages by `target_type` and `target_id` for precise routing
- 📊 **Load Testing Suite**: Comprehensive testing framework with configurable scenarios
- 🔄 **Auto-Reconnect**: Built-in connection resilience and message offset management
- 🛠️ **Developer Friendly**: Interactive shell mode and simple CLI commands
- 📦 **Consumer Groups**: Support for scalable message processing with offset tracking

## Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [Usage](#usage)
  - [Library](#library)
  - [CLI](#cli)
  - [Load Testing](#load-testing)
- [Architecture](#architecture)
- [Configuration](#configuration)
- [Examples](#examples)
- [Developer Guide](#developer-guide)
- [Performance](#performance)
- [Contributing](#contributing)
- [License](#license)

## Installation

### Prerequisites

- Go 1.24.3 or later
- Access to an Event Bus server endpoint

### Install as Library

```bash
go get github.com/ambientlabscomputing/event_bus_client
```

### Build CLI and Tools

```bash
# Clone the repository
git clone https://github.com/ambientlabscomputing/event_bus_client.git
cd event_bus_client

# Build the CLI
go build -o bin/eventbus_cli cmd/eventbus_cli/main.go

# Build the load testing tool
go build -o bin/loadtest cmd/loadtest/main.go
```

## Quick Start

### Using the Library

```go
package main

import (
    "context"
    "fmt"
    "log"
    
    client "github.com/ambientlabscomputing/event_bus_client"
)

func main() {
    // Initialize client
    opts := client.EventClientOpts{
        Endpoint:       "ws://localhost:9000/ws",
        AuthToken:      "your-jwt-token",
        CommitInterval: "5s",
        GroupID:        "my-consumer-group",
    }
    
    ec := client.NewEventClient(opts)
    
    // Subscribe to topics
    subs := []client.SubscriptionRequest{
        {
            GroupID: "my-consumer-group",
            Topic:   "orders",
        },
    }
    
    ctx := context.Background()
    if err := ec.Connect(ctx, &subs); err != nil {
        log.Fatal(err)
    }
    
    // Process messages
    msgChan := ec.IncomingMsgChannel()
    for msg := range msgChan {
        fmt.Printf("Received: %s\n", msg.Content)
    }
}
```

### Using the CLI

```bash
# Configure credentials (one time)
export EVENT_BUS_ENDPOINT="ws://localhost:9000/ws"
export EVENT_BUS_TOKEN="your-jwt-token"
export EVENT_BUS_GROUP_ID="my-group"

# Publish a message
./bin/eventbus_cli publish my-topic "Hello, World!"

# Subscribe to messages
./bin/eventbus_cli subscribe my-topic

# Interactive shell mode
./bin/eventbus_cli shell
```

## Usage

### Library

#### Publishing Messages

```go
// HTTP POST publishing (recommended for reliability)
err := ec.PublishHTTP(ctx, client.HTTPPublishRequest{
    Topic:      "orders",
    Content:    `{"order_id": "123", "amount": 99.99}`,
    TargetType: "user",
    TargetID:   "user-456",
    TraceID:    "trace-789",
})
```

#### Subscribing with Filters

```go
// Helper function for string pointers
func stringPtr(s string) *string { return &s }

subs := []client.SubscriptionRequest{
    {
        GroupID:    "payment-processor",
        Topic:      "orders",
        TargetType: stringPtr("user"),      // Only receive user-targeted messages
        TargetID:   stringPtr("user-456"),  // Only for this specific user
    },
}
```

#### Processing Messages

```go
msgChan := ec.IncomingMsgChannel()
for msg := range msgChan {
    // Access message fields
    fmt.Printf("Topic: %s\n", msg.Topic)
    fmt.Printf("Content: %s\n", msg.Content)
    
    // Access metadata
    if msg.TargetType != nil {
        fmt.Printf("Target Type: %s\n", *msg.TargetType)
    }
    if msg.TargetID != nil {
        fmt.Printf("Target ID: %s\n", *msg.TargetID)
    }
    
    // Offsets are automatically committed based on CommitInterval
}
```

### CLI

#### Configuration

Create a `config.yaml` file (see `config.yaml.example`):

```yaml
endpoint: ws://localhost:9000/ws
token: your-jwt-token
group_id: my-consumer-group
```

Or use environment variables:
```bash
export EVENT_BUS_ENDPOINT="ws://localhost:9000/ws"
export EVENT_BUS_TOKEN="your-jwt-token"
export EVENT_BUS_GROUP_ID="my-group"
```

#### Publishing

```bash
# Basic publish
./bin/eventbus_cli publish orders "New order received"

# With target fields
./bin/eventbus_cli publish orders "Payment required" \
  --target-type user \
  --target-id user-123

# With all metadata
./bin/eventbus_cli publish orders '{"order_id": "456"}' \
  --target-type user \
  --target-id user-789 \
  --trace-id trace-abc \
  --org-id org-xyz
```

#### Subscribing

```bash
# Basic subscribe
./bin/eventbus_cli subscribe orders

# Subscribe with filters (only receive matching messages)
./bin/eventbus_cli subscribe orders \
  --target-type user \
  --target-id user-123

# Subscribe with trace filtering
./bin/eventbus_cli subscribe orders \
  --trace-id trace-abc
```

#### Interactive Shell

```bash
./bin/eventbus_cli shell

# In shell mode:
> subscribe orders
> publish orders "Test message"
> help
> exit
```

### Load Testing

The load testing suite allows you to simulate high-volume workloads and test various scenarios.

#### Quick Test

```bash
./bin/loadtest -config loadtest/examples/quick.json -cli bin/eventbus_cli
```

#### Stress Test

```bash
./bin/loadtest -config loadtest/examples/stress.json -cli bin/eventbus_cli
```

#### Accuracy Test (Target Filtering)

```bash
./bin/loadtest -config loadtest/examples/accuracy.json -cli bin/eventbus_cli
```

See [loadtest/README.md](loadtest/README.md) for detailed documentation on creating custom test scenarios.

## Architecture

### Hybrid Publish/Subscribe Model

The client uses an optimized architecture:

- **Publishing**: HTTP POST to `/api/v1/publish` for 100% reliability
- **Subscribing**: WebSocket connection to `/ws` with long-polling (5s hold time)

This hybrid approach provides:
- ✅ High publish success rate (100% vs 77% with WebSocket-only)
- ✅ Efficient message delivery with long-polling
- ✅ Automatic reconnection and offset management
- ✅ Consumer group support for horizontal scaling

### Message Flow

```
┌──────────────┐      HTTP POST       ┌──────────────┐
│   Publisher  │ ──────────────────> │  Event Bus   │
└──────────────┘    /api/v1/publish   │    Server    │
                                      └──────┬───────┘
                                             │
                    WebSocket + Long Poll    │
                                             ▼
┌──────────────┐                      ┌──────────────┐
│  Subscriber  │ <────────────────── │   Message    │
│   (Group A)  │      Filtered        │    Queue     │
└──────────────┘                      └──────────────┘
```

### Target Field Filtering

Messages can be filtered on the server side based on:

- **target_type**: Category of the target entity (e.g., "user", "organization", "order")
- **target_id**: Specific identifier (e.g., "user-123", "org-456")

**Filter Behavior:**
- Exact match on both type and ID if both specified
- Type-only match if only target_type specified
- Broadcast messages (no target fields) are delivered to all subscribers
- No filter = receive all messages

## Configuration

### Client Options

```go
type EventClientOpts struct {
    Endpoint       string  // WebSocket endpoint (e.g., "ws://localhost:9000/ws")
    AuthToken      string  // JWT authentication token
    CommitInterval string  // Offset commit frequency (e.g., "5s", "10s")
    GroupID        string  // Consumer group identifier
}
```

### Subscription Request

```go
type SubscriptionRequest struct {
    GroupID    string   // Consumer group ID
    Topic      string   // Topic name
    TargetType *string  // Optional: filter by target type
    TargetID   *string  // Optional: filter by target ID
    TraceID    *string  // Optional: filter by trace ID
    OrgID      *string  // Optional: filter by organization ID
    Limit      int      // Optional: max messages per fetch
}
```

## Examples

### Example 1: Simple Publisher

```go
package main

import (
    "context"
    "fmt"
    "log"
    
    client "github.com/ambientlabscomputing/event_bus_client"
)

func main() {
    opts := client.EventClientOpts{
        Endpoint:  "ws://localhost:9000/ws",
        AuthToken: "your-token",
    }
    
    ec := client.NewEventClient(opts)
    ctx := context.Background()
    
    // Connect without subscriptions (publish-only)
    if err := ec.Connect(ctx, nil); err != nil {
        log.Fatal(err)
    }
    
    // Publish messages
    for i := 0; i < 100; i++ {
        err := ec.PublishHTTP(ctx, client.HTTPPublishRequest{
            Topic:   "events",
            Content: fmt.Sprintf("Message %d", i),
        })
        if err != nil {
            log.Printf("Publish error: %v", err)
        }
    }
}
```

### Example 2: Filtered Subscriber

```go
package main

import (
    "context"
    "fmt"
    "log"
    
    client "github.com/ambientlabscomputing/event_bus_client"
)

func main() {
    opts := client.EventClientOpts{
        Endpoint:       "ws://localhost:9000/ws",
        AuthToken:      "your-token",
        CommitInterval: "5s",
        GroupID:        "notification-service",
    }
    
    ec := client.NewEventClient(opts)
    
    // Subscribe with target filter
    userType := "user"
    userID := "user-123"
    
    subs := []client.SubscriptionRequest{
        {
            GroupID:    "notification-service",
            Topic:      "notifications",
            TargetType: &userType,
            TargetID:   &userID,
        },
    }
    
    ctx := context.Background()
    if err := ec.Connect(ctx, &subs); err != nil {
        log.Fatal(err)
    }
    
    // Process filtered messages
    msgChan := ec.IncomingMsgChannel()
    for msg := range msgChan {
        fmt.Printf("Notification for user-123: %s\n", msg.Content)
        // Automatically committed every 5s
    }
}
```

### Example 3: Multi-Topic Subscriber

```go
subs := []client.SubscriptionRequest{
    {
        GroupID: "analytics-service",
        Topic:   "orders",
    },
    {
        GroupID: "analytics-service",
        Topic:   "payments",
    },
    {
        GroupID: "analytics-service",
        Topic:   "shipments",
    },
}

if err := ec.Connect(ctx, &subs); err != nil {
    log.Fatal(err)
}

msgChan := ec.IncomingMsgChannel()
for msg := range msgChan {
    switch msg.Topic {
    case "orders":
        handleOrder(msg)
    case "payments":
        handlePayment(msg)
    case "shipments":
        handleShipment(msg)
    }
}
```

## Developer Guide

### Project Structure

```
event_bus_client/
├── cmd/
│   ├── eventbus_cli/       # CLI application entry point
│   │   └── main.go
│   └── loadtest/           # Load testing tool entry point
│       └── main.go
├── internal/
│   └── cli/                # CLI command implementations
│       ├── root.go         # Root command and setup
│       ├── publish.go      # Publish command
│       ├── subscribe.go    # Subscribe command
│       ├── shell.go        # Interactive shell
│       └── config.go       # Configuration management
├── loadtest/               # Load testing framework
│   ├── config.go           # Test configuration structures
│   ├── runner.go           # Test orchestration
│   ├── metrics.go          # Metrics collection
│   ├── README.md           # Load testing documentation
│   └── examples/           # Example test configurations
│       ├── quick.json
│       ├── basic.json
│       ├── stress.json
│       └── accuracy.json
├── docs/                   # Documentation and test results
├── bin/                    # Compiled binaries (gitignored)
├── main.go                 # Core client library
├── types.go                # Type definitions
├── go.mod                  # Go module definition
├── README.md               # This file
└── config.yaml.example     # Configuration template
```

### Building from Source

```bash
# Install dependencies
go mod download

# Build CLI
go build -o bin/eventbus_cli cmd/eventbus_cli/main.go

# Build load tester
go build -o bin/loadtest cmd/loadtest/main.go

# Build for multiple platforms
GOOS=linux GOARCH=amd64 go build -o bin/eventbus_cli-linux-amd64 cmd/eventbus_cli/main.go
GOOS=darwin GOARCH=arm64 go build -o bin/eventbus_cli-darwin-arm64 cmd/eventbus_cli/main.go
GOOS=windows GOARCH=amd64 go build -o bin/eventbus_cli-windows-amd64.exe cmd/eventbus_cli/main.go
```

### Running Tests

```bash
# Quick validation test (30 seconds)
./bin/loadtest -config loadtest/examples/quick.json -cli bin/eventbus_cli

# Full load test (2 minutes)
./bin/loadtest -config loadtest/examples/basic.json -cli bin/eventbus_cli

# Stress test (5 minutes)
./bin/loadtest -config loadtest/examples/stress.json -cli bin/eventbus_cli

# Accuracy test for filtering (1 minute)
./bin/loadtest -config loadtest/examples/accuracy.json -cli bin/eventbus_cli
```

## Performance

Based on production testing with backend optimizations:

| Metric | Value | Notes |
|--------|-------|-------|
| **Publish Rate** | 1,287 msg/s | 77,240 msgs in 60s, 100% success via HTTP POST |
| **Subscribe Rate** | 596 msg/s | Per subscriber, with backend goroutine leak fixes |
| **Message Fanout** | 5:1 ratio | 5 subscribers × N messages = expected behavior |
| **Target Filtering** | Server-side | Reduces client-side processing overhead |
| **Connection Model** | Hybrid | HTTP POST publish + WebSocket subscribe |

### Before and After Optimizations

| Component | Before | After | Improvement |
|-----------|--------|-------|-------------|
| Publish Success | 77% (WebSocket) | 100% (HTTP POST) | +23% |
| Subscribe Rate | 1.4 msg/s | 596 msg/s | 423× |
| Target Filtering | Not implemented | Working | ✅ |

See [docs/](docs/) for detailed test results and analysis.

## Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

### Quick Contribution Guide

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Add tests if applicable
5. Commit your changes (`git commit -m 'Add amazing feature'`)
6. Push to the branch (`git push origin feature/amazing-feature`)
7. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Support

- **Issues**: [GitHub Issues](https://github.com/ambientlabscomputing/event_bus_client/issues)
- **Documentation**: [GitHub Pages](https://ambientlabscomputing.github.io/event_bus_client/)
- **Email**: support@ambientlabs.io

## Acknowledgments

Built with:
- [gorilla/websocket](https://github.com/gorilla/websocket) - WebSocket implementation
- [spf13/cobra](https://github.com/spf13/cobra) - CLI framework
- [spf13/viper](https://github.com/spf13/viper) - Configuration management
- [peterh/liner](https://github.com/peterh/liner) - Interactive shell
- [fatih/color](https://github.com/fatih/color) - Terminal colors

---

**Maintained by Ambient Labs Computing** • [Website](https://ambientlabs.io) • [GitHub](https://github.com/ambientlabscomputing)
