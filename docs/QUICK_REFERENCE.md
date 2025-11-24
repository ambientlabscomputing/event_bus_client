# Quick Reference Guide

## Installation

```bash
# Install as library
go get github.com/ambientlabscomputing/event_bus_client

# Or build from source
git clone https://github.com/ambientlabscomputing/event_bus_client.git
cd event_bus_client
go build -o bin/eventbus_cli cmd/eventbus_cli/main.go
```

## Configuration

```bash
# Option 1: Environment variables
export EVENT_BUS_ENDPOINT="ws://localhost:9000/ws"
export EVENT_BUS_TOKEN="your-jwt-token"
export EVENT_BUS_GROUP_ID="my-group"

# Option 2: Config file (copy config.yaml.example to config.yaml)
```

## CLI Quick Commands

### Publish

```bash
# Basic
./bin/eventbus_cli publish topic-name "message content"

# With target fields
./bin/eventbus_cli publish orders "New order" \
  --target-type user \
  --target-id user-123
```

### Subscribe

```bash
# Basic
./bin/eventbus_cli subscribe topic-name

# With filters
./bin/eventbus_cli subscribe orders \
  --target-type user \
  --target-id user-123
```

### Interactive Shell

```bash
./bin/eventbus_cli shell
```

## Library Quick Start

```go
package main

import (
    "context"
    client "github.com/ambientlabscomputing/event_bus_client"
)

func main() {
    // Initialize
    opts := client.EventClientOpts{
        Endpoint:       "ws://localhost:9000/ws",
        AuthToken:      "token",
        CommitInterval: "5s",
        GroupID:        "my-group",
    }
    ec := client.NewEventClient(opts)
    
    // Subscribe
    subs := []client.SubscriptionRequest{{
        GroupID: "my-group",
        Topic:   "orders",
    }}
    
    ctx := context.Background()
    ec.Connect(ctx, &subs)
    
    // Publish
    ec.PublishHTTP(ctx, client.HTTPPublishRequest{
        Topic:   "orders",
        Content: "Hello",
    })
    
    // Consume
    for msg := range ec.IncomingMsgChannel() {
        println(msg.Content)
    }
}
```

## Load Testing

```bash
# Quick test (30s)
./bin/loadtest -config loadtest/examples/quick.json -cli bin/eventbus_cli

# Stress test (5m)
./bin/loadtest -config loadtest/examples/stress.json -cli bin/eventbus_cli

# Accuracy test (1m)
./bin/loadtest -config loadtest/examples/accuracy.json -cli bin/eventbus_cli
```

## Common Patterns

### Target Field Filtering

```go
userType := "user"
userId := "user-123"

subs := []client.SubscriptionRequest{{
    GroupID:    "my-group",
    Topic:      "notifications",
    TargetType: &userType,  // Filter by type
    TargetID:   &userId,    // Filter by specific ID
}}
```

### Multi-Topic Subscription

```go
topics := []string{"orders", "payments", "shipments"}
subs := make([]client.SubscriptionRequest, len(topics))
for i, topic := range topics {
    subs[i] = client.SubscriptionRequest{
        GroupID: "analytics",
        Topic:   topic,
    }
}
```

## Troubleshooting

### No messages received
- Check `fetchTrigger` is sent after initial subscription
- Verify consumer group ID is correct
- Ensure WebSocket connection is established

### Low publish success rate
- Use HTTP POST instead of WebSocket publishing
- Check network connectivity
- Verify authentication token

### Filtering not working
- Ensure filters are set in `SubscriptionRequest`
- Verify backend filtering is enabled
- Check server logs for filter application

## Performance Tips

1. **Use HTTP POST for publishing** - 100% success vs 77% with WebSocket
2. **Set appropriate CommitInterval** - Balance between consistency and performance
3. **Use target field filtering** - Reduce client-side processing
4. **Consumer groups** - Scale horizontally with multiple instances

## Links

- [Full Documentation](../README.md)
- [API Reference](https://pkg.go.dev/github.com/ambientlabscomputing/event_bus_client)
- [GitHub Repository](https://github.com/ambientlabscomputing/event_bus_client)
- [Issue Tracker](https://github.com/ambientlabscomputing/event_bus_client/issues)
