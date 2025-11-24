# Event Bus Load Testing Suite

A comprehensive load testing tool for the event bus that uses the `eventbus_cli` to orchestrate multiple concurrent publishers and subscribers.

## Features

- **Multiple Scenario Types**: Publisher-only, subscriber-only, or mixed workloads
- **Target Field Impact Testing**: Compare performance with and without `target_type`/`target_id` fields
- **Configurable Load Patterns**: Fixed, random, or incremental message generation
- **Real-time Metrics**: Live progress reporting during test execution
- **Ramp-up Support**: Gradually increase load to avoid overwhelming the system
- **Flexible Configuration**: JSON-based test definitions

## Building

```bash
# Build the main CLI first
cd /Users/jose/ambient_labs/event_bus_client
go build -o eventbus_cli ./cmd/eventbus_cli

# Build the load tester
go build -o loadtest ./cmd/loadtest
```

## Configuration

Create a JSON configuration file with your test scenarios. See `examples/` for templates.

### Configuration Structure

```json
{
  "test_name": "my_load_test",
  "event_bus": {
    "endpoint": "ws://localhost:9000/ws",
    "token": "your-jwt-token"
  },
  "duration": "5m",
  "report_interval": "10s",
  "ramp_up": "30s",
  "scenarios": [...]
}
```

### Scenario Types

#### Publisher
```json
{
  "name": "publishers",
  "type": "publisher",
  "instances": 10,
  "topic": "test_topic",
  "group_id": "test_group",
  "publish_config": {
    "messages_per_second": 100,
    "message_size": 512,
    "batch_size": 10,
    "message_pattern": "random",
    "include_target_fields": false
  },
  "metadata": {
    "trace_id": "trace-123",
    "org_id": "org-456",
    "target_type": "user",
    "target_id": "user-789"
  }
}
```

#### Subscriber
```json
{
  "name": "subscribers",
  "type": "subscriber",
  "instances": 5,
  "topic": "test_topic",
  "group_id": "subscriber_group"
}
```

#### Mixed (Both Publisher and Subscriber)
```json
{
  "name": "mixed_workload",
  "type": "mixed",
  "instances": 10,
  "topic": "test_topic",
  "group_id": "mixed_group",
  "publish_config": {
    "messages_per_second": 50,
    "message_size": 256,
    "batch_size": 5,
    "message_pattern": "incremental",
    "include_target_fields": true
  }
}
```

## Usage

### Basic Test
```bash
./loadtest -config examples/basic.json
```

### Custom CLI Path
```bash
./loadtest -config mytest.json -cli /path/to/eventbus_cli
```

### Dry Run (Validate Config)
```bash
./loadtest -config mytest.json -dry-run
```

## Testing Target Field Impact

The `include_target_fields` flag in the publish configuration allows you to test the performance impact of including `target_type` and `target_id` fields:

```json
{
  "scenarios": [
    {
      "name": "without_target_fields",
      "publish_config": {
        "include_target_fields": false
      }
    },
    {
      "name": "with_target_fields",
      "publish_config": {
        "include_target_fields": true
      },
      "metadata": {
        "target_type": "order",
        "target_id": "order-123"
      }
    }
  ]
}
```

## Message Patterns

- **fixed**: Same message content repeated
- **random**: Random content each time (default)
- **incremental**: Numbered messages (msg-1, msg-2, etc.) for tracking

## Example Tests

### Basic Test (2 minutes)
Tests basic functionality with 10 publishers and 5 subscribers:
```bash
./loadtest -config examples/basic.json
```

### Stress Test (5 minutes)
High volume test comparing performance with/without target fields:
```bash
./loadtest -config examples/stress.json
```

## Output

### Live Progress
```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📈 Progress Report - 15:04:23
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

publishers_without_target_fields:
  Published: 12,000 msgs (100.0/s)
  Received:  0 msgs (0.0/s)
  Errors:    0 (0.00%)
  Elapsed:   120.0s

publishers_with_target_fields:
  Published: 12,000 msgs (100.0/s)
  Received:  0 msgs (0.0/s)
  Errors:    0 (0.00%)
  Elapsed:   120.0s
```

### Final Report
```
============================================================
📊 FINAL REPORT - basic_load_test
============================================================

publishers_without_target_fields:
  Total Published:  24,000 msgs
  Total Received:   0 msgs
  Errors:           0
  Publish Rate:     200.0 msg/s
  Receive Rate:     0.0 msg/s
  Error Rate:       0.00%

publishers_with_target_fields:
  Total Published:  24,000 msgs
  Total Received:   0 msgs
  Errors:           0
  Publish Rate:     200.0 msg/s
  Receive Rate:     0.0 msg/s
  Error Rate:       0.00%

------------------------------------------------------------
TOTAL:
  Messages Published: 48,000
  Messages Received:  0
  Total Errors:       0
============================================================
```

## Notes

- Make sure `eventbus_cli` is built before running load tests
- Update the JWT token in your configuration files
- Start with small tests and gradually increase load
- Monitor server resources during stress tests
- Use `ramp_up` to avoid sudden load spikes

## Troubleshooting

**Error: CLI binary not found**
```bash
go build -o eventbus_cli ./cmd/eventbus_cli
```

**Error: Configuration error**
Use `--dry-run` to validate your configuration:
```bash
./loadtest -config mytest.json -dry-run
```
