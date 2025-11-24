# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2025-11-23

### Added

#### Core Library
- **Hybrid Publishing Architecture**: HTTP POST to `/api/v1/publish` for 100% publish success rate
- **WebSocket Subscriptions**: Long-polling (5s hold time) for efficient message delivery
- **Target Field Filtering**: Server-side filtering by `target_type` and `target_id`
- **Consumer Groups**: Support for scalable message processing with automatic offset tracking
- **Auto-Reconnect**: Built-in connection resilience and reconnection logic
- **Offset Management**: Automatic offset commits at configurable intervals
- **Multi-Topic Support**: Subscribe to multiple topics simultaneously

#### CLI Tool
- **Interactive Shell Mode**: User-friendly REPL for exploring the event bus
- **Publish Command**: Send messages with optional target fields and metadata
- **Subscribe Command**: Receive messages with filtering support
- **Configuration Management**: Support for both config files and environment variables
- **Colorized Output**: Enhanced readability with colored terminal output

#### Load Testing Suite
- **Scenario-Based Testing**: Define complex test scenarios with JSON configuration
- **Publisher/Subscriber Workloads**: Test publish-only, subscribe-only, or mixed scenarios
- **Target Field Testing**: Compare performance with and without target fields
- **Real-Time Metrics**: Live progress reporting during test execution
- **Configurable Parameters**: Message rate, size, pattern, and duration
- **Multiple Test Scenarios**: Quick (30s), basic (2m), stress (5m), and accuracy (1m) tests

#### Documentation
- Comprehensive README with quick start guide
- Load testing documentation with example configurations
- API examples for common use cases
- Performance benchmarks and optimization notes

### Performance

- **Publish Rate**: 1,287 msg/s (100% success via HTTP POST)
- **Subscribe Rate**: 596 msg/s per subscriber (with backend optimizations)
- **Connection Model**: Hybrid HTTP POST + WebSocket for optimal performance
- **Target Filtering**: Server-side filtering reduces client-side processing

### Fixed

- **Polling Loop Activation**: Fixed critical bug where subscribers weren't triggering message fetch after initial subscription
- **Target Filter Transmission**: Fixed CLI bug where `--target-type` and `--target-id` flags weren't being sent to backend
- **Message Reception**: Improved from 0 msg/s to 596 msg/s with fetchTrigger fix

### Technical Details

#### Architecture Changes
- Migrated from WebSocket-only publishing (77% success) to HTTP POST (100% success)
- Implemented long-polling subscription model for efficient message delivery
- Added `fetchTrigger` channel signal for polling loop activation

#### Backend Collaboration
- Worked with backend team to implement server-side target field filtering
- Identified and resolved goroutine leak issues (423× performance improvement)
- Validated index optimization for target field queries

### Testing

- **Stress Tests**: Validated 1.9M+ messages under high load
- **Accuracy Tests**: Confirmed target field filtering works correctly with varied message counts
- **Load Profiles**: Tested with 10-100 concurrent publishers/subscribers

---

## [Unreleased]

### Planned Features
- Unit test suite
- Example applications
- GitHub Actions CI/CD pipeline
- Metrics export (Prometheus format)
- Message batching optimization

---

[1.0.0]: https://github.com/ambientlabscomputing/event_bus_client/releases/tag/v1.0.0
