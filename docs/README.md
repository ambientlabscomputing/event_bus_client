# Event Bus Client - Documentation Index

Welcome to the Event Bus Client documentation. This directory contains test results, performance analysis, and optimization guides.

## Contents

### Test Results

- **[ACCURACY_TEST_RESULTS.md](ACCURACY_TEST_RESULTS.md)** - Target field filtering accuracy test results showing the validation of server-side filtering implementation
- **[final_test_results.txt](final_test_results.txt)** - Final comprehensive test results after all optimizations
- **[load_test_results.txt](load_test_results.txt)** - Initial load test results showing baseline performance
- **[load_test_with_subscriber_metrics.txt](load_test_with_subscriber_metrics.txt)** - Detailed subscriber performance metrics

### Optimization Guides

- **[# Event Bus V4 - Client Optimization Gui.md](# Event Bus V4 - Client Optimization Gui.md)** - Comprehensive guide documenting the optimization process from initial implementation through production-ready performance

## Key Findings

### Performance Benchmarks

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| **Publish Success Rate** | 77% (WebSocket) | 100% (HTTP POST) | +23% |
| **Subscribe Rate** | 1.4 msg/s | 596 msg/s | 423× |
| **Publish Throughput** | ~800 msg/s | 1,287 msg/s | 1.6× |

### Critical Fixes

1. **Polling Loop Activation** - Fixed missing `fetchTrigger` signal that prevented subscribers from receiving messages
2. **Backend Goroutine Leaks** - Identified and helped resolve server-side resource leaks
3. **Target Field Filtering** - Fixed client bug where filter flags weren't being sent in subscription payloads
4. **Architecture Migration** - Moved from WebSocket-only to hybrid HTTP POST + WebSocket for optimal reliability

### Architecture Evolution

```
Initial:              Optimized:
WebSocket Only   →    HTTP POST Publish + WebSocket Subscribe
77% Success      →    100% Success
Single Protocol  →    Hybrid Model
```

## Navigation

- [Back to Main README](../README.md)
- [Load Testing Documentation](../loadtest/README.md)
- [Contributing Guidelines](../CONTRIBUTING.md)
- [Changelog](../CHANGELOG.md)

## GitHub Pages

This documentation is also available at: [https://ambientlabscomputing.github.io/event_bus_client/](https://ambientlabscomputing.github.io/event_bus_client/)
