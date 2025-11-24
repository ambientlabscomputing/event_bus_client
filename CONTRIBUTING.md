# Contributing to Event Bus Client

Thank you for your interest in contributing to the Event Bus Client! This document provides guidelines and instructions for contributing.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [How to Contribute](#how-to-contribute)
- [Development Setup](#development-setup)
- [Coding Standards](#coding-standards)
- [Testing Requirements](#testing-requirements)
- [Pull Request Process](#pull-request-process)
- [Reporting Bugs](#reporting-bugs)
- [Suggesting Features](#suggesting-features)

## Code of Conduct

By participating in this project, you agree to maintain a respectful and inclusive environment for all contributors.

## Getting Started

1. **Fork the repository** on GitHub
2. **Clone your fork** locally:
   ```bash
   git clone https://github.com/YOUR-USERNAME/event_bus_client.git
   cd event_bus_client
   ```
3. **Add upstream remote**:
   ```bash
   git remote add upstream https://github.com/ambientlabscomputing/event_bus_client.git
   ```
4. **Create a branch** for your changes:
   ```bash
   git checkout -b feature/my-new-feature
   ```

## How to Contribute

### Types of Contributions

We welcome various types of contributions:

- **Bug fixes**: Fix issues in existing code
- **Features**: Add new functionality
- **Documentation**: Improve or add documentation
- **Tests**: Add or improve test coverage
- **Performance**: Optimize existing code
- **Examples**: Add example applications or use cases

## Development Setup

### Prerequisites

- Go 1.24.3 or later
- Access to an Event Bus server for testing (or use mock server)

### Build the Project

```bash
# Install dependencies
go mod download

# Build CLI
go build -o bin/eventbus_cli cmd/eventbus_cli/main.go

# Build load tester
go build -o bin/loadtest cmd/loadtest/main.go

# Run tests (when available)
go test ./...
```

### Configuration for Testing

Copy the example configuration:
```bash
cp config.yaml.example config.yaml
```

Edit `config.yaml` with your test server credentials.

## Coding Standards

### Go Code Style

- Follow standard Go conventions and idioms
- Use `gofmt` to format your code before committing:
  ```bash
  gofmt -w .
  ```
- Use `go vet` to check for common mistakes:
  ```bash
  go vet ./...
  ```
- Run `golint` if available:
  ```bash
  golint ./...
  ```

### Naming Conventions

- **Variables**: Use camelCase (e.g., `messageCount`, `targetType`)
- **Constants**: Use PascalCase for exported, SCREAMING_SNAKE_CASE for internal
- **Functions**: Use PascalCase for exported, camelCase for internal
- **Types**: Use PascalCase (e.g., `EventClient`, `SubscriptionRequest`)

### Comments and Documentation

- Add godoc comments for all exported functions, types, and constants:
  ```go
  // PublishHTTP sends a message to the event bus via HTTP POST.
  // It returns an error if the publish fails.
  func (ec *Client) PublishHTTP(ctx context.Context, req HTTPPublishRequest) error {
      // ...
  }
  ```
- Use inline comments to explain complex logic
- Keep comments up to date with code changes

### Error Handling

- Always handle errors explicitly
- Provide context with errors:
  ```go
  if err != nil {
      return fmt.Errorf("failed to connect to event bus: %w", err)
  }
  ```
- Use error wrapping (`%w`) to preserve error chains

## Testing Requirements

### Writing Tests

- Add tests for new functionality
- Ensure tests are deterministic and can run in isolation
- Use table-driven tests where appropriate:
  ```go
  func TestSomething(t *testing.T) {
      tests := []struct {
          name    string
          input   string
          want    string
          wantErr bool
      }{
          {"valid input", "test", "test", false},
          {"empty input", "", "", true},
      }
      
      for _, tt := range tests {
          t.Run(tt.name, func(t *testing.T) {
              got, err := Something(tt.input)
              if (err != nil) != tt.wantErr {
                  t.Errorf("Something() error = %v, wantErr %v", err, tt.wantErr)
                  return
              }
              if got != tt.want {
                  t.Errorf("Something() = %v, want %v", got, tt.want)
              }
          })
      }
  }
  ```

### Running Tests

```bash
# Run all tests
go test ./...

# Run tests with coverage
go test -cover ./...

# Run tests with verbose output
go test -v ./...

# Run specific test
go test -run TestSpecificFunction ./...
```

### Load Testing

Before submitting performance-related changes, run load tests:

```bash
# Quick validation
./bin/loadtest -config loadtest/examples/quick.json -cli bin/eventbus_cli

# Full test
./bin/loadtest -config loadtest/examples/basic.json -cli bin/eventbus_cli
```

## Pull Request Process

### Before Submitting

1. **Update your branch** with the latest upstream changes:
   ```bash
   git fetch upstream
   git rebase upstream/main
   ```

2. **Run all checks**:
   ```bash
   gofmt -w .
   go vet ./...
   go test ./...
   ```

3. **Update documentation** if you've changed:
   - Public APIs
   - Configuration options
   - CLI commands
   - Architecture

4. **Add or update examples** if you've added new features

### Commit Messages

Write clear, descriptive commit messages:

```
Add target field filtering to subscription requests

- Added TargetType and TargetID fields to SubscriptionRequest
- Updated CLI to accept --target-type and --target-id flags
- Modified buildSubscribeCommand to pass filters to backend
- Added accuracy test configuration for filter validation

Fixes #123
```

Format:
- **First line**: Brief summary (50 chars or less)
- **Body**: Detailed explanation of what and why
- **Footer**: Reference related issues

### Creating the Pull Request

1. **Push your branch** to your fork:
   ```bash
   git push origin feature/my-new-feature
   ```

2. **Create a pull request** on GitHub from your branch to `main`

3. **Fill out the PR template** with:
   - Description of changes
   - Related issue numbers
   - Testing performed
   - Screenshots (if UI changes)

4. **Wait for review** and address feedback

### Review Process

- At least one maintainer must approve the PR
- CI checks must pass
- All conversations must be resolved
- Branch must be up to date with main

## Reporting Bugs

### Before Reporting

- Check if the bug has already been reported in [Issues](https://github.com/ambientlabscomputing/event_bus_client/issues)
- Test with the latest version
- Gather relevant information (logs, configuration, steps to reproduce)

### Bug Report Template

```markdown
**Description**
A clear description of the bug.

**To Reproduce**
Steps to reproduce the behavior:
1. Configure client with '...'
2. Execute command '...'
3. Observe error '...'

**Expected Behavior**
What you expected to happen.

**Actual Behavior**
What actually happened.

**Environment**
- OS: [e.g., macOS 14.0]
- Go version: [e.g., 1.24.3]
- Client version: [e.g., v1.0.0]
- Server version: [if known]

**Logs**
```
Paste relevant logs here
```

**Additional Context**
Any other information that might be helpful.
```

## Suggesting Features

### Feature Request Template

```markdown
**Feature Description**
A clear description of the feature you'd like to see.

**Use Case**
Describe the problem this feature would solve or the use case it enables.

**Proposed Solution**
How you envision this feature working.

**Alternatives Considered**
Other solutions you've considered.

**Additional Context**
Any other context, mockups, or examples.
```

## Development Tips

### Debugging

- Use the interactive shell for manual testing:
  ```bash
  ./bin/eventbus_cli shell
  ```

- Enable verbose logging in your code:
  ```go
  logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
      Level: slog.LevelDebug,
  }))
  ```

### Testing Locally

- Use the load tester to validate performance:
  ```bash
  ./bin/loadtest -config loadtest/examples/accuracy.json -cli bin/eventbus_cli
  ```

- Test with various configurations:
  - Different message sizes
  - Different publish rates
  - With and without target fields

## Questions?

If you have questions that aren't covered in this guide:

- Check existing issues and discussions
- Ask in a new GitHub issue
- Email: support@ambientlabs.io

---

Thank you for contributing to Event Bus Client! 🎉
