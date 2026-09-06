# AGENTS.md - Agentic Coding Guidelines

Behavioral guidelines to reduce common LLM coding mistakes. Merge with project-specific instructions as needed.

**Tradeoff:** These guidelines bias toward caution over speed. For trivial tasks, use judgment.
## 1. Think Before Coding

**Don't assume. Don't hide confusion. Surface tradeoffs.**

Before implementing:
- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them - don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

## 2. Simplicity First

**Minimum code that solves the problem. Nothing speculative.**

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.

Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

## 3. Surgical Changes

**Touch only what you must. Clean up only your own mess.**

When editing existing code:
- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it - don't delete it.

When your changes create orphans:
- Remove imports/variables/functions that YOUR changes made unused.
- Don't remove pre-existing dead code unless asked.

The test: Every changed line should trace directly to the user's request.

## 4. Goal-Driven Execution

**Define success criteria. Loop until verified.**

Transform tasks into verifiable goals:
- "Add validation" → "Write tests for invalid inputs, then make them pass"
- "Fix the bug" → "Write a test that reproduces it, then make it pass"
- "Refactor X" → "Ensure tests pass before and after"

For multi-step tasks, state a brief plan:
```
1. [Step] → verify: [check]
2. [Step] → verify: [check]
3. [Step] → verify: [check]
```

Strong success criteria let you loop independently. Weak criteria ("make it work") require constant clarification.

---

## Project Overview

This is a PostgreSQL logical replication to JSON/SQS/Redis tool. It uses the pglogrepl package to produce wal2json-compatible output.

## Build Commands

### Running Tests

```bash
# Run all tests
go test ./...

# Run tests in a specific package
go test ./replicator/...

# Run a single test
go test -run TestName ./package/path

# Run a single test with verbose output
go test -v -run TestName ./package/path

# Run tests with coverage
go test -cover ./...
```

### Building

```bash
# Build for local development
go run ./pg2redis/cmd/main.go
go run ./pg2sqs/cmd/main.go
go run ./example/cmd/main.go

# Build for Linux AMD64 (using Makefile - Windows syntax)
make build-amd64        # builds pg2redis
make build-amd64-sqs    # builds pg2sqs

# Build Docker image
make docker-pg2sqs
```

### Linting

```bash
# Run golangci-lint
golangci-lint run
golangci-lint run ./...
```

---

## Code Style Guidelines

### Formatting

- Use `gofmt` or an IDE with Go formatting support
- Use tabs for indentation
- Keep lines under 100 characters when practical
- No trailing whitespace

### Naming Conventions

- **Variables/Functions**: camelCase (e.g., `replConn`, `startReplication`)
- **Exported Types/Constants**: PascalCase (e.g., `PGReplicator`, `CommitPoint`)
- **Unexported**: starts with lowercase (e.g., `state`, `errNotSupported`)
- **Constants**: PascalCase for exported, camelCase or PascalCase for unexported (e.g., `defaultClosedMaxErrors`)
- **Acronyms**: Use PascalCase (e.g., `LSN`, `PG`, not `Lsn`, `Pg`)
- **Interfaces**: Name after the method they describe + "er" (e.g., `ReplicationListener`, `LSNStartReader`)
- **Test files**: `*_test.go` suffix

### Types

- Use explicit types for public fields in structs
- Use interfaces for dependencies (e.g., `ReplicationListener`)
- Prefer `context.Context` as first parameter for methods that can be cancelled
- Use `atomic` package for simple concurrent counters/flags

### Error Handling

- Return errors with `fmt.Errorf` using `%w` for wrapping
- Check for specific error types with `errors.Is()` and `errors.As()`
- Handle errors early; avoid nesting success paths
- Use sentinel errors for expected error conditions

### Logging

- Use `uber.org/zap` for structured logging
- Use named loggers with `logger.Named("component_name")`
- Use zap fields (`zap.Error`, `zap.String`, etc.) instead of string interpolation
- Log levels: Debug for development, Info for important events, Warn for recoverable issues, Error for failures

### Testing

- Use `testify` for assertions and require
- Use `assert` for assertions that should not stop test on failure
- Use `require` for assertions that must pass (fails fast)
- Use table-driven tests when testing multiple cases
- Test file naming: `package_test.go`
- Integration tests using testcontainers are in `internal/integrationtest/`. They require Docker and may take longer to run.

### Context Usage

- Pass `context.Context` as first argument to methods that perform I/O or can be cancelled
- Use `context.Background()` for top-level operations
- Use `context.WithCancel()`, `context.WithTimeout()` for derived contexts
- Check for context cancellation in loops

### Concurrency

- Use `sync/atomic` for simple atomic operations
- Use `sync.WaitGroup` for goroutine synchronization
- Use channels for communication between goroutines
- Document goroutine lifecycles in comments

### Package Structure

- Use descriptive package names (e.g., `replicator`, `circuitbreaker`, `pg2stats`)
- Keep related functionality together
- Internal packages should be under meaningful directories