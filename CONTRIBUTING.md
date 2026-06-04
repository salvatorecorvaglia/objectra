# Contributing to Objectra 🖼️

Thank you for your interest in contributing to Objectra! We welcome and appreciate contributions of all kinds, whether you are fixing a bug, improving documentation, or proposing new features.

Please take a moment to review this document to make the contribution process smooth and effective for everyone.

---

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [How to Contribute](#how-to-contribute)
  - [Reporting Bugs](#reporting-bugs)
  - [Suggesting Enhancements](#suggesting-enhancements)
  - [Pull Request Process](#pull-request-process)
- [Local Development Setup](#local-development-setup)
  - [Prerequisites](#prerequisites)
  - [Running Locally](#running-locally)
  - [Running Tests](#running-tests)
  - [Linting](#linting)
- [Coding Guidelines](#coding-guidelines)
  - [Go Conventions](#go-conventions)
  - [Concurrency & Performance](#concurrency--performance)
  - [Aesthetic & Design Principles](#aesthetic--design-principles)
- [Commit Message Guidelines](#commit-message-guidelines)
- [CI/CD Pipeline](#cicd-pipeline)
- [Releasing](#releasing)
- [Licensing](#licensing)

---

## Code of Conduct

By participating in this project, you agree to abide by basic professional standards:

- Be respectful, welcoming, and inclusive of all contributors.
- Focus on constructive feedback and collaboration.
- Avoid personal attacks or exclusionary behavior.

---

## How to Contribute

### Reporting Bugs

If you find a bug, please check the existing [GitHub Issues](https://github.com/salvatorecorvaglia/objectra/issues) to see if it has already been reported. If not, feel free to open a new issue. Include:

1. **Clear title and description** of the bug.
2. **Steps to reproduce** the issue.
3. **Expected vs. actual behavior**.
4. **Environment details** (Go version, OS, Docker version, client S3 SDK used).
5. Relevant logs or error messages (with sensitive keys redacted).

### Suggesting Enhancements

We are always looking for ways to make Objectra better! If you have a feature request or enhancement idea:

1. Search the open issues to see if the feature has already been suggested.
2. Open a new issue detailing your proposal, explaining the use case and how it benefits the project.
3. Keep in mind Objectra's goal of being a lightweight, high-performance, and self-hosted S3-compatible storage engine.

### Pull Request Process

1. **Fork** the repository and create your branch from the `main` branch:
   ```bash
   git checkout -b feature/my-cool-feature
   ```
2. **Implement your changes**. Make sure to write unit tests for any new features or bug fixes.
3. **Verify** your changes locally (build successfully and pass all tests — see [Running Tests](#running-tests) and [Linting](#linting)).
4. **Commit your changes** using clean, descriptive commit messages (see [Commit Message Guidelines](#commit-message-guidelines)).
5. **Push** your branch to your fork.
6. **Open a Pull Request** against the `main` branch of Objectra. Provide a clear description of what your PR does, referencing any related issues.

> **Note**: CI will automatically run lint checks, tests (with race detection), build verification, and a Docker build on your PR. All checks must pass before merging.

---

## Local Development Setup

### Prerequisites

- **Go 1.23+** (installed on your local machine)
- **Docker & Docker Compose** (optional, for containerized environment tests)
- **AWS CLI / Boto3** (optional, for S3 API compatibility verification)
- **golangci-lint** (optional for local linting — CI runs this automatically)

### Running Locally

1. **Clone your fork**:

   ```bash
   git clone https://github.com/YOUR-USERNAME/objectra.git
   cd objectra
   ```

2. **Set up configuration**:
   Copy the example environment file:

   ```bash
   cp .env.example .env
   ```

3. **Build from source**:

   ```bash
   go build -o objectra ./cmd/objectra
   ```

4. **Run the server**:
   Load your environment variables and start Objectra:
   ```bash
   export $(grep -v '^#' .env | xargs) && ./objectra
   ```
   The S3 API will be available at `http://localhost:9000` and the web console at `http://localhost:9001`.

### Running Tests

Objectra has unit and integration tests across the codebase. Make sure all tests pass before submitting a pull request:

```bash
# Run all tests in the workspace
go test -v ./...

# Run tests with race detector (used in CI)
go test -race ./...

# Run tests with coverage report
go test -race -coverprofile=coverage.out -covermode=atomic ./...
go tool cover -html=coverage.out -o coverage.html
```

### Linting

The project uses [golangci-lint](https://golangci-lint.run/) with the configuration defined in [.golangci.yml](.golangci.yml). Enabled linters include: `govet`, `staticcheck`, `errcheck`, `ineffassign`, `unused`, and `gosimple`.

```bash
# Install golangci-lint (if not already installed)
go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest

# Run the linter
golangci-lint run

# Or use go fmt and go vet directly
go fmt ./...
go vet ./...
```

> **Tip**: CI runs `golangci-lint` automatically on every push and pull request, so you don't need it installed locally — but running it before pushing helps catch issues early.

---

## Coding Guidelines

### Go Conventions

- Follow standard Go idioms and style conventions.
- All Go files must be formatted using `gofmt` (or `go fmt ./...`).
- Avoid adding unnecessary external dependencies to keep the project lightweight.
- Ensure proper error handling: return errors to callers instead of ignoring them, and wrap errors with context where appropriate (e.g., `fmt.Errorf("error doing x: %w", err)`).
- **Structured slog Logging**: Always use the structured logger `log/slog` rather than standard `log` or print statements. Include key-value context attributes where appropriate (e.g., `slog.Info("msg", "key", value)` or `slog.Error("msg", "error", err)`).
- **Constant-time Security Checks**: When performing cryptographic or security-sensitive comparisons (e.g., SSE-C MD5 checksums, token validation, password verification), always use constant-time functions (e.g., `subtle.ConstantTimeCompare`) to mitigate side-channel timing attacks.

### Concurrency & Performance

- **Bucket-Level Locking**: When reading or modifying bucket resources, acquire the corresponding bucket-level lock using the metadata store's locking mechanism (`acquireBucketLock`). Never use global package-level variables or raw mutexes for bucket operations.
- **Asynchronous Execution & Queues**: Performance-critical async side-effects, such as triggering webhooks or replication mirroring tasks, should utilize the buffered dispatcher queues (e.g., `syncQueue` or `webhookQueue`) rather than spawning unmanaged goroutines. This prevents resource exhaustion under heavy loads.
- **Bounded Access Logging Queue**: S3 API access logging must utilize a bounded worker pool queue (e.g., `logChan` with a capacity and a dedicated set of worker goroutines) to avoid goroutine explosion under high-load situations. Log events should be enqueued asynchronously, dropping them if the log queue is fully saturated.
- **Passive Map Cleanups**: Stateful in-memory maps (e.g., console rate limit trackers, active user session mappings) must implement a passive cleanup mechanism (e.g., periodically purging expired/stale entries inline during request handling paths) to ensure memory footprint remains bounded.
- **Streaming I/O**: Keep streaming operations memory-efficient. Avoid buffering large S3 objects in memory.

### Aesthetic & Design Principles

- **Web Console**: The built-in web console should be responsive, modern, and provide a premium user experience. Avoid visual placeholders and implement clean user interaction flows.

---

## Commit Message Guidelines

We prefer clean and descriptive commit messages following the [Conventional Commits](https://www.conventionalcommits.org/) specification:

- `feat: add support for S3 Lifecycle policies`
- `fix: resolve memory leak in multipart upload`
- `docs: update setup instructions in README`
- `test: add tests for path traversal protection`
- `refactor: simplify metadata store locking`
- `perf: optimize streaming I/O buffer allocation`
- `ci: update golangci-lint to latest version`

Keep commit titles short (under 50-60 characters) and use the message body for more detail if necessary.

> **Why this matters**: The release workflow uses [GoReleaser](https://goreleaser.com/) which auto-generates changelogs grouped by commit type (`feat`, `fix`, `docs`, `test`, `refactor`, `perf`). Commits prefixed with `chore:`, `ci:`, or `style:` are excluded from release notes.

---

## CI/CD Pipeline

Every push and pull request to `main` triggers the **CI workflow** (`.github/workflows/ci.yml`):

| Job        | Description                                           |
| ---------- | ----------------------------------------------------- |
| **Lint**   | Runs `golangci-lint` with the project configuration   |
| **Test**   | Runs `go test -race` across Go 1.23 and stable        |
| **Build**  | Compiles a static binary (`CGO_ENABLED=0`)            |
| **Docker** | Builds the Docker image (no push) to verify the build |

All jobs must pass for a PR to be mergeable.

---

## Releasing

Releases are automated via the **Release workflow** (`.github/workflows/release.yml`), triggered by pushing a semver tag:

```bash
git tag v1.0.0
git push origin v1.0.0
```

This will:

1. **Run tests** to verify the tagged commit.
2. **GoReleaser** creates GitHub releases with cross-compiled binaries (Linux, macOS, Windows — amd64/arm64) and auto-generated changelogs.
3. **Docker Publish** builds and pushes multi-architecture images (`linux/amd64`, `linux/arm64`) to `ghcr.io/salvatorecorvaglia/objectra`.

## 📜 Code of Conduct

Please maintain a respectful and professional tone in all communications.

---

Happy coding! 🌑
