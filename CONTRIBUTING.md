# Contributing to Objectra 🖼️

First off, thank you for taking the time to contribute! Contributions from the community are what make open-source projects like Objectra better. 

Whether you want to fix a bug, suggest an enhancement, improve documentation, or implement a new S3 API feature, this guide will help you get started.

---

## 📖 Table of Contents

- [Code of Conduct](#-code-of-conduct)
- [Codebase Architecture](#-codebase-architecture)
- [How Can I Contribute?](#-how-can-i-contribute)
  - [Reporting Bugs](#reporting-bugs)
  - [Suggesting Enhancements](#suggesting-enhancements)
- [Setting Up Your Development Environment](#-setting-up-your-development-environment)
  - [Prerequisites](#prerequisites)
  - [Running Locally](#running-locally)
  - [Running Tests](#running-tests)
  - [Linting](#linting)
- [Development & Design Guidelines](#-development--design-guidelines)
  - [Go Conventions](#go-conventions)
  - [Concurrency & Performance](#concurrency--performance)
  - [Aesthetic & Design Principles](#aesthetic--design-principles)
- [Submitting Your Changes](#-submitting-your-changes)
  - [Branch Naming](#branch-naming)
  - [Commit Message Guidelines](#commit-message-guidelines)
  - [Pull Request Process](#pull-request-process)
- [CI/CD & Releases](#-cicd--releases)
  - [CI/CD Pipeline](#cicd-pipeline)
  - [Releasing](#releasing)
- [Add Yourself to Contributors](#-add-yourself-to-contributors)

---

## 📄 Code of Conduct

We are committed to fostering a welcoming, respectful, and inclusive community. By participating in this project, you agree to treat all contributors with respect, maintain constructive feedback, and keep discussions friendly and professional.

If you encounter any behavior that violates these principles, please see our [Security Policy](SECURITY.md) or contact the maintainers.

---

## 🏛️ Codebase Architecture

Objectra is written in Go and structured to separate core storage engine logic, API handling, and the built-in web console:

- **[`cmd/objectra/`](cmd/objectra)**: The main entry point. Initializes configuration, storage backends, routers, and starts the S3 API and Console HTTP servers.
- **[`internal/auth/`](internal/auth)**: Houses consolidated cryptographic helper utilities (AWS Signature V4, HmacSHA256, token generation) and manages console session JWT validation.
- **[`internal/config/`](internal/config)**: Declares configuration models, binds environment variables, and manages default profile values.
- **[`internal/console/`](internal/console)**: Serves the Single-Page Web Console UI, handles CORS origin verification, WebSocket security checks, SPA wildcard fallback routing, and exposes Prometheus metrics.
- **[`internal/s3api/`](internal/s3api)**: Exposes the AWS S3-compatible REST API endpoints, handling authentication, CORS, multipart uploads, and range requests.
- **[`internal/server/`](internal/server)**: Orchestrates the HTTP/HTTPS listeners, configures TLS, and manages custom rate limiting.
- **[`internal/storage/`](internal/storage)**: The core storage abstraction. Implements local disk mapping, streaming I/O, bucket metadata management, lifecycle expiration execution, and bucket-level concurrency controls.

---

## 💡 How Can I Contribute?

### Reporting Bugs

If you find a bug in Objectra, please open a GitHub Issue with the following details:
1. **Clear title** and description of the issue.
2. **Steps to reproduce** the bug.
3. **Expected vs. actual behavior**.
4. **Environment details** (Go version, OS, Docker version, client S3 SDK used).
5. Relevant logs or error messages (with sensitive keys redacted).

### Suggesting Enhancements

We are always looking for ways to make Objectra better! If you have a feature request or enhancement idea:
1. Search the open issues to see if the feature has already been suggested.
2. Open a new issue detailing your proposal, explaining the use case and how it benefits the project.
3. Keep in mind Objectra's goal of being a lightweight, high-performance, and self-hosted S3-compatible storage engine.

---

## 💻 Setting Up Your Development Environment

### Prerequisites

- **Go 1.23 or higher** (Required to compile and run the project)
- **Docker & Docker Compose** (Optional, for building/testing containerized builds)
- **golangci-lint** (For running lint checks locally)
- **AWS CLI / Boto3** (Optional, for S3 API compatibility verification)

### Running Locally

1. **Fork and Clone the Repository**
   Fork the repository on GitHub and clone it to your local machine:
   ```bash
   git clone https://github.com/<your-username>/objectra.git
   cd objectra
   ```

2. **Configure Environment Variables**
   Create a local configuration file by copying the template file:
   ```bash
   cp .env.example .env
   ```
   Modify the `.env` variables if you need to run ports on different addresses or change the default storage directory (`OBJECTRA_DATA_DIR`).

3. **Build the Server**
   To verify that everything compiles correctly, build the binary:
   ```bash
   go build -o objectra ./cmd/objectra
   ```

4. **Run the Server**
   Load your environment variables and start Objectra:
   ```bash
   export $(grep -v '^#' .env | xargs) && ./objectra
   ```
   *Alternatively, run and compile in one step:*
   ```bash
   export $(grep -v '^#' .env | xargs) && go run ./cmd/objectra
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

# View coverage in HTML format
go tool cover -html=coverage.out -o coverage.html
```

### Linting

Before pushing your changes, run `golangci-lint` to check code quality. We enforce strict linting rules on every pull request:
```bash
# Install golangci-lint (if not already installed)
go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest

# Run the linter
golangci-lint run
```
Lint rules are defined in [`.golangci.yml`](.golangci.yml).

---

## 🛠️ Development & Design Guidelines

### Go Conventions

- **Idiomatic Code**: Follow standard Go idioms and style conventions.
- **Formatting**: All Go files must be formatted using `gofmt` (or `go fmt ./...`).
- **Dependencies**: Avoid adding unnecessary external dependencies to keep the project lightweight.
- **Error Handling**: Always check and handle errors explicitly. Return errors to callers instead of ignoring them, and wrap errors with context where appropriate (e.g., `fmt.Errorf("error doing x: %w", err)`).
- **Structured slog Logging**: Always use the structured logger `log/slog` rather than standard `log` or print statements. Include key-value context attributes where appropriate:
  - `slog.Debug`: Highly verbose details helpful for trace diagnostics.
  - `slog.Info`: High-level operational events (e.g., server started, port bound).
  - `slog.Warn`: Non-fatal issues (e.g., rate limits hit, configuration warnings).
  - `slog.Error`: Fatal issues or operations that failed.
- **Constant-time Security Checks**: When performing cryptographic or security-sensitive comparisons (e.g., SSE-C MD5 checksums, token validation, password verification), always use constant-time comparisons (e.g., `subtle.ConstantTimeCompare`) to mitigate side-channel timing attacks.
- **Console Request Origin Verification**: All custom web console APIs and WebSocket connection handshakes must validate the request's origin against the request host, local loopback (localhost/127.0.0.1), or explicitly allowed origins to prevent Cross-Site Request Forgery (CSRF) and unauthorized cross-origin requests.
- **Consolidated Cryptographic Utilities**: Reuse central cryptographic implementation helpers in `internal/auth` rather than introducing duplicate verification blocks across different internal packages.

### Concurrency & Performance

- **Bucket-Level Locking**: When reading or modifying bucket resources, acquire the corresponding bucket-level lock using the metadata store's locking mechanism (`acquireBucketLock`). Never use global package-level variables or raw mutexes for bucket operations to avoid race conditions.
- **Asynchronous Execution & Queues**: Performance-critical async side-effects, such as triggering webhooks or replication mirroring tasks, should utilize the buffered dispatcher queues (e.g., `syncQueue` or `webhookQueue`) rather than spawning unmanaged goroutines. This prevents resource exhaustion under heavy loads.
- **Bounded Access Logging Queue**: S3 API access logging must utilize a bounded worker pool queue (e.g., `logChan` with a capacity and a dedicated set of worker goroutines) to avoid goroutine explosion under high-load situations. Log events should be enqueued asynchronously, dropping them if the log queue is fully saturated.
- **Passive Map Cleanups**: Stateful in-memory maps (e.g., console rate limit trackers, active user session mappings) must implement a passive cleanup mechanism (e.g., periodically purging expired/stale entries inline during request handling paths) to ensure memory footprint remains bounded.
- **Streaming I/O**: Keep streaming operations memory-efficient. Avoid buffering large S3 objects in memory. Stream bytes directly to/from disk where possible.
- **HTTP Client Reuse**: Reuse `http.Client` instances across outbound requests (e.g., replication mirror syncing) to utilize TCP connection pooling and prevent system socket/port exhaustion under heavy request loads.
- **HTTP Response Body Draining**: Always drain HTTP response bodies (e.g., via `io.Copy(io.Discard, resp.Body)`) before closing them. This allows the client transport layer to keep TCP connections alive and reuse them for subsequent requests.
- **HTTP Transport Pooling & Timeouts**: When configuring long-lived `http.Client` instances (e.g. for replication/sync workers), configure explicit timeouts, enable HTTP/2 support, and customize the underlying `http.Transport` connection pool configuration parameters (such as `MaxIdleConns`, `IdleConnTimeout`, and `MaxIdleConnsPerHost`) to prevent connection leaks and TCP port exhaustion.
- **Reference-Counted Initialization Mutexes**: For dynamic, on-demand resource setup (e.g., opening per-bucket metadata databases), protect critical paths with reference-counted mutexes. Ensure the lock metadata is removed from internal maps only when the reference count reaches zero.
- **Global DB Registry Verification**: Before instantiating or initializing per-bucket dynamic database instances, verify that the bucket exists in the global metadata registry. This prevents dynamic tasks from unintentionally re-creating deleted database files on disk.
- **LIFO Resource Teardown**: In closer wrappers handling multiple resources, invoke close operations in Last-In-First-Out (LIFO) order to guarantee dependencies are cleaned up in the correct sequence.

### Aesthetic & Design Principles

- **Web Console**: The built-in web console should be responsive, modern, and provide a premium user experience. Avoid visual placeholders and implement clean user interaction flows.

---

## 🚀 Submitting Your Changes

### Branch Naming

Use clear branch names prefixing the type of changes you are making:
- `feature/` for new features (e.g., `feature/lifecycle-rules`)
- `bugfix/` for bug fixes (e.g., `bugfix/multipart-part-size`)
- `docs/` for documentation updates (e.g., `docs/add-contributing-guide`)
- `refactor/` for code refactoring
- `test/` for testing additions

### Commit Message Guidelines

We prefer clean and descriptive commit messages following the [Conventional Commits](https://www.conventionalcommits.org/) specification:
```
<type>(<scope>): <short description>

[Optional longer body describing details]
```
Common types include:
- `feat`: A new feature (e.g., `feat(s3api): add support for S3 Lifecycle policies`)
- `fix`: A bug fix (e.g., `fix(storage): resolve memory leak in multipart upload`)
- `docs`: Documentation updates (e.g., `docs: update setup instructions in README`)
- `test`: Add or modify tests (e.g., `test: add tests for path traversal protection`)
- `refactor`: Code changes that neither fix a bug nor add a feature (e.g., `refactor: simplify metadata store locking`)
- `perf`: Performance optimizations (e.g., `perf: optimize streaming I/O buffer allocation`)
- `ci`: CI pipeline updates (e.g., `ci: update golangci-lint version`)

> **Why this matters**: The release workflow uses [GoReleaser](https://goreleaser.com/) which auto-generates changelogs grouped by commit type (`feat`, `fix`, `docs`, `test`, `refactor`, `perf`). Commits prefixed with `chore:`, `ci:`, or `style:` are excluded from release notes.

### Pull Request Process

1. Fork the repository and create your branch:
   ```bash
   git checkout -b feature/your-feature-name
   ```
2. Implement your changes, ensuring you write relevant unit/integration tests.
3. Push your branch to your fork:
   ```bash
   git push origin feature/your-feature-name
   ```
4. Open a Pull Request against the `main` branch of the `salvatorecorvaglia/objectra` repository.
5. Fill out the pull request template completely, detailing:
   - What problem is solved by the PR.
   - The approach taken.
   - Verification and manual testing steps.
6. Address any feedback during code review and update the branch as needed.

---

Happy coding! 🖼️
