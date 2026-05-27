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
- [Coding Guidelines](#coding-guidelines)
  - [Go Conventions](#go-conventions)
  - [Aesthetic & Design Principles](#aesthetic--design-principles)
- [Commit Message Guidelines](#commit-message-guidelines)
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
3. **Verify** your changes locally (build successfully and pass all tests).
4. **Format your code**: Run `go fmt ./...` and `go vet ./...`.
5. **Commit your changes** using clean, descriptive commit messages (see [Commit Message Guidelines](#commit-message-guidelines)).
6. **Push** your branch to your fork.
7. **Open a Pull Request** against the `main` branch of Objectra. Provide a clear description of what your PR does, referencing any related issues.

---

## Local Development Setup

### Prerequisites

- **Go 1.23+** (installed on your local machine)
- **Docker & Docker Compose** (optional, for containerized environment tests)
- **AWS CLI / Boto3** (optional, for S3 API compatibility verification)

### Running Locally

1. **Clone your fork**:

   ```bash
   git clone https://github.com/YOUR-USERNAME/objectra.git
   cd objectra
   ```

2. **Set up configuration**:
   Copy the example environment file:

   ```bash
   cp env.example .env
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

# Run tests with race detector
go test -race ./...
```

---

## Coding Guidelines

### Go Conventions

- Follow standard Go idioms and style conventions.
- All Go files must be formatted using `gofmt` (or `go fmt ./...`).
- Avoid adding unnecessary external dependencies to keep the project lightweight.
- Ensure proper error handling: return errors to callers instead of ignoring them, and wrap errors with context where appropriate (e.g., `fmt.Errorf("error doing x: %w", err)`).
- **Structured slog Logging**: Always use the structured logger `log/slog` rather than standard `log` or print statements. Include key-value context attributes where appropriate (e.g., `slog.Info("msg", "key", value)` or `slog.Error("msg", "error", err)`).

### Concurrency & Performance

- **Bucket-Level Locking**: When reading or modifying bucket resources, acquire the corresponding bucket-level lock using the metadata store's locking mechanism (`acquireBucketLock`). Never use global package-level variables or raw mutexes for bucket operations.
- **Asynchronous Execution & Queues**: Performance-critical async side-effects, such as triggering webhooks or replication mirroring tasks, should utilize the buffered dispatcher queues (e.g., `syncQueue` or `webhookQueue`) rather than spawning unmanaged goroutines. This prevents resource exhaustion under heavy loads.
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

Keep commit titles short (under 50-60 characters) and use the message body for more detail if necessary.

## 📜 Code of Conduct

Please maintain a respectful and professional tone in all communications.

---

Happy coding! 🌑
