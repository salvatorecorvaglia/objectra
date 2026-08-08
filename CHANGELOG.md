# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Reorganized project test suite into a standalone `tests/` root directory (`tests/auth`, `tests/config`, `tests/console`, `tests/httpx`, `tests/s3api`, `tests/server`, `tests/storage`).
- Added new integration and fuzz test suites including `paths_fuzz_test.go`, `ssec_multipart_test.go`, `operations_test.go`, `subresource_test.go`, `dbcache_test.go`, `listing_test.go`, `clientip_test.go`, `presign_test.go`, and `validate_test.go`.
- Added `internal/httpx` package providing reliable client IP resolution (`clientip.go`) supporting trusted proxy chains.
- Added decoupled object listing module (`internal/storage/listing.go`) for structured bucket/key listing operations.

### Changed

- Enhanced Admin Web Console accessibility with ARIA attributes, semantic structure, and improved drag-and-drop file upload reliability.
- Upgraded `.golangci.yml` linter configuration to version 2 schema and updated CI linter rules.
- Refactored S3 API router subresource handling, error rendering, and bulk object delete operations.

### Fixed

- Fixed database handle leaks in `TestDeleteBucketVersionedEmptiness` and unclosed server instances in `TestServerStartPortConflict` that caused `unlinkat` access denied errors on Windows CI runners.
- Fixed false-positive integer conversion linting warnings and simplified server shutdown flag assertions.

## [1.0.0] - 2026-08-06

### Chore

- Promoted package version to 1.0.0 for initial official Docker Hub release.

## [0.6.0] - 2026-07-27

### Added

- LRU connection cache (bounded capacity) for per-bucket metadata SQLite databases in `MetadataStore` to optimize open file descriptor usage and memory overhead.
- CORS credential header support (`Access-Control-Allow-Credentials: true`) when cross-origin request credentials are enabled.
- Modal dialog keyboard navigation (`ESC` key to dismiss) and rate-limiting error feedback in the Admin Web Console.

### Changed

- Upgraded minimum Go version requirement to Go 1.25 across project dependencies, Dockerfile, CI workflows, and documentation.
- Refactored S3 API router test teardown to perform synchronous log flushing during server shutdown.
- Optimized GitHub Actions CI workflows by enabling Go build caching and smart concurrency cancellation (`cancel-in-progress`).

### Removed

- Redundant `docker-publish` job from the GitHub Actions release workflow.

## [0.5.0] - 2026-07-19

### Added

- Multi-platform Docker image publishing (`linux/amd64`, `linux/arm64`) to GitHub Container Registry (GHCR) upon release tag pushes.
- Concurrency controls (`cancel-in-progress`) to the CI workflow to automatically terminate redundant runs.
- `STIVA_TRUST_PROXY` configuration to support correct client IP extraction from proxy environments using the first address in the `X-Forwarded-For` header.

### Changed

- Centralized request signing logic inside the `internal/auth` package (`auth.SignRequest`), replacing duplicate inline implementations.
- Optimized Prometheus `/metrics` endpoint by caching system disk space query responses for 30 seconds.
- Refactored CORS origin matching to perform domain suffix checks directly against the parsed hostname, fixing wildcard matches on custom port combinations.
- Hardened GitHub Actions release workflow permissions by restricting write permissions to the specific job level.
- Cleaned up duplicate metadata store retrieval logic in `UploadPart` to optimize lock contention on multipart uploads.
- Updated Docker GitHub Actions in release workflow to support Node.js 24 (`setup-qemu-action@v4`, `login-action@v4`, `metadata-action@v6`).

### Fixed

- Expiration rule logic in bucket lifecycle management to correctly calculate object age based on its actual `LastModified` timestamp.

## [0.4.0] - 2026-07-13

### Added

- Aggregated and buffered access logging inside the S3 API router to optimize disk I/O, writing logs in batches (up to 100 entries or every 5 seconds).
- Active request tracking with a `WaitGroup` to ensure graceful shutdown of all outstanding API requests before terminating router log workers.
- Truncated text preview support in the console viewer for files larger than 256 KB using S3 `Range` requests.
- Background silent auto-refresh mechanism (every 10 seconds) for the web console objects list.

### Changed

- Decoupled the monolithic storage engine inside `internal/storage/filesystem.go` into dedicated files: `multipart.go` (multipart uploads), `lifecycle.go` (lifecycle rules/expiration), and `paths.go` (path validation).
- Refactored access log delivery to run on a single background worker thread instead of multiple concurrent workers.
- Streamlined CORS preflight responses to return a forbidden status code (403) upon failure instead of structured S3 errors.
- Updated documentation in `README.md` and `CONTRIBUTING.md` to reflect range requests, metrics, and internal package structure changes.
- Updated `.gitignore` to exclude IDE/editor-specific directories (`.copilot`, `.codex`, `.cagent`).

## [0.3.0] - 2026-07-05

### Added

- `STIVA_METRICS_TOKEN` environment variable configuration to secure the `/metrics` Prometheus endpoint.
- SPA client-side routing wildcard fallback support in the web console, preventing 404 errors on browser page reloads or deep-linked URL paths.
- S3 `GetObject` range request support for compressed/non-seekable streams, dynamically buffering stream chunks and returning `206 Partial Content`.
- Integration tests in `internal/s3api` for non-seekable compressed range request operations.

### Changed

- Consolidated cryptographic signature helpers: moved signature utilities to `internal/auth` (`auth.HmacSHA256`) and reuse them across S3 API verification and mirroring replication.
- Optimized storage database retrieval inside `MetadataStore` to verify bucket registration in the global DB registry prior to instantiating per-bucket connection handlers, preventing automatic creation of deleted/dangling database files on disk.
- Enhanced mirroring replication client by optimizing connection pooling parameters on `http.Transport` to prevent port and socket exhaustion under high loads.
- Adjusted multipart upload completion to strip surrounding quotes from part ETags before validation.
- Fixed S3 `ListObjects` key seek positioning when using delimiters and start-after parameters.

### Security

- Enforced console endpoint and WebSocket security by validating incoming `Origin` headers against the request host and local loopback addresses (`localhost`, `127.0.0.1`).

## [0.2.0] - 2026-06-22

### Added

- `STIVA_S3_ENDPOINT` environment variable configuration to customize public S3 endpoint URLs for console presigned links.
- Skeleton loading shimmers to the Admin Web Console for buckets and objects loading states.
- Concurrency integration tests for MetadataStore `initLocks` reference counting.

### Changed

- Reused `http.Client` for replication mirroring dispatcher (`performSync`) to prevent port/connection exhaustion.
- Optimized metadata database initialization `initLocks` using reference-counted mutexes to prevent memory leak and race conditions on concurrent database lookups.
- Enhanced PDF preview sandboxing in the console iframe to enforce strict script-only permissions (removing `allow-same-origin`).
- Improved webhook dispatcher connection reuse by discarding and closing response bodies.
- Cleaned up unused `logStop` channel in the S3 API router.
- Refactored `readCloserWrapper.Close()` to close underlying closers in LIFO (Last-In-First-Out) order.

## [0.1.0] - 2026-06-15

### Added

- Bucket metadata caching within the metadata store to reduce database lookups.
- Integration of replication sync configurations into the filesystem storage engine.
- Multi-version object deletion support to ensure all historical versions are removed from disk upon object deletion.
- CORS origin wildcard matching with scheme validation (e.g., `https://*.example.com`).
- Multipart upload support for Server-Side Encryption with Customer-provided Keys (SSE-C).
- GitHub Issue and PR templates for standardized community contributions.
- Bounded access log worker pool inside the S3 API Router to queue logs and prevent goroutine explosion under load.
- `STIVA_TRUST_PROXY` environment variable option to respect proxy headers (like `X-Forwarded-For`) for rate limiting console access.
- Startup security warning if default S3 credentials (`stiva` / `stiva123`) are detected.
- Storage engine startup sweep that cleans orphaned temporary/multipart files (`.stiva-tmp-`, etc.) left by previous crashes.
- Webhook and replication mirroring test suites (`webhook_test.go` and `sync_test.go`) covering asynchronous events.

### Changed

- Updated filesystem storage engine initialization in tests to utilize temporary directories and configuration parameters.
- Updated storage engine initialization in console tests with required initialization parameters.
- Simplified image tagging strategy for Docker release publishing to use semver tags and `latest`.
- Updated documentation references to use relative paths/links instead of absolute/hardcoded links.
- Optimized multipart upload performance by reducing lock contention, holding metadata locks only briefly during validation and updates while allowing concurrent disk writes.
- Upgraded GitHub Actions and CI workflow runner environments to use the latest versions.
- Refactored code style, improved error message clarity, and modernized `golangci-lint` configuration settings.
- Removed default credentials from Dockerfile for security hardening.
- Standardized coding guidelines in `CONTRIBUTING.md` regarding constant-time security checks, bounded logging workers, and passive map cleanups.
- Removed loopback IP address (`127.0.0.1`) from auto-generated TLS certificate SAN `DNSNames`.

### Fixed

- Excluded internal system keys (prefixed with `_sys_`) from S3 `ListBuckets` API results to prevent system metadata leakage.
- Deadlock and race conditions in webhook and mirror sync dispatchers by unlocking locks prior to closing queues during shutdown.
- Lock leak in metadata store initialization by ensuring lock cleanup runs via `defer` on errors.
- Flaky integration test assertions in webhook and mirror sync tests by replacing sleep-based waits with polling logic.
- Cleaned up orphaned temporary body files (`stiva-body-*`) in the `tmp` data directory during startup.
- Bucket stripe locking around database scans in `CleanExpiredMultipartUploads` to prevent race conditions during concurrent bucket deletions.
- S3 `DeleteBucketLifecycle` API handler to correctly validate bucket existence before returning `204 No Content`.
- Safely handled GET object errors to avoid potential nil pointer dereference on readers.
- Timing race conditions in the webhook and mirror sync integration tests.

### Removed

- Docker Hub publishing support from the GitHub Actions release workflow.

### Security

- Bumped `github.com/golang-jwt/jwt/v5` from `5.2.1` to `5.2.2`.
- Passive inline garbage collection in the console rate limiter map to prune inactive clients and mitigate memory leak vulnerability.
- Constant-time comparisons (via `crypto/subtle`) for custom SSE-C customer key MD5 comparisons.

## [0.0.1] - 2026-05-27

### Added

- First implementation of Stiva.