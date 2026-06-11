# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [0.1.0] - 2026-06-11

### Added
- Multi-version object deletion support to ensure all historical versions are removed from disk upon object deletion.
- CORS origin wildcard matching with scheme validation (e.g., `https://*.example.com`).
- Multipart upload support for Server-Side Encryption with Customer-provided Keys (SSE-C).
- GitHub Issue and PR templates for standardized community contributions.
- Bounded access log worker pool inside the S3 API Router to queue logs and prevent goroutine explosion under load.
- `OBJECTRA_TRUST_PROXY` environment variable option to respect proxy headers (like `X-Forwarded-For`) for rate limiting console access.
- Startup security warning if default S3 credentials (`objectra` / `objectra123`) are detected.
- Storage engine startup sweep that cleans orphaned temporary/multipart files (`.objectra-tmp-`, etc.) left by previous crashes.
- Webhook and replication mirroring test suites (`webhook_test.go` and `sync_test.go`) covering asynchronous events.
- Automated Docker Hub publishing support in the GitHub Actions release workflow.

### Changed
- Optimized multipart upload performance by reducing lock contention, holding metadata locks only briefly during validation and updates while allowing concurrent disk writes.
- Upgraded GitHub Actions and CI workflow runner environments to use the latest versions.
- Refactored code style, improved error message clarity, and modernized `golangci-lint` configuration settings.
- Removed default credentials from Dockerfile for security hardening.
- Standardized coding guidelines in `CONTRIBUTING.md` regarding constant-time security checks, bounded logging workers, and passive map cleanups.
- Removed loopback IP address (`127.0.0.1`) from auto-generated TLS certificate SAN `DNSNames`.

### Fixed
- Deadlock and race conditions in webhook and mirror sync dispatchers by unlocking locks prior to closing queues during shutdown.
- Lock leak in metadata store initialization by ensuring lock cleanup runs via `defer` on errors.
- Flaky integration test assertions in webhook and mirror sync tests by replacing sleep-based waits with polling logic.
- Cleaned up orphaned temporary body files (`objectra-body-*`) in the `tmp` data directory during startup.
- Bucket stripe locking around database scans in `CleanExpiredMultipartUploads` to prevent race conditions during concurrent bucket deletions.
- S3 `DeleteBucketLifecycle` API handler to correctly validate bucket existence before returning `204 No Content`.
- Safely handled GET object errors to avoid potential nil pointer dereference on readers.
- Timing race conditions in the webhook and mirror sync integration tests.

### Security
- Bumped `github.com/golang-jwt/jwt/v5` from `5.2.1` to `5.2.2`.
- Passive inline garbage collection in the console rate limiter map to prune inactive clients and mitigate memory leak vulnerability.
- Constant-time comparisons (via `crypto/subtle`) for custom SSE-C customer key MD5 comparisons.

## [0.0.1] - 2026-05-27

### Added

- First implementation of Objectra.
