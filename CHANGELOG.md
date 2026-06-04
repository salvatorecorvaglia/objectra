# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [0.1.0] - 2026-06-04

### Added
- Bounded access log worker pool inside the S3 API Router to queue logs and prevent goroutine explosion under load.
- `OBJECTRA_TRUST_PROXY` environment variable option to respect proxy headers (like `X-Forwarded-For`) for rate limiting console access.
- Startup security warning if default S3 credentials (`objectra` / `objectra123`) are detected.
- Storage engine startup sweep that cleans orphaned temporary/multipart files (`.objectra-tmp-`, etc.) left by previous crashes.
- Webhook and replication mirroring test suites (`webhook_test.go` and `sync_test.go`) covering asynchronous events.

### Changed
- Standardized coding guidelines in `CONTRIBUTING.md` regarding constant-time security checks, bounded logging workers, and passive map cleanups.
- Removed loopback IP address (`127.0.0.1`) from auto-generated TLS certificate SAN `DNSNames`.

### Fixed
- Bucket stripe locking around database scans in `CleanExpiredMultipartUploads` to prevent race conditions during concurrent bucket deletions.
- S3 `DeleteBucketLifecycle` API handler to correctly validate bucket existence before returning `204 No Content`.
- Safely handled GET object errors to avoid potential nil pointer dereference on readers.
- Timing race conditions in the webhook and mirror sync integration tests.

### Security
- Passive inline garbage collection in the console rate limiter map to prune inactive clients and mitigate memory leak vulnerability.
- Constant-time comparisons (via `crypto/subtle`) for custom SSE-C customer key MD5 comparisons.

## [0.0.1] - 2026-05-27

### Added

- First implementation of Objectra.
