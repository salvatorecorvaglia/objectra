# Objectra 🖼️

**Self-hosted, high-performance S3-compatible object storage server with web console**

**Objectra** is a high-performance object storage server written in Go that implements the Amazon S3 API. It's designed for self-hosted environments where you need S3 compatibility without the complexity of a full distributed system.

---

## ✨ Features

### 📡 S3 API Compatibility
*   **Core Bucket & Object Operations**: Full support for listing, creating, and deleting buckets, as well as putting, getting, and deleting objects.
*   **Multipart Uploads**: High-performance concurrent multipart upload support, allowing you to upload large files in chunks with optimized lock contention.
*   **Bucket Lifecycle Rules**: Automatic object expiration rules based on actual object age (`LastModified` timestamp checks).
*   **Virtual-Host Routing**: Seamless virtual-host bucket resolution support based on custom base domain suffixes.
*   **CORS (Cross-Origin Resource Sharing)**: Highly configurable CORS handlers supporting exact/wildcard domain matches with scheme and port validation.
*   **SSE-C Encryption**: Server-Side Encryption with Customer-provided keys utilizing constant-time cryptographic checks.
*   **Partial Content / Range Requests**: Seekable `GetObject` range requests supporting chunk buffering for compressed streams.

### 🖥️ Built-In Web Admin Console
*   **Stunning Dashboard UI**: Modern SPA dashboard built with vanilla HTML, CSS, and JS (zero heavy npm builds required).
*   **Bucket Management**: Create and delete buckets directly from the UI.
*   **Object Browser**: Interactive object navigation, supporting folder-like path hierarchies.
*   **Drag & Drop Uploads**: Fast, intuitive file uploading directly to your S3 storage.
*   **Rich Previews**: Instant browser previews for text, images, and sandboxed PDF files (with strict script-only iframe boundaries).
*   **Presigned Links**: Generate temporary, shareable download links for objects with custom expiry windows.

### 🔄 Replication & Event Notifications
*   **Active-Passive Mirroring**: Asynchronous replication dispatcher that automatically mirrors newly uploaded objects to a remote S3-compatible destination.
*   **Webhook Events**: Lightweight JSON payload webhook dispatcher that POSTs notifications to target endpoints on object creation or deletion.
*   **Prometheus Metrics**: High-performance `/metrics` endpoint presenting native storage metrics, secured with a custom token or Console session JWT.

### 🛡️ Production Hardening & Reliability
*   **Structured Logging**: Production-grade JSON or text structured logs via Go's native `log/slog`.
*   **Access Log Buffering**: High-throughput access log worker that batches disk writes (up to 100 entries or every 5s) to reduce lock contention and I/O.
*   **Graceful Shutdown**: Monitors shutdown signals (`SIGINT`/`SIGTERM`) and tracks active requests using a `sync.WaitGroup` to ensure no connection is dropped mid-flight.
*   **Orphan Cleanups**: Automated startup sweep that identifies and deletes partial multipart/temporary files left over by unexpected system crashes.

---

## 🚀 Getting Started

### Prerequisites
- **Go 1.25 or higher** (to compile and run locally)
- **Docker** and **Docker Compose** (for containerized deployments)

---

### Method 1: Using Docker Compose (Recommended)

Objectra is distributed as a multi-arch container image. You can spin up Objectra with a persistent volume using `docker-compose.yml`:

1. Copy the environment variables:
   ```bash
   cp .env.example .env
   ```
2. Run Docker Compose:
   ```bash
   docker compose up -d
   ```
3. Access the services:
   *   **S3 API**: `http://localhost:9000`
   *   **Web Console**: `http://localhost:9001` (Default login credentials: Access Key: `objectra` / Secret Key: `objectra123`)

---

### Method 2: Running with Docker CLI

Alternatively, run the official pre-built image from GitHub Container Registry (GHCR):

```bash
docker run -d \
  -p 9000:9000 \
  -p 9001:9001 \
  -v $(pwd)/data:/data \
  -e OBJECTRA_ACCESS_KEY=objectra \
  -e OBJECTRA_SECRET_KEY=objectra123 \
  ghcr.io/salvatorecorvaglia/objectra:latest
```

To build and run the image locally instead:

```bash
# Build the Docker image locally
docker build -t objectra .

# Run the local container
docker run -d \
  -p 9000:9000 \
  -p 9001:9001 \
  -v $(pwd)/data:/data \
  -e OBJECTRA_ACCESS_KEY=objectra \
  -e OBJECTRA_SECRET_KEY=objectra123 \
  objectra
```

---

### Method 3: Building from Source

1. Clone the repository:
   ```bash
   git clone https://github.com/salvatorecorvaglia/objectra.git
   cd objectra
   ```
2. Configure your environment variables:
   ```bash
   cp .env.example .env
   ```
3. Build the binary:
   ```bash
   go build -ldflags="-w -s" -o objectra ./cmd/objectra
   ```
4. Run Objectra with environment configuration loaded:
   ```bash
   export $(grep -v '^#' .env | xargs) && ./objectra
   ```

---

## ⚙️ Configuration Reference

Objectra is configured exclusively via environment variables. You can find a baseline configuration file template in [.env.example](.env.example).

| Environment Variable | Default Value | Description |
| :--- | :--- | :--- |
| **`OBJECTRA_ACCESS_KEY`** | `objectra` | Access key used for S3 client credentials and Web Console login. |
| **`OBJECTRA_SECRET_KEY`** | `objectra123` | Secret key used for S3 client credentials and Web Console login. |
| **`OBJECTRA_DATA_DIR`** | `/data` (Docker) / `./data` (source) | Path to the directory where metadata and S3 objects are stored. |
| **`OBJECTRA_S3_PORT`** | `9000` | Port the S3-compatible API server listens on. |
| **`OBJECTRA_CONSOLE_PORT`**| `9001` | Port the Admin Web Console listens on. |
| **`OBJECTRA_REGION`** | `us-east-1` | S3 region reported by the API. |
| **`OBJECTRA_DOMAIN`** | *None* | Base domain for virtual-host style bucket requests (e.g., `mybucket.domain.com`). |
| **`OBJECTRA_S3_ENDPOINT`** | *None* | Custom public S3 endpoint URL for generating presigned links in the console. |
| **`OBJECTRA_TLS_ENABLED`** | `false` | Set to `true` to enable TLS/HTTPS for S3 API and Console. |
| **`OBJECTRA_TLS_CERT`** | *None* | Absolute path to SSL/TLS certificate file. |
| **`OBJECTRA_TLS_KEY`** | *None* | Absolute path to SSL/TLS private key file. |
| **`OBJECTRA_JWT_SECRET`** | *Autogenerated* | Secret key for Console session signing. Recommended to set statically to preserve sessions. |
| **`OBJECTRA_TRUST_PROXY`** | `false` | Trusts `X-Forwarded-For` proxy headers from reverse proxies (Nginx/Caddy/Cloudflare). |
| **`OBJECTRA_LOGIN_RATE_LIMIT`**| `5` | Maximum console login attempts allowed per minute per IP address. |
| **`OBJECTRA_API_RATE_LIMIT`**  | `60` | Maximum console API requests allowed per minute per IP address. |
| **`OBJECTRA_METRICS_TOKEN`** | *None* | Bearer token required to scrape `/metrics`. If empty, requires a console JWT session. |
| **`OBJECTRA_LOG_LEVEL`** | `info` | Logging verbosity level (`debug`, `info`, `warn`, `error`). |
| **`OBJECTRA_LOG_FORMAT`** | `text` | Log presentation format (`text` or `json`). |
| **`OBJECTRA_SYNC_ENDPOINT`** | *None* | Remote S3 API endpoint URL target for asynchronous replication. |
| **`OBJECTRA_SYNC_BUCKET`** | *None* | Remote target bucket name for asynchronous replication. |
| **`OBJECTRA_SYNC_ACCESS_KEY`**| *None* | Remote credentials Access Key. |
| **`OBJECTRA_SYNC_SECRET_KEY`**| *None* | Remote credentials Secret Key. |
| **`OBJECTRA_SYNC_REGION`** | `us-east-1` | Remote target region. |
| **`OBJECTRA_WEBHOOK_URL`** | *None* | Destination HTTP POST endpoint URL to receive webhook event payloads. |
| **`OBJECTRA_DISABLE_MIN_PART_SIZE`**| `false` | Disables S3 5MB minimum multipart part size requirement (highly useful for dev/testing). |

### 🔄 Active-Passive Replication Configuration

Setting these variables enables automated, asynchronous background replication. Whenever objects are written (`PUT`) or deleted (`DELETE`) in Objectra, the operations are queue-dispatched to the replication target.

### 🪝 Webhook Event Notifications Configuration

Set the `OBJECTRA_WEBHOOK_URL` variable to dispatch event JSON payloads to a webhook listener.

#### Example Webhook Event Payload:
```json
{
  "eventName": "s3:ObjectCreated",
  "bucket": "assets",
  "key": "images/photo.png",
  "size": 24590,
  "etag": "a3b2c1...",
  "versionId": "b18ca72c-...",
  "time": "2026-06-11T15:43:00Z"
}
```

---

## 🛠️ Interacting with S3 Clients

You can use standard S3-compatible tools to interact with Objectra.

### AWS CLI

Configure the AWS CLI profile or override the endpoint directly:

```bash
# Set credentials environment
export AWS_ACCESS_KEY_ID=objectra
export AWS_SECRET_ACCESS_KEY=objectra123

# Create a bucket
aws --endpoint-url http://localhost:9000 s3 mb s3://test-bucket

# Upload a file
aws --endpoint-url http://localhost:9000 s3 cp myfile.txt s3://test-bucket/

# List files
aws --endpoint-url http://localhost:9000 s3 ls s3://test-bucket/
```

### rclone

Add a remote section to your `rclone.conf`:

```ini
[objectra]
type = s3
provider = Other
env_auth = false
access_key_id = objectra
secret_access_key = objectra123
endpoint = http://localhost:9000
```

And run:
```bash
rclone lsd objectra:
```

---

## 🧪 Running Tests

To run the unit and integration tests:

```bash
# Run all tests
go test -v ./...

# Run tests with race detection
go test -race ./...
```

---

## 🤝 Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## 🔐 Security

If you discover a security vulnerability, please see our [Security Policy](SECURITY.md).

## 📝 License

Distributed under the MIT License. See [LICENSE](LICENSE) for more information.

---

**Author**: [Salvatore Corvaglia](https://github.com/salvatorecorvaglia)