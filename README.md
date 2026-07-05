# Objectra 🖼️

**Self-hosted, S3-compatible object storage with a built-in web console.**

Objectra is a high-performance object storage server written in Go that implements the Amazon S3 API. It's designed for self-hosted environments where you need S3 compatibility without the complexity of a full distributed system.

---

## ✨ Key Features

- **S3 API Compatibility**: Implements core bucket and object operations (Create, List, Delete, Check head, PUT/GET/DELETE, Multipart uploads).
- **Built-in Web Console**: A sleek, responsive dashboard served directly from the binary (default port `9001`) to browse, create, and delete buckets and objects visually.
- **Active-Passive Mirroring**: Asynchronous object replication to secondary S3 targets using AWS Signature V4 signing.
- **Event Webhooks**: Dispatch event notifications (such as `s3:ObjectCreated` and `s3:ObjectRemoved`) to external webhook endpoints with built-in retries and exponential backoff.
- **Robust Concurrency & Locking**: Granular bucket-level read/write locking to prevent race conditions without introducing bottleneck global mutexes.
- **Highly Modular Storage**: Central metadata is tracked in `objectra.db` (BoltDB), while object records are stored in dedicated per-bucket databases (`metadata/<bucket_name>.db`) to ensure isolation and performance.
- **Security-First**: Support for SSL/TLS encryption, Server-Side Encryption with Customer-provided Keys (SSE-C), persistent JWT session signing, and configurable login/API rate limits.
- **Graceful Shutdown**: Intercepts `SIGINT`/`SIGTERM` to safely write pending queues, flush logs, and close database descriptors cleanly.

---

## 🏛️ Codebase Architecture

Objectra is structured logically to separate the core storage logic from HTTP endpoints and web interfaces:

- **[`cmd/objectra/`](cmd/objectra)**: The entry point. Initializes runtime configurations, mounts databases, boots servers, and registers shutdown handlers.
- **[`internal/storage/`](internal/storage)**: The storage engine. Handles direct-to-disk layout mapping, chunked multipart uploads, metadata bookkeeping, lifecycle sweeps, replication queues, and webhook dispatching.
- **[`internal/s3api/`](internal/s3api)**: Exposes the AWS S3-compatible REST API endpoints.
- **[`internal/console/`](internal/console)**: Implements the API routes and serves the Single-Page Web Console UI.
- **[`internal/server/`](internal/server)**: Sets up the main HTTP/HTTPS server listeners, handles custom rate limiting, and configures TLS.
- **[`internal/auth/`](internal/auth)**: AWS Signature V4 verification algorithms and console credentials checking.
- **[`internal/config/`](internal/config)**: Server configuration binding and default value administration.

---

## 💻 Getting Started

### Prerequisites

- **Go 1.23 or higher** (to compile and run locally)
- **Docker** and **Docker Compose** (for containerized deployments)

### Running Locally

1. **Clone the Repository**
   ```bash
   git clone https://github.com/salvatorecorvaglia/objectra.git
   cd objectra
   ```

2. **Configure Environment Variables**
   ```bash
   cp .env.example .env
   ```
   Modify `.env` to customize access credentials, ports, and directory locations.

3. **Build and Run**
   ```bash
   # Build the binary
   go build -o objectra ./cmd/objectra

   # Run with configuration loaded
   export $(grep -v '^#' .env | xargs) && ./objectra
   ```

Objectra will boot and print the operational banner:
```text
   ____  __     _           __            
  / __ \/ /_   (_)__  _____/ /__________ _
 / / / / __ \ / / _ \/ ___/ __/ ___/ __  /
/ /_/ / /_/ // /  __/ /__/ /_/ /  / /_/ / 
\____/_.___// /\___/\___/\__/_/   \__,_/  
         /___/                             

  S3-Compatible Object Storage Server (dev)
─────────────────────────────────────────
  S3 API:     http://0.0.0.0:9000
  Console:    http://0.0.0.0:9001
  Data Dir:   ./data
  Access Key: objectra
  Region:     us-east-1
─────────────────────────────────────────
```

### Running with Docker

You can spin up Objectra immediately via Docker Compose.

```bash
docker compose up -d
```

To run manually using the CLI, build the Docker image locally first:

```bash
# Build the Docker image locally
docker build -t objectra .

# Run the container
docker run -d \
  -p 9000:9000 \
  -p 9001:9001 \
  -v $(pwd)/data:/data \
  -e OBJECTRA_ACCESS_KEY=myaccesskey \
  -e OBJECTRA_SECRET_KEY=mysecretkey \
  objectra
```

---

## ⚙️ Configuration Reference

Objectra can be configured entirely via environment variables.

| Environment Variable | Default Value | Description |
| :--- | :--- | :--- |
| **`OBJECTRA_ACCESS_KEY`** | `objectra` | Access key used for S3 clients and console login. |
| **`OBJECTRA_SECRET_KEY`** | `objectra123` | Secret key used for S3 clients and console login. |
| **`OBJECTRA_DATA_DIR`** | `/data` | Host directory to store buckets, object files, and metadata. |
| **`OBJECTRA_S3_PORT`** | `9000` | Port the S3 compatibility API will listen on. |
| **`OBJECTRA_CONSOLE_PORT`** | `9001` | Port the Admin Web Console will listen on. |
| **`OBJECTRA_REGION`** | `us-east-1` | S3 region signature validation default. |
| **`OBJECTRA_DOMAIN`** | *None* | Base domain used for virtual host S3 bucket routing (e.g. `bucket.domain.com`). |
| **`OBJECTRA_TLS_ENABLED`** | `false` | Enable to serve S3 and Console over HTTPS. |
| **`OBJECTRA_TLS_CERT`** | *None* | Path to the SSL/TLS certificate file (required if TLS is enabled). |
| **`OBJECTRA_TLS_KEY`** | *None* | Path to the SSL/TLS private key file (required if TLS is enabled). |
| **`OBJECTRA_LOGIN_RATE_LIMIT`** | `5` | Maximum console login requests allowed per minute per IP. |
| **`OBJECTRA_API_RATE_LIMIT`** | `60` | Maximum console API requests allowed per minute per IP. |
| **`OBJECTRA_TRUST_PROXY`** | `false` | Set to `true` to trust `X-Forwarded-For` headers when behind a reverse proxy (e.g. Nginx). |
| **`OBJECTRA_LOG_LEVEL`** | `info` | Output log verbosity (`debug`, `info`, `warn`, `error`). |
| **`OBJECTRA_LOG_FORMAT`** | `text` | Logger format (`text` or `json`). |
| **`OBJECTRA_JWT_SECRET`** | *None* | Explicit secret key for console JWT session tokens. (Generated randomly and stored in DB if omitted). |
| **`OBJECTRA_METRICS_TOKEN`** | *None* | Authentication token required to access the `/metrics` endpoint. If blank, falls back to Console JWT verification. |
| **`OBJECTRA_DISABLE_MIN_PART_SIZE`**| `false` | Set to `true` to disable the S3 5MB minimum multipart size limitation (useful for testing). |
| **`OBJECTRA_S3_ENDPOINT`** | *None* | Custom public S3 endpoint URL (e.g. `https://s3.example.com`) used when generating presigned object URLs in the Console. Overrides request-host based resolution. |

### 🔄 Active-Passive Replication Configuration

Setting these variables enables automated, asynchronous background replication. Whenever objects are written (`PUT`) or deleted (`DELETE`) in Objectra, the operations are queue-dispatched to the replication target.

| Environment Variable | Description |
| :--- | :--- |
| **`OBJECTRA_SYNC_ENDPOINT`** | Endpoint URL of the replication destination (e.g. `https://s3.eu-west-1.amazonaws.com`). |
| **`OBJECTRA_SYNC_BUCKET`** | Target bucket name at the replication destination. |
| **`OBJECTRA_SYNC_ACCESS_KEY`** | Access Key of the replication target credentials. |
| **`OBJECTRA_SYNC_SECRET_KEY`** | Secret Key of the replication target credentials. |
| **`OBJECTRA_SYNC_REGION`** | Region of the replication target credentials (defaults to `us-east-1`). |

### 🪝 Webhook Event Notifications Configuration

Set the variable below to dispatch event JSON payloads to a webhook listener.

| Environment Variable | Description |
| :--- | :--- |
| **`OBJECTRA_WEBHOOK_URL`** | Destination POST endpoint to receive webhook notifications (e.g., `http://my-service/events`). |

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
