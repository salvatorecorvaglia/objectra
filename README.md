# Objectra

**Self-hosted, S3-compatible object storage with a built-in web console.**

Objectra is a high-performance object storage server written in Go that implements the Amazon S3 API. It's designed for self-hosted environments where you need S3 compatibility without the complexity of a full distributed system.

## Features

- **S3-Compatible API** — Works with existing S3 tools (`aws` CLI, Boto3, any S3 SDK)
- **Built-in Web Console** — Browse buckets and objects, upload/download files, create and delete resources
- **Multipart Upload** — Upload large files efficiently with chunked multipart uploads
- **AWS Signature V4** — Standard S3 authentication for secure access
- **Docker Ready** — Multi-stage build produces a ~15MB image
- **Streaming I/O** — Objects are streamed directly to/from disk without buffering in memory

## Quick Start

### Docker Compose (Recommended)

```bash
git clone https://github.com/salvatorecorvaglia/objectra.git
cd objectra
docker compose up -d
```

The server will start with:
- **S3 API**: `http://localhost:9000`
- **Web Console**: `http://localhost:9001`
- **Default credentials**: Access Key `objectra` / Secret Key `objectra123`

### Docker Build

```bash
docker build -t objectra:latest .
docker run -d \
  --name objectra \
  -p 9000:9000 \
  -p 9001:9001 \
  -v $(pwd)/data:/data \
  objectra:latest
```

### Build from Source

Requires Go 1.23+.

```bash
go build -o objectra ./cmd/objectra
OBJECTRA_DATA_DIR=./data ./objectra
```

## Configuration

All settings are configured via environment variables.

### Environment File Template

A template configuration file [env.example](env.example) is provided in the repository. To configure the server using this template, copy it to `.env` in the root directory:

```bash
cp env.example .env
```

To run the server locally from source using these variables, you can load them in your shell:

```bash
export $(grep -v '^#' .env | xargs) && ./objectra
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `OBJECTRA_ACCESS_KEY` | `objectra` | S3 access key |
| `OBJECTRA_SECRET_KEY` | `objectra123` | S3 secret key |
| `OBJECTRA_DATA_DIR` | `/data` | Storage directory |
| `OBJECTRA_S3_PORT` | `9000` | S3 API port |
| `OBJECTRA_CONSOLE_PORT` | `9001` | Web console port |
| `OBJECTRA_REGION` | `us-east-1` | Reported S3 region |
| `OBJECTRA_JWT_SECRET` | *random* | JWT signing key used for persistent console sessions |

> **⚠️ Change the default credentials before using in production!**

## Usage

### AWS CLI

```bash
# Configure
aws configure set aws_access_key_id objectra
aws configure set aws_secret_access_key objectra123

# Create a bucket
aws --endpoint-url http://localhost:9000 s3 mb s3://my-bucket

# Upload a file
aws --endpoint-url http://localhost:9000 s3 cp myfile.txt s3://my-bucket/myfile.txt

# List objects
aws --endpoint-url http://localhost:9000 s3 ls s3://my-bucket/

# Download a file
aws --endpoint-url http://localhost:9000 s3 cp s3://my-bucket/myfile.txt ./downloaded.txt

# Delete a file
aws --endpoint-url http://localhost:9000 s3 rm s3://my-bucket/myfile.txt

# Delete a bucket
aws --endpoint-url http://localhost:9000 s3 rb s3://my-bucket
```

### Python (Boto3)

```python
import boto3

s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id='objectra',
    aws_secret_access_key='objectra123',
    region_name='us-east-1',
)

# Create bucket
s3.create_bucket(Bucket='my-bucket')

# Upload
s3.upload_file('local-file.txt', 'my-bucket', 'remote-file.txt')

# List
response = s3.list_objects_v2(Bucket='my-bucket')
for obj in response.get('Contents', []):
    print(obj['Key'], obj['Size'])
```

### Web Console

Open `http://localhost:9001` in your browser and log in with your access key and secret key.

## S3 API Coverage

| Operation | Status |
|-----------|--------|
| ListBuckets | ✅ |
| CreateBucket | ✅ |
| DeleteBucket | ✅ |
| HeadBucket | ✅ |
| GetBucketLocation | ✅ |
| PutObject | ✅ |
| GetObject | ✅ |
| HeadObject | ✅ |
| DeleteObject | ✅ |
| CopyObject | ✅ |
| ListObjectsV2 | ✅ |
| CreateMultipartUpload | ✅ |
| UploadPart | ✅ |
| CompleteMultipartUpload | ✅ |
| AbortMultipartUpload | ✅ |

## Architecture

```
objectra/
├── cmd/objectra/         # Entry point
├── internal/
│   ├── auth/             # AWS Signature V4 verification
│   ├── config/           # Environment-based configuration
│   ├── console/          # Web console (handlers + embedded frontend)
│   ├── s3api/            # S3 API handlers
│   ├── server/           # HTTP server orchestration
│   └── storage/          # Storage engine (filesystem + bbolt metadata)
├── Dockerfile
├── docker-compose.yml
└── README.md
```

## License

MIT
