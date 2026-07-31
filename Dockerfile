# ================================================================
# Stiva — Multi-stage Docker Build
# ================================================================
# Stage 1: Build the Go binary
# Stage 2: Package into a minimal runtime image
# ================================================================

# --- Builder Stage ---
FROM golang:1.25-alpine AS builder

RUN apk add --no-cache git ca-certificates

WORKDIR /build

# Copy module files first for better layer caching
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build a statically-linked binary
RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags="-w -s" \
    -o stiva \
    ./cmd/stiva

# --- Runtime Stage ---
FROM alpine:3.20

RUN apk add --no-cache ca-certificates tzdata

# Create non-root user
RUN addgroup -S stiva && adduser -S stiva -G stiva

# Create data directory
RUN mkdir -p /data && chown stiva:stiva /data

WORKDIR /app

# Copy binary from builder
COPY --from=builder /build/stiva .

# Set default environment variables
ENV STIVA_DATA_DIR=/data \
    STIVA_S3_PORT=9000 \
    STIVA_CONSOLE_PORT=9001 \
    STIVA_REGION=us-east-1

# Expose ports
EXPOSE 9000 9001

# Use data directory as volume
VOLUME ["/data"]

# Run as non-root user
USER stiva

ENTRYPOINT ["./stiva"]
