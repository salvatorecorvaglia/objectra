# ================================================================
# Objectra — Multi-stage Docker Build
# ================================================================
# Stage 1: Build the Go binary
# Stage 2: Package into a minimal runtime image
# ================================================================

# --- Builder Stage ---
FROM golang:1.23-alpine AS builder

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
    -o objectra \
    ./cmd/objectra

# --- Runtime Stage ---
FROM alpine:3.20

RUN apk add --no-cache ca-certificates tzdata

# Create non-root user
RUN addgroup -S objectra && adduser -S objectra -G objectra

# Create data directory
RUN mkdir -p /data && chown objectra:objectra /data

WORKDIR /app

# Copy binary from builder
COPY --from=builder /build/objectra .

# Set default environment variables
ENV OBJECTRA_ACCESS_KEY=objectra \
    OBJECTRA_SECRET_KEY=objectra123 \
    OBJECTRA_DATA_DIR=/data \
    OBJECTRA_S3_PORT=9000 \
    OBJECTRA_CONSOLE_PORT=9001 \
    OBJECTRA_REGION=us-east-1

# Expose ports
EXPOSE 9000 9001

# Use data directory as volume
VOLUME ["/data"]

# Run as non-root user
USER objectra

ENTRYPOINT ["./objectra"]
