package storage

import (
	"bufio"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// SyncConfig holds S3 replication target credentials and endpoint.
type SyncConfig struct {
	Endpoint  string
	Bucket    string
	AccessKey string
	SecretKey string
	Region    string
}

// LoadSyncConfig loads sync configuration from environment variables.
func LoadSyncConfig() *SyncConfig {
	endpoint := os.Getenv("OBJECTRA_SYNC_ENDPOINT")
	if endpoint == "" {
		return nil
	}
	bucket := os.Getenv("OBJECTRA_SYNC_BUCKET")
	accessKey := os.Getenv("OBJECTRA_SYNC_ACCESS_KEY")
	secretKey := os.Getenv("OBJECTRA_SYNC_SECRET_KEY")
	region := os.Getenv("OBJECTRA_SYNC_REGION")
	if region == "" {
		region = "us-east-1"
	}
	return &SyncConfig{
		Endpoint:  strings.TrimSuffix(endpoint, "/"),
		Bucket:    bucket,
		AccessKey: accessKey,
		SecretKey: secretKey,
		Region:    region,
	}
}

type syncTask struct {
	fs     *FilesystemEngine
	cfg    *SyncConfig
	bucket string
	key    string
	op     string
}

// Globals deleted; moved to FilesystemEngine struct.

const syncQueueSize = 5000
const syncWorkerCount = 10

func (fs *FilesystemEngine) initSyncDispatcher() {
	fs.syncQueue = make(chan syncTask, syncQueueSize)
	for i := 0; i < syncWorkerCount; i++ {
		fs.syncWG.Add(1)
		go func() {
			defer fs.syncWG.Done()
			for task := range fs.syncQueue {
				err := performSync(task.fs, task.cfg, task.bucket, task.key, task.op)
				if err != nil {
					slog.Error("[Sync] Mirroring failed", "op", task.op, "bucket", task.bucket, "key", task.key, "error", err)
				} else {
					slog.Info("[Sync] Mirroring succeeded", "op", task.op, "bucket", task.bucket, "key", task.key)
				}
			}
		}()
	}
}

// StopSyncDispatcher halts replication queue processing, waits for pending transfers, and shuts down workers.
func (fs *FilesystemEngine) StopSyncDispatcher() {
	fs.syncMu.Lock()
	if atomic.SwapInt32(&fs.isSyncShuttingDown, 1) == 1 {
		fs.syncMu.Unlock()
		return
	}

	q := fs.syncQueue
	fs.syncMu.Unlock()

	if q != nil {
		close(q)
		fs.syncWG.Wait()
	}
}

// MirrorSync schedules an async mirroring operation to the backup S3 bucket.
func (fs *FilesystemEngine) MirrorSync(bucket, key, op string) {
	if fs.syncConfig == nil {
		return
	}

	fs.syncMu.Lock()
	defer fs.syncMu.Unlock()

	if atomic.LoadInt32(&fs.isSyncShuttingDown) == 1 {
		slog.Warn("[Sync] Sync dispatcher shutting down, ignoring replication request", "op", op, "bucket", bucket, "key", key)
		return
	}

	fs.syncOnce.Do(func() {
		fs.initSyncDispatcher()
	})

	task := syncTask{
		fs:     fs,
		cfg:    fs.syncConfig,
		bucket: bucket,
		key:    key,
		op:     op,
	}

	select {
	case fs.syncQueue <- task:
	default:
		slog.Warn("[Sync] Sync queue full, dropping sync task", "op", op, "bucket", bucket, "key", key)
	}
}

func performSync(fs *FilesystemEngine, cfg *SyncConfig, bucket, key, op string) error {
	// Construct the destination URL
	// Endpoint is e.g. http://localhost:9002
	destURL := fmt.Sprintf("%s/%s/%s", cfg.Endpoint, cfg.Bucket, strings.TrimPrefix(key, "/"))

	var req *http.Request
	var err error

	switch op {
	case "DELETE":
		req, err = http.NewRequest("DELETE", destURL, nil)
		if err != nil {
			return err
		}
	case "PUT":
		info, err := fs.metadata.GetObjectMeta(bucket, key, "")
		if err != nil {
			return fmt.Errorf("failed to fetch object metadata: %w", err)
		}

		reader, _, err := fs.GetObject(context.Background(), bucket, key, "")
		if err != nil {
			return fmt.Errorf("failed to get object reader: %w", err)
		}
		defer reader.Close()

		bufferedReader := &bufferedReadCloser{
			Reader: bufio.NewReaderSize(reader, 64*1024),
			Closer: reader,
		}

		req, err = http.NewRequest("PUT", destURL, bufferedReader)
		if err != nil {
			return err
		}
		req.ContentLength = info.Size
		req.Header.Set("Content-Length", strconv.FormatInt(info.Size, 10))
		if info.ContentType != "" {
			req.Header.Set("Content-Type", info.ContentType)
		}
	default:
		return fmt.Errorf("unsupported sync operation: %s", op)
	}

	// Sign request
	signRequest(req, cfg.AccessKey, cfg.SecretKey, cfg.Region, "s3")

	client := &http.Client{
		Timeout: 5 * time.Minute,
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("HTTP error %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

type bufferedReadCloser struct {
	*bufio.Reader
	io.Closer
}

func hmacSHA256(key []byte, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	_, _ = h.Write(data)
	return h.Sum(nil)
}

func signRequest(req *http.Request, accessKey, secretKey, region, service string) {
	t := time.Now().UTC()
	dateBasic := t.Format("20060102T150405Z")
	dateDay := t.Format("20060102")

	req.Header.Set("X-Amz-Date", dateBasic)

	// Set Host header
	host := req.URL.Host
	req.Header.Set("Host", host)

	// For S3 compatibility with large stream requests, we sign with UNSIGNED-PAYLOAD.
	req.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
	hashedPayload := "UNSIGNED-PAYLOAD"

	// Canonical headers to sign
	headers := []string{"host", "x-amz-date", "x-amz-content-sha256"}
	if req.Header.Get("Content-Type") != "" {
		headers = append(headers, "content-type")
	}
	sort.Strings(headers)

	var canonicalHeaders strings.Builder
	for _, h := range headers {
		canonicalHeaders.WriteString(h)
		canonicalHeaders.WriteString(":")
		canonicalHeaders.WriteString(strings.TrimSpace(req.Header.Get(h)))
		canonicalHeaders.WriteString("\n")
	}

	signedHeaders := strings.Join(headers, ";")

	// Escaping path correctly
	escapedPath := req.URL.EscapedPath()
	if escapedPath == "" {
		escapedPath = "/"
	}

	canonicalQuery := req.URL.Query().Encode()

	// Canonical Request
	canonicalRequest := fmt.Sprintf("%s\n%s\n%s\n%s\n%s\n%s",
		req.Method,
		escapedPath,
		canonicalQuery,
		canonicalHeaders.String(),
		signedHeaders,
		hashedPayload,
	)

	hReq := sha256.Sum256([]byte(canonicalRequest))
	hashedCanonicalRequest := hex.EncodeToString(hReq[:])

	// Credential Scope
	credentialScope := fmt.Sprintf("%s/%s/%s/aws4_request", dateDay, region, service)

	// String to Sign
	stringToSign := fmt.Sprintf("AWS4-HMAC-SHA256\n%s\n%s\n%s",
		dateBasic,
		credentialScope,
		hashedCanonicalRequest,
	)

	// Deriving Key
	kDate := hmacSHA256([]byte("AWS4"+secretKey), []byte(dateDay))
	kRegion := hmacSHA256(kDate, []byte(region))
	kService := hmacSHA256(kRegion, []byte(service))
	kSigning := hmacSHA256(kService, []byte("aws4_request"))

	// Signature
	signature := hex.EncodeToString(hmacSHA256(kSigning, []byte(stringToSign)))

	// Auth Header
	authHeader := fmt.Sprintf("AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		accessKey,
		credentialScope,
		signedHeaders,
		signature,
	)

	req.Header.Set("Authorization", authHeader)
}
