package storage

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
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

var (
	syncQueue chan syncTask
	syncOnce  sync.Once
)

const syncQueueSize = 5000
const syncWorkerCount = 10

func initSyncDispatcher() {
	syncQueue = make(chan syncTask, syncQueueSize)
	for i := 0; i < syncWorkerCount; i++ {
		go func() {
			for task := range syncQueue {
				err := performSync(task.fs, task.cfg, task.bucket, task.key, task.op)
				if err != nil {
					log.Printf("[Sync] Mirroring %s failed for %s/%s: %v", task.op, task.bucket, task.key, err)
				} else {
					log.Printf("[Sync] Mirroring %s succeeded for %s/%s", task.op, task.bucket, task.key)
				}
			}
		}()
	}
}

// MirrorSync schedules an async mirroring operation to the backup S3 bucket.
func MirrorSync(fs *FilesystemEngine, bucket, key, op string) {
	cfg := LoadSyncConfig()
	if cfg == nil {
		return
	}

	syncOnce.Do(initSyncDispatcher)

	task := syncTask{
		fs:     fs,
		cfg:    cfg,
		bucket: bucket,
		key:    key,
		op:     op,
	}

	select {
	case syncQueue <- task:
	default:
		log.Printf("[Sync] Warning: sync queue full, dropping sync task %s for %s/%s", op, bucket, key)
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

		req, err = http.NewRequest("PUT", destURL, reader)
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

func hmacSHA256(key []byte, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
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
