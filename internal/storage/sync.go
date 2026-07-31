package storage

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/salvatorecorvaglia/stiva/internal/auth"
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
	endpoint := os.Getenv("STIVA_SYNC_ENDPOINT")
	if endpoint == "" {
		return nil
	}
	bucket := os.Getenv("STIVA_SYNC_BUCKET")
	accessKey := os.Getenv("STIVA_SYNC_ACCESS_KEY")
	secretKey := os.Getenv("STIVA_SYNC_SECRET_KEY")
	region := os.Getenv("STIVA_SYNC_REGION")
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
				err := performSync(task.fs, task.fs.syncClient, task.cfg, task.bucket, task.key, task.op)
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

func performSync(fs *FilesystemEngine, client *http.Client, cfg *SyncConfig, bucket, key, op string) error {
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
	auth.SignRequest(req, cfg.AccessKey, cfg.SecretKey, cfg.Region, "s3")

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
