package storage

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"
)

// WebhookPayload represents the event payload sent to the configured webhook URL.
type WebhookPayload struct {
	EventName string    `json:"eventName"`
	Bucket    string    `json:"bucket"`
	Key       string    `json:"key"`
	Size      int64     `json:"size,omitempty"`
	ETag      string    `json:"etag,omitempty"`
	VersionID string    `json:"versionId,omitempty"`
	Time      time.Time `json:"time"`
}

var webhookClient = &http.Client{
	Timeout: 5 * time.Second,
}

type webhookTask struct {
	url     string
	payload WebhookPayload
}

// Globals deleted; moved to FilesystemEngine struct.

const webhookQueueSize = 5000
const webhookWorkerCount = 10

func (fs *FilesystemEngine) initWebhookDispatcher() {
	fs.webhookQueue = make(chan webhookTask, webhookQueueSize)
	for i := 0; i < webhookWorkerCount; i++ {
		fs.webhookWG.Add(1)
		go func() {
			defer fs.webhookWG.Done()
			for task := range fs.webhookQueue {
				sendWebhookEvent(task.url, task.payload)
			}
		}()
	}
}

// StopWebhookDispatcher halts webhook processing, flushes remaining notifications, and waits for workers to terminate.
func (fs *FilesystemEngine) StopWebhookDispatcher() {
	fs.webhookMu.Lock()
	if atomic.SwapInt32(&fs.isWebhookShuttingDown, 1) == 1 {
		fs.webhookMu.Unlock()
		return
	}

	q := fs.webhookQueue
	fs.webhookMu.Unlock()

	if q != nil {
		close(q)
		fs.webhookWG.Wait()
	}
}

// triggerWebhook parses the configuration and sends the event asynchronously.
func (fs *FilesystemEngine) triggerWebhook(eventName string, info *ObjectInfo) {
	if fs.webhookURL == "" {
		return
	}

	fs.webhookMu.Lock()
	defer fs.webhookMu.Unlock()

	if atomic.LoadInt32(&fs.isWebhookShuttingDown) == 1 {
		slog.Warn("[Webhook] Webhook dispatcher shutting down, ignoring event", "event", eventName, "bucket", info.Bucket, "key", info.Key)
		return
	}

	fs.webhookOnce.Do(func() {
		fs.initWebhookDispatcher()
	})

	payload := WebhookPayload{
		EventName: "s3:" + eventName,
		Bucket:    info.Bucket,
		Key:       info.Key,
		Size:      info.Size,
		ETag:      info.ETag,
		VersionID: info.VersionID,
		Time:      time.Now().UTC(),
	}

	task := webhookTask{
		url:     fs.webhookURL,
		payload: payload,
	}

	select {
	case fs.webhookQueue <- task:
	default:
		slog.Warn("[Webhook] Webhook queue full, dropping event", "event", eventName, "bucket", info.Bucket, "key", info.Key)
	}
}

func sendWebhookEvent(url string, payload WebhookPayload) {
	data, err := json.Marshal(payload)
	if err != nil {
		slog.Error("[Webhook] Failed to marshal payload", "error", err)
		return
	}


	backoff := 1 * time.Second
	maxRetries := 3

	for attempt := 0; attempt <= maxRetries; attempt++ {
		req, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(data))
		if err != nil {
			slog.Error("[Webhook] Failed to create request", "error", err)
			return
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("User-Agent", "Stiva-Webhook-Dispatcher")

		resp, err := webhookClient.Do(req)
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				return // Success!
			}
			slog.Warn("[Webhook] Server returned error status", "status", resp.StatusCode, "attempt", attempt+1)
		} else {
			slog.Warn("[Webhook] Dispatch failed", "attempt", attempt+1, "error", err)
		}

		if attempt < maxRetries {
			time.Sleep(backoff)
			backoff *= 2
		}
	}

	slog.Error("[Webhook] Failed to deliver event after retries", "event", payload.EventName, "retries", maxRetries)
}
