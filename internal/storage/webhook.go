package storage

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"sync"
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

var (
	webhookQueue chan webhookTask
	webhookOnce  sync.Once
)

const webhookQueueSize = 5000
const webhookWorkerCount = 10

func initWebhookDispatcher() {
	webhookQueue = make(chan webhookTask, webhookQueueSize)
	for i := 0; i < webhookWorkerCount; i++ {
		go func() {
			for task := range webhookQueue {
				sendWebhookEvent(task.url, task.payload)
			}
		}()
	}
}

// triggerWebhook parses the configuration and sends the event asynchronously.
func triggerWebhook(eventName string, info *ObjectInfo) {
	url := os.Getenv("OBJECTRA_WEBHOOK_URL")
	if url == "" {
		return
	}

	webhookOnce.Do(initWebhookDispatcher)

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
		url:     url,
		payload: payload,
	}

	select {
	case webhookQueue <- task:
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
		req.Header.Set("User-Agent", "Objectra-Webhook-Dispatcher")

		resp, err := webhookClient.Do(req)
		if err == nil {
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
