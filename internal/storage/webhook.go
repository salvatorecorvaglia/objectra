package storage

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"os"
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

// triggerWebhook parses the configuration and sends the event asynchronously.
func triggerWebhook(eventName string, info *ObjectInfo) {
	url := os.Getenv("OBJECTRA_WEBHOOK_URL")
	if url == "" {
		return
	}

	payload := WebhookPayload{
		EventName: eventName,
		Bucket:    info.Bucket,
		Key:       info.Key,
		Size:      info.Size,
		ETag:      info.ETag,
		VersionID: info.VersionID,
		Time:      time.Now().UTC(),
	}

	go sendWebhookEvent(url, payload)
}

func sendWebhookEvent(url string, payload WebhookPayload) {
	data, err := json.Marshal(payload)
	if err != nil {
		log.Printf("[Webhook] Failed to marshal payload: %v", err)
		return
	}

	client := &http.Client{
		Timeout: 5 * time.Second,
	}

	backoff := 1 * time.Second
	maxRetries := 3

	for attempt := 0; attempt <= maxRetries; attempt++ {
		req, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(data))
		if err != nil {
			log.Printf("[Webhook] Failed to create request: %v", err)
			return
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("User-Agent", "Objectra-Webhook-Dispatcher")

		resp, err := client.Do(req)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				return // Success!
			}
			log.Printf("[Webhook] Server returned status %d on attempt %d", resp.StatusCode, attempt+1)
		} else {
			log.Printf("[Webhook] Dispatch failed on attempt %d: %v", attempt+1, err)
		}

		if attempt < maxRetries {
			time.Sleep(backoff)
			backoff *= 2
		}
	}

	log.Printf("[Webhook] Failed to deliver event %s after %d retries", payload.EventName, maxRetries)
}
