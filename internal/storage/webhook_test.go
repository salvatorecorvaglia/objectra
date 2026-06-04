package storage

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestWebhook_Integration(t *testing.T) {
	var mu sync.Mutex
	var receivedPayloads []WebhookPayload

	// Create mock webhook receiver
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		var payload WebhookPayload
		if err := json.Unmarshal(body, &payload); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		mu.Lock()
		receivedPayloads = append(receivedPayloads, payload)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	// Set env
	t.Setenv("OBJECTRA_WEBHOOK_URL", server.URL)

	tempDir := t.TempDir()
	engine, err := NewFilesystemEngine(tempDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Create bucket
	err = engine.CreateBucket("webhooks")
	if err != nil {
		t.Fatalf("Failed to create bucket: %v", err)
	}

	// Put object to trigger event
	content := "webhook-test-payload"
	_, err = engine.PutObject(context.Background(), "webhooks", "test.txt", strings.NewReader(content), int64(len(content)), "text/plain")
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	// Delete object to trigger delete event
	err = engine.DeleteObject("webhooks", "test.txt", "")
	if err != nil {
		t.Fatalf("DeleteObject failed: %v", err)
	}

	// Give async worker time to deliver
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if len(receivedPayloads) < 2 {
		t.Fatalf("expected at least 2 webhook payloads, got %d", len(receivedPayloads))
	}

	putEvent := receivedPayloads[0]
	if putEvent.EventName != "s3:ObjectCreated:Put" {
		t.Errorf("expected event s3:ObjectCreated:Put, got %q", putEvent.EventName)
	}
	if putEvent.Bucket != "webhooks" || putEvent.Key != "test.txt" {
		t.Errorf("unexpected event source info: %+v", putEvent)
	}

	deleteEvent := receivedPayloads[1]
	if deleteEvent.EventName != "s3:ObjectRemoved:Delete" {
		t.Errorf("expected event s3:ObjectRemoved:Delete, got %q", deleteEvent.EventName)
	}
}
