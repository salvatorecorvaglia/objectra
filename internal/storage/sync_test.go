package storage

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestMirrorSync_Integration(t *testing.T) {
	var mu sync.Mutex
	type reqRecord struct {
		Method string
		URI    string
		Auth   string
		Body   string
	}
	var receivedReqs []reqRecord

	// Mock backup target S3 service
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bodyBytes, _ := io.ReadAll(r.Body)
		mu.Lock()
		receivedReqs = append(receivedReqs, reqRecord{
			Method: r.Method,
			URI:    r.URL.Path,
			Auth:   r.Header.Get("Authorization"),
			Body:   string(bodyBytes),
		})
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	// Set env config for sync replication
	t.Setenv("OBJECTRA_SYNC_ENDPOINT", server.URL)
	t.Setenv("OBJECTRA_SYNC_BUCKET", "backup-bucket")
	t.Setenv("OBJECTRA_SYNC_ACCESS_KEY", "backupaccess")
	t.Setenv("OBJECTRA_SYNC_SECRET_KEY", "backupsecret")
	t.Setenv("OBJECTRA_SYNC_REGION", "us-west-2")

	tempDir := t.TempDir()
	engine, err := NewFilesystemEngine(tempDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Create bucket
	err = engine.CreateBucket("primary-bucket")
	if err != nil {
		t.Fatalf("Failed to create bucket: %v", err)
	}

	// 1. Trigger mirroring via PutObject
	content := "mirror-replication-payload"
	_, err = engine.PutObject(context.Background(), "primary-bucket", "docs/report.txt", strings.NewReader(content), int64(len(content)), "text/plain")
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	// Wait for the PUT request to be received before deleting, to ensure the async worker
	// has read the file for mirroring before it gets deleted from primary storage.
	deadline := time.Now().Add(5 * time.Second)
	for {
		mu.Lock()
		count := len(receivedReqs)
		mu.Unlock()
		if count >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for PUT mirroring request")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// 2. Trigger mirroring via DeleteObject
	err = engine.DeleteObject("primary-bucket", "docs/report.txt", "")
	if err != nil {
		t.Fatalf("DeleteObject failed: %v", err)
	}

	// Wait for async mirroring worker to complete the DELETE request
	deadline = time.Now().Add(5 * time.Second)
	for {
		mu.Lock()
		count := len(receivedReqs)
		mu.Unlock()
		if count >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for DELETE mirroring request")
		}
		time.Sleep(10 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()

	if len(receivedReqs) < 2 {
		t.Fatalf("expected at least 2 requests, got %d", len(receivedReqs))
	}

	putReq := receivedReqs[0]
	if putReq.Method != "PUT" {
		t.Errorf("expected PUT method, got %q", putReq.Method)
	}
	if putReq.URI != "/backup-bucket/docs/report.txt" {
		t.Errorf("expected destination URI '/backup-bucket/docs/report.txt', got %q", putReq.URI)
	}
	if putReq.Body != content {
		t.Errorf("expected body %q, got %q", content, putReq.Body)
	}
	if !strings.HasPrefix(putReq.Auth, "AWS4-HMAC-SHA256") {
		t.Errorf("expected AWS SigV4 authorization header, got %q", putReq.Auth)
	}

	deleteReq := receivedReqs[1]
	if deleteReq.Method != "DELETE" {
		t.Errorf("expected DELETE method, got %q", deleteReq.Method)
	}
	if deleteReq.URI != "/backup-bucket/docs/report.txt" {
		t.Errorf("expected destination URI '/backup-bucket/docs/report.txt', got %q", deleteReq.URI)
	}
}
