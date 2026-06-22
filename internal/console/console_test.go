package console

import (
	"bytes"
	"encoding/json"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/salvatorecorvaglia/objectra/internal/auth"
	"github.com/salvatorecorvaglia/objectra/internal/storage"
)

func TestConsoleEndpoints(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "console-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	engine, err := storage.NewFilesystemEngine(tempDir, nil, "")
	if err != nil {
		t.Fatalf("failed to initialize storage engine: %v", err)
	}
	defer engine.Close()

	creds := auth.NewCredentials("access", "secret")
	handler := NewHandler(engine, creds, 9000, "us-east-1", "", 100, 1000)

	err = engine.CreateBucket("test-bucket")
	if err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}

	token, err := GenerateToken("access")
	if err != nil {
		t.Fatalf("failed to generate token: %v", err)
	}

	// 1. Test Multipart Initiate
	initiateBody, _ := json.Marshal(map[string]string{
		"key":         "large-file.bin",
		"contentType": "application/octet-stream",
	})
	req := httptest.NewRequest("POST", "/api/buckets/test-bucket/multipart/initiate", bytes.NewReader(initiateBody))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d, body: %s", w.Code, w.Body.String())
	}

	var initResp struct {
		UploadID string `json:"uploadId"`
		Key      string `json:"key"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &initResp); err != nil {
		t.Fatalf("failed to parse initiate response: %v", err)
	}

	if initResp.UploadID == "" || initResp.Key != "large-file.bin" {
		t.Errorf("invalid initiate response: %+v", initResp)
	}

	// 2. Test Multipart Upload Part
	bodyBuf := &bytes.Buffer{}
	writer := multipart.NewWriter(bodyBuf)
	writer.WriteField("uploadId", initResp.UploadID)
	writer.WriteField("key", "large-file.bin")
	writer.WriteField("partNumber", "1")
	partWriter, _ := writer.CreateFormFile("file", "part1.bin")
	partWriter.Write([]byte("chunk-data-1"))
	writer.Close()

	req = httptest.NewRequest("POST", "/api/buckets/test-bucket/multipart/upload-part", bodyBuf)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d, body: %s", w.Code, w.Body.String())
	}

	var partResp struct {
		ETag       string `json:"etag"`
		PartNumber int    `json:"partNumber"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &partResp); err != nil {
		t.Fatalf("failed to parse upload-part response: %v", err)
	}

	if partResp.PartNumber != 1 || partResp.ETag == "" {
		t.Errorf("invalid part response: %+v", partResp)
	}

	// 3. Test Multipart Complete
	completeBody, _ := json.Marshal(map[string]interface{}{
		"uploadId": initResp.UploadID,
		"key":      "large-file.bin",
		"parts": []map[string]interface{}{
			{"partNumber": 1, "etag": partResp.ETag},
		},
	})

	req = httptest.NewRequest("POST", "/api/buckets/test-bucket/multipart/complete", bytes.NewReader(completeBody))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d, body: %s", w.Code, w.Body.String())
	}

	// 4. Test List Objects with pagination and search
	req = httptest.NewRequest("GET", "/api/buckets/test-bucket/objects?prefix=&delimiter=/&maxKeys=5", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d, body: %s", w.Code, w.Body.String())
	}

	var listResp struct {
		Items []struct {
			Key string `json:"key"`
		} `json:"items"`
		IsTruncated           bool   `json:"isTruncated"`
		NextContinuationToken string `json:"nextContinuationToken"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &listResp); err != nil {
		t.Fatalf("failed to parse list response: %v", err)
	}

	if len(listResp.Items) != 1 || listResp.Items[0].Key != "large-file.bin" {
		t.Errorf("expected 1 item with key 'large-file.bin', got items: %+v", listResp.Items)
	}

	// Test Search
	req = httptest.NewRequest("GET", "/api/buckets/test-bucket/objects?searchPrefix=large", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	var searchResp struct {
		Items []struct {
			Key string `json:"key"`
		} `json:"items"`
	}
	json.Unmarshal(w.Body.Bytes(), &searchResp)
	if len(searchResp.Items) != 1 || searchResp.Items[0].Key != "large-file.bin" {
		t.Errorf("search failed: expected large-file.bin, got %+v", searchResp.Items)
	}

	// Test Search with no matches
	req = httptest.NewRequest("GET", "/api/buckets/test-bucket/objects?searchPrefix=nonexistent", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	json.Unmarshal(w.Body.Bytes(), &searchResp)
	if len(searchResp.Items) != 0 {
		t.Errorf("expected 0 matches, got %d", len(searchResp.Items))
	}

	// 5. Test Presign Object
	req = httptest.NewRequest("GET", "/api/buckets/test-bucket/objects/presign?key=large-file.bin&expires=3600", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200 for presign, got %d, body: %s", w.Code, w.Body.String())
	}

	var presignResp struct {
		URL string `json:"url"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &presignResp); err != nil {
		t.Fatalf("failed to parse presign response: %v", err)
	}

	if presignResp.URL == "" {
		t.Errorf("expected presigned URL, got empty")
	}
}

func TestConsoleRateLimiting(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "console-rl-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	engine, err := storage.NewFilesystemEngine(tempDir, nil, "")
	if err != nil {
		t.Fatalf("failed to initialize storage engine: %v", err)
	}
	defer engine.Close()

	creds := auth.NewCredentials("access", "secret")
	// Set very tight rate limit (burst of 1 for login, burst of 2 for API)
	handler := NewHandler(engine, creds, 9000, "us-east-1", "", 1, 2)

	// Test login rate limiting
	loginBody, _ := json.Marshal(map[string]string{
		"accessKey": "access",
		"secretKey": "secret",
	})

	// Request 1: should pass
	req := httptest.NewRequest("POST", "/api/login", bytes.NewReader(loginBody))
	req.RemoteAddr = "1.2.3.4:1234"
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}

	// Request 2 (immediate): should fail with 429
	req = httptest.NewRequest("POST", "/api/login", bytes.NewReader(loginBody))
	req.RemoteAddr = "1.2.3.4:1234"
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusTooManyRequests {
		t.Errorf("expected 429, got %d", w.Code)
	}

	// Request 3 (from different IP): should pass
	req = httptest.NewRequest("POST", "/api/login", bytes.NewReader(loginBody))
	req.RemoteAddr = "5.6.7.8:1234"
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}
