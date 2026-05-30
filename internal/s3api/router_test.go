package s3api

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/salvatorecorvaglia/objectra/internal/auth"
	"github.com/salvatorecorvaglia/objectra/internal/storage"
)

func hmacSHA256(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}

func hashSHA256(data []byte) string {
	h := sha256.New()
	h.Write(data)
	return hex.EncodeToString(h.Sum(nil))
}

func signTestRequest(r *http.Request, accessKey, secretKey string, region, service string, t time.Time, body []byte) {
	datestamp := t.Format("20060102")
	amzDate := t.Format("20060102T150405Z")

	r.Header.Set("X-Amz-Date", amzDate)
	r.Header.Set("Host", r.Host)

	// Build canonical headers and signed headers
	signedHeaders := []string{"host", "x-amz-date"}

	var canonicalHeaders strings.Builder
	canonicalHeaders.WriteString(fmt.Sprintf("host:%s\n", r.Host))
	canonicalHeaders.WriteString(fmt.Sprintf("x-amz-date:%s\n", amzDate))

	payloadHash := hashSHA256(body)
	r.Header.Set("X-Amz-Content-Sha256", payloadHash)

	if body != nil {
		r.Body = io.NopCloser(bytes.NewReader(body))
	}

	// Canonical request
	canonicalRequest := fmt.Sprintf("%s\n%s\n%s\n%s\n%s\n%s",
		r.Method,
		r.URL.Path,
		r.URL.RawQuery,
		canonicalHeaders.String(),
		strings.Join(signedHeaders, ";"),
		payloadHash,
	)

	// String to sign
	scope := fmt.Sprintf("%s/%s/%s/aws4_request", datestamp, region, service)
	stringToSign := fmt.Sprintf("AWS4-HMAC-SHA256\n%s\n%s\n%s",
		amzDate,
		scope,
		hashSHA256([]byte(canonicalRequest)),
	)

	// Derive signing key
	kDate := hmacSHA256([]byte("AWS4"+secretKey), []byte(datestamp))
	kRegion := hmacSHA256(kDate, []byte(region))
	kService := hmacSHA256(kRegion, []byte(service))
	kSigning := hmacSHA256(kService, []byte("aws4_request"))

	// Sign
	h := hmac.New(sha256.New, kSigning)
	h.Write([]byte(stringToSign))
	signature := hex.EncodeToString(h.Sum(nil))

	// Build authorization header
	authHeader := fmt.Sprintf("AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		accessKey,
		scope,
		strings.Join(signedHeaders, ";"),
		signature,
	)
	r.Header.Set("Authorization", authHeader)
}

func TestResolveBucketAndKey(t *testing.T) {
	tests := []struct {
		name           string
		domain         string
		host           string
		path           string
		expectedBucket string
		expectedKey    string
	}{
		{
			name:           "Path style (no domain config)",
			domain:         "",
			host:           "localhost:9000",
			path:           "/mybucket/mykey/file.txt",
			expectedBucket: "mybucket",
			expectedKey:    "mykey/file.txt",
		},
		{
			name:           "Path style (with domain config)",
			domain:         "objectra.local",
			host:           "objectra.local:9000",
			path:           "/mybucket/mykey/file.txt",
			expectedBucket: "mybucket",
			expectedKey:    "mykey/file.txt",
		},
		{
			name:           "Virtual Host style (localhost domain)",
			domain:         "localhost",
			host:           "mybucket.localhost:9000",
			path:           "/mykey/file.txt",
			expectedBucket: "mybucket",
			expectedKey:    "mykey/file.txt",
		},
		{
			name:           "Virtual Host style (custom domain)",
			domain:         "objectra.local",
			host:           "mybucket.objectra.local",
			path:           "/mykey/file.txt",
			expectedBucket: "mybucket",
			expectedKey:    "mykey/file.txt",
		},
		{
			name:           "Virtual Host style root path",
			domain:         "objectra.local",
			host:           "mybucket.objectra.local",
			path:           "/",
			expectedBucket: "mybucket",
			expectedKey:    "",
		},
		{
			name:           "Fallback on domain mismatch",
			domain:         "objectra.local",
			host:           "mybucket.different.com:9000",
			path:           "/anotherbucket/key.txt",
			expectedBucket: "anotherbucket",
			expectedKey:    "key.txt",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rt := &Router{
				domain: tc.domain,
			}
			req := httptest.NewRequest("GET", tc.path, nil)
			req.Host = tc.host

			bucket, key := rt.resolveBucketAndKey(req)
			if bucket != tc.expectedBucket {
				t.Errorf("expected bucket %q, got %q", tc.expectedBucket, bucket)
			}
			if key != tc.expectedKey {
				t.Errorf("expected key %q, got %q", tc.expectedKey, key)
			}
		})
	}
}

func TestS3API_Integration(t *testing.T) {
	tempDir := t.TempDir()
	engine, err := storage.NewFilesystemEngine(tempDir)
	if err != nil {
		t.Fatalf("Failed to create storage engine: %v", err)
	}
	defer engine.Close()

	accessKey := "testaccess"
	secretKey := "testsecret"
	creds := auth.NewCredentials(accessKey, secretKey)
	router := NewRouter(engine, creds, "us-east-1", "")

	// 1. Create Bucket
	t.Run("CreateBucket", func(t *testing.T) {
		req := httptest.NewRequest("PUT", "/testbucket", nil)
		req.Host = "localhost:9000"
		signTestRequest(req, accessKey, secretKey, "us-east-1", "s3", time.Now().UTC(), nil)

		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d, body: %s", rec.Code, rec.Body.String())
		}
	})

	// 2. Put Object
	t.Run("PutObject", func(t *testing.T) {
		content := []byte("hello world")
		req := httptest.NewRequest("PUT", "/testbucket/hello.txt", bytes.NewReader(content))
		req.Host = "localhost:9000"
		signTestRequest(req, accessKey, secretKey, "us-east-1", "s3", time.Now().UTC(), content)

		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d, body: %s", rec.Code, rec.Body.String())
		}
	})

	// 3. Get Object
	t.Run("GetObject", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/testbucket/hello.txt", nil)
		req.Host = "localhost:9000"
		signTestRequest(req, accessKey, secretKey, "us-east-1", "s3", time.Now().UTC(), nil)

		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d, body: %s", rec.Code, rec.Body.String())
		}
		if rec.Body.String() != "hello world" {
			t.Errorf("expected body 'hello world', got %q", rec.Body.String())
		}
	})

	// 3b. Get Object Range
	t.Run("GetObjectRange", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/testbucket/hello.txt", nil)
		req.Host = "localhost:9000"
		req.Header.Set("Range", "bytes=6-10")
		signTestRequest(req, accessKey, secretKey, "us-east-1", "s3", time.Now().UTC(), nil)

		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusPartialContent {
			t.Errorf("expected status 206, got %d, body: %s", rec.Code, rec.Body.String())
		}
		if rec.Body.String() != "world" {
			t.Errorf("expected body 'world', got %q", rec.Body.String())
		}
		if rec.Header().Get("Content-Range") != "bytes 6-10/11" {
			t.Errorf("expected Content-Range 'bytes 6-10/11', got %q", rec.Header().Get("Content-Range"))
		}
	})

	// 4. Delete Object
	t.Run("DeleteObject", func(t *testing.T) {
		req := httptest.NewRequest("DELETE", "/testbucket/hello.txt", nil)
		req.Host = "localhost:9000"
		signTestRequest(req, accessKey, secretKey, "us-east-1", "s3", time.Now().UTC(), nil)

		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusNoContent {
			t.Errorf("expected status 204, got %d, body: %s", rec.Code, rec.Body.String())
		}
	})

	// 5. Auth Error (Invalid Access Key ID)
	t.Run("AuthError_InvalidKey", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/testbucket/hello.txt", nil)
		req.Host = "localhost:9000"
		signTestRequest(req, "WRONGKEY", secretKey, "us-east-1", "s3", time.Now().UTC(), nil)

		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusForbidden {
			t.Errorf("expected status 403, got %d", rec.Code)
		}

		var errResp struct {
			XMLName xml.Name `xml:"Error"`
			Code    string   `xml:"Code"`
			Message string   `xml:"Message"`
		}
		if err := xml.NewDecoder(rec.Body).Decode(&errResp); err != nil {
			t.Fatalf("Failed to decode XML error: %v", err)
		}
		if errResp.Code != "InvalidAccessKeyId" {
			t.Errorf("expected XML Code 'InvalidAccessKeyId', got %q", errResp.Code)
		}
	})

	// 6. Auth Error (Clock Skew)
	t.Run("AuthError_ClockSkew", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/testbucket/hello.txt", nil)
		req.Host = "localhost:9000"
		// 20 minutes in the past
		skewedTime := time.Now().UTC().Add(-20 * time.Minute)
		signTestRequest(req, accessKey, secretKey, "us-east-1", "s3", skewedTime, nil)

		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusForbidden {
			t.Errorf("expected status 403, got %d", rec.Code)
		}

		var errResp struct {
			XMLName xml.Name `xml:"Error"`
			Code    string   `xml:"Code"`
			Message string   `xml:"Message"`
		}
		if err := xml.NewDecoder(rec.Body).Decode(&errResp); err != nil {
			t.Fatalf("Failed to decode XML error: %v", err)
		}
		if errResp.Code != "RequestTimeTooSkewed" {
			t.Errorf("expected XML Code 'RequestTimeTooSkewed', got %q", errResp.Code)
		}
	})
}

func TestS3AccessLogging(t *testing.T) {
	tempDir := t.TempDir()
	engine, err := storage.NewFilesystemEngine(tempDir)
	if err != nil {
		t.Fatalf("Failed to create storage engine: %v", err)
	}
	defer engine.Close()

	accessKey := "testaccess"
	secretKey := "testsecret"
	creds := auth.NewCredentials(accessKey, secretKey)
	router := NewRouter(engine, creds, "us-east-1", "")

	// Create src-bucket and dest-bucket
	engine.CreateBucket("src-bucket")
	engine.CreateBucket("dest-bucket")

	// Put logging status on src-bucket
	loggingStatus := &storage.BucketLoggingStatus{
		LoggingEnabled: &storage.LoggingEnabled{
			TargetBucket: "dest-bucket",
			TargetPrefix: "access-logs/",
		},
	}
	err = engine.PutBucketLogging("src-bucket", loggingStatus)
	if err != nil {
		t.Fatalf("PutBucketLogging failed: %v", err)
	}

	// Issue a GET request to src-bucket
	req := httptest.NewRequest("GET", "/src-bucket?list-type=2", nil)
	req.Host = "localhost:9000"
	signTestRequest(req, accessKey, secretKey, "us-east-1", "s3", time.Now().UTC(), nil)

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d, body: %s", rec.Code, rec.Body.String())
	}

	// Wait for async log delivery
	time.Sleep(100 * time.Millisecond)

	// List objects in dest-bucket to find the log file
	output, err := engine.ListObjects(&storage.ListObjectsInput{
		Bucket: "dest-bucket",
		Prefix: "access-logs/",
	})
	if err != nil {
		t.Fatalf("failed to list dest-bucket: %v", err)
	}

	if len(output.Objects) != 1 {
		t.Fatalf("expected exactly 1 log file in dest-bucket, got %d", len(output.Objects))
	}

	logObj := output.Objects[0]
	if !strings.HasPrefix(logObj.Key, "access-logs/") {
		t.Errorf("unexpected log key: %s", logObj.Key)
	}

	// Read log content
	reader, _, err := engine.GetObject(context.Background(), "dest-bucket", logObj.Key, "")
	if err != nil {
		t.Fatalf("failed to get log object: %v", err)
	}
	defer reader.Close()

	logBytes, _ := io.ReadAll(reader)
	logStr := string(logBytes)

	if !strings.Contains(logStr, "src-bucket") {
		t.Errorf("expected log to contain 'src-bucket', got: %s", logStr)
	}
	if !strings.Contains(logStr, "REST.GET.BUCKET") {
		t.Errorf("expected log to contain S3 operation 'REST.GET.BUCKET', got: %s", logStr)
	}
}
