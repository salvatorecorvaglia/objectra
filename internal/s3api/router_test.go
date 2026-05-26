package s3api

import (
	"bytes"
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
