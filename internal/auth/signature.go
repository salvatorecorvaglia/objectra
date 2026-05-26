package auth

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"
)

// SigV4Verifier verifies AWS Signature Version 4 requests.
type SigV4Verifier struct {
	creds *Credentials
}

// NewSigV4Verifier creates a new verifier with the given credentials.
func NewSigV4Verifier(creds *Credentials) *SigV4Verifier {
	return &SigV4Verifier{creds: creds}
}

// parsedAuth holds the components parsed from the Authorization header.
type parsedAuth struct {
	Algorithm     string
	Credential    string
	AccessKey     string
	Date          string
	Region        string
	Service       string
	SignedHeaders []string
	Signature     string
}

// Verify checks the AWS SigV4 signature on an HTTP request.
// Returns nil if the signature is valid, or an error describing the failure.
func (v *SigV4Verifier) Verify(r *http.Request) error {
	// Check for query string authentication (presigned URLs)
	if r.URL.Query().Get("X-Amz-Algorithm") != "" {
		return v.verifyPresigned(r)
	}

	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		return fmt.Errorf("missing Authorization header")
	}

	parsed, err := parseAuthHeader(authHeader)
	if err != nil {
		return err
	}

	if parsed.AccessKey != v.creds.AccessKey {
		return fmt.Errorf("invalid access key")
	}

	// Get the date for signing
	dateStr := r.Header.Get("X-Amz-Date")
	if dateStr == "" {
		dateStr = r.Header.Get("Date")
	}
	if dateStr == "" {
		return fmt.Errorf("missing date header")
	}

	// Parse the date
	t, err := time.Parse("20060102T150405Z", dateStr)
	if err != nil {
		// Try RFC2616 format
		t, err = time.Parse(time.RFC1123, dateStr)
		if err != nil {
			return fmt.Errorf("invalid date format")
		}
	}

	// Check for date/time skew (standard is 15 minutes)
	if time.Now().UTC().Sub(t).Abs() > 15*time.Minute {
		return fmt.Errorf("request time too far in the past or future")
	}

	// Verify request payload matches content SHA256 header (if not unsigned or streaming)
	payloadHash := r.Header.Get("X-Amz-Content-Sha256")
	if payloadHash != "" && payloadHash != "UNSIGNED-PAYLOAD" && payloadHash != "STREAMING-AWS4-HMAC-SHA256-PAYLOAD" {
		actualHash, err := HashPayload(r)
		if err != nil {
			return fmt.Errorf("failed to hash request payload: %w", err)
		}
		if actualHash != payloadHash {
			return fmt.Errorf("payload hash mismatch: expected %s, got %s", payloadHash, actualHash)
		}
	}

	datestamp := t.Format("20060102")

	// Reconstruct canonical request
	canonicalRequest := v.buildCanonicalRequest(r, parsed.SignedHeaders)

	// Build string to sign
	scope := fmt.Sprintf("%s/%s/%s/aws4_request", datestamp, parsed.Region, parsed.Service)
	stringToSign := fmt.Sprintf("AWS4-HMAC-SHA256\n%s\n%s\n%s",
		dateStr,
		scope,
		hashSHA256([]byte(canonicalRequest)),
	)

	// Calculate expected signature
	signingKey := v.deriveSigningKey(datestamp, parsed.Region, parsed.Service)
	expectedSig := hex.EncodeToString(hmacSHA256(signingKey, []byte(stringToSign)))

	if !hmac.Equal([]byte(expectedSig), []byte(parsed.Signature)) {
		return fmt.Errorf("signature does not match")
	}

	return nil
}

// verifyPresigned handles presigned URL verification.
func (v *SigV4Verifier) verifyPresigned(r *http.Request) error {
	q := r.URL.Query()

	credential := q.Get("X-Amz-Credential")
	parts := strings.Split(credential, "/")
	if len(parts) != 5 {
		return fmt.Errorf("invalid credential format")
	}

	accessKey := parts[0]
	datestamp := parts[1]
	region := parts[2]
	service := parts[3]

	if accessKey != v.creds.AccessKey {
		return fmt.Errorf("invalid access key")
	}

	dateStr := q.Get("X-Amz-Date")
	signedHeaders := strings.Split(q.Get("X-Amz-SignedHeaders"), ";")
	signature := q.Get("X-Amz-Signature")

	// Check expiration
	expires := q.Get("X-Amz-Expires")
	if expires != "" {
		t, err := time.Parse("20060102T150405Z", dateStr)
		if err != nil {
			return fmt.Errorf("invalid date format")
		}
		var dur int
		_, err = fmt.Sscanf(expires, "%d", &dur)
		if err != nil {
			return fmt.Errorf("invalid expires value")
		}
		if time.Now().UTC().After(t.Add(time.Duration(dur) * time.Second)) {
			return fmt.Errorf("presigned URL has expired")
		}
	}

	// Build canonical request for presigned URL
	// For presigned, the payload hash is "UNSIGNED-PAYLOAD"
	canonicalRequest := v.buildCanonicalRequestPresigned(r, signedHeaders)

	scope := fmt.Sprintf("%s/%s/%s/aws4_request", datestamp, region, service)
	stringToSign := fmt.Sprintf("AWS4-HMAC-SHA256\n%s\n%s\n%s",
		dateStr,
		scope,
		hashSHA256([]byte(canonicalRequest)),
	)

	signingKey := v.deriveSigningKey(datestamp, region, service)
	expectedSig := hex.EncodeToString(hmacSHA256(signingKey, []byte(stringToSign)))

	if !hmac.Equal([]byte(expectedSig), []byte(signature)) {
		return fmt.Errorf("signature does not match")
	}

	return nil
}

// parseAuthHeader parses an AWS SigV4 Authorization header.
// Format: AWS4-HMAC-SHA256 Credential=<accessKey>/<date>/<region>/<service>/aws4_request, SignedHeaders=<headers>, Signature=<sig>
func parseAuthHeader(header string) (*parsedAuth, error) {
	if !strings.HasPrefix(header, "AWS4-HMAC-SHA256 ") {
		return nil, fmt.Errorf("unsupported auth algorithm")
	}

	content := strings.TrimPrefix(header, "AWS4-HMAC-SHA256 ")
	parts := strings.Split(content, ", ")

	parsed := &parsedAuth{Algorithm: "AWS4-HMAC-SHA256"}

	for _, part := range parts {
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			continue
		}
		key := strings.TrimSpace(kv[0])
		value := strings.TrimSpace(kv[1])

		switch key {
		case "Credential":
			parsed.Credential = value
			credParts := strings.Split(value, "/")
			if len(credParts) >= 5 {
				parsed.AccessKey = credParts[0]
				parsed.Date = credParts[1]
				parsed.Region = credParts[2]
				parsed.Service = credParts[3]
			}
		case "SignedHeaders":
			parsed.SignedHeaders = strings.Split(value, ";")
		case "Signature":
			parsed.Signature = value
		}
	}

	if parsed.AccessKey == "" || parsed.Signature == "" {
		return nil, fmt.Errorf("incomplete authorization header")
	}

	return parsed, nil
}

// buildCanonicalRequest constructs the canonical request string for SigV4.
func (v *SigV4Verifier) buildCanonicalRequest(r *http.Request, signedHeaders []string) string {
	// HTTP method
	method := r.Method

	// Canonical URI (URL-encoded path)
	canonicalURI := getCanonicalURI(r.URL)

	// Canonical query string
	canonicalQueryString := getCanonicalQueryString(r.URL.Query())

	// Canonical headers
	sort.Strings(signedHeaders)
	var canonicalHeaders strings.Builder
	for _, h := range signedHeaders {
		h = strings.ToLower(strings.TrimSpace(h))
		value := strings.TrimSpace(r.Header.Get(h))
		if h == "host" && value == "" {
			value = r.Host
		}
		canonicalHeaders.WriteString(h)
		canonicalHeaders.WriteString(":")
		canonicalHeaders.WriteString(value)
		canonicalHeaders.WriteString("\n")
	}

	signedHeadersStr := strings.Join(signedHeaders, ";")

	// Payload hash
	payloadHash := r.Header.Get("X-Amz-Content-Sha256")
	if payloadHash == "" {
		payloadHash = "UNSIGNED-PAYLOAD"
	}

	return fmt.Sprintf("%s\n%s\n%s\n%s\n%s\n%s",
		method,
		canonicalURI,
		canonicalQueryString,
		canonicalHeaders.String(),
		signedHeadersStr,
		payloadHash,
	)
}

// buildCanonicalRequestPresigned constructs the canonical request for presigned URLs.
func (v *SigV4Verifier) buildCanonicalRequestPresigned(r *http.Request, signedHeaders []string) string {
	method := r.Method
	canonicalURI := getCanonicalURI(r.URL)

	// For presigned URLs, exclude X-Amz-Signature from query string
	queryParams := r.URL.Query()
	queryParams.Del("X-Amz-Signature")
	canonicalQueryString := getCanonicalQueryString(queryParams)

	sort.Strings(signedHeaders)
	var canonicalHeaders strings.Builder
	for _, h := range signedHeaders {
		h = strings.ToLower(strings.TrimSpace(h))
		value := strings.TrimSpace(r.Header.Get(h))
		if h == "host" && value == "" {
			value = r.Host
		}
		canonicalHeaders.WriteString(h)
		canonicalHeaders.WriteString(":")
		canonicalHeaders.WriteString(value)
		canonicalHeaders.WriteString("\n")
	}

	signedHeadersStr := strings.Join(signedHeaders, ";")

	return fmt.Sprintf("%s\n%s\n%s\n%s\n%s\n%s",
		method,
		canonicalURI,
		canonicalQueryString,
		canonicalHeaders.String(),
		signedHeadersStr,
		"UNSIGNED-PAYLOAD",
	)
}

// getCanonicalURI returns the URI-encoded path.
func getCanonicalURI(u *url.URL) string {
	path := u.EscapedPath()
	if path == "" {
		path = "/"
	}
	return path
}

// getCanonicalQueryString returns the sorted, URL-encoded query string.
func getCanonicalQueryString(values url.Values) string {
	if len(values) == 0 {
		return ""
	}

	var keys []string
	for k := range values {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var pairs []string
	for _, k := range keys {
		vs := values[k]
		sort.Strings(vs)
		for _, v := range vs {
			pairs = append(pairs, url.QueryEscape(k)+"="+url.QueryEscape(v))
		}
	}

	return strings.Join(pairs, "&")
}

// deriveSigningKey derives the SigV4 signing key.
func (v *SigV4Verifier) deriveSigningKey(datestamp, region, service string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+v.creds.SecretKey), []byte(datestamp))
	kRegion := hmacSHA256(kDate, []byte(region))
	kService := hmacSHA256(kRegion, []byte(service))
	kSigning := hmacSHA256(kService, []byte("aws4_request"))
	return kSigning
}

// hmacSHA256 computes HMAC-SHA256.
func hmacSHA256(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}

// hashSHA256 computes SHA256 hash and returns hex string.
func hashSHA256(data []byte) string {
	h := sha256.New()
	h.Write(data)
	return hex.EncodeToString(h.Sum(nil))
}

// HashPayload reads the request body, computes its SHA256, and returns the hash.
// It uses a temporary file to buffer large request bodies to prevent OOM errors.
func HashPayload(r *http.Request) (string, error) {
	if r.Body == nil {
		return hashSHA256([]byte("")), nil
	}

	const maxMemoryBuffer = 2 * 1024 * 1024 // 2MB
	lr := io.LimitReader(r.Body, maxMemoryBuffer+1)
	buf, err := io.ReadAll(lr)
	if err != nil {
		return "", err
	}

	if len(buf) <= maxMemoryBuffer {
		r.Body = io.NopCloser(bytes.NewReader(buf))
		return hashSHA256(buf), nil
	}

	tmpFile, err := os.CreateTemp("", "objectra-body-*")
	if err != nil {
		return "", fmt.Errorf("failed to create temp file for request body: %w", err)
	}
	tmpName := tmpFile.Name()

	if _, err := tmpFile.Write(buf); err != nil {
		tmpFile.Close()
		os.Remove(tmpName)
		return "", err
	}

	shaWriter := sha256.New()
	shaWriter.Write(buf)

	_, err = io.Copy(io.MultiWriter(tmpFile, shaWriter), r.Body)
	if err != nil {
		tmpFile.Close()
		os.Remove(tmpName)
		return "", err
	}

	if err := tmpFile.Close(); err != nil {
		os.Remove(tmpName)
		return "", err
	}

	fileRead, err := os.Open(tmpName)
	if err != nil {
		os.Remove(tmpName)
		return "", err
	}

	r.Body = &tempFileReadCloser{
		File: fileRead,
		path: tmpName,
	}

	return hex.EncodeToString(shaWriter.Sum(nil)), nil
}

type tempFileReadCloser struct {
	*os.File
	path string
}

func (t *tempFileReadCloser) Close() error {
	err := t.File.Close()
	os.Remove(t.path)
	return err
}
