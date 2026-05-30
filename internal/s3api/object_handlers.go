package s3api

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/salvatorecorvaglia/objectra/internal/storage"
)

const (
	amzSSECAlgorithmHeader = "x-amz-server-side-encryption-customer-algorithm"
	amzSSECKeyMD5Header    = "x-amz-server-side-encryption-customer-key-MD5"
	contentTypeHeader      = "Content-Type"
	amzVersionIDHeader     = "x-amz-version-id"
	amzDeleteMarkerHeader  = "x-amz-delete-marker"
)

// extractSSECParams extracts SSE-C headers from the HTTP request.
func extractSSECParams(r *http.Request) (*storage.SSECParams, error) {
	algo := r.Header.Get(amzSSECAlgorithmHeader)
	if algo == "" {
		return nil, nil
	}
	if algo != "AES256" {
		return nil, fmt.Errorf("UnsupportedEncryptionAlgorithm: The encryption algorithm specified is not supported")
	}
	keyB64 := r.Header.Get("x-amz-server-side-encryption-customer-key")
	if keyB64 == "" {
		return nil, fmt.Errorf("MissingEncryptionKey: The encryption key is missing")
	}
	key, err := base64.StdEncoding.DecodeString(keyB64)
	if err != nil || len(key) != 32 {
		return nil, fmt.Errorf("InvalidEncryptionKey: The encryption key is invalid")
	}
	keyMD5B64 := r.Header.Get(amzSSECKeyMD5Header)
	if keyMD5B64 == "" {
		return nil, fmt.Errorf("MissingEncryptionKeyMD5: The encryption key MD5 is missing")
	}
	expectedMD5Raw := md5.Sum(key)
	expectedMD5 := base64.StdEncoding.EncodeToString(expectedMD5Raw[:])
	if keyMD5B64 != expectedMD5 {
		return nil, fmt.Errorf("InvalidEncryptionKeyMD5: The customer-provided encryption key MD5 does not match")
	}
	return &storage.SSECParams{
		Algorithm: algo,
		Key:       key,
		KeyMD5:    keyMD5B64,
	}, nil
}

// handlePutObject handles PUT /<bucket>/<key> (PutObject).
func (rt *Router) handlePutObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	contentType := r.Header.Get(contentTypeHeader)
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	size := r.ContentLength
	resource := "/" + bucket + "/" + key

	params, err := extractSSECParams(r)
	if err != nil {
		writeS3Error(w, "InvalidArgument", err.Error(), resource)
		return
	}

	ctx := r.Context()
	if params != nil {
		ctx = context.WithValue(ctx, storage.SSECContextKey, params)
	}

	info, err := rt.engine.PutObject(ctx, bucket, key, r.Body, size, contentType)
	if handleStorageError(w, err, resource) {
		return
	}

	w.Header().Set("ETag", fmt.Sprintf(`"%s"`, info.ETag))
	if info.VersionID != "" {
		w.Header().Set(amzVersionIDHeader, info.VersionID)
	}
	if info.SSECustomerAlgorithm != "" {
		w.Header().Set(amzSSECAlgorithmHeader, info.SSECustomerAlgorithm)
		w.Header().Set(amzSSECKeyMD5Header, info.SSECustomerKeyMD5)
	}
	w.WriteHeader(http.StatusOK)
}

// handleGetObject handles GET /<bucket>/<key> (GetObject).
func (rt *Router) handleGetObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	resource := "/" + bucket + "/" + key
	versionID := r.URL.Query().Get("versionId")

	params, err := extractSSECParams(r)
	if err != nil {
		writeS3Error(w, "InvalidArgument", err.Error(), resource)
		return
	}

	ctx := r.Context()
	if params != nil {
		ctx = context.WithValue(ctx, storage.SSECContextKey, params)
	}

	reader, info, err := rt.engine.GetObject(ctx, bucket, key, versionID)
	if err != nil {
		if info != nil && info.IsDeleteMarker {
			w.Header().Set(amzDeleteMarkerHeader, "true")
			w.Header().Set(amzVersionIDHeader, info.VersionID)
			writeS3Error(w, "NoSuchKey", "The specified key does not exist.", resource)
			return
		}
		if handleStorageError(w, err, resource) {
			return
		}
	}
	defer reader.Close()

	w.Header().Set(contentTypeHeader, info.ContentType)
	w.Header().Set("Content-Length", strconv.FormatInt(info.Size, 10))
	w.Header().Set("ETag", fmt.Sprintf(`"%s"`, info.ETag))
	w.Header().Set("Last-Modified", info.LastModified.UTC().Format(http.TimeFormat))
	w.Header().Set("Accept-Ranges", "bytes")
	if info.VersionID != "" {
		w.Header().Set(amzVersionIDHeader, info.VersionID)
	}
	if info.SSECustomerAlgorithm != "" {
		w.Header().Set(amzSSECAlgorithmHeader, info.SSECustomerAlgorithm)
		w.Header().Set(amzSSECKeyMD5Header, info.SSECustomerKeyMD5)
	}

	// If the reader supports seeking, use http.ServeContent which handles
	// Range requests, conditional headers, and status codes automatically.
	if rs, ok := reader.(io.ReadSeeker); ok {
		http.ServeContent(w, r, key, info.LastModified, rs)
		return
	}

	// Fallback for non-seekable readers: serve full content only.
	w.WriteHeader(http.StatusOK)
	_, _ = io.Copy(w, reader)
}

// handleHeadObject handles HEAD /<bucket>/<key> (HeadObject).
func (rt *Router) handleHeadObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	resource := "/" + bucket + "/" + key
	versionID := r.URL.Query().Get("versionId")

	params, err := extractSSECParams(r)
	if err != nil {
		writeS3Error(w, "InvalidArgument", err.Error(), resource)
		return
	}

	ctx := r.Context()
	if params != nil {
		ctx = context.WithValue(ctx, storage.SSECContextKey, params)
	}

	info, err := rt.engine.HeadObject(ctx, bucket, key, versionID)
	if err != nil {
		if info != nil && info.IsDeleteMarker {
			w.Header().Set(amzDeleteMarkerHeader, "true")
			w.Header().Set(amzVersionIDHeader, info.VersionID)
			writeS3Error(w, "NoSuchKey", "The specified key does not exist.", resource)
			return
		}
		if handleStorageError(w, err, resource) {
			return
		}
	}

	w.Header().Set(contentTypeHeader, info.ContentType)
	w.Header().Set("Content-Length", strconv.FormatInt(info.Size, 10))
	w.Header().Set("ETag", fmt.Sprintf(`"%s"`, info.ETag))
	w.Header().Set("Last-Modified", info.LastModified.UTC().Format(http.TimeFormat))
	w.Header().Set("Accept-Ranges", "bytes")
	if info.VersionID != "" {
		w.Header().Set(amzVersionIDHeader, info.VersionID)
	}
	if info.SSECustomerAlgorithm != "" {
		w.Header().Set(amzSSECAlgorithmHeader, info.SSECustomerAlgorithm)
		w.Header().Set(amzSSECKeyMD5Header, info.SSECustomerKeyMD5)
	}
	w.WriteHeader(http.StatusOK)
}

func (rt *Router) handleDeleteObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	versionID := r.URL.Query().Get("versionId")
	err := rt.engine.DeleteObject(bucket, key, versionID)
	resource := "/" + bucket + "/" + key

	if err != nil {
		if handleStorageError(w, err, resource) {
			return
		}
	}

	// If a delete marker was created, S3 returns 204 with delete marker headers
	if err == nil {
		versionStatus, _ := rt.engine.GetBucketVersioning(bucket)
		if versionID == "" && (versionStatus == "Enabled" || versionStatus == "Suspended") {
			w.Header().Set(amzDeleteMarkerHeader, "true")
			if info, err := rt.engine.HeadObject(r.Context(), bucket, key, ""); err == nil && info.IsDeleteMarker {
				w.Header().Set(amzVersionIDHeader, info.VersionID)
			}
		} else if versionID != "" {
			w.Header().Set(amzVersionIDHeader, versionID)
		}
	}

	// S3 returns 204 even if the key doesn't exist
	w.WriteHeader(http.StatusNoContent)
}

// handleCopyObject handles PUT /<bucket>/<key> with x-amz-copy-source header (CopyObject).
func (rt *Router) handleCopyObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	copySource := r.Header.Get("x-amz-copy-source")
	copySource = strings.TrimPrefix(copySource, "/")
	resource := "/" + bucket + "/" + key

	parts := strings.SplitN(copySource, "/", 2)
	if len(parts) != 2 {
		writeS3Error(w, "InvalidArgument", "Invalid copy source", resource)
		return
	}

	srcBucket := parts[0]
	srcKey := parts[1]

	info, err := rt.engine.CopyObject(srcBucket, srcKey, bucket, key)
	if handleStorageError(w, err, resource) {
		return
	}

	result := CopyObjectResult{
		LastModified: info.LastModified.UTC().Format("2006-01-02T15:04:05.000Z"),
		ETag:         fmt.Sprintf(`"%s"`, info.ETag),
	}

	writeXML(w, http.StatusOK, result)
}
