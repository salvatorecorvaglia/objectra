package s3api

import (
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
)

// handlePutObject handles PUT /<bucket>/<key> (PutObject).
func (rt *Router) handlePutObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	size := r.ContentLength
	resource := "/" + bucket + "/" + key

	info, err := rt.engine.PutObject(bucket, key, r.Body, size, contentType)
	if handleStorageError(w, err, resource) {
		return
	}

	w.Header().Set("ETag", fmt.Sprintf(`"%s"`, info.ETag))
	w.WriteHeader(http.StatusOK)
}

// handleGetObject handles GET /<bucket>/<key> (GetObject).
func (rt *Router) handleGetObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	resource := "/" + bucket + "/" + key

	reader, info, err := rt.engine.GetObject(bucket, key)
	if handleStorageError(w, err, resource) {
		return
	}
	defer reader.Close()

	w.Header().Set("Content-Type", info.ContentType)
	w.Header().Set("Content-Length", strconv.FormatInt(info.Size, 10))
	w.Header().Set("ETag", fmt.Sprintf(`"%s"`, info.ETag))
	w.Header().Set("Last-Modified", info.LastModified.UTC().Format(http.TimeFormat))
	w.Header().Set("Accept-Ranges", "bytes")

	// If the reader supports seeking, use http.ServeContent which handles
	// Range requests, conditional headers, and status codes automatically.
	if rs, ok := reader.(io.ReadSeeker); ok {
		http.ServeContent(w, r, key, info.LastModified, rs)
		return
	}

	// Fallback for non-seekable readers: serve full content only.
	w.WriteHeader(http.StatusOK)
	io.Copy(w, reader)
}

// handleHeadObject handles HEAD /<bucket>/<key> (HeadObject).
func (rt *Router) handleHeadObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	resource := "/" + bucket + "/" + key

	info, err := rt.engine.HeadObject(bucket, key)
	if handleStorageError(w, err, resource) {
		return
	}

	w.Header().Set("Content-Type", info.ContentType)
	w.Header().Set("Content-Length", strconv.FormatInt(info.Size, 10))
	w.Header().Set("ETag", fmt.Sprintf(`"%s"`, info.ETag))
	w.Header().Set("Last-Modified", info.LastModified.UTC().Format(http.TimeFormat))
	w.Header().Set("Accept-Ranges", "bytes")
	w.WriteHeader(http.StatusOK)
}

// handleDeleteObject handles DELETE /<bucket>/<key> (DeleteObject).
func (rt *Router) handleDeleteObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	_ = rt.engine.DeleteObject(bucket, key)
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
