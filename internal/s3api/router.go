package s3api

import (
	"log"
	"net/http"
	"strings"

	"github.com/salvatorecorvaglia/objectra/internal/auth"
	"github.com/salvatorecorvaglia/objectra/internal/storage"
)

// Router handles S3 API request routing and dispatching.
type Router struct {
	engine   storage.Engine
	verifier *auth.SigV4Verifier
	region   string
}

// NewRouter creates a new S3 API router.
func NewRouter(engine storage.Engine, creds *auth.Credentials, region string) *Router {
	return &Router{
		engine:   engine,
		verifier: auth.NewSigV4Verifier(creds),
		region:   region,
	}
}

// ServeHTTP implements the http.Handler interface for the S3 API.
func (rt *Router) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Set common S3 response headers
	w.Header().Set("Server", "Objectra")
	w.Header().Set("x-amz-request-id", "objectra")

	// Authenticate the request
	if err := rt.verifier.Verify(r); err != nil {
		log.Printf("[S3] Auth failed: %s %s - %v", r.Method, r.URL.Path, err)
		writeS3Error(w, "AccessDenied", "Access Denied", r.URL.Path)
		return
	}

	// Parse the path to determine bucket and key
	path := strings.TrimPrefix(r.URL.Path, "/")
	parts := strings.SplitN(path, "/", 2)

	bucket := ""
	key := ""
	if len(parts) > 0 {
		bucket = parts[0]
	}
	if len(parts) > 1 {
		key = parts[1]
	}

	log.Printf("[S3] %s %s (bucket=%q, key=%q)", r.Method, r.URL.Path, bucket, key)

	// Route based on path structure and method
	if bucket == "" {
		// Service-level operations
		rt.handleServiceOps(w, r)
		return
	}

	if key == "" {
		// Bucket-level operations
		rt.handleBucketOps(w, r, bucket)
		return
	}

	// Object-level operations
	rt.handleObjectOps(w, r, bucket, key)
}

// handleServiceOps handles service-level operations (e.g., ListBuckets).
func (rt *Router) handleServiceOps(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		rt.handleListBuckets(w, r)
	default:
		writeS3Error(w, "MethodNotAllowed", "Method not allowed", "/")
	}
}

// handleBucketOps handles bucket-level operations.
func (rt *Router) handleBucketOps(w http.ResponseWriter, r *http.Request, bucket string) {
	query := r.URL.Query()

	switch r.Method {
	case http.MethodGet:
		if query.Get("location") != "" || query.Has("location") {
			rt.handleGetBucketLocation(w, r, bucket)
		} else {
			// ListObjectsV2
			rt.handleListObjectsV2(w, r, bucket)
		}
	case http.MethodPut:
		rt.handleCreateBucket(w, r, bucket)
	case http.MethodDelete:
		rt.handleDeleteBucket(w, r, bucket)
	case http.MethodHead:
		rt.handleHeadBucket(w, r, bucket)
	default:
		writeS3Error(w, "MethodNotAllowed", "Method not allowed", "/"+bucket)
	}
}

// handleObjectOps handles object-level operations.
func (rt *Router) handleObjectOps(w http.ResponseWriter, r *http.Request, bucket, key string) {
	query := r.URL.Query()

	switch r.Method {
	case http.MethodGet:
		rt.handleGetObject(w, r, bucket, key)
	case http.MethodPut:
		if r.Header.Get("x-amz-copy-source") != "" {
			rt.handleCopyObject(w, r, bucket, key)
		} else if query.Get("partNumber") != "" && query.Get("uploadId") != "" {
			rt.handleUploadPart(w, r, bucket, key)
		} else {
			rt.handlePutObject(w, r, bucket, key)
		}
	case http.MethodDelete:
		if query.Get("uploadId") != "" {
			rt.handleAbortMultipartUpload(w, r, bucket, key)
		} else {
			rt.handleDeleteObject(w, r, bucket, key)
		}
	case http.MethodHead:
		rt.handleHeadObject(w, r, bucket, key)
	case http.MethodPost:
		if query.Has("uploads") {
			rt.handleCreateMultipartUpload(w, r, bucket, key)
		} else if query.Get("uploadId") != "" {
			rt.handleCompleteMultipartUpload(w, r, bucket, key)
		} else {
			writeS3Error(w, "MethodNotAllowed", "Method not allowed", "/"+bucket+"/"+key)
		}
	default:
		writeS3Error(w, "MethodNotAllowed", "Method not allowed", "/"+bucket+"/"+key)
	}
}
