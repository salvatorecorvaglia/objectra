package s3api

import (
	"encoding/xml"
	"log"
	"net"
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
	domain   string
}

// NewRouter creates a new S3 API router.
func NewRouter(engine storage.Engine, creds *auth.Credentials, region string, domain string) *Router {
	return &Router{
		engine:   engine,
		verifier: auth.NewSigV4Verifier(creds),
		region:   region,
		domain:   domain,
	}
}

type metricsResponseWriter struct {
	http.ResponseWriter
	statusCode   int
	bytesWritten int
}

func (w *metricsResponseWriter) WriteHeader(code int) {
	w.statusCode = code
	w.ResponseWriter.WriteHeader(code)
}

func (w *metricsResponseWriter) Write(b []byte) (int, error) {
	n, err := w.ResponseWriter.Write(b)
	w.bytesWritten += n
	return n, err
}

// ServeHTTP implements the http.Handler interface for the S3 API.
func (rt *Router) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	mrw := &metricsResponseWriter{ResponseWriter: w, statusCode: http.StatusOK}
	storage.GlobalMetrics.IncRequests()

	rt.serveHTTPInternal(mrw, r)

	if mrw.statusCode >= 400 {
		storage.GlobalMetrics.IncErrors()
	}
}

func (rt *Router) serveHTTPInternal(w http.ResponseWriter, r *http.Request) {
	// Set common S3 response headers
	w.Header().Set("Server", "Objectra")
	w.Header().Set("x-amz-request-id", "objectra")

	bucket, key := rt.resolveBucketAndKey(r)

	// Apply CORS headers on all matching requests (preflight or normal)
	if bucket != "" {
		hasCORS := rt.handleCORS(w, r, bucket)
		if r.Method == http.MethodOptions {
			if hasCORS {
				return // Preflight completed successfully
			}
			// If preflight failed to match CORS, S3 returns a 403 or 400
			writeS3Error(w, "AccessDenied", "CORS preflight request failed", r.URL.Path)
			return
		}
	} else if r.Method == http.MethodOptions {
		// OPTIONS request at service level is not supported
		writeS3Error(w, "MethodNotAllowed", "Method not allowed", r.URL.Path)
		return
	}

	// Authenticate the request (bypass for OPTIONS or public bucket object reads)
	bypassAuth := false
	if bucket != "" && key != "" && (r.Method == http.MethodGet || r.Method == http.MethodHead) {
		if public, err := rt.engine.IsBucketPublic(bucket); err == nil && public {
			bypassAuth = true
		}
	}

	if !bypassAuth {
		if err := rt.verifier.Verify(r); err != nil {
			log.Printf("[S3] Auth failed: %s %s - %v", r.Method, r.URL.Path, err)
			writeS3Error(w, "AccessDenied", "Access Denied", r.URL.Path)
			return
		}
	}

	if bucket != "" && !isValidBucketName(bucket) {
		writeS3Error(w, "InvalidBucketName", "The specified bucket is not valid.", "/"+bucket)
		return
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

func (rt *Router) resolveBucketAndKey(r *http.Request) (string, string) {
	host := r.Host
	if strings.Contains(host, ":") {
		h, _, err := net.SplitHostPort(host)
		if err == nil {
			host = h
		}
	}

	path := strings.TrimPrefix(r.URL.Path, "/")

	if rt.domain != "" && strings.HasSuffix(host, "."+rt.domain) {
		bucket := strings.TrimSuffix(host, "."+rt.domain)
		if bucket != "" && !strings.Contains(bucket, ".") {
			return bucket, path
		}
	}

	// Fallback to path style routing
	parts := strings.SplitN(path, "/", 2)
	bucket := ""
	key := ""
	if len(parts) > 0 {
		bucket = parts[0]
	}
	if len(parts) > 1 {
		key = parts[1]
	}
	return bucket, key
}

func (rt *Router) handleCORS(w http.ResponseWriter, r *http.Request, bucket string) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return false
	}

	cors, err := rt.engine.GetBucketCORS(bucket)
	if err != nil || cors == nil {
		return false
	}

	headers, matched := EvaluateCORS(r, cors)
	if matched {
		for k, v := range headers {
			w.Header().Set(k, v)
		}
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
		}
		return true
	}
	return false
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
		if query.Has("cors") {
			rt.handleGetBucketCORS(w, r, bucket)
		} else if query.Has("versioning") {
			rt.handleGetBucketVersioning(w, r, bucket)
		} else if query.Get("location") != "" || query.Has("location") {
			rt.handleGetBucketLocation(w, r, bucket)
		} else {
			// ListObjectsV2
			rt.handleListObjectsV2(w, r, bucket)
		}
	case http.MethodPut:
		if query.Has("cors") {
			rt.handlePutBucketCORS(w, r, bucket)
		} else if query.Has("versioning") {
			rt.handlePutBucketVersioning(w, r, bucket)
		} else {
			rt.handleCreateBucket(w, r, bucket)
		}
	case http.MethodDelete:
		if query.Has("cors") {
			rt.handleDeleteBucketCORS(w, r, bucket)
		} else {
			rt.handleDeleteBucket(w, r, bucket)
		}
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

func (rt *Router) handleGetBucketCORS(w http.ResponseWriter, _ *http.Request, bucket string) {
	cors, err := rt.engine.GetBucketCORS(bucket)
	if handleStorageError(w, err, "/"+bucket) {
		return
	}

	if cors == nil {
		writeS3Error(w, "NoSuchCORSConfiguration", "The CORS configuration does not exist", "/"+bucket)
		return
	}

	// Map storage CORS to XML CORS
	var rulesXML []CORSRuleXML
	for _, rule := range cors.CORSRules {
		rulesXML = append(rulesXML, CORSRuleXML{
			AllowedHeader: rule.AllowedHeaders,
			AllowedMethod: rule.AllowedMethods,
			AllowedOrigin: rule.AllowedOrigins,
			ExposeHeader:  rule.ExposeHeaders,
			MaxAgeSeconds: rule.MaxAgeSeconds,
		})
	}

	result := CORSConfigurationXML{
		Xmlns:     s3XmlNamespace,
		CORSRules: rulesXML,
	}

	writeXML(w, http.StatusOK, result)
}

func (rt *Router) handlePutBucketCORS(w http.ResponseWriter, r *http.Request, bucket string) {
	var reqBody CORSConfigurationXML
	if err := xml.NewDecoder(r.Body).Decode(&reqBody); err != nil {
		writeS3Error(w, "MalformedXML", "The XML you provided was not well-formed", "/"+bucket)
		return
	}

	// Map XML CORS to storage CORS
	var rules []storage.CORSRule
	for _, rule := range reqBody.CORSRules {
		rules = append(rules, storage.CORSRule{
			AllowedHeaders: rule.AllowedHeader,
			AllowedMethods: rule.AllowedMethod,
			AllowedOrigins: rule.AllowedOrigin,
			ExposeHeaders:  rule.ExposeHeader,
			MaxAgeSeconds:  rule.MaxAgeSeconds,
		})
	}

	cors := &storage.CORSConfiguration{
		CORSRules: rules,
	}

	err := rt.engine.PutBucketCORS(bucket, cors)
	if handleStorageError(w, err, "/"+bucket) {
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (rt *Router) handleDeleteBucketCORS(w http.ResponseWriter, _ *http.Request, bucket string) {
	err := rt.engine.DeleteBucketCORS(bucket)
	if handleStorageError(w, err, "/"+bucket) {
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
