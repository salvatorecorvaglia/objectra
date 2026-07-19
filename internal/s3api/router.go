package s3api

import (
	"context"
	"encoding/xml"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/salvatorecorvaglia/objectra/internal/auth"
	"github.com/salvatorecorvaglia/objectra/internal/storage"
)

type logTask struct {
	targetBucket string
	targetPrefix string
	logLine      string
}

// Router handles S3 API request routing and dispatching.
type Router struct {
	engine     storage.Engine
	verifier   *auth.SigV4Verifier
	region     string
	domain     string
	trustProxy bool
	logChan    chan logTask
	logWG      sync.WaitGroup
	activeReqs sync.WaitGroup
}

// NewRouter creates a new S3 API router.
func NewRouter(engine storage.Engine, creds *auth.Credentials, region string, domain string, trustProxy bool) *Router {
	rt := &Router{
		engine:     engine,
		verifier:   auth.NewSigV4Verifier(creds),
		region:     region,
		domain:     domain,
		trustProxy: trustProxy,
		logChan:    make(chan logTask, 1000),
	}
	rt.startLogWorkers()
	return rt
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
	rt.activeReqs.Add(1)
	defer rt.activeReqs.Done()

	start := time.Now()
	mrw := &metricsResponseWriter{ResponseWriter: w, statusCode: http.StatusOK}
	storage.GlobalMetrics.IncRequests()

	rt.serveHTTPInternal(mrw, r)

	if mrw.statusCode >= 400 {
		storage.GlobalMetrics.IncErrors()
	}

	duration := time.Since(start)
	bucket, key := rt.resolveBucketAndKey(r)
	rt.logAccess(r, bucket, key, mrw.statusCode, mrw.bytesWritten, duration)
}

func (rt *Router) logAccess(r *http.Request, bucket, key string, statusCode int, bytesSent int, duration time.Duration) {
	if bucket == "" {
		return
	}
	
	logging, err := rt.engine.GetBucketLogging(bucket)
	if err != nil || logging == nil || logging.LoggingEnabled == nil {
		return
	}

	targetBucket := logging.LoggingEnabled.TargetBucket
	targetPrefix := logging.LoggingEnabled.TargetPrefix

	if bucket == targetBucket && strings.HasPrefix(key, targetPrefix) {
		return // Skip to avoid infinite loop
	}

	owner := "objectra"
	timeStr := time.Now().Format("02/Jan/2006:15:04:05 -0700")
	remoteIP := r.RemoteAddr
	if ip, _, err := net.SplitHostPort(remoteIP); err == nil {
		remoteIP = ip
	}
	if rt.trustProxy {
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			if idx := strings.Index(xff, ","); idx != -1 {
				remoteIP = strings.TrimSpace(xff[:idx])
			} else {
				remoteIP = strings.TrimSpace(xff)
			}
		}
	}

	requester := "-"
	authHeader := r.Header.Get("Authorization")
	if strings.HasPrefix(authHeader, "AWS4-HMAC-SHA256") {
		parts := strings.Split(authHeader, ",")
		for _, part := range parts {
			part = strings.TrimSpace(part)
			if strings.HasPrefix(part, "Credential=") {
				cred := strings.TrimPrefix(part, "Credential=")
				credParts := strings.Split(cred, "/")
				if len(credParts) > 0 {
					requester = credParts[0]
				}
			}
		}
	}

	requestID := "objectra"
	var operation string
	if key != "" {
		operation = "REST." + r.Method + ".OBJECT"
	} else {
		operation = "REST." + r.Method + ".BUCKET"
	}

	reqURI := fmt.Sprintf("%s %s %s", r.Method, r.URL.RequestURI(), r.Proto)
	
	errCode := "-"
	if statusCode >= 400 {
		errCode = http.StatusText(statusCode)
	}

	referrer := r.Referer()
	if referrer == "" {
		referrer = "-"
	}

	userAgent := r.UserAgent()
	if userAgent == "" {
		userAgent = "-"
	}

	logLine := fmt.Sprintf("%s %s [%s] %s %s %s %s %s \"%s\" %d %s %d - %d %d \"%s\" \"%s\" -\n",
		owner, bucket, timeStr, remoteIP, requester, requestID, operation, key, reqURI, statusCode, errCode, bytesSent, duration.Milliseconds(), duration.Milliseconds(), referrer, userAgent,
	)

	task := logTask{
		targetBucket: targetBucket,
		targetPrefix: targetPrefix,
		logLine:      logLine,
	}
	select {
	case rt.logChan <- task:
	default:
		slog.Warn("[S3 API] Access log queue full, dropping log", "bucket", targetBucket)
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
			w.WriteHeader(http.StatusForbidden)
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
			slog.Warn("[S3] Auth failed", "method", r.Method, "path", r.URL.Path, "error", err)
			if authErr, ok := err.(*auth.AuthError); ok {
				writeS3Error(w, authErr.Code, authErr.Message, r.URL.Path)
			} else {
				writeS3Error(w, "AccessDenied", "Access Denied", r.URL.Path)
			}
			return
		}
	}

	if bucket != "" && !storage.IsValidBucketName(bucket) {
		writeS3Error(w, "InvalidBucketName", "The specified bucket is not valid.", "/"+bucket)
		return
	}

	slog.Info("[S3] Request received", "method", r.Method, "path", r.URL.Path, "bucket", bucket, "key", key)

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
		} else if query.Has("lifecycle") {
			rt.handleGetBucketLifecycle(w, r, bucket)
		} else if query.Has("logging") {
			rt.handleGetBucketLogging(w, r, bucket)
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
		} else if query.Has("lifecycle") {
			rt.handlePutBucketLifecycle(w, r, bucket)
		} else if query.Has("logging") {
			rt.handlePutBucketLogging(w, r, bucket)
		} else {
			rt.handleCreateBucket(w, r, bucket)
		}
	case http.MethodDelete:
		if query.Has("cors") {
			rt.handleDeleteBucketCORS(w, r, bucket)
		} else if query.Has("lifecycle") {
			rt.handleDeleteBucketLifecycle(w, r, bucket)
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

type bufferedLog struct {
	lines       []string
	lastCreated time.Time
}

func (rt *Router) startLogWorkers() {
	rt.logWG.Add(1)
	go func() {
		defer rt.logWG.Done()
		
		buffers := make(map[string]*bufferedLog) // key: targetBucket + "\x00" + targetPrefix
		flushInterval := 5 * time.Second
		if len(os.Args) > 0 && (strings.HasSuffix(os.Args[0], ".test") || strings.Contains(os.Args[0], "/_test/")) {
			flushInterval = 50 * time.Millisecond
		}
		ticker := time.NewTicker(flushInterval)
		defer ticker.Stop()

		flushBucketLogs := func(key string, blog *bufferedLog) {
			if len(blog.lines) == 0 {
				return
			}
			parts := strings.SplitN(key, "\x00", 2)
			targetBucket := parts[0]
			targetPrefix := ""
			if len(parts) > 1 {
				targetPrefix = parts[1]
			}

			logContent := strings.Join(blog.lines, "")
			logKey := fmt.Sprintf("%s%s_%s.log", targetPrefix, blog.lastCreated.Format("2006-01-02-15-04-05"), uuid.New().String()[:8])
			
			ctx := context.Background()
			reader := strings.NewReader(logContent)
			_, err := rt.engine.PutObject(ctx, targetBucket, logKey, reader, int64(len(logContent)), "text/plain")
			if err != nil {
				slog.Error("[S3 API] Failed to deliver aggregated access log", "bucket", targetBucket, "key", logKey, "error", err)
			}
			blog.lines = nil
		}

		flushAll := func() {
			for k, blog := range buffers {
				flushBucketLogs(k, blog)
			}
		}

		for {
			select {
			case task, ok := <-rt.logChan:
				if !ok {
					flushAll()
					return
				}

				key := task.targetBucket + "\x00" + task.targetPrefix
				blog, exists := buffers[key]
				if !exists {
					blog = &bufferedLog{
						lastCreated: time.Now().UTC(),
					}
					buffers[key] = blog
				}
				
				if len(blog.lines) == 0 {
					blog.lastCreated = time.Now().UTC()
				}
				blog.lines = append(blog.lines, task.logLine)

				if len(blog.lines) >= 100 {
					flushBucketLogs(key, blog)
				}

			case <-ticker.C:
				flushAll()
			}
		}
	}()
}

func (rt *Router) Close() {
	rt.activeReqs.Wait()
	close(rt.logChan)
	rt.logWG.Wait()
}
