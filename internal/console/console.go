package console

import (
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"log"
	"mime"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/salvatorecorvaglia/objectra/internal/auth"
	"github.com/salvatorecorvaglia/objectra/internal/storage"
)

// bucketNameRegexp validates S3 bucket naming rules: 3-63 chars, lowercase letters, numbers, hyphens, periods.
var bucketNameRegexp = regexp.MustCompile(`^[a-z0-9][a-z0-9.\-]{1,61}[a-z0-9]$`)

//go:embed static/*
var staticFiles embed.FS

// Handler serves the web console UI and its REST API.
type Handler struct {
	engine storage.Engine
	creds  *auth.Credentials
	s3Port int
	region string
	mux    *http.ServeMux
}

// NewHandler creates a new console handler.
func NewHandler(engine storage.Engine, creds *auth.Credentials, s3Port int, region string) *Handler {
	h := &Handler{
		engine: engine,
		creds:  creds,
		s3Port: s3Port,
		region: region,
		mux:    http.NewServeMux(),
	}
	h.setupRoutes()
	return h
}

func (h *Handler) setupRoutes() {
	// API routes
	h.mux.HandleFunc("/api/login", h.handleLogin)
	h.mux.HandleFunc("/api/buckets", h.authMiddleware(h.handleBuckets))
	h.mux.HandleFunc("/api/buckets/", h.authMiddleware(h.handleBucketObjects))

	// Static files (embedded frontend)
	staticFS, _ := fs.Sub(staticFiles, "static")
	fileServer := http.FileServer(http.FS(staticFS))
	h.mux.Handle("/", fileServer)
}

// ServeHTTP implements the http.Handler interface.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Set CORS headers for console
	// CORS: Only allow requests from the same origin.
	// In production, this should be set to the actual console URL.
	origin := r.Header.Get("Origin")
	if origin != "" {
		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Set("Vary", "Origin")
	}
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
	w.Header().Set("Access-Control-Max-Age", "3600")

	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	h.mux.ServeHTTP(w, r)
}

// authMiddleware wraps a handler with JWT authentication.
func (h *Handler) authMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" || !strings.HasPrefix(authHeader, "Bearer ") {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
			return
		}

		token := strings.TrimPrefix(authHeader, "Bearer ")
		_, err := ValidateToken(token)
		if err != nil {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid token"})
			return
		}

		next(w, r)
	}
}

// handleLogin handles POST /api/login.
func (h *Handler) handleLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}

	var req struct {
		AccessKey string `json:"accessKey"`
		SecretKey string `json:"secretKey"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request"})
		return
	}

	if req.AccessKey != h.creds.AccessKey || req.SecretKey != h.creds.SecretKey {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid credentials"})
		return
	}

	token, err := GenerateToken(req.AccessKey)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to generate token"})
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"token": token})
}

// handleBuckets handles /api/buckets (GET = list, POST = create).
func (h *Handler) handleBuckets(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		h.listBuckets(w, r)
	case http.MethodPost:
		h.createBucket(w, r)
	default:
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
	}
}

func (h *Handler) listBuckets(w http.ResponseWriter, _ *http.Request) {
	buckets, err := h.engine.ListBuckets()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	type bucketResp struct {
		Name         string `json:"name"`
		CreationDate string `json:"creationDate"`
		ObjectCount  int    `json:"objectCount"`
	}

	var result []bucketResp
	for _, b := range buckets {
		count, _ := h.engine.CountObjects(b.Name)
		result = append(result, bucketResp{
			Name:         b.Name,
			CreationDate: b.CreationDate.UTC().Format("2006-01-02T15:04:05.000Z"),
			ObjectCount:  count,
		})
	}

	if result == nil {
		result = []bucketResp{}
	}

	writeJSON(w, http.StatusOK, result)
}

func (h *Handler) createBucket(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request"})
		return
	}

	// Validate bucket name (SEC-7)
	if !bucketNameRegexp.MatchString(req.Name) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid bucket name: must be 3-63 characters, lowercase letters, numbers, hyphens and periods"})
		return
	}

	if err := h.engine.CreateBucket(req.Name); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusCreated, map[string]string{"name": req.Name})
}

// handleBucketObjects handles /api/buckets/<name>/... routes.
func (h *Handler) handleBucketObjects(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/buckets/")
	parts := strings.SplitN(path, "/", 2)
	bucketName := parts[0]

	// Validate bucket name (SEC-7 / Traversal Protection)
	if !bucketNameRegexp.MatchString(bucketName) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid bucket name"})
		return
	}

	// Check if this is a bucket delete or object operations
	if len(parts) == 1 || parts[1] == "" {
		if r.Method == http.MethodDelete {
			h.deleteBucket(w, r, bucketName)
			return
		}
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}

	action := parts[1]

	switch {
	case action == "objects" && r.Method == http.MethodGet:
		h.listObjects(w, r, bucketName)
	case action == "objects/presign" && r.Method == http.MethodGet:
		h.handlePresignObject(w, r, bucketName)
	case action == "objects/upload" && r.Method == http.MethodPost:
		h.uploadObject(w, r, bucketName)
	case action == "objects/download" && r.Method == http.MethodGet:
		h.downloadObject(w, r, bucketName)
	case action == "objects" && r.Method == http.MethodDelete:
		h.deleteObject(w, r, bucketName)
	case action == "multipart/initiate" && r.Method == http.MethodPost:
		h.initiateMultipart(w, r, bucketName)
	case action == "multipart/upload-part" && r.Method == http.MethodPost:
		h.uploadPart(w, r, bucketName)
	case action == "multipart/complete" && r.Method == http.MethodPost:
		h.completeMultipart(w, r, bucketName)
	case action == "multipart/abort" && r.Method == http.MethodPost:
		h.abortMultipart(w, r, bucketName)
	default:
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "not found"})
	}
}

func (h *Handler) deleteBucket(w http.ResponseWriter, _ *http.Request, name string) {
	if err := h.engine.DeleteBucket(name); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"deleted": name})
}

func (h *Handler) listObjects(w http.ResponseWriter, r *http.Request, bucket string) {
	prefix := r.URL.Query().Get("prefix")
	delimiter := r.URL.Query().Get("delimiter")
	if delimiter == "" {
		delimiter = "/"
	}

	maxKeysStr := r.URL.Query().Get("maxKeys")
	maxKeys := 1000
	if maxKeysStr != "" {
		if mk, err := strconv.Atoi(maxKeysStr); err == nil && mk > 0 {
			maxKeys = mk
		}
	}

	continuationToken := r.URL.Query().Get("continuationToken")

	effectivePrefix := prefix
	effectiveDelimiter := delimiter
	if searchPrefix := r.URL.Query().Get("searchPrefix"); searchPrefix != "" {
		effectivePrefix = prefix + searchPrefix
		effectiveDelimiter = "" // Recursive search
	}

	output, err := h.engine.ListObjects(&storage.ListObjectsInput{
		Bucket:            bucket,
		Prefix:            effectivePrefix,
		Delimiter:         effectiveDelimiter,
		MaxKeys:           maxKeys,
		ContinuationToken: continuationToken,
	})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	type objectResp struct {
		Key          string `json:"key"`
		Size         int64  `json:"size"`
		LastModified string `json:"lastModified"`
		ETag         string `json:"etag"`
		IsPrefix     bool   `json:"isPrefix"`
	}

	var items []objectResp

	// Add common prefixes (folders)
	for _, p := range output.CommonPrefixes {
		items = append(items, objectResp{
			Key:      p,
			IsPrefix: true,
		})
	}

	// Add objects
	for _, obj := range output.Objects {
		items = append(items, objectResp{
			Key:          obj.Key,
			Size:         obj.Size,
			LastModified: obj.LastModified.UTC().Format("2006-01-02T15:04:05.000Z"),
			ETag:         obj.ETag,
			IsPrefix:     false,
		})
	}

	if items == nil {
		items = []objectResp{}
	}

	type listObjectsResp struct {
		Items                 []objectResp `json:"items"`
		IsTruncated           bool         `json:"isTruncated"`
		NextContinuationToken string       `json:"nextContinuationToken,omitempty"`
	}

	writeJSON(w, http.StatusOK, listObjectsResp{
		Items:                 items,
		IsTruncated:           output.IsTruncated,
		NextContinuationToken: output.NextContinuationToken,
	})
}

func (h *Handler) uploadObject(w http.ResponseWriter, r *http.Request, bucket string) {
	// Parse multipart form (max 32MB in memory; larger files spill to temp disk)
	if err := r.ParseMultipartForm(32 << 20); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "failed to parse upload"})
		return
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "no file provided"})
		return
	}
	defer file.Close()

	// Get the key (path) for the object
	key := r.FormValue("key")
	if key == "" {
		key = header.Filename
	}

	contentType := header.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	info, err := h.engine.PutObject(bucket, key, file, header.Size, contentType)
	if err != nil {
		log.Printf("[Console] Upload error: %v", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"key":  info.Key,
		"size": info.Size,
		"etag": info.ETag,
	})
}

func (h *Handler) downloadObject(w http.ResponseWriter, r *http.Request, bucket string) {
	key := r.URL.Query().Get("key")
	if key == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "missing key parameter"})
		return
	}

	reader, info, err := h.engine.GetObject(bucket, key)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "object not found"})
		return
	}
	defer reader.Close()

	// Extract filename from key and sanitize for Content-Disposition header (SEC-6)
	parts := strings.Split(key, "/")
	filename := parts[len(parts)-1]

	w.Header().Set("Content-Type", info.ContentType)
	w.Header().Set("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": filename}))
	w.WriteHeader(http.StatusOK)
	io.Copy(w, reader)
}

func (h *Handler) deleteObject(w http.ResponseWriter, r *http.Request, bucket string) {
	key := r.URL.Query().Get("key")
	if key == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "missing key parameter"})
		return
	}

	if err := h.engine.DeleteObject(bucket, key); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"deleted": key})
}

func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func (h *Handler) initiateMultipart(w http.ResponseWriter, r *http.Request, bucket string) {
	var req struct {
		Key         string `json:"key"`
		ContentType string `json:"contentType"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request"})
		return
	}
	if req.Key == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "missing key"})
		return
	}
	if req.ContentType == "" {
		req.ContentType = "application/octet-stream"
	}

	info, err := h.engine.CreateMultipartUpload(bucket, req.Key, req.ContentType)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"uploadId": info.UploadID,
		"key":      info.Key,
	})
}

func (h *Handler) uploadPart(w http.ResponseWriter, r *http.Request, bucket string) {
	if err := r.ParseMultipartForm(10 << 20); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "failed to parse upload"})
		return
	}

	uploadID := r.FormValue("uploadId")
	key := r.FormValue("key")
	partNumberStr := r.FormValue("partNumber")

	partNumber, err := strconv.Atoi(partNumberStr)
	if err != nil || partNumber < 1 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid part number"})
		return
	}

	file, _, err := r.FormFile("file")
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "no file chunk provided"})
		return
	}
	defer file.Close()

	seeker, ok := file.(io.Seeker)
	var size int64
	if ok {
		size, _ = seeker.Seek(0, io.SeekEnd)
		seeker.Seek(0, io.SeekStart)
	} else {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid file stream"})
		return
	}

	partInfo, err := h.engine.UploadPart(bucket, key, uploadID, partNumber, file, size)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"etag":       partInfo.ETag,
		"partNumber": partInfo.PartNumber,
	})
}

func (h *Handler) completeMultipart(w http.ResponseWriter, r *http.Request, bucket string) {
	var req struct {
		UploadID string `json:"uploadId"`
		Key      string `json:"key"`
		Parts    []struct {
			PartNumber int    `json:"partNumber"`
			ETag       string `json:"etag"`
		} `json:"parts"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request"})
		return
	}

	var s3Parts []storage.CompletePart
	for _, p := range req.Parts {
		s3Parts = append(s3Parts, storage.CompletePart{
			PartNumber: p.PartNumber,
			ETag:       p.ETag,
		})
	}

	info, err := h.engine.CompleteMultipartUpload(bucket, req.Key, req.UploadID, s3Parts)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"key":  info.Key,
		"size": info.Size,
		"etag": info.ETag,
	})
}

func (h *Handler) abortMultipart(w http.ResponseWriter, r *http.Request, bucket string) {
	var req struct {
		UploadID string `json:"uploadId"`
		Key      string `json:"key"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request"})
		return
	}

	err := h.engine.AbortMultipartUpload(bucket, req.Key, req.UploadID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "aborted"})
}

func (h *Handler) handlePresignObject(w http.ResponseWriter, r *http.Request, bucket string) {
	key := r.URL.Query().Get("key")
	if key == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "missing key parameter"})
		return
	}

	expiresStr := r.URL.Query().Get("expires")
	expiresSec := 3600 // Default 1 hour
	if expiresStr != "" {
		if val, err := strconv.Atoi(expiresStr); err == nil && val > 0 {
			expiresSec = val
		}
	}

	// Resolve the S3 host from the incoming request (replacing port)
	scheme := "http"
	if r.TLS != nil || r.Header.Get("X-Forwarded-Proto") == "https" {
		scheme = "https"
	}
	
	hostWithoutPort := r.Host
	if strings.Contains(r.Host, ":") {
		h, _, err := net.SplitHostPort(r.Host)
		if err == nil {
			hostWithoutPort = h
		}
	}

	// Build the S3 endpoint URL
	s3Endpoint := fmt.Sprintf("%s://%s:%d", scheme, hostWithoutPort, h.s3Port)

	presignedURL, err := auth.PresignGetObject(
		h.creds.AccessKey,
		h.creds.SecretKey,
		h.region,
		bucket,
		key,
		time.Duration(expiresSec)*time.Second,
		s3Endpoint,
	)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"url": presignedURL})
}
