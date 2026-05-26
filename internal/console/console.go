package console

import (
	"embed"
	"encoding/json"
	"io"
	"io/fs"
	"log"
	"mime"
	"net/http"
	"regexp"
	"strings"

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
	mux    *http.ServeMux
}

// NewHandler creates a new console handler.
func NewHandler(engine storage.Engine, creds *auth.Credentials) *Handler {
	h := &Handler{
		engine: engine,
		creds:  creds,
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
	case action == "objects/upload" && r.Method == http.MethodPost:
		h.uploadObject(w, r, bucketName)
	case action == "objects/download" && r.Method == http.MethodGet:
		h.downloadObject(w, r, bucketName)
	case action == "objects" && r.Method == http.MethodDelete:
		h.deleteObject(w, r, bucketName)
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

	output, err := h.engine.ListObjects(&storage.ListObjectsInput{
		Bucket:    bucket,
		Prefix:    prefix,
		Delimiter: delimiter,
		MaxKeys:   1000,
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

	var result []objectResp

	// Add common prefixes (folders)
	for _, p := range output.CommonPrefixes {
		result = append(result, objectResp{
			Key:      p,
			IsPrefix: true,
		})
	}

	// Add objects
	for _, obj := range output.Objects {
		result = append(result, objectResp{
			Key:          obj.Key,
			Size:         obj.Size,
			LastModified: obj.LastModified.UTC().Format("2006-01-02T15:04:05.000Z"),
			ETag:         obj.ETag,
			IsPrefix:     false,
		})
	}

	if result == nil {
		result = []objectResp{}
	}

	writeJSON(w, http.StatusOK, result)
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
