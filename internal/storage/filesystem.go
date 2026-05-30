package storage

import (
	"bufio"
	"context"
	"compress/gzip"
	"crypto/aes"
	"crypto/cipher"
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	bolt "go.etcd.io/bbolt"
)

// FilesystemEngine implements the Engine interface using the local filesystem.
// Objects are stored as files under <dataDir>/buckets/<bucket>/<key>.
// Metadata is persisted in a bbolt database.
type FilesystemEngine struct {
	dataDir  string
	metadata *MetadataStore
	mu       sync.Mutex
	locks    map[string]*uploadLock

	// Sync replication queue
	syncQueue          chan syncTask
	syncOnce           sync.Once
	syncWG             sync.WaitGroup
	isSyncShuttingDown int32
	syncMu             sync.Mutex

	// Webhook queue
	webhookQueue          chan webhookTask
	webhookOnce           sync.Once
	webhookWG             sync.WaitGroup
	isWebhookShuttingDown int32
	webhookMu             sync.Mutex
}

type uploadLock struct {
	sync.Mutex
	refCount int
}

// NewFilesystemEngine creates a new filesystem-backed storage engine.
func NewFilesystemEngine(dataDir string) (*FilesystemEngine, error) {
	bucketsDir := filepath.Join(dataDir, "buckets")
	if err := os.MkdirAll(bucketsDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create buckets directory: %w", err)
	}

	multipartDir := filepath.Join(dataDir, "multipart")
	if err := os.MkdirAll(multipartDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create multipart directory: %w", err)
	}

	meta, err := NewMetadataStore(dataDir)
	if err != nil {
		return nil, err
	}

	return &FilesystemEngine{
		dataDir:  dataDir,
		metadata: meta,
		locks:    make(map[string]*uploadLock),
	}, nil
}

// Close closes the underlying metadata store and stops workers.
func (fs *FilesystemEngine) Close() error {
	fs.StopSyncDispatcher()
	fs.StopWebhookDispatcher()
	return fs.metadata.Close()
}

func (fs *FilesystemEngine) GetSystemValue(key string) (string, error) {
	return fs.metadata.GetSystemValue(key)
}

func (fs *FilesystemEngine) PutSystemValue(key, val string) error {
	return fs.metadata.PutSystemValue(key, val)
}

func (fs *FilesystemEngine) bucketPath(name string) string {
	return filepath.Join(fs.dataDir, "buckets", name)
}

func (fs *FilesystemEngine) validatePathSafety(bucket, key, versionID string) error {
	if !filepath.IsLocal(bucket) {
		return fmt.Errorf("invalid bucket name: path traversal detected")
	}
	trimmedKey := strings.TrimLeft(key, "/\\")
	if trimmedKey != "" {
		if !filepath.IsLocal(filepath.FromSlash(trimmedKey)) {
			return fmt.Errorf("invalid object key: path traversal detected")
		}
	} else if key != "" {
		return fmt.Errorf("invalid object key: path traversal detected")
	}
	if versionID != "" {
		if !filepath.IsLocal(versionID) {
			return fmt.Errorf("invalid version ID: path traversal detected")
		}
		if strings.ContainsAny(versionID, "/\\") || strings.Contains(versionID, "..") {
			return fmt.Errorf("invalid version ID: path traversal detected")
		}
	}
	return nil
}

// objectPath returns the filesystem path for an object, with path traversal protection.
func (fs *FilesystemEngine) objectPath(bucket, key string) (string, error) {
	if err := fs.validatePathSafety(bucket, key, ""); err != nil {
		return "", err
	}

	base := fs.bucketPath(bucket)
	resolved := filepath.Join(base, filepath.FromSlash(key))
	// Ensure the resolved path stays within the bucket directory
	rel, err := filepath.Rel(base, resolved)
	if err != nil || !isSafeRelPath(rel) {
		return "", fmt.Errorf("invalid object key: path traversal detected")
	}
	return resolved, nil
}

func (fs *FilesystemEngine) objectPathWithVersion(bucket, key, versionID string) (string, error) {
	if err := fs.validatePathSafety(bucket, key, versionID); err != nil {
		return "", err
	}
	base, err := fs.objectPath(bucket, key)
	if err != nil {
		return "", err
	}
	resolved := base
	if versionID != "" {
		resolved = base + "." + versionID
	}
	resolved = filepath.Clean(resolved)
	// Ensure the resolved path stays within the bucket directory
	bucketBase := fs.bucketPath(bucket)
	rel, err := filepath.Rel(bucketBase, resolved)
	if err != nil || !isSafeRelPath(rel) {
		return "", fmt.Errorf("invalid object key or version: path traversal detected")
	}
	return resolved, nil
}

func (fs *FilesystemEngine) cleanupParentDirs(objPath string, bucket string) {
	if !filepath.IsLocal(bucket) {
		return
	}
	bucketDir := fs.bucketPath(bucket)
	rel, err := filepath.Rel(bucketDir, objPath)
	if err != nil || !isSafeRelPath(rel) {
		return
	}

	dir := filepath.Dir(objPath)
	for dir != bucketDir {
		checkRel, err := filepath.Rel(bucketDir, dir)
		if err != nil || !isSafeRelPath(checkRel) {
			break
		}

		entries, _ := os.ReadDir(dir)
		if len(entries) > 0 {
			break
		}
		os.Remove(dir)
		dir = filepath.Dir(dir)
	}
}

func (fs *FilesystemEngine) multipartUploadPath(bucket, uploadID string) (string, error) {
	if !filepath.IsLocal(bucket) {
		return "", fmt.Errorf("invalid bucket name: path traversal detected")
	}
	if !filepath.IsLocal(uploadID) {
		return "", fmt.Errorf("invalid upload ID: path traversal detected")
	}
	if strings.ContainsAny(uploadID, "/\\") || strings.Contains(uploadID, "..") {
		return "", fmt.Errorf("invalid upload ID: path traversal detected")
	}
	base := filepath.Join(fs.dataDir, "multipart", bucket)
	resolved := filepath.Join(base, uploadID)
	// Ensure the resolved path stays within the bucket's multipart directory
	rel, err := filepath.Rel(base, resolved)
	if err != nil || !isSafeRelPath(rel) {
		return "", fmt.Errorf("invalid upload ID: path traversal detected")
	}
	return resolved, nil
}

func (fs *FilesystemEngine) multipartDir(bucket, key, uploadID string) (string, error) {
	if !filepath.IsLocal(bucket) {
		return "", fmt.Errorf("invalid bucket name: path traversal detected")
	}
	trimmedKey := strings.TrimLeft(key, "/\\")
	if trimmedKey != "" {
		if !filepath.IsLocal(filepath.FromSlash(trimmedKey)) {
			return "", fmt.Errorf("invalid object key: path traversal detected")
		}
	} else if key != "" {
		return "", fmt.Errorf("invalid object key: path traversal detected")
	}
	if !filepath.IsLocal(uploadID) {
		return "", fmt.Errorf("invalid upload ID: path traversal detected")
	}
	uploadPath, err := fs.multipartUploadPath(bucket, uploadID)
	if err != nil {
		return "", err
	}
	resolved := filepath.Join(uploadPath, filepath.FromSlash(key))
	// Ensure the resolved path stays within the uploadPath directory
	rel, err := filepath.Rel(uploadPath, resolved)
	if err != nil || !isSafeRelPath(rel) {
		return "", fmt.Errorf("invalid object key: path traversal detected")
	}
	return resolved, nil
}

func isSafeRelPath(rel string) bool {
	if filepath.IsAbs(rel) {
		return false
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return false
	}
	return true
}

// CreateBucket creates a new storage bucket.
func (fs *FilesystemEngine) CreateBucket(name string) error {
	if err := fs.validateBucketName(name); err != nil {
		return err
	}

	exists, err := fs.metadata.BucketExists(name)
	if err != nil {
		return err
	}
	if exists {
		return &S3Error{Code: "BucketAlreadyOwnedByYou", Message: "Your previous request to create the named bucket succeeded and you already own it."}
	}

	if err := os.MkdirAll(fs.bucketPath(name), 0755); err != nil {
		return fmt.Errorf("failed to create bucket directory: %w", err)
	}

	return fs.metadata.PutBucket(&BucketInfo{
		Name:         name,
		CreationDate: time.Now().UTC(),
	})
}

// DeleteBucket deletes a bucket if it's empty.
func (fs *FilesystemEngine) DeleteBucket(name string) error {
	if err := fs.validateBucketName(name); err != nil {
		return err
	}

	exists, err := fs.metadata.BucketExists(name)
	if err != nil {
		return err
	}
	if !exists {
		return &S3Error{Code: "NoSuchBucket", Message: "The specified bucket does not exist"}
	}

	// Check if bucket has objects, delete markers, or active multipart uploads
	empty, err := fs.metadata.IsBucketEmpty(name)
	if err != nil {
		return err
	}
	if !empty {
		return &S3Error{Code: "BucketNotEmpty", Message: "The bucket you tried to delete is not empty"}
	}

	if err := os.RemoveAll(fs.bucketPath(name)); err != nil {
		return fmt.Errorf("failed to remove bucket directory: %w", err)
	}

	return fs.metadata.DeleteBucket(name)
}

// BucketExists checks if a bucket exists.
func (fs *FilesystemEngine) BucketExists(name string) (bool, error) {
	if err := fs.validateBucketName(name); err != nil {
		return false, err
	}
	return fs.metadata.BucketExists(name)
}

// ListBuckets returns all buckets.
func (fs *FilesystemEngine) ListBuckets() ([]BucketInfo, error) {
	return fs.metadata.ListBuckets()
}

// PutBucketCORS sets CORS configuration for a bucket.
func (fs *FilesystemEngine) PutBucketCORS(bucket string, cors *CORSConfiguration) error {
	if err := fs.validateBucketName(bucket); err != nil {
		return err
	}
	exists, err := fs.metadata.BucketExists(bucket)
	if err != nil {
		return err
	}
	if !exists {
		return &S3Error{Code: "NoSuchBucket", Message: "The specified bucket does not exist"}
	}
	return fs.metadata.PutBucketCORS(bucket, cors)
}

// GetBucketCORS gets CORS configuration for a bucket.
func (fs *FilesystemEngine) GetBucketCORS(bucket string) (*CORSConfiguration, error) {
	if err := fs.validateBucketName(bucket); err != nil {
		return nil, err
	}
	exists, err := fs.metadata.BucketExists(bucket)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, &S3Error{Code: "NoSuchBucket", Message: "The specified bucket does not exist"}
	}
	return fs.metadata.GetBucketCORS(bucket)
}

// DeleteBucketCORS deletes CORS configuration for a bucket.
func (fs *FilesystemEngine) DeleteBucketCORS(bucket string) error {
	if err := fs.validateBucketName(bucket); err != nil {
		return err
	}
	exists, err := fs.metadata.BucketExists(bucket)
	if err != nil {
		return err
	}
	if !exists {
		return &S3Error{Code: "NoSuchBucket", Message: "The specified bucket does not exist"}
	}
	return fs.metadata.DeleteBucketCORS(bucket)
}

// CountObjects returns the number of objects in a bucket without loading metadata.
func (fs *FilesystemEngine) CountObjects(bucket string) (int, error) {
	if err := fs.validateBucketName(bucket); err != nil {
		return 0, err
	}
	exists, err := fs.metadata.BucketExists(bucket)
	if err != nil {
		return 0, err
	}
	if !exists {
		return 0, &S3Error{Code: "NoSuchBucket", Message: "The specified bucket does not exist"}
	}
	return fs.metadata.CountObjectMetas(bucket)
}

// PutObject stores an object, streaming data directly to disk.
func (fs *FilesystemEngine) PutObject(ctx context.Context, bucket, key string, reader io.Reader, size int64, contentType string) (*ObjectInfo, error) {
	if err := fs.validateBucketName(bucket); err != nil {
		return nil, err
	}

	exists, err := fs.metadata.BucketExists(bucket)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, &S3Error{Code: "NoSuchBucket", Message: "The specified bucket does not exist"}
	}

	versionStatus, err := fs.metadata.GetBucketVersioning(bucket)
	if err != nil {
		versionStatus = ""
	}

	var versionID string
	switch versionStatus {
	case "Enabled":
		versionID = uuid.New().String()
	case "Suspended":
		versionID = "null"
	}

	objPath, err := fs.objectPathWithVersion(bucket, key, versionID)
	if err != nil {
		return nil, &S3Error{Code: "InvalidArgument", Message: err.Error()}
	}

	// Check for flat namespace directory/file path conflicts
	if err := fs.checkPathConflict(objPath, bucket); err != nil {
		return nil, err
	}

	// Ensure parent directory exists (for nested keys like "photos/2024/img.jpg")
	if err := os.MkdirAll(filepath.Dir(objPath), 0755); err != nil {
		return nil, fmt.Errorf("failed to create object directory: %w", err)
	}

	// Write to a temp file first, then rename for atomicity
	tmpFile, err := os.CreateTemp(filepath.Dir(objPath), ".objectra-tmp-*")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp file: %w", err)
	}
	tmpPath := tmpFile.Name()
	defer func() {
		tmpFile.Close()
		os.Remove(tmpPath) // Clean up on failure; no-op if already renamed
	}()

	// Extract SSECParams from context
	var ssecParams *SSECParams
	if ctx != nil {
		if params, ok := ctx.Value(SSECContextKey).(*SSECParams); ok && params != nil {
			ssecParams = params
		}
	}

	var iv []byte
	if ssecParams != nil {
		// Generate random IV
		iv = make([]byte, 16)
		if _, err := io.ReadFull(rand.Reader, iv); err != nil {
			return nil, fmt.Errorf("failed to generate random IV: %w", err)
		}
		if _, err := tmpFile.Write(iv); err != nil {
			return nil, fmt.Errorf("failed to write IV to file: %w", err)
		}
	}

	bufWriter := bufio.NewWriterSize(tmpFile, 64*1024)
	var out io.Writer = bufWriter

	if ssecParams != nil {
		block, err := aes.NewCipher(ssecParams.Key)
		if err != nil {
			return nil, fmt.Errorf("failed to create AES cipher: %w", err)
		}
		stream := cipher.NewCTR(block, iv)
		out = &cipher.StreamWriter{S: stream, W: out}
	}

	var gzipWriter *gzip.Writer
	compressed := false
	if isCompressibleContentType(contentType) {
		compressed = true
	}

	if compressed {
		gzipWriter = gzip.NewWriter(out)
		out = gzipWriter
	}

	// Stream data to disk while computing MD5
	hash := md5.New()
	written, err := io.Copy(io.MultiWriter(out, hash), reader)
	if err != nil {
		return nil, fmt.Errorf("failed to write object data: %w", err)
	}

	if gzipWriter != nil {
		if err := gzipWriter.Close(); err != nil {
			return nil, fmt.Errorf("failed to close gzip writer: %w", err)
		}
	}

	if err := bufWriter.Flush(); err != nil {
		return nil, fmt.Errorf("failed to flush buffer: %w", err)
	}

	// Validate size mismatch (truncated uploads)
	if size >= 0 && written != size {
		return nil, &S3Error{Code: "BadRequest", Message: fmt.Sprintf("Size mismatch: expected %d bytes, wrote %d bytes", size, written)}
	}

	if err := tmpFile.Close(); err != nil {
		return nil, fmt.Errorf("failed to close temp file: %w", err)
	}

	// Atomically move temp file to final location
	if err := os.Rename(tmpPath, objPath); err != nil {
		return nil, fmt.Errorf("failed to rename temp file: %w", err)
	}

	etag := hex.EncodeToString(hash.Sum(nil))
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	info := &ObjectInfo{
		Bucket:       bucket,
		Key:          key,
		Size:         written,
		ETag:         etag,
		ContentType:  contentType,
		LastModified: time.Now().UTC(),
		VersionID:    versionID,
		Compressed:   compressed,
	}

	if ssecParams != nil {
		info.SSECustomerAlgorithm = ssecParams.Algorithm
		info.SSECustomerKeyMD5 = ssecParams.KeyMD5
	}

	if err := fs.metadata.PutObjectMeta(info); err != nil {
		return nil, err
	}

	GlobalMetrics.AddUploaded(uint64(written))

	fs.triggerWebhook("ObjectCreated:Put", info)

	fs.MirrorSync(bucket, key, "PUT")

	return info, nil
}

// GetObject retrieves an object's data and metadata.
func (fs *FilesystemEngine) GetObject(ctx context.Context, bucket, key, versionID string) (io.ReadCloser, *ObjectInfo, error) {
	if err := fs.validateBucketName(bucket); err != nil {
		return nil, nil, err
	}

	info, err := fs.HeadObject(ctx, bucket, key, versionID)
	if err != nil {
		return nil, info, err
	}

	objPath, err := fs.objectPathWithVersion(bucket, key, info.VersionID)
	if err != nil {
		return nil, nil, &S3Error{Code: "InvalidArgument", Message: err.Error()}
	}

	file, err := os.Open(objPath)
	if err != nil {
		return nil, nil, &S3Error{Code: "NoSuchKey", Message: "The specified key does not exist."}
	}

	var closers []io.Closer
	closers = append(closers, file)
	var reader io.Reader = file

	var ssecParams *SSECParams
	if ctx != nil {
		if params, ok := ctx.Value(SSECContextKey).(*SSECParams); ok && params != nil {
			ssecParams = params
		}
	}

	if info.SSECustomerAlgorithm != "" && ssecParams != nil {
		iv := make([]byte, 16)
		if _, err := io.ReadFull(file, iv); err != nil {
			file.Close()
			return nil, nil, fmt.Errorf("failed to read IV: %w", err)
		}
		block, err := aes.NewCipher(ssecParams.Key)
		if err != nil {
			file.Close()
			return nil, nil, fmt.Errorf("failed to create AES cipher: %w", err)
		}
		stream := cipher.NewCTR(block, iv)
		reader = &cipher.StreamReader{S: stream, R: file}
	}

	if info.Compressed {
		gzipReader, err := gzip.NewReader(reader)
		if err != nil {
			file.Close()
			return nil, nil, fmt.Errorf("failed to initialize gzip reader: %w", err)
		}
		closers = append(closers, gzipReader)
		reader = gzipReader
	}

	rc := &readCloserWrapper{Reader: reader, closers: closers}
	if rs, ok := reader.(io.ReadSeeker); ok {
		return &metricsReadSeekCloser{
			metricsReadCloser: metricsReadCloser{ReadCloser: rc},
			seeker:            rs,
		}, info, nil
	}
	return &metricsReadCloser{ReadCloser: rc}, info, nil
}

// HeadObject retrieves object metadata without the data.
func (fs *FilesystemEngine) HeadObject(ctx context.Context, bucket, key, versionID string) (*ObjectInfo, error) {
	if err := fs.validateBucketName(bucket); err != nil {
		return nil, err
	}
	if _, err := fs.objectPathWithVersion(bucket, key, versionID); err != nil {
		return nil, &S3Error{Code: "InvalidArgument", Message: err.Error()}
	}

	info, err := fs.metadata.GetObjectMeta(bucket, key, versionID)
	if err != nil {
		return nil, &S3Error{Code: "NoSuchKey", Message: "The specified key does not exist."}
	}

	if info.IsDeleteMarker {
		return info, &S3Error{Code: "NoSuchKey", Message: "The specified key does not exist."}
	}

	// Extract SSECParams from context
	var ssecParams *SSECParams
	if ctx != nil {
		if params, ok := ctx.Value(SSECContextKey).(*SSECParams); ok && params != nil {
			ssecParams = params
		}
	}

	// 1. If object is encrypted but no SSE-C params provided
	if info.SSECustomerAlgorithm != "" && ssecParams == nil {
		return info, &S3Error{
			Code:    "InvalidArgument",
			Message: "The object was stored using a Server-side Encryption with Customer-provided Keys (SSE-C) and cannot be retrieved without it",
		}
	}

	// 2. If object is NOT encrypted but SSE-C params ARE provided
	if info.SSECustomerAlgorithm == "" && ssecParams != nil {
		return info, &S3Error{
			Code:    "InvalidArgument",
			Message: "The object was not stored using a Server-side Encryption with Customer-provided Keys (SSE-C) and cannot be retrieved with it",
		}
	}

	// 3. If object is encrypted and SSE-C params are provided, check MD5 match
	if info.SSECustomerAlgorithm != "" && ssecParams != nil {
		if ssecParams.KeyMD5 != info.SSECustomerKeyMD5 {
			return info, &S3Error{
				Code:    "InvalidDigest",
				Message: "The customer-provided encryption key MD5 does not match",
			}
		}
	}

	return info, nil
}

// DeleteObject removes an object version or creates a delete marker.
func (fs *FilesystemEngine) DeleteObject(bucket, key, versionID string) error {
	if err := fs.validateBucketName(bucket); err != nil {
		return err
	}

	exists, err := fs.metadata.BucketExists(bucket)
	if err != nil {
		return err
	}
	if !exists {
		return &S3Error{Code: "NoSuchBucket", Message: "The specified bucket does not exist"}
	}

	versionStatus, err := fs.metadata.GetBucketVersioning(bucket)
	if err != nil {
		versionStatus = ""
	}

	if versionID != "" {
		objPath, err := fs.objectPathWithVersion(bucket, key, versionID)
		if err != nil {
			return &S3Error{Code: "InvalidArgument", Message: err.Error()}
		}
		os.Remove(objPath)
		fs.cleanupParentDirs(objPath, bucket)

		err = fs.metadata.DeleteObjectMeta(bucket, key, versionID)
		if err != nil {
			return err
		}

		info := &ObjectInfo{
			Bucket:    bucket,
			Key:       key,
			VersionID: versionID,
		}
		fs.triggerWebhook("ObjectRemoved:Delete", info)
		fs.MirrorSync(bucket, key, "DELETE")
		return nil
	}

	if versionStatus == "Enabled" || versionStatus == "Suspended" {
		var delVersionID string
		switch versionStatus {
		case "Enabled":
			delVersionID = uuid.New().String()
		case "Suspended":
			delVersionID = "null"
			objPath, err := fs.objectPathWithVersion(bucket, key, "null")
			if err == nil {
				os.Remove(objPath)
				fs.cleanupParentDirs(objPath, bucket)
			}
		}

		info := &ObjectInfo{
			Bucket:         bucket,
			Key:            key,
			Size:           0,
			ETag:           "",
			ContentType:    "",
			LastModified:   time.Now().UTC(),
			VersionID:      delVersionID,
			IsDeleteMarker: true,
		}

		if err := fs.metadata.PutObjectMeta(info); err != nil {
			return err
		}

		fs.triggerWebhook("ObjectRemoved:DeleteMarkerCreated", info)
		fs.MirrorSync(bucket, key, "DELETE")
		return nil
	}

	info, err := fs.metadata.GetObjectMeta(bucket, key, "")
	if err != nil {
		return nil
	}

	objPath, err := fs.objectPathWithVersion(bucket, key, info.VersionID)
	if err != nil {
		return &S3Error{Code: "InvalidArgument", Message: err.Error()}
	}
	os.Remove(objPath)
	fs.cleanupParentDirs(objPath, bucket)

	err = fs.metadata.DeleteObjectMeta(bucket, key, "")
	if err != nil {
		return err
	}

	fs.triggerWebhook("ObjectRemoved:Delete", info)
	fs.MirrorSync(bucket, key, "DELETE")
	return nil
}

// CopyObject copies an object from one location to another.
func (fs *FilesystemEngine) CopyObject(srcBucket, srcKey, dstBucket, dstKey string) (*ObjectInfo, error) {
	if err := fs.validateBucketName(srcBucket); err != nil {
		return nil, err
	}
	if err := fs.validateBucketName(dstBucket); err != nil {
		return nil, err
	}

	var srcVersionID string
	if parts := strings.SplitN(srcKey, "?versionId=", 2); len(parts) == 2 {
		srcKey = parts[0]
		srcVersionID = parts[1]
	}

	reader, srcInfo, err := fs.GetObject(context.Background(), srcBucket, srcKey, srcVersionID)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	return fs.PutObject(context.Background(), dstBucket, dstKey, reader, srcInfo.Size, srcInfo.ContentType)
}

// GetBucketVersioning gets the versioning status of a bucket.
func (fs *FilesystemEngine) GetBucketVersioning(bucket string) (string, error) {
	if err := fs.validateBucketName(bucket); err != nil {
		return "", err
	}
	exists, err := fs.metadata.BucketExists(bucket)
	if err != nil {
		return "", err
	}
	if !exists {
		return "", &S3Error{Code: "NoSuchBucket", Message: "The specified bucket does not exist"}
	}
	return fs.metadata.GetBucketVersioning(bucket)
}

// SetBucketVersioning sets the versioning status of a bucket.
func (fs *FilesystemEngine) SetBucketVersioning(bucket, status string) error {
	if err := fs.validateBucketName(bucket); err != nil {
		return err
	}
	exists, err := fs.metadata.BucketExists(bucket)
	if err != nil {
		return err
	}
	if !exists {
		return &S3Error{Code: "NoSuchBucket", Message: "The specified bucket does not exist"}
	}
	if status != "Enabled" && status != "Suspended" && status != "Disabled" {
		return &S3Error{Code: "InvalidArgument", Message: "Versioning status must be Enabled, Suspended, or Disabled"}
	}
	return fs.metadata.PutBucketVersioning(bucket, status)
}

// SetBucketPublic sets the public status of a bucket.
func (fs *FilesystemEngine) SetBucketPublic(bucket string, public bool) error {
	if err := fs.validateBucketName(bucket); err != nil {
		return err
	}
	exists, err := fs.metadata.BucketExists(bucket)
	if err != nil {
		return err
	}
	if !exists {
		return &S3Error{Code: "NoSuchBucket", Message: "The specified bucket does not exist"}
	}
	return fs.metadata.SetBucketPublic(bucket, public)
}

// IsBucketPublic gets the public status of a bucket.
func (fs *FilesystemEngine) IsBucketPublic(bucket string) (bool, error) {
	if err := fs.validateBucketName(bucket); err != nil {
		return false, err
	}
	exists, err := fs.metadata.BucketExists(bucket)
	if err != nil {
		return false, err
	}
	if !exists {
		return false, &S3Error{Code: "NoSuchBucket", Message: "The specified bucket does not exist"}
	}
	return fs.metadata.IsBucketPublic(bucket)
}

// ListObjects lists objects in a bucket with prefix, delimiter, and pagination support.
// Performs cursor-based pagination and skip-scanning direct query inside bbolt transaction
// to avoid loading all keys in memory (OOM safety).
func (fs *FilesystemEngine) ListObjects(input *ListObjectsInput) (*ListObjectsOutput, error) {
	if err := fs.validateBucketName(input.Bucket); err != nil {
		return nil, err
	}

	exists, err := fs.metadata.BucketExists(input.Bucket)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, &S3Error{Code: "NoSuchBucket", Message: "The specified bucket does not exist"}
	}

	maxKeys := input.MaxKeys
	if maxKeys <= 0 {
		maxKeys = 1000
	}

	// Determine the start position
	startAfter := input.StartAfter
	if input.ContinuationToken != "" {
		startAfter = input.ContinuationToken
	}

	var objects []ObjectInfo
	var commonPrefixes []string
	commonPrefixSet := make(map[string]bool)

	bucketPrefix := input.Bucket + "\x00"

	seekKey := bucketPrefix
	if startAfter != "" {
		seekKey = bucketPrefix + startAfter + "\x00"
	} else if input.Prefix != "" {
		seekKey = bucketPrefix + input.Prefix
	}

	isTruncated := false
	nextToken := ""

	db, err := fs.metadata.getBucketDB(input.Bucket)
	if err != nil {
		return nil, err
	}

	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(objectsBucket)
		c := b.Cursor()

		k, v := c.Seek([]byte(seekKey))
		for k != nil {
			if !strings.HasPrefix(string(k), bucketPrefix) {
				break
			}

			objectKey := string(k[len(bucketPrefix):])

			if strings.Contains(objectKey, "\x00") {
				k, v = c.Next()
				continue
			}

			if startAfter != "" && objectKey <= startAfter {
				k, v = c.Next()
				continue
			}

			if input.Prefix != "" {
				if !strings.HasPrefix(objectKey, input.Prefix) {
					break
				}
			}

			var obj ObjectInfo
			if err := json.Unmarshal(v, &obj); err != nil {
				return err
			}
			if obj.IsDeleteMarker {
				k, v = c.Next()
				continue
			}

			if input.Delimiter != "" {
				remaining := objectKey[len(input.Prefix):]
				delimIdx := strings.Index(remaining, input.Delimiter)
				if delimIdx >= 0 {
					dirPrefix := input.Prefix + remaining[:delimIdx+len(input.Delimiter)]
					if !commonPrefixSet[dirPrefix] {
						if len(objects)+len(commonPrefixes) >= maxKeys {
							isTruncated = true
							break
						}
						commonPrefixSet[dirPrefix] = true
						commonPrefixes = append(commonPrefixes, dirPrefix)
					}
					nextSeekKey := bucketPrefix + dirPrefix + "\xff"
					k, v = c.Seek([]byte(nextSeekKey))
					continue
				}
			}

			if len(objects)+len(commonPrefixes) >= maxKeys {
				isTruncated = true
				break
			}

			objects = append(objects, obj)
			nextToken = obj.Key

			k, v = c.Next()
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	sort.Strings(commonPrefixes)

	if isTruncated && nextToken == "" && len(commonPrefixes) > 0 {
		nextToken = commonPrefixes[len(commonPrefixes)-1]
	}

	return &ListObjectsOutput{
		Objects:               objects,
		CommonPrefixes:        commonPrefixes,
		IsTruncated:           isTruncated,
		NextContinuationToken: nextToken,
		KeyCount:              len(objects) + len(commonPrefixes),
	}, nil
}

// CreateMultipartUpload initiates a multipart upload.
func (fs *FilesystemEngine) CreateMultipartUpload(bucket, key, contentType string) (*MultipartUploadInfo, error) {
	if err := fs.validateBucketName(bucket); err != nil {
		return nil, err
	}

	if _, err := fs.objectPath(bucket, key); err != nil {
		return nil, &S3Error{Code: "InvalidArgument", Message: err.Error()}
	}

	exists, err := fs.metadata.BucketExists(bucket)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, &S3Error{Code: "NoSuchBucket", Message: "The specified bucket does not exist"}
	}

	uploadID := uuid.New().String()
	partDir, err := fs.multipartDir(bucket, key, uploadID)
	if err != nil {
		return nil, &S3Error{Code: "InvalidArgument", Message: err.Error()}
	}
	if err := os.MkdirAll(partDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create multipart directory: %w", err)
	}

	meta := &MultipartMeta{
		UploadID:    uploadID,
		Bucket:      bucket,
		Key:         key,
		ContentType: contentType,
		Created:     time.Now().UTC(),
	}

	if err := fs.metadata.PutMultipartMeta(meta); err != nil {
		return nil, err
	}

	GlobalMetrics.IncActiveMultiparts()

	return &MultipartUploadInfo{
		UploadID: uploadID,
		Bucket:   bucket,
		Key:      key,
		Created:  meta.Created,
	}, nil
}

func (fs *FilesystemEngine) UploadPart(ctx context.Context, bucket, key, uploadID string, partNumber int, reader io.Reader, size int64) (*PartInfo, error) {
	if err := fs.validateBucketName(bucket); err != nil {
		return nil, err
	}

	if _, err := fs.objectPath(bucket, key); err != nil {
		return nil, &S3Error{Code: "InvalidArgument", Message: err.Error()}
	}

	unlock := fs.lockUpload(uploadID)
	defer unlock()

	meta, err := fs.metadata.GetMultipartMeta(bucket, key, uploadID)
	if err != nil {
		return nil, &S3Error{Code: "NoSuchUpload", Message: "The specified multipart upload does not exist."}
	}

	// Extract SSECParams from context
	var ssecParams *SSECParams
	if ctx != nil {
		if params, ok := ctx.Value(SSECContextKey).(*SSECParams); ok && params != nil {
			ssecParams = params
		}
	}

	// SSE-C validation
	if meta.SSECustomerAlgorithm != "" && ssecParams == nil {
		return nil, &S3Error{Code: "InvalidArgument", Message: "The object was stored using a Server-side Encryption with Customer-provided Keys (SSE-C) and cannot be retrieved without it"}
	}
	if meta.SSECustomerAlgorithm == "" && len(meta.Parts) > 0 && ssecParams != nil {
		return nil, &S3Error{Code: "InvalidArgument", Message: "The object was not stored using a Server-side Encryption with Customer-provided Keys (SSE-C) and cannot be retrieved with it"}
	}
	if meta.SSECustomerAlgorithm != "" && ssecParams != nil {
		if ssecParams.KeyMD5 != meta.SSECustomerKeyMD5 {
			return nil, &S3Error{Code: "InvalidDigest", Message: "The customer-provided encryption key MD5 does not match"}
		}
	}

	// First part upload with SSE-C: initialize parameters in metadata
	if meta.SSECustomerAlgorithm == "" && len(meta.Parts) == 0 && ssecParams != nil {
		meta.SSECustomerAlgorithm = ssecParams.Algorithm
		meta.SSECustomerKey = ssecParams.Key
		meta.SSECustomerKeyMD5 = ssecParams.KeyMD5
		if err := fs.metadata.PutMultipartMeta(meta); err != nil {
			return nil, err
		}
	}

	partDir, err := fs.multipartDir(bucket, key, uploadID)
	if err != nil {
		return nil, &S3Error{Code: "InvalidArgument", Message: err.Error()}
	}
	partPath := filepath.Join(partDir, fmt.Sprintf("part-%05d", partNumber))

	tmpFile, err := os.CreateTemp(partDir, ".part-tmp-*")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp part file: %w", err)
	}
	tmpPath := tmpFile.Name()
	defer func() {
		tmpFile.Close()
		os.Remove(tmpPath)
	}()

	var iv []byte
	if ssecParams != nil {
		iv = make([]byte, 16)
		if _, err := io.ReadFull(rand.Reader, iv); err != nil {
			return nil, fmt.Errorf("failed to generate random IV: %w", err)
		}
		if _, err := tmpFile.Write(iv); err != nil {
			return nil, fmt.Errorf("failed to write IV: %w", err)
		}
	}

	bufWriter := bufio.NewWriterSize(tmpFile, 64*1024)
	var out io.Writer = bufWriter

	if ssecParams != nil {
		block, err := aes.NewCipher(ssecParams.Key)
		if err != nil {
			return nil, fmt.Errorf("failed to create AES cipher: %w", err)
		}
		stream := cipher.NewCTR(block, iv)
		out = &cipher.StreamWriter{S: stream, W: out}
	}

	hash := md5.New()
	written, err := io.Copy(io.MultiWriter(out, hash), reader)
	if err != nil {
		return nil, fmt.Errorf("failed to write part data: %w", err)
	}

	if err := bufWriter.Flush(); err != nil {
		return nil, fmt.Errorf("failed to flush part buffer: %w", err)
	}

	if err := tmpFile.Close(); err != nil {
		return nil, fmt.Errorf("failed to close temp part file: %w", err)
	}

	if size >= 0 && written != size {
		return nil, &S3Error{Code: "BadRequest", Message: fmt.Sprintf("Size mismatch: expected %d bytes, wrote %d bytes", size, written)}
	}

	if err := os.Rename(tmpPath, partPath); err != nil {
		return nil, fmt.Errorf("failed to rename part file: %w", err)
	}

	etag := hex.EncodeToString(hash.Sum(nil))
	partInfo := PartInfo{
		PartNumber: partNumber,
		ETag:       etag,
		Size:       written,
	}

	// Update metadata with part info
	found := false
	for i, p := range meta.Parts {
		if p.PartNumber == partNumber {
			meta.Parts[i] = partInfo
			found = true
			break
		}
	}
	if !found {
		meta.Parts = append(meta.Parts, partInfo)
	}

	if err := fs.metadata.PutMultipartMeta(meta); err != nil {
		return nil, err
	}

	GlobalMetrics.AddUploaded(uint64(written))

	return &partInfo, nil
}

// CompleteMultipartUpload assembles all parts into the final object.
func (fs *FilesystemEngine) CompleteMultipartUpload(bucket, key, uploadID string, parts []CompletePart) (*ObjectInfo, error) {
	if err := fs.validateBucketName(bucket); err != nil {
		return nil, err
	}

	unlock := fs.lockUpload(uploadID)
	defer unlock()

	meta, err := fs.metadata.GetMultipartMeta(bucket, key, uploadID)
	if err != nil {
		return nil, &S3Error{Code: "NoSuchUpload", Message: "The specified multipart upload does not exist."}
	}

	// Sort requested parts by part number
	sort.Slice(parts, func(i, j int) bool {
		return parts[i].PartNumber < parts[j].PartNumber
	})

	// Create a map for quick lookup of uploaded parts
	uploadedParts := make(map[int]PartInfo)
	for _, p := range meta.Parts {
		uploadedParts[p.PartNumber] = p
	}

	disableMinSize := os.Getenv("OBJECTRA_DISABLE_MIN_PART_SIZE") == "true"

	for i, part := range parts {
		uPart, exists := uploadedParts[part.PartNumber]
		if !exists {
			return nil, &S3Error{Code: "InvalidPart", Message: fmt.Sprintf("One or more of the specified parts could not be found. Part %d was not uploaded.", part.PartNumber)}
		}

		uETag := strings.Trim(uPart.ETag, `"`)
		reqETag := strings.Trim(part.ETag, `"`)
		if uETag != reqETag {
			return nil, &S3Error{Code: "InvalidPart", Message: fmt.Sprintf("ETag mismatch for part %d. Expected %s, got %s.", part.PartNumber, uETag, reqETag)}
		}

		// Enforce minimum part size of 5MB for all parts except the last one
		if !disableMinSize && i < len(parts)-1 {
			if uPart.Size < 5*1024*1024 {
				return nil, &S3Error{
					Code:    "EntityTooSmall",
					Message: fmt.Sprintf("Your proposed upload is smaller than the minimum allowed size. Each part must be at least 5 MB in size, except the last part. Part %d is %d bytes.", part.PartNumber, uPart.Size),
				}
			}
		}
	}

	versionStatus, err := fs.metadata.GetBucketVersioning(bucket)
	if err != nil {
		versionStatus = ""
	}

	var versionID string
	switch versionStatus {
	case "Enabled":
		versionID = uuid.New().String()
	case "Suspended":
		versionID = "null"
	}

	objPath, err := fs.objectPathWithVersion(bucket, key, versionID)
	if err != nil {
		return nil, &S3Error{Code: "InvalidArgument", Message: err.Error()}
	}

	// Check for flat namespace directory/file path conflicts
	if err := fs.checkPathConflict(objPath, bucket); err != nil {
		return nil, err
	}

	if err := os.MkdirAll(filepath.Dir(objPath), 0755); err != nil {
		return nil, fmt.Errorf("failed to create object directory: %w", err)
	}

	// Use atomic temp-file + rename for crash safety
	tmpFile, err := os.CreateTemp(filepath.Dir(objPath), ".objectra-multipart-*")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp file: %w", err)
	}
	tmpPath := tmpFile.Name()
	defer func() {
		tmpFile.Close()
		os.Remove(tmpPath)
	}()

	var out io.Writer = tmpFile
	var gzipWriter *gzip.Writer

	if meta.SSECustomerAlgorithm != "" {
		newIV := make([]byte, 16)
		if _, err := io.ReadFull(rand.Reader, newIV); err != nil {
			return nil, fmt.Errorf("failed to generate random IV: %w", err)
		}
		if _, err := tmpFile.Write(newIV); err != nil {
			return nil, fmt.Errorf("failed to write IV: %w", err)
		}

		block, err := aes.NewCipher(meta.SSECustomerKey)
		if err != nil {
			return nil, fmt.Errorf("failed to create AES cipher: %w", err)
		}
		stream := cipher.NewCTR(block, newIV)
		out = &cipher.StreamWriter{S: stream, W: out}
	}

	compressed := isCompressibleContentType(meta.ContentType)
	if compressed {
		gzipWriter = gzip.NewWriter(out)
		out = gzipWriter
	}

	hash := md5.New()
	var totalSize int64
	partDir, err := fs.multipartDir(bucket, key, uploadID)
	if err != nil {
		return nil, &S3Error{Code: "InvalidArgument", Message: err.Error()}
	}

	for _, part := range parts {
		partPath := filepath.Join(partDir, fmt.Sprintf("part-%05d", part.PartNumber))
		partFile, err := os.Open(partPath)
		if err != nil {
			return nil, fmt.Errorf("part %d not found: %w", part.PartNumber, err)
		}

		var partReader io.Reader = partFile
		var closers []io.Closer
		closers = append(closers, partFile)

		if meta.SSECustomerAlgorithm != "" {
			partIV := make([]byte, 16)
			if _, err := io.ReadFull(partFile, partIV); err != nil {
				for _, c := range closers {
					c.Close()
				}
				return nil, fmt.Errorf("failed to read part IV: %w", err)
			}
			block, err := aes.NewCipher(meta.SSECustomerKey)
			if err != nil {
				for _, c := range closers {
					c.Close()
				}
				return nil, fmt.Errorf("failed to create AES cipher: %w", err)
			}
			stream := cipher.NewCTR(block, partIV)
			partReader = &cipher.StreamReader{S: stream, R: partFile}
		}

		n, err := io.Copy(io.MultiWriter(out, hash), partReader)
		for _, c := range closers {
			c.Close()
		}
		if err != nil {
			return nil, fmt.Errorf("failed to copy part %d: %w", part.PartNumber, err)
		}
		totalSize += n
	}

	if gzipWriter != nil {
		if err := gzipWriter.Close(); err != nil {
			return nil, fmt.Errorf("failed to close gzip writer: %w", err)
		}
	}

	if err := tmpFile.Close(); err != nil {
		return nil, fmt.Errorf("failed to close temp file: %w", err)
	}

	// Atomically move temp file to final location
	if err := os.Rename(tmpPath, objPath); err != nil {
		return nil, fmt.Errorf("failed to rename temp file: %w", err)
	}

	// Calculate S3-compliant multipart ETag
	var md5s []byte
	for _, part := range parts {
		b, err := hex.DecodeString(part.ETag)
		if err == nil && len(b) == 16 {
			md5s = append(md5s, b...)
		}
	}
	var etag string
	if len(md5s) > 0 {
		mHash := md5.New()
		mHash.Write(md5s)
		etag = fmt.Sprintf("%s-%d", hex.EncodeToString(mHash.Sum(nil)), len(parts))
	} else {
		etag = hex.EncodeToString(hash.Sum(nil))
	}

	contentType := meta.ContentType
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	info := &ObjectInfo{
		Bucket:       bucket,
		Key:          key,
		Size:         totalSize,
		ETag:         etag,
		ContentType:  contentType,
		LastModified: time.Now().UTC(),
		VersionID:    versionID,
		Compressed:   compressed,
	}

	if meta.SSECustomerAlgorithm != "" {
		info.SSECustomerAlgorithm = meta.SSECustomerAlgorithm
		info.SSECustomerKeyMD5 = meta.SSECustomerKeyMD5
	}

	if err := fs.metadata.PutObjectMeta(info); err != nil {
		return nil, err
	}

	// Clean up multipart temp files
	if uploadPath, err := fs.multipartUploadPath(bucket, uploadID); err == nil {
		os.RemoveAll(uploadPath)
	}
	fs.metadata.DeleteMultipartMeta(bucket, key, uploadID)

	GlobalMetrics.DecActiveMultiparts()

	fs.triggerWebhook("ObjectCreated:CompleteMultipartUpload", info)

	fs.MirrorSync(bucket, key, "PUT")

	return info, nil
}

// AbortMultipartUpload cancels a multipart upload and cleans up parts.
func (fs *FilesystemEngine) AbortMultipartUpload(bucket, key, uploadID string) error {
	if err := fs.validateBucketName(bucket); err != nil {
		return err
	}

	if _, err := fs.objectPath(bucket, key); err != nil {
		return &S3Error{Code: "InvalidArgument", Message: err.Error()}
	}

	unlock := fs.lockUpload(uploadID)
	defer unlock()

	_, err := fs.metadata.GetMultipartMeta(bucket, key, uploadID)
	if err != nil {
		return &S3Error{Code: "NoSuchUpload", Message: "The specified multipart upload does not exist."}
	}

	uploadPath, err := fs.multipartUploadPath(bucket, uploadID)
	if err != nil {
		return &S3Error{Code: "InvalidArgument", Message: err.Error()}
	}
	os.RemoveAll(uploadPath)
	err = fs.metadata.DeleteMultipartMeta(bucket, key, uploadID)
	if err == nil {
		GlobalMetrics.DecActiveMultiparts()
	}
	return err
}

func (fs *FilesystemEngine) checkPathConflict(objPath string, bucket string) error {
	// Check if the proposed object path is already a directory (file-directory conflict)
	fi, err := os.Stat(objPath)
	if err == nil && fi.IsDir() {
		return &S3Error{Code: "InvalidRequest", Message: "Object key name conflicts with an existing directory path."}
	}

	// Check if any parent path is a regular file (directory-file conflict)
	bucketDir := fs.bucketPath(bucket)
	dir := filepath.Dir(objPath)
	for dir != bucketDir {
		checkRel, err := filepath.Rel(bucketDir, dir)
		if err != nil || !isSafeRelPath(checkRel) {
			break
		}

		s, err := os.Stat(dir)
		if err == nil && !s.IsDir() {
			return &S3Error{Code: "InvalidRequest", Message: "Parent path conflicts with an existing object."}
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return nil
}

func (fs *FilesystemEngine) validateBucketName(name string) error {
	if !IsValidBucketName(name) {
		return &S3Error{Code: "InvalidBucketName", Message: "The specified bucket is not valid."}
	}
	return nil
}
func (fs *FilesystemEngine) lockUpload(uploadID string) func() {
	fs.mu.Lock()
	if fs.locks == nil {
		fs.locks = make(map[string]*uploadLock)
	}
	l, exists := fs.locks[uploadID]
	if !exists {
		l = &uploadLock{}
		fs.locks[uploadID] = l
	}
	l.refCount++
	fs.mu.Unlock()

	l.Lock()
	return func() {
		l.Unlock()
		fs.mu.Lock()
		l.refCount--
		if l.refCount == 0 {
			delete(fs.locks, uploadID)
		}
		fs.mu.Unlock()
	}
}

// S3Error represents an S3 API error with a code and message.
type S3Error struct {
	Code    string
	Message string
}

func (e *S3Error) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// CleanExpiredMultipartUploads scans the database for multipart uploads older than cutoff duration and aborts them.
func (fs *FilesystemEngine) CleanExpiredMultipartUploads(cutoff time.Duration) error {
	type uploadToAbort struct {
		bucket   string
		key      string
		uploadID string
	}
	var aborts []uploadToAbort

	buckets, err := fs.metadata.ListBuckets()
	if err != nil {
		return err
	}

	now := time.Now().UTC()
	for _, bInfo := range buckets {
		db, err := fs.metadata.getBucketDB(bInfo.Name)
		if err != nil {
			slog.Error("[Storage] Failed to open DB for bucket during multipart cleanup", "bucket", bInfo.Name, "error", err)
			continue
		}

		err = db.View(func(tx *bolt.Tx) error {
			b := tx.Bucket(multipartBucket)
			if b == nil {
				return nil
			}
			return b.ForEach(func(k, v []byte) error {
				var meta MultipartMeta
				if err := json.Unmarshal(v, &meta); err == nil {
					if now.Sub(meta.Created) > cutoff {
						aborts = append(aborts, uploadToAbort{
							bucket:   meta.Bucket,
							key:      meta.Key,
							uploadID: meta.UploadID,
						})
					}
				}
				return nil
			})
		})
		if err != nil {
			slog.Error("[Storage] Failed to scan multipart uploads for bucket", "bucket", bInfo.Name, "error", err)
		}
	}

	for _, u := range aborts {
		slog.Info("[Storage] Cleaning up expired multipart upload", "uploadID", u.uploadID, "bucket", u.bucket, "key", u.key)
		_ = fs.AbortMultipartUpload(u.bucket, u.key, u.uploadID)
	}

	return nil
}

type readCloserWrapper struct {
	io.Reader
	closers []io.Closer
}

func (w *readCloserWrapper) Close() error {
	var firstErr error
	for _, c := range w.closers {
		if err := c.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func isCompressibleContentType(contentType string) bool {
	contentType = strings.ToLower(strings.TrimSpace(contentType))
	if idx := strings.Index(contentType, ";"); idx != -1 {
		contentType = contentType[:idx]
		contentType = strings.TrimSpace(contentType)
	}
	if strings.HasPrefix(contentType, "text/") {
		return true
	}
	switch contentType {
	case "application/json", "application/xml", "application/javascript", "application/x-javascript", "image/svg+xml":
		return true
	}
	return false
}

type metricsReadCloser struct {
	io.ReadCloser
}

func (m *metricsReadCloser) Read(p []byte) (int, error) {
	n, err := m.ReadCloser.Read(p)
	GlobalMetrics.AddDownloaded(uint64(n))
	return n, err
}

type metricsReadSeekCloser struct {
	metricsReadCloser
	seeker io.ReadSeeker
}

func (m *metricsReadSeekCloser) Seek(offset int64, whence int) (int64, error) {
	return m.seeker.Seek(offset, whence)
}

// DataDir returns the path to the storage data directory.
func (fs *FilesystemEngine) DataDir() string {
	return fs.dataDir
}

// PutBucketLifecycle sets the lifecycle configuration of a bucket.
func (fs *FilesystemEngine) PutBucketLifecycle(bucket string, lc *LifecycleConfiguration) error {
	if err := fs.validateBucketName(bucket); err != nil {
		return err
	}
	exists, err := fs.metadata.BucketExists(bucket)
	if err != nil {
		return err
	}
	if !exists {
		return &S3Error{Code: "NoSuchBucket", Message: "The specified bucket does not exist"}
	}
	return fs.metadata.PutBucketLifecycle(bucket, lc)
}

// GetBucketLifecycle gets the lifecycle configuration of a bucket.
func (fs *FilesystemEngine) GetBucketLifecycle(bucket string) (*LifecycleConfiguration, error) {
	if err := fs.validateBucketName(bucket); err != nil {
		return nil, err
	}
	exists, err := fs.metadata.BucketExists(bucket)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, &S3Error{Code: "NoSuchBucket", Message: "The specified bucket does not exist"}
	}
	lc, err := fs.metadata.GetBucketLifecycle(bucket)
	if err != nil {
		return nil, err
	}
	if lc == nil {
		return nil, &S3Error{Code: "NoSuchLifecycleConfiguration", Message: "The lifecycle configuration does not exist"}
	}
	return lc, nil
}

// DeleteBucketLifecycle deletes the lifecycle configuration of a bucket.
func (fs *FilesystemEngine) DeleteBucketLifecycle(bucket string) error {
	if err := fs.validateBucketName(bucket); err != nil {
		return err
	}
	exists, err := fs.metadata.BucketExists(bucket)
	if err != nil {
		return err
	}
	if !exists {
		return &S3Error{Code: "NoSuchBucket", Message: "The specified bucket does not exist"}
	}
	return fs.metadata.DeleteBucketLifecycle(bucket)
}

// PutBucketLogging sets the logging configuration of a bucket.
func (fs *FilesystemEngine) PutBucketLogging(bucket string, logging *BucketLoggingStatus) error {
	if err := fs.validateBucketName(bucket); err != nil {
		return err
	}
	exists, err := fs.metadata.BucketExists(bucket)
	if err != nil {
		return err
	}
	if !exists {
		return &S3Error{Code: "NoSuchBucket", Message: "The specified bucket does not exist"}
	}
	return fs.metadata.PutBucketLogging(bucket, logging)
}

// GetBucketLogging gets the logging configuration of a bucket.
func (fs *FilesystemEngine) GetBucketLogging(bucket string) (*BucketLoggingStatus, error) {
	if err := fs.validateBucketName(bucket); err != nil {
		return nil, err
	}
	exists, err := fs.metadata.BucketExists(bucket)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, &S3Error{Code: "NoSuchBucket", Message: "The specified bucket does not exist"}
	}
	return fs.metadata.GetBucketLogging(bucket)
}

// CleanExpiredObjects scans all buckets for objects that meet expiration rules and removes them.
func (fs *FilesystemEngine) CleanExpiredObjects() error {
	buckets, err := fs.ListBuckets()
	if err != nil {
		return err
	}

	now := time.Now().UTC()
	for _, bInfo := range buckets {
		if bInfo.Lifecycle == nil || len(bInfo.Lifecycle.Rules) == 0 {
			continue
		}

		metas, err := fs.metadata.ListAllObjectMetas(bInfo.Name)
		if err != nil {
			slog.Error("[Storage] Failed to list object metas for bucket during cleanup", "bucket", bInfo.Name, "error", err)
			continue
		}

		versions, err := fs.metadata.ListAllObjectVersions(bInfo.Name)
		if err != nil {
			slog.Error("[Storage] Failed to list object versions for bucket during cleanup", "bucket", bInfo.Name, "error", err)
			continue
		}

		for _, rule := range bInfo.Lifecycle.Rules {
			if strings.ToLower(rule.Status) != "enabled" {
				continue
			}

			// 1. Current Version Expiration
			if rule.Expiration != nil && rule.Expiration.Days > 0 {
				cutoff := time.Duration(rule.Expiration.Days) * 24 * time.Hour
				for _, m := range metas {
					if !strings.HasPrefix(m.Key, rule.Filter.Prefix) {
						continue
					}
					// Check if expired
					if now.Sub(m.LastModified) > cutoff {
						if m.IsDeleteMarker {
							// If versioning is enabled and the only version left is a delete marker, expire it fully
							hasNoncurrent := false
							for _, v := range versions {
								if v.Key == m.Key && v.VersionID != m.VersionID {
									hasNoncurrent = true
									break
								}
							}
							if !hasNoncurrent {
								slog.Info("[Storage] Removing expired object delete marker", "key", m.Key, "bucket", bInfo.Name, "versionID", m.VersionID)
								_ = fs.DeleteObject(bInfo.Name, m.Key, m.VersionID)
							}
							continue
						}
						slog.Info("[Storage] Expiring current version of key in bucket", "key", m.Key, "bucket", bInfo.Name, "age", now.Sub(m.LastModified), "cutoff", cutoff)
						_ = fs.DeleteObject(bInfo.Name, m.Key, "")
					}
				}
			}

			// 2. Noncurrent Version Expiration
			if rule.NoncurrentVersionExpiration != nil && rule.NoncurrentVersionExpiration.NoncurrentDays > 0 {
				cutoff := time.Duration(rule.NoncurrentVersionExpiration.NoncurrentDays) * 24 * time.Hour
				for _, v := range versions {
					if v.IsLatest {
						continue // Only noncurrent versions
					}
					if !strings.HasPrefix(v.Key, rule.Filter.Prefix) {
						continue
					}
					if now.Sub(v.LastModified) > cutoff {
						slog.Info("[Storage] Expiring noncurrent version of key in bucket", "versionID", v.VersionID, "key", v.Key, "bucket", bInfo.Name, "age", now.Sub(v.LastModified), "cutoff", cutoff)
						_ = fs.DeleteObject(bInfo.Name, v.Key, v.VersionID)
					}
				}
			}

			// 3. Abort Incomplete Multipart Upload
			if rule.AbortIncompleteMultipartUpload != nil && rule.AbortIncompleteMultipartUpload.DaysAfterInitiation > 0 {
				cutoff := time.Duration(rule.AbortIncompleteMultipartUpload.DaysAfterInitiation) * 24 * time.Hour
				db, err := fs.metadata.getBucketDB(bInfo.Name)
				if err == nil {
					var aborts []struct {
						key      string
						uploadID string
					}
					_ = db.View(func(tx *bolt.Tx) error {
						b := tx.Bucket(multipartBucket)
						if b == nil {
							return nil
						}
						return b.ForEach(func(k, v []byte) error {
							var m MultipartMeta
							if err := json.Unmarshal(v, &m); err == nil {
								if strings.HasPrefix(m.Key, rule.Filter.Prefix) && now.Sub(m.Created) > cutoff {
									aborts = append(aborts, struct {
										key      string
										uploadID string
									}{key: m.Key, uploadID: m.UploadID})
								}
							}
							return nil
						})
					})
					for _, a := range aborts {
						slog.Info("[Storage] Aborting incomplete multipart upload of key in bucket", "uploadID", a.uploadID, "key", a.key, "bucket", bInfo.Name, "cutoff", cutoff)
						_ = fs.AbortMultipartUpload(bInfo.Name, a.key, a.uploadID)
					}
				}
			}
		}
	}
	return nil
}



