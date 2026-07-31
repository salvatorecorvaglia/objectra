package storage

import (
	"bufio"
	"context"
	"compress/gzip"
	"crypto/aes"
	"crypto/cipher"
	"crypto/md5"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	bolt "go.etcd.io/bbolt"
)

const (
	errInvalidBucketTraversal = "invalid bucket name: path traversal detected"
	errInvalidKeyTraversal    = "invalid object key: path traversal detected"
	errInvalidUploadID        = "invalid upload ID: path traversal detected"
	errBucketNotFound         = "The specified bucket does not exist"
	errKeyNotFound            = "The specified key does not exist."
	errUploadNotFound         = "The specified multipart upload does not exist."
	errFailedGenerateIV       = "failed to generate random IV: %w"
	errFailedCreateAES        = "failed to create AES cipher: %w"
)

var (
	gzipWriterPool = sync.Pool{
		New: func() interface{} {
			w, _ := gzip.NewWriterLevel(nil, gzip.DefaultCompression)
			return w
		},
	}
	gzipReaderPool sync.Pool
)

func getGzipReader(r io.Reader) (*gzip.Reader, error) {
	if v := gzipReaderPool.Get(); v != nil {
		gr := v.(*gzip.Reader)
		if err := gr.Reset(r); err != nil {
			return nil, err
		}
		return gr, nil
	}
	return gzip.NewReader(r)
}

func putGzipReader(gr *gzip.Reader) {
	gzipReaderPool.Put(gr)
}

type pooledGzipReader struct {
	*gzip.Reader
}

func (p *pooledGzipReader) Close() error {
	err := p.Reader.Close()
	putGzipReader(p.Reader)
	return err
}

// FilesystemEngine implements the Engine interface using the local filesystem.
// Objects are stored as files under <dataDir>/buckets/<bucket>/<key>.
// Metadata is persisted in a bbolt database.
type FilesystemEngine struct {
	dataDir  string
	metadata *MetadataStore
	mu       sync.Mutex
	locks    map[string]*uploadLock

	// Sync replication queue
	syncConfig         *SyncConfig
	syncQueue          chan syncTask
	syncOnce           sync.Once
	syncWG             sync.WaitGroup
	isSyncShuttingDown int32
	syncMu             sync.Mutex
	syncClient         *http.Client

	// Webhook queue
	webhookURL            string
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
func NewFilesystemEngine(dataDir string, syncCfg *SyncConfig, webhookURL string) (*FilesystemEngine, error) {
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

	cleanupOrphanedTempFiles(dataDir)

	return &FilesystemEngine{
		dataDir:    dataDir,
		metadata:   meta,
		locks:      make(map[string]*uploadLock),
		syncConfig: syncCfg,
		syncClient: &http.Client{
			Timeout: 5 * time.Minute,
			Transport: &http.Transport{
				Proxy: http.ProxyFromEnvironment,
				DialContext: (&net.Dialer{
					Timeout:   30 * time.Second,
					KeepAlive: 30 * time.Second,
				}).DialContext,
				ForceAttemptHTTP2:     true,
				MaxIdleConns:          100,
				IdleConnTimeout:       90 * time.Second,
				TLSHandshakeTimeout:   10 * time.Second,
				ExpectContinueTimeout: 1 * time.Second,
				MaxIdleConnsPerHost:   10,
			},
		},
		webhookURL: webhookURL,
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



// objectPath returns the filesystem path for an object, with path traversal protection.






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
		return &S3Error{Code: "NoSuchBucket", Message: errBucketNotFound}
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
		return &S3Error{Code: "NoSuchBucket", Message: errBucketNotFound}
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
		return nil, &S3Error{Code: "NoSuchBucket", Message: errBucketNotFound}
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
		return &S3Error{Code: "NoSuchBucket", Message: errBucketNotFound}
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
		return 0, &S3Error{Code: "NoSuchBucket", Message: errBucketNotFound}
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
		return nil, &S3Error{Code: "NoSuchBucket", Message: errBucketNotFound}
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

	bucketDir := filepath.Clean(fs.bucketPath(bucket))
	rel, err := filepath.Rel(bucketDir, objPath)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || strings.HasPrefix(rel, "../") || strings.HasPrefix(rel, "..\\") || filepath.IsAbs(rel) {
		return nil, &S3Error{Code: "InvalidArgument", Message: errInvalidKeyTraversal}
	}
	objDir := filepath.Dir(objPath)
	relDir, err := filepath.Rel(bucketDir, objDir)
	if err != nil || relDir == ".." || strings.HasPrefix(relDir, ".."+string(filepath.Separator)) || strings.HasPrefix(relDir, "../") || strings.HasPrefix(relDir, "..\\") || filepath.IsAbs(relDir) {
		return nil, &S3Error{Code: "InvalidArgument", Message: errInvalidKeyTraversal}
	}

	// Check for flat namespace directory/file path conflicts
	if err := fs.checkPathConflict(objPath, bucket); err != nil {
		return nil, err
	}

	// Ensure parent directory exists (for nested keys like "photos/2024/img.jpg")
	if err := os.MkdirAll(objDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create object directory: %w", err)
	}

	// Write to a temp file first, then rename for atomicity
	tmpFile, err := os.CreateTemp(objDir, ".stiva-tmp-*")
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
			return nil, fmt.Errorf(errFailedGenerateIV, err)
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
			return nil, fmt.Errorf(errFailedCreateAES, err)
		}
		stream := cipher.NewCTR(block, iv)
		out = &cipher.StreamWriter{S: stream, W: out}
	}

	var gzipWriter *gzip.Writer
	defer func() {
		if gzipWriter != nil {
			gzipWriterPool.Put(gzipWriter)
		}
	}()

	compressed := isCompressibleContentType(contentType)

	if compressed {
		gw := gzipWriterPool.Get().(*gzip.Writer)
		gw.Reset(out)
		gzipWriter = gw
		out = gw
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
		gzipWriterPool.Put(gzipWriter)
		gzipWriter = nil
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
		return nil, nil, &S3Error{Code: "NoSuchKey", Message: errKeyNotFound}
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
			return nil, nil, fmt.Errorf(errFailedCreateAES, err)
		}
		crs, err := newCTRReadSeeker(file, block, iv)
		if err != nil {
			file.Close()
			return nil, nil, fmt.Errorf("failed to initialize seekable decryption: %w", err)
		}
		reader = crs
	}

	if info.Compressed {
		gzipReader, err := getGzipReader(reader)
		if err != nil {
			file.Close()
			return nil, nil, fmt.Errorf("failed to initialize gzip reader: %w", err)
		}
		pReader := &pooledGzipReader{Reader: gzipReader}
		closers = append(closers, pReader)
		reader = pReader
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
		return nil, &S3Error{Code: "NoSuchKey", Message: errKeyNotFound}
	}

	if info.IsDeleteMarker {
		return info, &S3Error{Code: "NoSuchKey", Message: errKeyNotFound}
	}

	ssecParams := extractSSECParams(ctx)
	if err := validateSSECParams(info, ssecParams); err != nil {
		return info, err
	}

	return info, nil
}

// DeleteObject removes an object version or creates a delete marker.
func (fs *FilesystemEngine) DeleteObject(bucket, key, versionID string) (isDeleteMarker bool, delVersionID string, err error) {
	if err := fs.validateBucketName(bucket); err != nil {
		return false, "", err
	}

	exists, err := fs.metadata.BucketExists(bucket)
	if err != nil {
		return false, "", err
	}
	if !exists {
		return false, "", &S3Error{Code: "NoSuchBucket", Message: errBucketNotFound}
	}

	versionStatus, err := fs.metadata.GetBucketVersioning(bucket)
	if err != nil {
		versionStatus = ""
	}

	if versionID != "" {
		objPath, err := fs.objectPathWithVersion(bucket, key, versionID)
		if err != nil {
			return false, "", &S3Error{Code: "InvalidArgument", Message: err.Error()}
		}
		os.Remove(objPath)
		fs.cleanupParentDirs(objPath, bucket)

		err = fs.metadata.DeleteObjectMeta(bucket, key, versionID)
		if err != nil {
			return false, "", err
		}

		info := &ObjectInfo{
			Bucket:    bucket,
			Key:       key,
			VersionID: versionID,
		}
		fs.triggerWebhook("ObjectRemoved:Delete", info)
		fs.MirrorSync(bucket, key, "DELETE")
		return false, "", nil
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
			return false, "", err
		}

		fs.triggerWebhook("ObjectRemoved:DeleteMarkerCreated", info)
		fs.MirrorSync(bucket, key, "DELETE")
		return true, delVersionID, nil
	}

	info, err := fs.metadata.GetObjectMeta(bucket, key, "")
	if err != nil {
		return false, "", nil
	}

	// Fetch all versions of the object to remove their files from disk
	versions, err := fs.metadata.GetObjectVersions(bucket, key)
	if err == nil {
		for _, v := range versions {
			objPath, err := fs.objectPathWithVersion(bucket, key, v.VersionID)
			if err == nil {
				os.Remove(objPath)
				fs.cleanupParentDirs(objPath, bucket)
			}
		}
	} else {
		// Fallback to deleting the latest version's file if GetObjectVersions fails
		objPath, err := fs.objectPathWithVersion(bucket, key, info.VersionID)
		if err == nil {
			os.Remove(objPath)
			fs.cleanupParentDirs(objPath, bucket)
		}
	}

	err = fs.metadata.DeleteObjectMeta(bucket, key, "")
	if err != nil {
		return false, "", err
	}

	fs.triggerWebhook("ObjectRemoved:Delete", info)
	fs.MirrorSync(bucket, key, "DELETE")
	return false, "", nil
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
		return "", &S3Error{Code: "NoSuchBucket", Message: errBucketNotFound}
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
		return &S3Error{Code: "NoSuchBucket", Message: errBucketNotFound}
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
		return &S3Error{Code: "NoSuchBucket", Message: errBucketNotFound}
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
		return false, &S3Error{Code: "NoSuchBucket", Message: errBucketNotFound}
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
		return nil, &S3Error{Code: "NoSuchBucket", Message: errBucketNotFound}
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
		if input.Delimiter != "" && strings.HasSuffix(startAfter, input.Delimiter) {
			seekKey = bucketPrefix + startAfter + "\xff"
		} else {
			seekKey = bucketPrefix + startAfter + "\x00"
		}
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


// CompleteMultipartUpload assembles all parts into the final object.

// AbortMultipartUpload cancels a multipart upload and cleans up parts.



// S3Error represents an S3 API error with a code and message.
type S3Error struct {
	Code    string
	Message string
}

func (e *S3Error) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// CleanExpiredMultipartUploads scans the database for multipart uploads older than cutoff duration and aborts them.

type readCloserWrapper struct {
	io.Reader
	closers []io.Closer
}

func (w *readCloserWrapper) Close() error {
	var firstErr error
	for i := len(w.closers) - 1; i >= 0; i-- {
		if err := w.closers[i].Close(); err != nil && firstErr == nil {
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

// GetBucketLifecycle gets the lifecycle configuration of a bucket.

// DeleteBucketLifecycle deletes the lifecycle configuration of a bucket.

// PutBucketLogging sets the logging configuration of a bucket.

// GetBucketLogging gets the logging configuration of a bucket.

// CleanExpiredObjects scans all buckets for objects that meet expiration rules and removes them.

func extractSSECParams(ctx context.Context) *SSECParams {
	if ctx == nil {
		return nil
	}
	if params, ok := ctx.Value(SSECContextKey).(*SSECParams); ok {
		return params
	}
	return nil
}

func validateSSECParams(info *ObjectInfo, params *SSECParams) error {
	if info.SSECustomerAlgorithm != "" {
		if params == nil {
			return &S3Error{
				Code:    "InvalidArgument",
				Message: "The object was stored using a Server-side Encryption with Customer-provided Keys (SSE-C) and cannot be retrieved without it",
			}
		}
		if subtle.ConstantTimeCompare([]byte(params.KeyMD5), []byte(info.SSECustomerKeyMD5)) != 1 {
			return &S3Error{
				Code:    "InvalidDigest",
				Message: "The customer-provided encryption key MD5 does not match",
			}
		}
	} else if params != nil {
		return &S3Error{
			Code:    "InvalidArgument",
			Message: "The object was not stored using a Server-side Encryption with Customer-provided Keys (SSE-C) and cannot be retrieved with it",
		}
	}
	return nil
}

func cleanupOrphanedTempFiles(dataDir string) {
	walkDirs := []string{
		filepath.Join(dataDir, "buckets"),
		filepath.Join(dataDir, "multipart"),
		filepath.Join(dataDir, "tmp"),
	}
	for _, dir := range walkDirs {
		_ = filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return nil
			}
			if !d.IsDir() {
				name := d.Name()
				if strings.HasPrefix(name, ".stiva-tmp-") || strings.HasPrefix(name, ".part-tmp-") || strings.HasPrefix(name, ".stiva-multipart-") || strings.HasPrefix(name, "stiva-body-") {
					_ = os.Remove(path)
				}
			}
			return nil
		})
	}
}




type ctrReadSeeker struct {
	file          *os.File
	block         cipher.Block
	iv            []byte
	currentOffset int64
	stream        cipher.Stream
}

func newCTRReadSeeker(file *os.File, block cipher.Block, iv []byte) (*ctrReadSeeker, error) {
	crs := &ctrReadSeeker{
		file:  file,
		block: block,
		iv:    iv,
	}
	_, err := crs.Seek(0, io.SeekStart)
	if err != nil {
		return nil, err
	}
	return crs, nil
}

func (crs *ctrReadSeeker) Read(p []byte) (int, error) {
	if crs.stream == nil {
		return 0, io.EOF
	}
	n, err := crs.file.Read(p)
	if n > 0 {
		crs.stream.XORKeyStream(p[:n], p[:n])
		crs.currentOffset += int64(n)
	}
	return n, err
}

func (crs *ctrReadSeeker) Seek(offset int64, whence int) (int64, error) {
	var target int64
	switch whence {
	case io.SeekStart:
		target = offset
	case io.SeekCurrent:
		target = crs.currentOffset + offset
	case io.SeekEnd:
		stat, err := crs.file.Stat()
		if err != nil {
			return 0, err
		}
		contentSize := stat.Size() - 16
		if contentSize < 0 {
			contentSize = 0
		}
		target = contentSize + offset
	default:
		return 0, fmt.Errorf("invalid whence: %d", whence)
	}

	if target < 0 {
		return 0, fmt.Errorf("negative seek offset: %d", target)
	}

	blockIndex := target / 16
	remainder := target % 16

	ctr := make([]byte, len(crs.iv))
	copy(ctr, crs.iv)
	addCtr(ctr, uint64(blockIndex))

	_, err := crs.file.Seek(16+blockIndex*16, io.SeekStart)
	if err != nil {
		return 0, err
	}

	crs.stream = cipher.NewCTR(crs.block, ctr)
	crs.currentOffset = target

	if remainder > 0 {
		discardBuf := make([]byte, remainder)
		_, err := io.ReadFull(crs, discardBuf)
		if err != nil {
			return 0, err
		}
	}

	return target, nil
}

func addCtr(ctr []byte, val uint64) {
	for i := len(ctr) - 1; i >= 0; i-- {
		val += uint64(ctr[i])
		ctr[i] = byte(val)
		val >>= 8
	}
}
