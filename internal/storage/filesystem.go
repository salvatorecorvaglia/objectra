package storage

import (
	"crypto/md5"
	"encoding/json"
	"encoding/hex"
	"fmt"
	"io"
	"log"
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

	metaPath := filepath.Join(dataDir, "objectra.db")
	meta, err := NewMetadataStore(metaPath)
	if err != nil {
		return nil, err
	}

	return &FilesystemEngine{
		dataDir:  dataDir,
		metadata: meta,
		locks:    make(map[string]*uploadLock),
	}, nil
}

// Close closes the underlying metadata store.
func (fs *FilesystemEngine) Close() error {
	return fs.metadata.Close()
}

func (fs *FilesystemEngine) bucketPath(name string) string {
	return filepath.Join(fs.dataDir, "buckets", name)
}

// objectPath returns the filesystem path for an object, with path traversal protection.
func (fs *FilesystemEngine) objectPath(bucket, key string) (string, error) {
	base := fs.bucketPath(bucket)
	resolved := filepath.Join(base, filepath.FromSlash(key))
	// Ensure the resolved path stays within the bucket directory
	if !strings.HasPrefix(resolved, base+string(filepath.Separator)) && resolved != base {
		return "", fmt.Errorf("invalid object key: path traversal detected")
	}
	return resolved, nil
}

func (fs *FilesystemEngine) multipartDir(bucket, key, uploadID string) string {
	return filepath.Join(fs.dataDir, "multipart", bucket, uploadID, filepath.FromSlash(key))
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

	// Check if bucket has objects
	objects, err := fs.metadata.ListAllObjectMetas(name)
	if err != nil {
		return err
	}
	if len(objects) > 0 {
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
func (fs *FilesystemEngine) PutObject(bucket, key string, reader io.Reader, size int64, contentType string) (*ObjectInfo, error) {
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

	objPath, err := fs.objectPath(bucket, key)
	if err != nil {
		return nil, &S3Error{Code: "InvalidArgument", Message: err.Error()}
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

	// Stream data to disk while computing MD5
	hash := md5.New()
	written, err := io.Copy(io.MultiWriter(tmpFile, hash), reader)
	if err != nil {
		return nil, fmt.Errorf("failed to write object data: %w", err)
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
	}

	if err := fs.metadata.PutObjectMeta(info); err != nil {
		return nil, err
	}

	return info, nil
}

// GetObject retrieves an object's data and metadata.
func (fs *FilesystemEngine) GetObject(bucket, key string) (io.ReadCloser, *ObjectInfo, error) {
	if err := fs.validateBucketName(bucket); err != nil {
		return nil, nil, err
	}

	info, err := fs.metadata.GetObjectMeta(bucket, key)
	if err != nil {
		return nil, nil, &S3Error{Code: "NoSuchKey", Message: "The specified key does not exist."}
	}

	objPath, err := fs.objectPath(bucket, key)
	if err != nil {
		return nil, nil, &S3Error{Code: "InvalidArgument", Message: err.Error()}
	}

	file, err := os.Open(objPath)
	if err != nil {
		return nil, nil, &S3Error{Code: "NoSuchKey", Message: "The specified key does not exist."}
	}

	return file, info, nil
}

// HeadObject retrieves object metadata without the data.
func (fs *FilesystemEngine) HeadObject(bucket, key string) (*ObjectInfo, error) {
	if err := fs.validateBucketName(bucket); err != nil {
		return nil, err
	}

	info, err := fs.metadata.GetObjectMeta(bucket, key)
	if err != nil {
		return nil, &S3Error{Code: "NoSuchKey", Message: "The specified key does not exist."}
	}
	return info, nil
}

// DeleteObject removes an object.
func (fs *FilesystemEngine) DeleteObject(bucket, key string) error {
	if err := fs.validateBucketName(bucket); err != nil {
		return err
	}

	objPath, err := fs.objectPath(bucket, key)
	if err != nil {
		return &S3Error{Code: "InvalidArgument", Message: err.Error()}
	}
	os.Remove(objPath) // Best-effort file removal

	// Clean up empty parent directories
	dir := filepath.Dir(objPath)
	bucketDir := fs.bucketPath(bucket)
	for dir != bucketDir {
		entries, _ := os.ReadDir(dir)
		if len(entries) > 0 {
			break
		}
		os.Remove(dir)
		dir = filepath.Dir(dir)
	}

	return fs.metadata.DeleteObjectMeta(bucket, key)
}

// CopyObject copies an object from one location to another.
func (fs *FilesystemEngine) CopyObject(srcBucket, srcKey, dstBucket, dstKey string) (*ObjectInfo, error) {
	if err := fs.validateBucketName(srcBucket); err != nil {
		return nil, err
	}
	if err := fs.validateBucketName(dstBucket); err != nil {
		return nil, err
	}

	reader, srcInfo, err := fs.GetObject(srcBucket, srcKey)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	return fs.PutObject(dstBucket, dstKey, reader, srcInfo.Size, srcInfo.ContentType)
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

	err = fs.metadata.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(objectsBucket)
		c := b.Cursor()

		k, v := c.Seek([]byte(seekKey))
		for k != nil {
			if !strings.HasPrefix(string(k), bucketPrefix) {
				break
			}

			objectKey := string(k[len(bucketPrefix):])

			if startAfter != "" && objectKey <= startAfter {
				k, v = c.Next()
				continue
			}

			if input.Prefix != "" {
				if !strings.HasPrefix(objectKey, input.Prefix) {
					break
				}
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

			var obj ObjectInfo
			if err := json.Unmarshal(v, &obj); err != nil {
				return err
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

	exists, err := fs.metadata.BucketExists(bucket)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, &S3Error{Code: "NoSuchBucket", Message: "The specified bucket does not exist"}
	}

	uploadID := uuid.New().String()
	partDir := fs.multipartDir(bucket, key, uploadID)
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

	return &MultipartUploadInfo{
		UploadID: uploadID,
		Bucket:   bucket,
		Key:      key,
		Created:  meta.Created,
	}, nil
}

// UploadPart stores a single part of a multipart upload.
func (fs *FilesystemEngine) UploadPart(bucket, key, uploadID string, partNumber int, reader io.Reader, size int64) (*PartInfo, error) {
	if err := fs.validateBucketName(bucket); err != nil {
		return nil, err
	}

	unlock := fs.lockUpload(uploadID)
	defer unlock()

	meta, err := fs.metadata.GetMultipartMeta(bucket, key, uploadID)
	if err != nil {
		return nil, &S3Error{Code: "NoSuchUpload", Message: "The specified multipart upload does not exist."}
	}

	partDir := fs.multipartDir(bucket, key, uploadID)
	partPath := filepath.Join(partDir, fmt.Sprintf("part-%05d", partNumber))

	file, err := os.Create(partPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create part file: %w", err)
	}
	defer file.Close()

	hash := md5.New()
	written, err := io.Copy(io.MultiWriter(file, hash), reader)
	if err != nil {
		os.Remove(partPath)
		return nil, fmt.Errorf("failed to write part data: %w", err)
	}

	// Validate size mismatch (truncated uploads)
	if size >= 0 && written != size {
		os.Remove(partPath)
		return nil, &S3Error{Code: "BadRequest", Message: fmt.Sprintf("Size mismatch: expected %d bytes, wrote %d bytes", size, written)}
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

	objPath, err := fs.objectPath(bucket, key)
	if err != nil {
		return nil, &S3Error{Code: "InvalidArgument", Message: err.Error()}
	}
	if err := os.MkdirAll(filepath.Dir(objPath), 0755); err != nil {
		return nil, fmt.Errorf("failed to create object directory: %w", err)
	}

	// BUG-12: Use atomic temp-file + rename for crash safety (matching PutObject behavior)
	tmpFile, err := os.CreateTemp(filepath.Dir(objPath), ".objectra-multipart-*")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp file: %w", err)
	}
	tmpPath := tmpFile.Name()
	defer func() {
		tmpFile.Close()
		os.Remove(tmpPath) // Clean up on failure; no-op if already renamed
	}()

	hash := md5.New()
	var totalSize int64
	partDir := fs.multipartDir(bucket, key, uploadID)

	for _, part := range parts {
		partPath := filepath.Join(partDir, fmt.Sprintf("part-%05d", part.PartNumber))
		partFile, err := os.Open(partPath)
		if err != nil {
			return nil, fmt.Errorf("part %d not found: %w", part.PartNumber, err)
		}
		n, err := io.Copy(io.MultiWriter(tmpFile, hash), partFile)
		partFile.Close()
		if err != nil {
			return nil, fmt.Errorf("failed to copy part %d: %w", part.PartNumber, err)
		}
		totalSize += n
	}

	if err := tmpFile.Close(); err != nil {
		return nil, fmt.Errorf("failed to close temp file: %w", err)
	}

	// Atomically move temp file to final location
	if err := os.Rename(tmpPath, objPath); err != nil {
		return nil, fmt.Errorf("failed to rename temp file: %w", err)
	}

	// Calculate S3-compliant multipart ETag (MD5 of concatenated parts' MD5s)
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
	}

	if err := fs.metadata.PutObjectMeta(info); err != nil {
		return nil, err
	}

	// Clean up multipart temp files
	os.RemoveAll(filepath.Join(fs.dataDir, "multipart", bucket, uploadID))
	fs.metadata.DeleteMultipartMeta(bucket, key, uploadID)

	return info, nil
}

// AbortMultipartUpload cancels a multipart upload and cleans up parts.
func (fs *FilesystemEngine) AbortMultipartUpload(bucket, key, uploadID string) error {
	if err := fs.validateBucketName(bucket); err != nil {
		return err
	}

	unlock := fs.lockUpload(uploadID)
	defer unlock()

	_, err := fs.metadata.GetMultipartMeta(bucket, key, uploadID)
	if err != nil {
		return &S3Error{Code: "NoSuchUpload", Message: "The specified multipart upload does not exist."}
	}

	os.RemoveAll(filepath.Join(fs.dataDir, "multipart", bucket, uploadID))
	return fs.metadata.DeleteMultipartMeta(bucket, key, uploadID)
}

func (fs *FilesystemEngine) validateBucketName(name string) error {
	if !isValidBucketName(name) {
		return &S3Error{Code: "InvalidBucketName", Message: "The specified bucket is not valid."}
	}
	return nil
}

func isValidBucketName(name string) bool {
	if len(name) < 3 || len(name) > 63 {
		return false
	}
	for _, c := range name {
		if !((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' || c == '.') {
			return false
		}
	}
	if name[0] == '-' || name[len(name)-1] == '-' {
		return false
	}
	return true
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

	now := time.Now().UTC()
	err := fs.metadata.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(multipartBucket)
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
		return err
	}

	for _, u := range aborts {
		log.Printf("[Storage] Cleaning up expired multipart upload %s (bucket=%s, key=%s)", u.uploadID, u.bucket, u.key)
		_ = fs.AbortMultipartUpload(u.bucket, u.key, u.uploadID)
	}

	return nil
}
