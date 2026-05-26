package storage

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
)

// FilesystemEngine implements the Engine interface using the local filesystem.
// Objects are stored as files under <dataDir>/buckets/<bucket>/<key>.
// Metadata is persisted in a bbolt database.
type FilesystemEngine struct {
	dataDir  string
	metadata *MetadataStore
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
	return fs.metadata.BucketExists(name)
}

// ListBuckets returns all buckets.
func (fs *FilesystemEngine) ListBuckets() ([]BucketInfo, error) {
	return fs.metadata.ListBuckets()
}

// CountObjects returns the number of objects in a bucket without loading metadata.
func (fs *FilesystemEngine) CountObjects(bucket string) (int, error) {
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
	info, err := fs.metadata.GetObjectMeta(bucket, key)
	if err != nil {
		return nil, &S3Error{Code: "NoSuchKey", Message: "The specified key does not exist."}
	}
	return info, nil
}

// DeleteObject removes an object.
func (fs *FilesystemEngine) DeleteObject(bucket, key string) error {
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
	reader, srcInfo, err := fs.GetObject(srcBucket, srcKey)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	return fs.PutObject(dstBucket, dstKey, reader, srcInfo.Size, srcInfo.ContentType)
}

// ListObjects lists objects in a bucket with prefix, delimiter, and pagination support.
func (fs *FilesystemEngine) ListObjects(input *ListObjectsInput) (*ListObjectsOutput, error) {
	exists, err := fs.metadata.BucketExists(input.Bucket)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, &S3Error{Code: "NoSuchBucket", Message: "The specified bucket does not exist"}
	}

	allObjects, err := fs.metadata.ListAllObjectMetas(input.Bucket)
	if err != nil {
		return nil, err
	}

	// Sort by key for consistent ordering
	sort.Slice(allObjects, func(i, j int) bool {
		return allObjects[i].Key < allObjects[j].Key
	})

	maxKeys := input.MaxKeys
	if maxKeys <= 0 {
		maxKeys = 1000
	}

	// Determine the start position
	startAfter := input.StartAfter
	if input.ContinuationToken != "" {
		startAfter = input.ContinuationToken
	}

	// Apply prefix filter and delimiter logic
	var objects []ObjectInfo
	commonPrefixSet := make(map[string]bool)

	for _, obj := range allObjects {
		// Filter by prefix
		if input.Prefix != "" && !strings.HasPrefix(obj.Key, input.Prefix) {
			continue
		}

		// Skip if before start position
		if startAfter != "" && obj.Key <= startAfter {
			continue
		}

		// Handle delimiter (folder simulation)
		if input.Delimiter != "" {
			remaining := obj.Key[len(input.Prefix):]
			delimIdx := strings.Index(remaining, input.Delimiter)
			if delimIdx >= 0 {
				prefix := input.Prefix + remaining[:delimIdx+len(input.Delimiter)]
				commonPrefixSet[prefix] = true
				continue
			}
		}

		objects = append(objects, obj)
	}

	// Sort common prefixes
	var commonPrefixes []string
	for p := range commonPrefixSet {
		commonPrefixes = append(commonPrefixes, p)
	}
	sort.Strings(commonPrefixes)

	// Apply pagination: MaxKeys applies to total of objects + commonPrefixes per S3 spec.
	isTruncated := false
	nextToken := ""

	// Interleave objects and prefixes by sort order, then truncate at maxKeys.
	// We only need to know *if* we exceed maxKeys and what the last included object key is.
	totalIncluded := 0
	truncatedObjects := objects[:0] // reuse backing array
	truncatedPrefixes := commonPrefixes[:0]

	oi, pi := 0, 0
	for totalIncluded < maxKeys && (oi < len(objects) || pi < len(commonPrefixes)) {
		// Pick the lexicographically smaller between next object and next prefix
		pickObject := false
		if oi < len(objects) && pi < len(commonPrefixes) {
			pickObject = objects[oi].Key <= commonPrefixes[pi]
		} else if oi < len(objects) {
			pickObject = true
		}

		if pickObject {
			truncatedObjects = append(truncatedObjects, objects[oi])
			nextToken = objects[oi].Key
			oi++
		} else {
			truncatedPrefixes = append(truncatedPrefixes, commonPrefixes[pi])
			pi++
		}
		totalIncluded++
	}

	if oi < len(objects) || pi < len(commonPrefixes) {
		isTruncated = true
	} else {
		nextToken = "" // no next token needed if not truncated
	}

	return &ListObjectsOutput{
		Objects:               truncatedObjects,
		CommonPrefixes:        truncatedPrefixes,
		IsTruncated:           isTruncated,
		NextContinuationToken: nextToken,
		KeyCount:              len(truncatedObjects) + len(truncatedPrefixes),
	}, nil
}

// CreateMultipartUpload initiates a multipart upload.
func (fs *FilesystemEngine) CreateMultipartUpload(bucket, key, contentType string) (*MultipartUploadInfo, error) {
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

	etag := hex.EncodeToString(hash.Sum(nil))
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
	_, err := fs.metadata.GetMultipartMeta(bucket, key, uploadID)
	if err != nil {
		return &S3Error{Code: "NoSuchUpload", Message: "The specified multipart upload does not exist."}
	}

	os.RemoveAll(filepath.Join(fs.dataDir, "multipart", bucket, uploadID))
	return fs.metadata.DeleteMultipartMeta(bucket, key, uploadID)
}

// S3Error represents an S3 API error with a code and message.
type S3Error struct {
	Code    string
	Message string
}

func (e *S3Error) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}
