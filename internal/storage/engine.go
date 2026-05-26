// Package storage defines the storage engine interface and types for Objectra.
package storage

import (
	"io"
	"time"
)

// BucketInfo holds metadata about a storage bucket.
type BucketInfo struct {
	Name         string    `json:"name"`
	CreationDate time.Time `json:"creationDate"`
}

// ObjectInfo holds metadata about a stored object.
type ObjectInfo struct {
	Bucket       string    `json:"bucket"`
	Key          string    `json:"key"`
	Size         int64     `json:"size"`
	ETag         string    `json:"etag"`
	ContentType  string    `json:"contentType"`
	LastModified time.Time `json:"lastModified"`
}

// ListObjectsInput holds parameters for listing objects in a bucket.
type ListObjectsInput struct {
	Bucket            string
	Prefix            string
	Delimiter         string
	MaxKeys           int
	ContinuationToken string
	StartAfter        string
}

// ListObjectsOutput holds the result of listing objects in a bucket.
type ListObjectsOutput struct {
	Objects               []ObjectInfo
	CommonPrefixes        []string
	IsTruncated           bool
	NextContinuationToken string
	KeyCount              int
}

// MultipartUploadInfo holds metadata about a multipart upload.
type MultipartUploadInfo struct {
	UploadID string
	Bucket   string
	Key      string
	Created  time.Time
}

// PartInfo holds metadata about a single uploaded part.
type PartInfo struct {
	PartNumber int
	ETag       string
	Size       int64
}

// CompletePart represents a part in a CompleteMultipartUpload request.
type CompletePart struct {
	PartNumber int
	ETag       string
}

// Engine is the interface that all storage backends must implement.
type Engine interface {
	// Bucket operations
	CreateBucket(name string) error
	DeleteBucket(name string) error
	BucketExists(name string) (bool, error)
	ListBuckets() ([]BucketInfo, error)

	// Object operations
	PutObject(bucket, key string, reader io.Reader, size int64, contentType string) (*ObjectInfo, error)
	GetObject(bucket, key string) (io.ReadCloser, *ObjectInfo, error)
	HeadObject(bucket, key string) (*ObjectInfo, error)
	DeleteObject(bucket, key string) error
	CopyObject(srcBucket, srcKey, dstBucket, dstKey string) (*ObjectInfo, error)

	// List operations
	ListObjects(input *ListObjectsInput) (*ListObjectsOutput, error)
	CountObjects(bucket string) (int, error)

	// Multipart upload operations
	CreateMultipartUpload(bucket, key, contentType string) (*MultipartUploadInfo, error)
	UploadPart(bucket, key, uploadID string, partNumber int, reader io.Reader, size int64) (*PartInfo, error)
	CompleteMultipartUpload(bucket, key, uploadID string, parts []CompletePart) (*ObjectInfo, error)
	AbortMultipartUpload(bucket, key, uploadID string) error

	// Lifecycle
	CleanExpiredMultipartUploads(cutoff time.Duration) error
	Close() error
}
