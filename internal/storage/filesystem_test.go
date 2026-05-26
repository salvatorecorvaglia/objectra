package storage

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"
)

// setupTestEngine creates a temporary FilesystemEngine for testing.
func setupTestEngine(t *testing.T) *FilesystemEngine {
	t.Helper()
	tmpDir := t.TempDir()
	engine, err := NewFilesystemEngine(tmpDir)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	t.Cleanup(func() { engine.Close() })
	return engine
}

// --- Bucket Operations ---

func TestCreateBucket(t *testing.T) {
	engine := setupTestEngine(t)

	err := engine.CreateBucket("test-bucket")
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	exists, err := engine.BucketExists("test-bucket")
	if err != nil {
		t.Fatalf("BucketExists failed: %v", err)
	}
	if !exists {
		t.Error("expected bucket to exist after creation")
	}

	// Verify directory was created
	bucketDir := filepath.Join(engine.dataDir, "buckets", "test-bucket")
	if _, err := os.Stat(bucketDir); os.IsNotExist(err) {
		t.Error("expected bucket directory to exist on disk")
	}
}

func TestCreateBucketDuplicate(t *testing.T) {
	engine := setupTestEngine(t)

	if err := engine.CreateBucket("test-bucket"); err != nil {
		t.Fatalf("first CreateBucket failed: %v", err)
	}

	err := engine.CreateBucket("test-bucket")
	if err == nil {
		t.Error("expected error when creating duplicate bucket")
	}
	s3Err, ok := err.(*S3Error)
	if !ok {
		t.Fatalf("expected S3Error, got %T", err)
	}
	if s3Err.Code != "BucketAlreadyOwnedByYou" {
		t.Errorf("expected BucketAlreadyOwnedByYou, got %s", s3Err.Code)
	}
}

func TestDeleteBucket(t *testing.T) {
	engine := setupTestEngine(t)

	if err := engine.CreateBucket("delete-me"); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	if err := engine.DeleteBucket("delete-me"); err != nil {
		t.Fatalf("DeleteBucket failed: %v", err)
	}

	exists, _ := engine.BucketExists("delete-me")
	if exists {
		t.Error("expected bucket to not exist after deletion")
	}
}

func TestDeleteBucketNotEmpty(t *testing.T) {
	engine := setupTestEngine(t)

	if err := engine.CreateBucket("notempty"); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	// Put an object in the bucket
	_, err := engine.PutObject("notempty", "file.txt", bytes.NewReader([]byte("data")), 4, "text/plain")
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	err = engine.DeleteBucket("notempty")
	if err == nil {
		t.Error("expected error when deleting non-empty bucket")
	}
}

func TestDeleteBucketNonexistent(t *testing.T) {
	engine := setupTestEngine(t)

	err := engine.DeleteBucket("no-such-bucket")
	if err == nil {
		t.Error("expected error when deleting non-existent bucket")
	}
}

func TestListBuckets(t *testing.T) {
	engine := setupTestEngine(t)

	// Initially empty
	buckets, err := engine.ListBuckets()
	if err != nil {
		t.Fatalf("ListBuckets failed: %v", err)
	}
	if len(buckets) != 0 {
		t.Errorf("expected 0 buckets, got %d", len(buckets))
	}

	// Create some buckets
	for _, name := range []string{"alpha", "beta", "gamma"} {
		if err := engine.CreateBucket(name); err != nil {
			t.Fatalf("CreateBucket(%s) failed: %v", name, err)
		}
	}

	buckets, err = engine.ListBuckets()
	if err != nil {
		t.Fatalf("ListBuckets failed: %v", err)
	}
	if len(buckets) != 3 {
		t.Errorf("expected 3 buckets, got %d", len(buckets))
	}
}

// --- Object Operations ---

func TestPutAndGetObject(t *testing.T) {
	engine := setupTestEngine(t)
	engine.CreateBucket("my-bucket")

	content := []byte("Hello, Objectra!")
	info, err := engine.PutObject("my-bucket", "greeting.txt", bytes.NewReader(content), int64(len(content)), "text/plain")
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	if info.Key != "greeting.txt" {
		t.Errorf("expected key 'greeting.txt', got '%s'", info.Key)
	}
	if info.Size != int64(len(content)) {
		t.Errorf("expected size %d, got %d", len(content), info.Size)
	}
	if info.ContentType != "text/plain" {
		t.Errorf("expected content type 'text/plain', got '%s'", info.ContentType)
	}
	if info.ETag == "" {
		t.Error("expected non-empty ETag")
	}

	// Get it back
	reader, getInfo, err := engine.GetObject("my-bucket", "greeting.txt")
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("failed to read object: %v", err)
	}

	if !bytes.Equal(data, content) {
		t.Errorf("content mismatch: got %q, want %q", data, content)
	}
	if getInfo.Size != int64(len(content)) {
		t.Errorf("size mismatch: got %d, want %d", getInfo.Size, len(content))
	}
}

func TestPutObjectNoBucket(t *testing.T) {
	engine := setupTestEngine(t)

	_, err := engine.PutObject("nonexistent", "file.txt", bytes.NewReader([]byte("data")), 4, "text/plain")
	if err == nil {
		t.Error("expected error when putting object to non-existent bucket")
	}
}

func TestGetObjectNotFound(t *testing.T) {
	engine := setupTestEngine(t)
	engine.CreateBucket("my-bucket")

	_, _, err := engine.GetObject("my-bucket", "no-such-key")
	if err == nil {
		t.Error("expected error when getting non-existent object")
	}
	s3Err, ok := err.(*S3Error)
	if !ok {
		t.Fatalf("expected S3Error, got %T", err)
	}
	if s3Err.Code != "NoSuchKey" {
		t.Errorf("expected NoSuchKey, got %s", s3Err.Code)
	}
}

func TestHeadObject(t *testing.T) {
	engine := setupTestEngine(t)
	engine.CreateBucket("bucket")

	content := []byte("metadata test")
	engine.PutObject("bucket", "doc.txt", bytes.NewReader(content), int64(len(content)), "text/plain")

	info, err := engine.HeadObject("bucket", "doc.txt")
	if err != nil {
		t.Fatalf("HeadObject failed: %v", err)
	}
	if info.Size != int64(len(content)) {
		t.Errorf("expected size %d, got %d", len(content), info.Size)
	}
	if info.ContentType != "text/plain" {
		t.Errorf("expected content type 'text/plain', got '%s'", info.ContentType)
	}
}

func TestDeleteObject(t *testing.T) {
	engine := setupTestEngine(t)
	engine.CreateBucket("bucket")
	engine.PutObject("bucket", "deleteme.txt", bytes.NewReader([]byte("bye")), 3, "text/plain")

	err := engine.DeleteObject("bucket", "deleteme.txt")
	if err != nil {
		t.Fatalf("DeleteObject failed: %v", err)
	}

	_, _, err = engine.GetObject("bucket", "deleteme.txt")
	if err == nil {
		t.Error("expected error when getting deleted object")
	}
}

func TestCopyObject(t *testing.T) {
	engine := setupTestEngine(t)
	engine.CreateBucket("src-bucket")
	engine.CreateBucket("dst-bucket")

	content := []byte("copy this content")
	engine.PutObject("src-bucket", "original.txt", bytes.NewReader(content), int64(len(content)), "text/plain")

	info, err := engine.CopyObject("src-bucket", "original.txt", "dst-bucket", "copy.txt")
	if err != nil {
		t.Fatalf("CopyObject failed: %v", err)
	}
	if info.Key != "copy.txt" {
		t.Errorf("expected key 'copy.txt', got '%s'", info.Key)
	}

	// Verify the copy
	reader, _, err := engine.GetObject("dst-bucket", "copy.txt")
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}
	defer reader.Close()
	data, _ := io.ReadAll(reader)
	if !bytes.Equal(data, content) {
		t.Errorf("copied content mismatch")
	}
}

func TestNestedKeyPaths(t *testing.T) {
	engine := setupTestEngine(t)
	engine.CreateBucket("bucket")

	content := []byte("nested file")
	_, err := engine.PutObject("bucket", "photos/2024/01/img.jpg", bytes.NewReader(content), int64(len(content)), "image/jpeg")
	if err != nil {
		t.Fatalf("PutObject with nested key failed: %v", err)
	}

	reader, _, err := engine.GetObject("bucket", "photos/2024/01/img.jpg")
	if err != nil {
		t.Fatalf("GetObject with nested key failed: %v", err)
	}
	defer reader.Close()
	data, _ := io.ReadAll(reader)
	if !bytes.Equal(data, content) {
		t.Error("nested object content mismatch")
	}
}

// --- Path Traversal Protection ---

func TestPathTraversalProtection(t *testing.T) {
	engine := setupTestEngine(t)
	engine.CreateBucket("bucket")

	_, err := engine.PutObject("bucket", "../../etc/passwd", bytes.NewReader([]byte("malicious")), 9, "text/plain")
	if err == nil {
		t.Error("expected error for path traversal key")
	}
}

// --- List Operations ---

func TestListObjects(t *testing.T) {
	engine := setupTestEngine(t)
	engine.CreateBucket("list-bucket")

	// Create several objects
	objects := []string{"a.txt", "b.txt", "c.txt", "dir/d.txt", "dir/e.txt"}
	for _, key := range objects {
		engine.PutObject("list-bucket", key, bytes.NewReader([]byte("x")), 1, "text/plain")
	}

	// List all
	output, err := engine.ListObjects(&ListObjectsInput{
		Bucket:  "list-bucket",
		MaxKeys: 1000,
	})
	if err != nil {
		t.Fatalf("ListObjects failed: %v", err)
	}
	if len(output.Objects) != 5 {
		t.Errorf("expected 5 objects, got %d", len(output.Objects))
	}
}

func TestListObjectsWithDelimiter(t *testing.T) {
	engine := setupTestEngine(t)
	engine.CreateBucket("delim-bucket")

	keys := []string{"a.txt", "b.txt", "dir/c.txt", "dir/d.txt", "other/e.txt"}
	for _, key := range keys {
		engine.PutObject("delim-bucket", key, bytes.NewReader([]byte("x")), 1, "text/plain")
	}

	output, err := engine.ListObjects(&ListObjectsInput{
		Bucket:    "delim-bucket",
		Delimiter: "/",
		MaxKeys:   1000,
	})
	if err != nil {
		t.Fatalf("ListObjects failed: %v", err)
	}

	// Should have 2 objects (a.txt, b.txt) and 2 common prefixes (dir/, other/)
	if len(output.Objects) != 2 {
		t.Errorf("expected 2 objects, got %d", len(output.Objects))
	}
	if len(output.CommonPrefixes) != 2 {
		t.Errorf("expected 2 common prefixes, got %d: %v", len(output.CommonPrefixes), output.CommonPrefixes)
	}
}

func TestListObjectsPagination(t *testing.T) {
	engine := setupTestEngine(t)
	engine.CreateBucket("page-bucket")

	// Create 5 objects
	for i := 0; i < 5; i++ {
		key := string(rune('a'+i)) + ".txt"
		engine.PutObject("page-bucket", key, bytes.NewReader([]byte("x")), 1, "text/plain")
	}

	// Paginate with maxKeys=2
	output, err := engine.ListObjects(&ListObjectsInput{
		Bucket:  "page-bucket",
		MaxKeys: 2,
	})
	if err != nil {
		t.Fatalf("ListObjects page 1 failed: %v", err)
	}
	if len(output.Objects) != 2 {
		t.Errorf("page 1: expected 2 objects, got %d", len(output.Objects))
	}
	if !output.IsTruncated {
		t.Error("page 1: expected IsTruncated=true")
	}
	if output.NextContinuationToken == "" {
		t.Error("page 1: expected NextContinuationToken to be set")
	}

	// Get page 2
	output2, err := engine.ListObjects(&ListObjectsInput{
		Bucket:            "page-bucket",
		MaxKeys:           2,
		ContinuationToken: output.NextContinuationToken,
	})
	if err != nil {
		t.Fatalf("ListObjects page 2 failed: %v", err)
	}
	if len(output2.Objects) != 2 {
		t.Errorf("page 2: expected 2 objects, got %d", len(output2.Objects))
	}

	// Get page 3 (last page)
	output3, err := engine.ListObjects(&ListObjectsInput{
		Bucket:            "page-bucket",
		MaxKeys:           2,
		ContinuationToken: output2.NextContinuationToken,
	})
	if err != nil {
		t.Fatalf("ListObjects page 3 failed: %v", err)
	}
	if len(output3.Objects) != 1 {
		t.Errorf("page 3: expected 1 object, got %d", len(output3.Objects))
	}
	if output3.IsTruncated {
		t.Error("page 3: expected IsTruncated=false")
	}
}

func TestCountObjects(t *testing.T) {
	engine := setupTestEngine(t)
	engine.CreateBucket("count-bucket")

	count, err := engine.CountObjects("count-bucket")
	if err != nil {
		t.Fatalf("CountObjects failed: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0, got %d", count)
	}

	// Add objects
	for i := 0; i < 10; i++ {
		key := string(rune('a'+i)) + ".txt"
		engine.PutObject("count-bucket", key, bytes.NewReader([]byte("x")), 1, "text/plain")
	}

	count, err = engine.CountObjects("count-bucket")
	if err != nil {
		t.Fatalf("CountObjects failed: %v", err)
	}
	if count != 10 {
		t.Errorf("expected 10, got %d", count)
	}
}

// --- Multipart Upload ---

func TestMultipartUpload(t *testing.T) {
	engine := setupTestEngine(t)
	engine.CreateBucket("multi-bucket")

	// Create multipart upload
	upload, err := engine.CreateMultipartUpload("multi-bucket", "large-file.bin", "application/octet-stream")
	if err != nil {
		t.Fatalf("CreateMultipartUpload failed: %v", err)
	}
	if upload.UploadID == "" {
		t.Error("expected non-empty UploadID")
	}

	// Upload 3 parts
	partData := [][]byte{
		bytes.Repeat([]byte("A"), 100),
		bytes.Repeat([]byte("B"), 200),
		bytes.Repeat([]byte("C"), 150),
	}

	var completeParts []CompletePart
	for i, data := range partData {
		partInfo, err := engine.UploadPart("multi-bucket", "large-file.bin", upload.UploadID, i+1, bytes.NewReader(data), int64(len(data)))
		if err != nil {
			t.Fatalf("UploadPart %d failed: %v", i+1, err)
		}
		completeParts = append(completeParts, CompletePart{
			PartNumber: i + 1,
			ETag:       partInfo.ETag,
		})
	}

	// Complete the upload
	info, err := engine.CompleteMultipartUpload("multi-bucket", "large-file.bin", upload.UploadID, completeParts)
	if err != nil {
		t.Fatalf("CompleteMultipartUpload failed: %v", err)
	}

	expectedSize := int64(100 + 200 + 150)
	if info.Size != expectedSize {
		t.Errorf("expected size %d, got %d", expectedSize, info.Size)
	}

	// Verify content
	reader, _, err := engine.GetObject("multi-bucket", "large-file.bin")
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}
	defer reader.Close()
	data, _ := io.ReadAll(reader)
	if int64(len(data)) != expectedSize {
		t.Errorf("content length mismatch: got %d, want %d", len(data), expectedSize)
	}
}

func TestAbortMultipartUpload(t *testing.T) {
	engine := setupTestEngine(t)
	engine.CreateBucket("abort-bucket")

	upload, err := engine.CreateMultipartUpload("abort-bucket", "aborted.txt", "text/plain")
	if err != nil {
		t.Fatalf("CreateMultipartUpload failed: %v", err)
	}

	// Upload a part
	engine.UploadPart("abort-bucket", "aborted.txt", upload.UploadID, 1, bytes.NewReader([]byte("data")), 4)

	// Abort
	err = engine.AbortMultipartUpload("abort-bucket", "aborted.txt", upload.UploadID)
	if err != nil {
		t.Fatalf("AbortMultipartUpload failed: %v", err)
	}

	// Verify object doesn't exist
	_, _, err = engine.GetObject("abort-bucket", "aborted.txt")
	if err == nil {
		t.Error("expected error: aborted upload should not create an object")
	}
}

// --- Unicode Keys ---

func TestUnicodeKeys(t *testing.T) {
	engine := setupTestEngine(t)
	engine.CreateBucket("unicode-bucket")

	keys := []string{
		"café/résumé.pdf",
		"日本語/ファイル.txt",
		"emoji/🎉.txt",
	}

	for _, key := range keys {
		_, err := engine.PutObject("unicode-bucket", key, bytes.NewReader([]byte("unicode")), 7, "text/plain")
		if err != nil {
			t.Errorf("PutObject(%q) failed: %v", key, err)
			continue
		}

		reader, _, err := engine.GetObject("unicode-bucket", key)
		if err != nil {
			t.Errorf("GetObject(%q) failed: %v", key, err)
			continue
		}
		reader.Close()
	}
}

// --- Empty Object ---

func TestEmptyObject(t *testing.T) {
	engine := setupTestEngine(t)
	engine.CreateBucket("empty-bucket")

	info, err := engine.PutObject("empty-bucket", "empty.txt", bytes.NewReader([]byte{}), 0, "text/plain")
	if err != nil {
		t.Fatalf("PutObject empty failed: %v", err)
	}
	if info.Size != 0 {
		t.Errorf("expected size 0, got %d", info.Size)
	}

	reader, _, err := engine.GetObject("empty-bucket", "empty.txt")
	if err != nil {
		t.Fatalf("GetObject empty failed: %v", err)
	}
	defer reader.Close()
	data, _ := io.ReadAll(reader)
	if len(data) != 0 {
		t.Errorf("expected empty data, got %d bytes", len(data))
	}
}

// --- Overwrite Object ---

func TestOverwriteObject(t *testing.T) {
	engine := setupTestEngine(t)
	engine.CreateBucket("overwrite-bucket")

	// Write v1
	engine.PutObject("overwrite-bucket", "file.txt", bytes.NewReader([]byte("version1")), 8, "text/plain")

	// Overwrite with v2
	info, err := engine.PutObject("overwrite-bucket", "file.txt", bytes.NewReader([]byte("version2-longer")), 15, "text/plain")
	if err != nil {
		t.Fatalf("PutObject overwrite failed: %v", err)
	}
	if info.Size != 15 {
		t.Errorf("expected size 15, got %d", info.Size)
	}

	// Verify content is v2
	reader, _, _ := engine.GetObject("overwrite-bucket", "file.txt")
	defer reader.Close()
	data, _ := io.ReadAll(reader)
	if string(data) != "version2-longer" {
		t.Errorf("expected 'version2-longer', got %q", data)
	}
}
