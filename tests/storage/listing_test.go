package storage_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/salvatorecorvaglia/stiva/internal/storage"
)

func newListEngine(t *testing.T, bucket string, keys ...string) *storage.FilesystemEngine {
	t.Helper()
	fs, err := storage.NewFilesystemEngine(t.TempDir(), nil, "")
	if err != nil {
		t.Fatalf("engine: %v", err)
	}
	t.Cleanup(func() { _ = fs.Close() })
	if err := fs.CreateBucket(bucket); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	for _, k := range keys {
		if _, err := fs.PutObject(context.Background(), bucket, k,
			strings.NewReader("x"), 1, "application/octet-stream"); err != nil {
			t.Fatalf("put %s: %v", k, err)
		}
	}
	return fs
}

// TestListObjectsPaginationNoDuplicates is the regression test for the audit
// finding that truncating on a CommonPrefix emitted a continuation token
// pointing at the last *object*, which sorts before the prefixes already
// returned. Paging then re-emitted those prefixes on every subsequent page.
func TestListObjectsPaginationNoDuplicates(t *testing.T) {
	fs := newListEngine(t, "testbucket",
		"aaa.txt", "d1/x", "d2/x", "d3/x", "d4/x")

	seenKeys := map[string]int{}
	seenPrefixes := map[string]int{}

	token := ""
	for page := 0; page < 10; page++ {
		out, err := fs.ListObjects(&storage.ListObjectsInput{
			Bucket:            "testbucket",
			Delimiter:         "/",
			MaxKeys:           3,
			ContinuationToken: token,
		})
		if err != nil {
			t.Fatalf("page %d: %v", page, err)
		}
		for _, o := range out.Objects {
			seenKeys[o.Key]++
		}
		for _, p := range out.CommonPrefixes {
			seenPrefixes[p]++
		}
		if !out.IsTruncated {
			break
		}
		if out.NextContinuationToken == "" {
			t.Fatalf("page %d: truncated but no continuation token", page)
		}
		if out.NextContinuationToken == token {
			t.Fatalf("page %d: continuation token did not advance (%q)", page, token)
		}
		token = out.NextContinuationToken
	}

	for k, n := range seenKeys {
		if n != 1 {
			t.Errorf("object %q returned %d times across pages, want exactly 1", k, n)
		}
	}
	for p, n := range seenPrefixes {
		if n != 1 {
			t.Errorf("prefix %q returned %d times across pages, want exactly 1", p, n)
		}
	}

	for _, want := range []string{"d1/", "d2/", "d3/", "d4/"} {
		if seenPrefixes[want] == 0 {
			t.Errorf("prefix %q was never returned", want)
		}
	}
	if seenKeys["aaa.txt"] == 0 {
		t.Error("object aaa.txt was never returned")
	}
}

// TestListObjectsPaginationCoversAllKeys walks a flat bucket in small pages and
// asserts every key appears exactly once.
func TestListObjectsPaginationCoversAllKeys(t *testing.T) {
	var keys []string
	for i := 0; i < 25; i++ {
		keys = append(keys, fmt.Sprintf("obj-%02d.txt", i))
	}
	fs := newListEngine(t, "flat", keys...)

	seen := map[string]int{}
	token := ""
	for page := 0; page < 20; page++ {
		out, err := fs.ListObjects(&storage.ListObjectsInput{
			Bucket:            "flat",
			MaxKeys:           4,
			ContinuationToken: token,
		})
		if err != nil {
			t.Fatalf("page %d: %v", page, err)
		}
		for _, o := range out.Objects {
			seen[o.Key]++
		}
		if !out.IsTruncated {
			break
		}
		token = out.NextContinuationToken
	}

	if len(seen) != len(keys) {
		t.Errorf("saw %d distinct keys, want %d", len(seen), len(keys))
	}
	for _, k := range keys {
		if seen[k] != 1 {
			t.Errorf("key %q seen %d times, want 1", k, seen[k])
		}
	}
}

// TestListObjectVersionsEnumeratesHistory covers the newly added
// ListObjectVersions surface: versioning existed in storage but had no API.
func TestListObjectVersionsEnumeratesHistory(t *testing.T) {
	fs := newListEngine(t, "versioned")
	if err := fs.SetBucketVersioning("versioned", "Enabled"); err != nil {
		t.Fatalf("enable versioning: %v", err)
	}

	for i := 0; i < 3; i++ {
		if _, err := fs.PutObject(context.Background(), "versioned", "file.txt",
			strings.NewReader(fmt.Sprintf("v%d", i)), 2, "text/plain"); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}

	out, err := fs.ListObjectVersions(&storage.ListVersionsInput{Bucket: "versioned"})
	if err != nil {
		t.Fatalf("ListObjectVersions: %v", err)
	}
	if len(out.Versions) != 3 {
		t.Errorf("got %d versions, want 3", len(out.Versions))
	}

	latest := 0
	ids := map[string]bool{}
	for _, v := range out.Versions {
		if v.IsLatest {
			latest++
		}
		if ids[v.VersionID] {
			t.Errorf("duplicate version ID %q", v.VersionID)
		}
		ids[v.VersionID] = true
	}
	if latest != 1 {
		t.Errorf("got %d versions flagged IsLatest, want exactly 1", latest)
	}
}

// TestListObjectVersionsIncludesDeleteMarkers ensures delete markers are
// reported separately from live versions.
func TestListObjectVersionsIncludesDeleteMarkers(t *testing.T) {
	fs := newListEngine(t, "versioned")
	if err := fs.SetBucketVersioning("versioned", "Enabled"); err != nil {
		t.Fatalf("enable versioning: %v", err)
	}
	if _, err := fs.PutObject(context.Background(), "versioned", "gone.txt",
		strings.NewReader("x"), 1, "text/plain"); err != nil {
		t.Fatalf("put: %v", err)
	}
	if _, _, err := fs.DeleteObject("versioned", "gone.txt", ""); err != nil {
		t.Fatalf("delete: %v", err)
	}

	out, err := fs.ListObjectVersions(&storage.ListVersionsInput{Bucket: "versioned"})
	if err != nil {
		t.Fatalf("ListObjectVersions: %v", err)
	}
	if len(out.DeleteMarkers) != 1 {
		t.Errorf("got %d delete markers, want 1", len(out.DeleteMarkers))
	}
	if len(out.Versions) != 1 {
		t.Errorf("got %d live versions, want 1", len(out.Versions))
	}
}

// TestListPartsReturnsUploadedParts covers the new ListParts surface.
func TestListPartsReturnsUploadedParts(t *testing.T) {
	fs := newListEngine(t, "mpu")
	up, err := fs.CreateMultipartUpload("mpu", "big.bin", "application/octet-stream")
	if err != nil {
		t.Fatalf("create mpu: %v", err)
	}

	for _, n := range []int{2, 1, 3} {
		body := strings.Repeat("z", 16)
		if _, err := fs.UploadPart(context.Background(), "mpu", "big.bin", up.UploadID, n,
			strings.NewReader(body), int64(len(body))); err != nil {
			t.Fatalf("upload part %d: %v", n, err)
		}
	}

	parts, err := fs.ListParts("mpu", "big.bin", up.UploadID)
	if err != nil {
		t.Fatalf("ListParts: %v", err)
	}
	if len(parts) != 3 {
		t.Fatalf("got %d parts, want 3", len(parts))
	}
	for i, p := range parts {
		if p.PartNumber != i+1 {
			t.Errorf("part %d has number %d, want ascending order", i, p.PartNumber)
		}
	}
}

// TestListMultipartUploadsReportsInFlight covers the new ListMultipartUploads
// surface, which clients need to discover and reclaim abandoned uploads.
func TestListMultipartUploadsReportsInFlight(t *testing.T) {
	fs := newListEngine(t, "mpu")
	for _, key := range []string{"b.bin", "a.bin"} {
		if _, err := fs.CreateMultipartUpload("mpu", key, "application/octet-stream"); err != nil {
			t.Fatalf("create mpu %s: %v", key, err)
		}
	}

	uploads, truncated, err := fs.ListMultipartUploads("mpu", "", 100)
	if err != nil {
		t.Fatalf("ListMultipartUploads: %v", err)
	}
	if truncated {
		t.Error("unexpectedly truncated")
	}
	if len(uploads) != 2 {
		t.Fatalf("got %d uploads, want 2", len(uploads))
	}
	if uploads[0].Key != "a.bin" || uploads[1].Key != "b.bin" {
		t.Errorf("uploads not sorted by key: %q, %q", uploads[0].Key, uploads[1].Key)
	}
}
