package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func (fs *FilesystemEngine) bucketPath(name string) string {
	return filepath.Join(fs.dataDir, "buckets", name)
}

func (fs *FilesystemEngine) validatePathSafety(bucket, key, versionID string) error {
	if !filepath.IsLocal(bucket) {
		return errors.New(errInvalidBucketTraversal)
	}
	trimmedKey := strings.TrimLeft(key, "/\\")
	if trimmedKey != "" {
		if !filepath.IsLocal(filepath.FromSlash(trimmedKey)) {
			return errors.New(errInvalidKeyTraversal)
		}
	} else if key != "" {
		return errors.New(errInvalidKeyTraversal)
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

func (fs *FilesystemEngine) objectPath(bucket, key string) (string, error) {
	if err := fs.validatePathSafety(bucket, key, ""); err != nil {
		return "", err
	}

	base := fs.bucketPath(bucket)
	resolved := filepath.Join(base, filepath.FromSlash(key))
	// Ensure the resolved path stays within the bucket directory
	rel, err := filepath.Rel(base, resolved)
	if err != nil || !isSafeRelPath(rel) {
		return "", errors.New(errInvalidKeyTraversal)
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
		return "", errors.New(errInvalidBucketTraversal)
	}
	if !filepath.IsLocal(uploadID) {
		return "", errors.New(errInvalidUploadID)
	}
	if strings.ContainsAny(uploadID, "/\\") || strings.Contains(uploadID, "..") {
		return "", errors.New(errInvalidUploadID)
	}
	base := filepath.Join(fs.dataDir, "multipart", bucket)
	resolved := filepath.Join(base, uploadID)
	// Ensure the resolved path stays within the bucket's multipart directory
	rel, err := filepath.Rel(base, resolved)
	if err != nil || !isSafeRelPath(rel) {
		return "", errors.New(errInvalidUploadID)
	}
	return resolved, nil
}

func (fs *FilesystemEngine) multipartDir(bucket, key, uploadID string) (string, error) {
	if !filepath.IsLocal(bucket) {
		return "", errors.New(errInvalidBucketTraversal)
	}
	trimmedKey := strings.TrimLeft(key, "/\\")
	if trimmedKey != "" {
		if !filepath.IsLocal(filepath.FromSlash(trimmedKey)) {
			return "", errors.New(errInvalidKeyTraversal)
		}
	} else if key != "" {
		return "", errors.New(errInvalidKeyTraversal)
	}
	if !filepath.IsLocal(uploadID) {
		return "", errors.New(errInvalidUploadID)
	}
	uploadPath, err := fs.multipartUploadPath(bucket, uploadID)
	if err != nil {
		return "", err
	}
	resolved := filepath.Join(uploadPath, filepath.FromSlash(key))
	// Ensure the resolved path stays within the uploadPath directory
	rel, err := filepath.Rel(uploadPath, resolved)
	if err != nil || !isSafeRelPath(rel) {
		return "", errors.New(errInvalidKeyTraversal)
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

