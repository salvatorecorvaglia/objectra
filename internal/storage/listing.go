package storage

import (
	"sort"
	"strings"
)

// ListVersionsInput holds parameters for enumerating object versions.
type ListVersionsInput struct {
	Bucket          string
	Prefix          string
	Delimiter       string
	MaxKeys         int
	KeyMarker       string
	VersionIDMarker string
}

// ListVersionsOutput holds the result of enumerating object versions.
type ListVersionsOutput struct {
	Versions            []ObjectInfo
	DeleteMarkers       []ObjectInfo
	CommonPrefixes      []string
	IsTruncated         bool
	NextKeyMarker       string
	NextVersionIDMarker string
}

// ListObjectVersions enumerates every version of every object in a bucket,
// separating live versions from delete markers.
//
// Versioning was already implemented in the metadata layer but had no API
// surface, so clients had no way to enumerate or reclaim old versions.
func (fs *FilesystemEngine) ListObjectVersions(input *ListVersionsInput) (*ListVersionsOutput, error) {
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

	// Collect the latest pointers and the historical records, de-duplicating by
	// (key, versionID): the latest pointer is also stored under its version key.
	seen := make(map[string]bool)
	var all []ObjectInfo

	collect := func(info *ObjectInfo) error {
		if input.Prefix != "" && !strings.HasPrefix(info.Key, input.Prefix) {
			return nil
		}
		id := info.Key + "\x00" + info.VersionID
		if seen[id] {
			return nil
		}
		seen[id] = true
		all = append(all, *info)
		return nil
	}

	if err := fs.metadata.IterateObjectMetas(input.Bucket, collect); err != nil {
		return nil, err
	}
	if err := fs.metadata.IterateObjectVersions(input.Bucket, collect); err != nil {
		return nil, err
	}

	// S3 orders by key ascending, then by version recency (newest first).
	sort.SliceStable(all, func(i, j int) bool {
		if all[i].Key != all[j].Key {
			return all[i].Key < all[j].Key
		}
		return all[i].LastModified.After(all[j].LastModified)
	})

	out := &ListVersionsOutput{}
	prefixSet := make(map[string]bool)
	started := input.KeyMarker == ""
	count := 0

	for i := range all {
		info := all[i]

		if !started {
			if info.Key == input.KeyMarker &&
				(input.VersionIDMarker == "" || info.VersionID == input.VersionIDMarker) {
				started = true
			}
			continue
		}

		// Roll keys below a delimiter up into a common prefix.
		if input.Delimiter != "" {
			remaining := info.Key[len(input.Prefix):]
			if idx := strings.Index(remaining, input.Delimiter); idx >= 0 {
				dirPrefix := input.Prefix + remaining[:idx+len(input.Delimiter)]
				if !prefixSet[dirPrefix] {
					if count >= maxKeys {
						out.IsTruncated = true
						out.NextKeyMarker = info.Key
						out.NextVersionIDMarker = info.VersionID
						break
					}
					prefixSet[dirPrefix] = true
					out.CommonPrefixes = append(out.CommonPrefixes, dirPrefix)
					count++
				}
				continue
			}
		}

		if count >= maxKeys {
			out.IsTruncated = true
			out.NextKeyMarker = info.Key
			out.NextVersionIDMarker = info.VersionID
			break
		}

		if info.IsDeleteMarker {
			out.DeleteMarkers = append(out.DeleteMarkers, info)
		} else {
			out.Versions = append(out.Versions, info)
		}
		count++
	}

	sort.Strings(out.CommonPrefixes)
	return out, nil
}

// ListMultipartUploads returns the in-progress multipart uploads for a bucket,
// ordered by key then upload ID.
func (fs *FilesystemEngine) ListMultipartUploads(bucket, prefix string, maxUploads int) ([]MultipartUploadInfo, bool, error) {
	if err := fs.validateBucketName(bucket); err != nil {
		return nil, false, err
	}
	exists, err := fs.metadata.BucketExists(bucket)
	if err != nil {
		return nil, false, err
	}
	if !exists {
		return nil, false, &S3Error{Code: "NoSuchBucket", Message: errBucketNotFound}
	}

	if maxUploads <= 0 {
		maxUploads = 1000
	}

	var uploads []MultipartUploadInfo
	err = fs.metadata.IterateMultipartMetas(bucket, func(meta *MultipartMeta) error {
		if prefix != "" && !strings.HasPrefix(meta.Key, prefix) {
			return nil
		}
		uploads = append(uploads, MultipartUploadInfo{
			UploadID: meta.UploadID,
			Bucket:   meta.Bucket,
			Key:      meta.Key,
			Created:  meta.Created,
		})
		return nil
	})
	if err != nil {
		return nil, false, err
	}

	sort.Slice(uploads, func(i, j int) bool {
		if uploads[i].Key != uploads[j].Key {
			return uploads[i].Key < uploads[j].Key
		}
		return uploads[i].UploadID < uploads[j].UploadID
	})

	truncated := false
	if len(uploads) > maxUploads {
		uploads = uploads[:maxUploads]
		truncated = true
	}
	return uploads, truncated, nil
}

// ListParts returns the parts uploaded so far for a multipart upload, ordered by
// part number.
func (fs *FilesystemEngine) ListParts(bucket, key, uploadID string) ([]PartInfo, error) {
	if err := fs.validateBucketName(bucket); err != nil {
		return nil, err
	}
	if _, err := fs.objectPath(bucket, key); err != nil {
		return nil, &S3Error{Code: "InvalidArgument", Message: err.Error()}
	}

	meta, err := fs.metadata.GetMultipartMeta(bucket, key, uploadID)
	if err != nil {
		return nil, &S3Error{Code: "NoSuchUpload", Message: errUploadNotFound}
	}

	parts := make([]PartInfo, len(meta.Parts))
	copy(parts, meta.Parts)
	sort.Slice(parts, func(i, j int) bool {
		return parts[i].PartNumber < parts[j].PartNumber
	})
	return parts, nil
}
