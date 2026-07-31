package s3api

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/salvatorecorvaglia/stiva/internal/storage"
)

// handleListObjectsV2 handles GET /<bucket>?list-type=2 (ListObjectsV2).
func (rt *Router) handleListObjectsV2(w http.ResponseWriter, r *http.Request, bucket string) {
	query := r.URL.Query()

	maxKeys := 1000
	if mk := query.Get("max-keys"); mk != "" {
		if v, err := strconv.Atoi(mk); err == nil && v > 0 {
			maxKeys = v
		}
	}

	input := &storage.ListObjectsInput{
		Bucket:            bucket,
		Prefix:            query.Get("prefix"),
		Delimiter:         query.Get("delimiter"),
		MaxKeys:           maxKeys,
		ContinuationToken: query.Get("continuation-token"),
		StartAfter:        query.Get("start-after"),
	}

	output, err := rt.engine.ListObjects(input)
	if handleStorageError(w, err, "/"+bucket) {
		return
	}

	var contents []ContentsXML
	for _, obj := range output.Objects {
		contents = append(contents, ContentsXML{
			Key:          obj.Key,
			LastModified: obj.LastModified.UTC().Format("2006-01-02T15:04:05.000Z"),
			ETag:         fmt.Sprintf(`"%s"`, obj.ETag),
			Size:         obj.Size,
			StorageClass: "STANDARD",
		})
	}

	var commonPrefixes []CommonPrefix
	for _, p := range output.CommonPrefixes {
		commonPrefixes = append(commonPrefixes, CommonPrefix{Prefix: p})
	}

	result := ListBucketResult{
		Xmlns:                 s3XmlNamespace,
		Name:                  bucket,
		Prefix:                input.Prefix,
		Delimiter:             input.Delimiter,
		MaxKeys:               maxKeys,
		IsTruncated:           output.IsTruncated,
		KeyCount:              output.KeyCount,
		ContinuationToken:     input.ContinuationToken,
		NextContinuationToken: output.NextContinuationToken,
		StartAfter:            input.StartAfter,
		Contents:              contents,
		CommonPrefixes:        commonPrefixes,
	}

	writeXML(w, http.StatusOK, result)
}
