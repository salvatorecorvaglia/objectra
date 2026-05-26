package s3api

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"strconv"

	"github.com/salvatorecorvaglia/objectra/internal/storage"
)

// handleCreateMultipartUpload handles POST /<bucket>/<key>?uploads (CreateMultipartUpload).
func (rt *Router) handleCreateMultipartUpload(w http.ResponseWriter, r *http.Request, bucket, key string) {
	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	resource := "/" + bucket + "/" + key
	info, err := rt.engine.CreateMultipartUpload(bucket, key, contentType)
	if handleStorageError(w, err, resource) {
		return
	}

	result := InitiateMultipartUploadResult{
		Xmlns:    s3XmlNamespace,
		Bucket:   bucket,
		Key:      key,
		UploadId: info.UploadID,
	}

	writeXML(w, http.StatusOK, result)
}

// handleUploadPart handles PUT /<bucket>/<key>?partNumber=N&uploadId=ID (UploadPart).
func (rt *Router) handleUploadPart(w http.ResponseWriter, r *http.Request, bucket, key string) {
	query := r.URL.Query()
	uploadID := query.Get("uploadId")
	partNumberStr := query.Get("partNumber")
	resource := "/" + bucket + "/" + key

	partNumber, err := strconv.Atoi(partNumberStr)
	if err != nil || partNumber < 1 {
		writeS3Error(w, "InvalidArgument", "Invalid part number", resource)
		return
	}

	partInfo, err := rt.engine.UploadPart(bucket, key, uploadID, partNumber, r.Body, r.ContentLength)
	if handleStorageError(w, err, resource) {
		return
	}

	w.Header().Set("ETag", fmt.Sprintf(`"%s"`, partInfo.ETag))
	w.WriteHeader(http.StatusOK)
}

// handleCompleteMultipartUpload handles POST /<bucket>/<key>?uploadId=ID (CompleteMultipartUpload).
func (rt *Router) handleCompleteMultipartUpload(w http.ResponseWriter, r *http.Request, bucket, key string) {
	uploadID := r.URL.Query().Get("uploadId")
	resource := "/" + bucket + "/" + key

	var reqBody CompleteMultipartUploadRequest
	if err := xml.NewDecoder(r.Body).Decode(&reqBody); err != nil {
		writeS3Error(w, "MalformedXML", "The XML you provided was not well-formed", resource)
		return
	}

	var parts []storage.CompletePart
	for _, p := range reqBody.Parts {
		etag := p.ETag
		// Strip surrounding quotes if present
		if len(etag) >= 2 && etag[0] == '"' && etag[len(etag)-1] == '"' {
			etag = etag[1 : len(etag)-1]
		}
		parts = append(parts, storage.CompletePart{
			PartNumber: p.PartNumber,
			ETag:       etag,
		})
	}

	info, err := rt.engine.CompleteMultipartUpload(bucket, key, uploadID, parts)
	if handleStorageError(w, err, resource) {
		return
	}

	result := CompleteMultipartUploadResult{
		Xmlns:    s3XmlNamespace,
		Location: fmt.Sprintf("/%s/%s", bucket, key),
		Bucket:   bucket,
		Key:      key,
		ETag:     fmt.Sprintf(`"%s"`, info.ETag),
	}

	writeXML(w, http.StatusOK, result)
}

// handleAbortMultipartUpload handles DELETE /<bucket>/<key>?uploadId=ID (AbortMultipartUpload).
func (rt *Router) handleAbortMultipartUpload(w http.ResponseWriter, r *http.Request, bucket, key string) {
	uploadID := r.URL.Query().Get("uploadId")
	resource := "/" + bucket + "/" + key

	if err := rt.engine.AbortMultipartUpload(bucket, key, uploadID); handleStorageError(w, err, resource) {
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
