package s3api

import (
	"encoding/xml"
	"net/http"

	"github.com/salvatorecorvaglia/objectra/internal/storage"
)

// handleListBuckets handles GET / (ListBuckets).
func (rt *Router) handleListBuckets(w http.ResponseWriter, _ *http.Request) {
	buckets, err := rt.engine.ListBuckets()
	if err != nil {
		writeS3Error(w, "InternalError", err.Error(), "/")
		return
	}

	var bucketList []BucketXML
	for _, b := range buckets {
		bucketList = append(bucketList, BucketXML{
			Name:         b.Name,
			CreationDate: b.CreationDate.UTC().Format("2006-01-02T15:04:05.000Z"),
		})
	}

	result := ListAllMyBucketsResult{
		Xmlns: s3XmlNamespace,
		Owner: Owner{
			ID:          "objectra",
			DisplayName: "objectra",
		},
		Buckets: BucketsList{
			Bucket: bucketList,
		},
	}

	writeXML(w, http.StatusOK, result)
}

// handleCreateBucket handles PUT /<bucket> (CreateBucket).
func (rt *Router) handleCreateBucket(w http.ResponseWriter, _ *http.Request, bucket string) {
	if !storage.IsValidBucketName(bucket) {
		writeS3Error(w, "InvalidBucketName", "The specified bucket is not valid.", "/"+bucket)
		return
	}

	if err := rt.engine.CreateBucket(bucket); handleStorageError(w, err, "/"+bucket) {
		return
	}

	w.Header().Set("Location", "/"+bucket)
	w.WriteHeader(http.StatusOK)
}

// handleDeleteBucket handles DELETE /<bucket> (DeleteBucket).
func (rt *Router) handleDeleteBucket(w http.ResponseWriter, _ *http.Request, bucket string) {
	if err := rt.engine.DeleteBucket(bucket); handleStorageError(w, err, "/"+bucket) {
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// handleHeadBucket handles HEAD /<bucket> (HeadBucket).
func (rt *Router) handleHeadBucket(w http.ResponseWriter, _ *http.Request, bucket string) {
	exists, err := rt.engine.BucketExists(bucket)
	if err != nil {
		writeS3Error(w, "InternalError", err.Error(), "/"+bucket)
		return
	}
	if !exists {
		writeS3Error(w, "NoSuchBucket", "The specified bucket does not exist", "/"+bucket)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// handleGetBucketLocation handles GET /<bucket>?location (GetBucketLocation).
func (rt *Router) handleGetBucketLocation(w http.ResponseWriter, _ *http.Request, bucket string) {
	exists, err := rt.engine.BucketExists(bucket)
	if err != nil {
		writeS3Error(w, "InternalError", err.Error(), "/"+bucket)
		return
	}
	if !exists {
		writeS3Error(w, "NoSuchBucket", "The specified bucket does not exist", "/"+bucket)
		return
	}

	result := LocationConstraintResult{
		Xmlns:    s3XmlNamespace,
		Location: rt.region,
	}
	writeXML(w, http.StatusOK, result)
}


// handleGetBucketVersioning handles GET /<bucket>?versioning.
func (rt *Router) handleGetBucketVersioning(w http.ResponseWriter, _ *http.Request, bucket string) {
	status, err := rt.engine.GetBucketVersioning(bucket)
	if handleStorageError(w, err, "/"+bucket) {
		return
	}

	result := VersioningConfigurationXML{
		Xmlns: s3XmlNamespace,
	}
	if status == "Enabled" || status == "Suspended" {
		result.Status = status
	}

	writeXML(w, http.StatusOK, result)
}

// handlePutBucketVersioning handles PUT /<bucket>?versioning.
func (rt *Router) handlePutBucketVersioning(w http.ResponseWriter, r *http.Request, bucket string) {
	var reqBody VersioningConfigurationXML
	if err := xml.NewDecoder(r.Body).Decode(&reqBody); err != nil {
		writeS3Error(w, "MalformedXML", "The XML you provided was not well-formed", "/"+bucket)
		return
	}

	status := reqBody.Status
	// S3 accepts empty status to disable or explicit Suspended/Enabled
	if status == "" {
		status = "Disabled"
	}

	err := rt.engine.SetBucketVersioning(bucket, status)
	if handleStorageError(w, err, "/"+bucket) {
		return
	}

	w.WriteHeader(http.StatusOK)
}
