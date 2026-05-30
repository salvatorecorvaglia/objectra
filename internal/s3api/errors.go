package s3api

import (
	"encoding/xml"
	"net/http"

	"github.com/google/uuid"
	"github.com/salvatorecorvaglia/objectra/internal/storage"
)

// s3XmlNamespace is the standard S3 XML namespace used in all responses.
const s3XmlNamespace = "http://s3.amazonaws.com/doc/2006-03-01/"

// S3 error code to HTTP status code mapping.
var errorHTTPStatus = map[string]int{
	"AccessDenied":            http.StatusForbidden,
	"BucketAlreadyExists":     http.StatusConflict,
	"BucketAlreadyOwnedByYou": http.StatusConflict,
	"BucketNotEmpty":          http.StatusConflict,
	"InvalidArgument":         http.StatusBadRequest,
	"InvalidBucketName":       http.StatusBadRequest,
	"NoSuchBucket":            http.StatusNotFound,
	"NoSuchKey":               http.StatusNotFound,
	"NoSuchUpload":            http.StatusNotFound,
	"NoSuchCORSConfiguration": http.StatusNotFound,
	"InternalError":           http.StatusInternalServerError,
	"MethodNotAllowed":        http.StatusMethodNotAllowed,
	"MalformedXML":            http.StatusBadRequest,
	"SignatureDoesNotMatch":   http.StatusForbidden,
	"InvalidAccessKeyId":      http.StatusForbidden,
	"RequestTimeTooSkewed":    http.StatusForbidden,
}

// handleStorageError writes the appropriate S3 error response for a storage error.
// Returns true if the error was handled, false if err is nil.
func handleStorageError(w http.ResponseWriter, err error, resource string) bool {
	if err == nil {
		return false
	}
	if s3Err, ok := err.(*storage.S3Error); ok {
		writeS3Error(w, s3Err.Code, s3Err.Message, resource)
	} else {
		writeS3Error(w, "InternalError", err.Error(), resource)
	}
	return true
}

// writeS3Error writes an S3-compatible XML error response.
func writeS3Error(w http.ResponseWriter, code, message, resource string) {
	status, ok := errorHTTPStatus[code]
	if !ok {
		status = http.StatusInternalServerError
	}

	resp := ErrorResponse{
		Code:      code,
		Message:   message,
		Resource:  resource,
		RequestId: uuid.New().String(),
	}

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_ = xml.NewEncoder(w).Encode(resp)
}

// writeXML writes an XML response with the given status code.
func writeXML(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(xml.Header))
	_ = xml.NewEncoder(w).Encode(v)
}
