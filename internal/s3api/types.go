// Package s3api implements the S3-compatible REST API handlers.
package s3api

import "encoding/xml"

// --- ListBuckets ---

// ListAllMyBucketsResult is the XML response for ListBuckets.
type ListAllMyBucketsResult struct {
	XMLName xml.Name      `xml:"ListAllMyBucketsResult"`
	Xmlns   string        `xml:"xmlns,attr"`
	Owner   Owner         `xml:"Owner"`
	Buckets BucketsList   `xml:"Buckets"`
}

// Owner represents the bucket owner.
type Owner struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}

// BucketsList wraps a list of buckets.
type BucketsList struct {
	Bucket []BucketXML `xml:"Bucket"`
}

// BucketXML represents a single bucket in XML responses.
type BucketXML struct {
	Name         string `xml:"Name"`
	CreationDate string `xml:"CreationDate"`
}

// --- ListObjectsV2 ---

// ListBucketResult is the XML response for ListObjectsV2.
type ListBucketResult struct {
	XMLName               xml.Name       `xml:"ListBucketResult"`
	Xmlns                 string         `xml:"xmlns,attr"`
	Name                  string         `xml:"Name"`
	Prefix                string         `xml:"Prefix"`
	Delimiter             string         `xml:"Delimiter,omitempty"`
	MaxKeys               int            `xml:"MaxKeys"`
	IsTruncated           bool           `xml:"IsTruncated"`
	KeyCount              int            `xml:"KeyCount"`
	ContinuationToken     string         `xml:"ContinuationToken,omitempty"`
	NextContinuationToken string         `xml:"NextContinuationToken,omitempty"`
	StartAfter            string         `xml:"StartAfter,omitempty"`
	Contents              []ContentsXML  `xml:"Contents"`
	CommonPrefixes        []CommonPrefix `xml:"CommonPrefixes,omitempty"`
}

// ContentsXML represents a single object in a list response.
type ContentsXML struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
}

// CommonPrefix represents a common prefix (virtual directory) in list responses.
type CommonPrefix struct {
	Prefix string `xml:"Prefix"`
}

// --- CreateBucket ---

// CreateBucketConfiguration is the XML body for CreateBucket requests.
type CreateBucketConfiguration struct {
	XMLName            xml.Name `xml:"CreateBucketConfiguration"`
	LocationConstraint string   `xml:"LocationConstraint"`
}

// --- Multipart Upload ---

// InitiateMultipartUploadResult is the XML response for CreateMultipartUpload.
type InitiateMultipartUploadResult struct {
	XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
	Xmlns    string   `xml:"xmlns,attr"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadId string   `xml:"UploadId"`
}

// CompleteMultipartUploadRequest is the XML body for CompleteMultipartUpload.
type CompleteMultipartUploadRequest struct {
	XMLName xml.Name       `xml:"CompleteMultipartUpload"`
	Parts   []PartXML      `xml:"Part"`
}

// PartXML represents a part in multipart upload XML.
type PartXML struct {
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}

// CompleteMultipartUploadResult is the XML response for CompleteMultipartUpload.
type CompleteMultipartUploadResult struct {
	XMLName  xml.Name `xml:"CompleteMultipartUploadResult"`
	Xmlns    string   `xml:"xmlns,attr"`
	Location string   `xml:"Location"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	ETag     string   `xml:"ETag"`
}

// --- CopyObject ---

// CopyObjectResult is the XML response for CopyObject.
type CopyObjectResult struct {
	XMLName      xml.Name `xml:"CopyObjectResult"`
	LastModified string   `xml:"LastModified"`
	ETag         string   `xml:"ETag"`
}

// --- Error ---

// ErrorResponse is the XML error response format.
type ErrorResponse struct {
	XMLName   xml.Name `xml:"Error"`
	Code      string   `xml:"Code"`
	Message   string   `xml:"Message"`
	Resource  string   `xml:"Resource,omitempty"`
	RequestId string   `xml:"RequestId"`
}

// --- Location ---

// LocationConstraint is the XML response for GetBucketLocation.
type LocationConstraintResult struct {
	XMLName  xml.Name `xml:"LocationConstraint"`
	Xmlns    string   `xml:"xmlns,attr"`
	Location string   `xml:",chardata"`
}

// --- CORS ---

// CORSConfigurationXML represents S3 CORS XML body.
type CORSConfigurationXML struct {
	XMLName   xml.Name      `xml:"CORSConfiguration"`
	Xmlns     string        `xml:"xmlns,attr,omitempty"`
	CORSRules []CORSRuleXML `xml:"CORSRule"`
}

// CORSRuleXML represents a single S3 CORS XML rule.
type CORSRuleXML struct {
	AllowedHeader []string `xml:"AllowedHeader,omitempty"`
	AllowedMethod []string `xml:"AllowedMethod"`
	AllowedOrigin []string `xml:"AllowedOrigin"`
	ExposeHeader  []string `xml:"ExposeHeader,omitempty"`
	MaxAgeSeconds int      `xml:"MaxAgeSeconds,omitempty"`
}
