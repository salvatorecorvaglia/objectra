package s3api_test

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/salvatorecorvaglia/stiva/internal/s3api"
)

// TestPutBucketVersioningRejectsOversizedBody guards against unbounded
// xml.Decoder allocation. Several subresource PUT handlers used to decode
// r.Body directly with no size limit; a request whose payload hash is
// UNSIGNED-PAYLOAD (or any mode auth.HashPayload doesn't size-check) could
// send an arbitrarily large body and force the decoder to keep allocating.
// This sends a body padded well past the configured limit and asserts the
// handler rejects it as malformed (truncated) rather than decoding the whole
// thing.
func TestPutBucketVersioningRejectsOversizedBody(t *testing.T) {
	rt, eng := newOpsRouter(t)
	if err := eng.CreateBucket("limits"); err != nil {
		t.Fatalf("create bucket: %v", err)
	}

	// A well-formed VersioningConfiguration padded with a huge comment before
	// the closing tag, so it would parse successfully if read in full but
	// must be rejected once truncated at the body-size limit.
	padding := strings.Repeat("A", 3<<20) // 3MB, above the 2MB limit
	body := `<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><!--` +
		padding + `--><Status>Enabled</Status></VersioningConfiguration>`

	req := httptest.NewRequest(http.MethodPut, "/limits?versioning", strings.NewReader(body))
	w := httptest.NewRecorder()
	rt.HandleBucketOps(w, req, "limits")

	if w.Code == http.StatusOK {
		t.Fatalf("expected oversized body to be rejected, got 200 OK (body limit not enforced)")
	}

	var errResp s3api.ErrorResponse
	if err := xml.Unmarshal(w.Body.Bytes(), &errResp); err != nil {
		t.Fatalf("decode error response: %v (body: %s)", err, w.Body.String())
	}
	if errResp.Code != "MalformedXML" {
		t.Errorf("error code = %q, want MalformedXML", errResp.Code)
	}
}
