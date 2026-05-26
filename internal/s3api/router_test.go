package s3api

import (
	"net/http/httptest"
	"testing"
)

func TestResolveBucketAndKey(t *testing.T) {
	tests := []struct {
		name           string
		domain         string
		host           string
		path           string
		expectedBucket string
		expectedKey    string
	}{
		{
			name:           "Path style (no domain config)",
			domain:         "",
			host:           "localhost:9000",
			path:           "/mybucket/mykey/file.txt",
			expectedBucket: "mybucket",
			expectedKey:    "mykey/file.txt",
		},
		{
			name:           "Path style (with domain config)",
			domain:         "objectra.local",
			host:           "objectra.local:9000",
			path:           "/mybucket/mykey/file.txt",
			expectedBucket: "mybucket",
			expectedKey:    "mykey/file.txt",
		},
		{
			name:           "Virtual Host style (localhost domain)",
			domain:         "localhost",
			host:           "mybucket.localhost:9000",
			path:           "/mykey/file.txt",
			expectedBucket: "mybucket",
			expectedKey:    "mykey/file.txt",
		},
		{
			name:           "Virtual Host style (custom domain)",
			domain:         "objectra.local",
			host:           "mybucket.objectra.local",
			path:           "/mykey/file.txt",
			expectedBucket: "mybucket",
			expectedKey:    "mykey/file.txt",
		},
		{
			name:           "Virtual Host style root path",
			domain:         "objectra.local",
			host:           "mybucket.objectra.local",
			path:           "/",
			expectedBucket: "mybucket",
			expectedKey:    "",
		},
		{
			name:           "Fallback on domain mismatch",
			domain:         "objectra.local",
			host:           "mybucket.different.com:9000",
			path:           "/anotherbucket/key.txt",
			expectedBucket: "anotherbucket",
			expectedKey:    "key.txt",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rt := &Router{
				domain: tc.domain,
			}
			req := httptest.NewRequest("GET", tc.path, nil)
			req.Host = tc.host

			bucket, key := rt.resolveBucketAndKey(req)
			if bucket != tc.expectedBucket {
				t.Errorf("expected bucket %q, got %q", tc.expectedBucket, bucket)
			}
			if key != tc.expectedKey {
				t.Errorf("expected key %q, got %q", tc.expectedKey, key)
			}
		})
	}
}
