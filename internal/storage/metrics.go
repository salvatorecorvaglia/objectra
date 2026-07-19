package storage

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// MetricsTracker holds thread-safe global statistics about the server operations.
type MetricsTracker struct {
	RequestsTotal    uint64
	RequestErrors    uint64
	BytesUploaded    uint64
	BytesDownloaded  uint64
	ActiveMultiparts int64

	// Disk space caching
	lastDiskCheck    time.Time
	cachedTotal      uint64
	cachedFree       uint64
	diskMu           sync.Mutex
}

// GlobalMetrics is the singleton instance of the metrics tracker.
var GlobalMetrics = &MetricsTracker{}

func (m *MetricsTracker) IncRequests() {
	atomic.AddUint64(&m.RequestsTotal, 1)
}

func (m *MetricsTracker) IncErrors() {
	atomic.AddUint64(&m.RequestErrors, 1)
}

func (m *MetricsTracker) AddUploaded(n uint64) {
	atomic.AddUint64(&m.BytesUploaded, n)
}

func (m *MetricsTracker) AddDownloaded(n uint64) {
	atomic.AddUint64(&m.BytesDownloaded, n)
}

func (m *MetricsTracker) IncActiveMultiparts() {
	atomic.AddInt64(&m.ActiveMultiparts, 1)
}

func (m *MetricsTracker) DecActiveMultiparts() {
	atomic.AddInt64(&m.ActiveMultiparts, -1)
}


// FormatPrometheus generates a plain-text Prometheus-compliant metrics string.
func (m *MetricsTracker) FormatPrometheus(dataDir string) string {
	m.diskMu.Lock()
	if time.Since(m.lastDiskCheck) > 30*time.Second {
		total, free, err := GetDiskSpace(dataDir)
		if err == nil {
			m.cachedTotal = total
			m.cachedFree = free
			m.lastDiskCheck = time.Now()
		}
	}
	diskTotalVal := m.cachedTotal
	diskFreeVal := m.cachedFree
	m.diskMu.Unlock()

	reqs := atomic.LoadUint64(&m.RequestsTotal)
	errs := atomic.LoadUint64(&m.RequestErrors)
	up := atomic.LoadUint64(&m.BytesUploaded)
	down := atomic.LoadUint64(&m.BytesDownloaded)
	activeMP := atomic.LoadInt64(&m.ActiveMultiparts)

	return fmt.Sprintf(`# HELP objectra_requests_total Total number of S3 requests processed.
# TYPE objectra_requests_total counter
objectra_requests_total %d

# HELP objectra_request_errors_total Total number of S3 request errors.
# TYPE objectra_request_errors_total counter
objectra_request_errors_total %d

# HELP objectra_bytes_uploaded_total Total bytes uploaded to Objectra.
# TYPE objectra_bytes_uploaded_total counter
objectra_bytes_uploaded_total %d

# HELP objectra_bytes_downloaded_total Total bytes downloaded from Objectra.
# TYPE objectra_bytes_downloaded_total counter
objectra_bytes_downloaded_total %d

# HELP objectra_active_multipart_uploads Number of active multipart uploads.
# TYPE objectra_active_multipart_uploads gauge
objectra_active_multipart_uploads %d

# HELP objectra_disk_total_bytes Total disk space of the storage directory in bytes.
# TYPE objectra_disk_total_bytes gauge
objectra_disk_total_bytes %d

# HELP objectra_disk_free_bytes Free disk space of the storage directory in bytes.
# TYPE objectra_disk_free_bytes gauge
objectra_disk_free_bytes %d
`, reqs, errs, up, down, activeMP, diskTotalVal, diskFreeVal)
}
