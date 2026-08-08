// Package httpx holds small HTTP helpers shared by the S3 API and the console.
package httpx

import (
	"net"
	"net/http"
	"strings"
)

// ClientIP returns the best-effort client address for a request.
//
// When proxy headers are trusted, the address is taken from the RIGHT of the
// X-Forwarded-For list, walking left by trustedHops entries — one per reverse
// proxy in front of Stiva.
//
// Reading the leftmost entry instead (the previous behaviour) is unsafe: that
// element is supplied verbatim by the caller, so anyone could rotate it per
// request to defeat the per-IP login rate limit and to forge the source address
// recorded in the S3 access log. Only the entries appended by infrastructure we
// control are trustworthy, and those sit at the right-hand end.
func ClientIP(r *http.Request, trustProxy bool, trustedHops int) string {
	direct := remoteHost(r.RemoteAddr)
	if !trustProxy {
		return direct
	}

	xff := r.Header.Get("X-Forwarded-For")
	if xff == "" {
		return direct
	}

	parts := strings.Split(xff, ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}

	if trustedHops < 1 {
		trustedHops = 1
	}

	// The rightmost entry was appended by the proxy directly in front of us;
	// each additional trusted hop moves one position further left.
	idx := len(parts) - trustedHops
	if idx < 0 {
		// The chain is shorter than the configured hop count, so every entry is
		// caller-controlled. Fall back to the peer address.
		return direct
	}

	candidate := parts[idx]
	if net.ParseIP(candidate) == nil {
		return direct
	}
	return candidate
}

// remoteHost strips the port from a RemoteAddr, tolerating addresses that
// carry no port.
func remoteHost(remoteAddr string) string {
	if host, _, err := net.SplitHostPort(remoteAddr); err == nil {
		return host
	}
	return remoteAddr
}
