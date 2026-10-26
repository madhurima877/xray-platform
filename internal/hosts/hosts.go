// Package hosts turns URLs into the Black/White List lookup key xray-api
// uses: lower-case host name without "www.".
package hosts

import (
	"net"
	"net/url"
	"strings"
	"unicode"
)

// Normalize accepts a full URL, a bare host or "host/path" and returns the
// list key, or "" when there is no usable host.
func Normalize(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if !strings.Contains(raw, "://") {
		raw = "http://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	h := strings.TrimSuffix(strings.ToLower(u.Hostname()), ".")
	h = strings.TrimPrefix(h, "www.")
	if !valid(h) {
		return ""
	}
	return h
}

// valid accepts an IP address or a dotted name of letters, digits and "-".
func valid(h string) bool {
	if net.ParseIP(h) != nil {
		return true
	}
	if !strings.Contains(h, ".") || strings.HasPrefix(h, ".") || strings.Contains(h, "..") {
		return false
	}
	for _, r := range h {
		if r != '.' && r != '-' && !unicode.IsLetter(r) && !unicode.IsDigit(r) {
			return false
		}
	}
	return true
}
