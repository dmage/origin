package httprequest

import (
	"net"
	"net/http"
	"strings"

	"bitbucket.org/ww/goautoneg"
)

// PrefersHTML returns true if the request was made by something that looks like a browser, or can receive HTML
func PrefersHTML(req *http.Request) bool {
	accepts := goautoneg.ParseAccept(req.Header.Get("Accept"))
	acceptsHTML := false
	acceptsJSON := false
	for _, accept := range accepts {
		if accept.Type == "text" && accept.SubType == "html" {
			acceptsHTML = true
		} else if accept.Type == "application" && accept.SubType == "json" {
			acceptsJSON = true
		}
	}

	// If HTML is accepted, return true
	if acceptsHTML {
		return true
	}

	// If JSON was specifically requested, return false
	// This gives browsers a way to make requests and add an "Accept" header to request JSON
	if acceptsJSON {
		return false
	}

	// In Intranet/Compatibility mode, IE sends an Accept header that does not contain "text/html".
	if strings.HasPrefix(req.UserAgent(), "Mozilla") {
		return true
	}

	return false
}

// isDefaultPort checks if port is a default port for scheme (80 for http, 443
// for https). If scheme is empty, then all schemes are checked.
func isDefaultPort(scheme, port string) bool {
	switch port {
	case "80":
		return scheme == "" || scheme == "http"
	case "443":
		return scheme == "" || scheme == "https"
	}
	return false
}

// SchemeHost returns the scheme and host used to make this request.
// Suitable for use to compute scheme/host in returned 302 redirect Location.
// Note the returned host is not normalized, and may or may not contain a port.
// Returned values are based on the following information:
//
// Host:
// * X-Forwarded-Host/X-Forwarded-Port/X-Forwarded-Proto headers
// * Host field on the request (parsed from Host header)
// * Host in the request's URL (parsed from Request-Line)
//
// Scheme:
// * X-Forwarded-Proto header
// * Existence of TLS information on the request implies https
// * Scheme in the request's URL (parsed from Request-Line)
// * Port (if included in calculated Host value, 443 implies https)
// * Otherwise, defaults to "http"
func SchemeHost(req *http.Request) (string /*scheme*/, string /*host*/) {
	forwarded := func(attr string) string {
		// Get the X-Forwarded-<attr> value
		value := req.Header.Get("X-Forwarded-" + attr)
		// Take the first comma-separated value, if multiple exist
		value = strings.SplitN(value, ",", 2)[0]
		// Trim whitespace
		return strings.TrimSpace(value)
	}

	forwardedProto := forwarded("Proto")
	forwardedHost := forwarded("Host")
	forwardedPort := forwarded("Port")

	scheme := ""
	switch {
	case len(forwardedProto) > 0:
		scheme = forwardedProto
	case req.TLS != nil:
		scheme = "https"
	case len(req.URL.Scheme) > 0:
		scheme = req.URL.Scheme
	}

	hostHeader := ""
	switch {
	case len(forwardedHost) > 0:
		hostHeader = forwardedHost
	case len(req.Host) > 0:
		hostHeader = req.Host
	case len(req.URL.Host) > 0:
		hostHeader = req.URL.Host
	}

	host, port, err := net.SplitHostPort(hostHeader)
	if err != nil {
		host = hostHeader
		port = ""
	}

	// If X-Forwarded-Port is sent, use the explicit port info
	if len(forwardedPort) > 0 && (len(port) != 0 || !isDefaultPort(scheme, forwardedPort)) {
		port = forwardedPort
	}

	if len(scheme) == 0 {
		if port == "443" || len(port) == 0 && forwardedPort == "443" {
			scheme = "https"
		} else {
			scheme = "http"
		}
	}

	if len(port) != 0 {
		hostHeader = net.JoinHostPort(host, port)
	} else {
		hostHeader = host
	}

	return scheme, hostHeader
}
