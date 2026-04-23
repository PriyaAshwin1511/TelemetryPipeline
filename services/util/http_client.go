package util

import "io"

// HTTPClient defines the interface for HTTP operations.
// It provides a method to execute HTTP requests.
type HTTPClient interface {
	// Do executes an HTTP request and returns the response.
	Do(req *HTTPRequest) (*HTTPResponse, error)
}

// HTTPRequest represents an HTTP request with method, URL, headers, and body.
type HTTPRequest struct {
	Method  string            // HTTP method (GET, POST, etc.)
	URL     string            // Request URL
	Headers map[string]string // HTTP headers
	Body    io.Reader         // Request body
}

// HTTPResponse represents an HTTP response with status code, body, and headers.
type HTTPResponse struct {
	StatusCode int               // HTTP status code
	Body       []byte            // Response body
	Headers    map[string]string // Response headers
}
