package util

import (
	"bytes"
	"io"
	"net/http"
	"time"
)

// RealHTTPClient implements HTTPClient using standard http.Client
type RealHTTPClient struct {
	client *http.Client
}

// NewRealHTTPClient creates a new RealHTTPClient
func NewRealHTTPClient(timeout time.Duration) HTTPClient {
	return &RealHTTPClient{
		client: &http.Client{
			Timeout: timeout,
		},
	}
}

// Do executes an HTTP request
func (r *RealHTTPClient) Do(req *HTTPRequest) (*HTTPResponse, error) {
	httpReq, err := http.NewRequest(req.Method, req.URL, req.Body)
	if err != nil {
		return nil, err
	}

	for k, v := range req.Headers {
		httpReq.Header.Set(k, v)
	}

	resp, err := r.client.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	headers := make(map[string]string)
	for k, v := range resp.Header {
		if len(v) > 0 {
			headers[k] = v[0]
		}
	}

	return &HTTPResponse{
		StatusCode: resp.StatusCode,
		Body:       body,
		Headers:    headers,
	}, nil
}

// NewRequestFromBytes creates an HTTPRequest from method, URL, and body bytes
func NewRequestFromBytes(method, url string, body []byte, contentType string) *HTTPRequest {
	headers := make(map[string]string)
	if contentType != "" {
		headers["Content-Type"] = contentType
	}

	return &HTTPRequest{
		Method:  method,
		URL:     url,
		Headers: headers,
		Body:    bytes.NewReader(body),
	}
}
