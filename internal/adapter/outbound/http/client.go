package http

import (
	"net/http"
	"time"
)

// NewClient builds the HTTP client every outbound call to another platform
// service uses. It returns a redirect as the answer instead of following it:
// these calls carry the system caller identity as headers, and a followed
// redirect would hand them to whatever the Location names.
func NewClient(timeout time.Duration) *http.Client {
	return &http.Client{
		Timeout:       timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
}
