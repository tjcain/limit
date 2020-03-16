// Package limit provides a http.RoundTripper implementation that works as a
// distributed leaky-bucket type rate limiter for HTTP requests.
//
package limit

import (
	"errors"
	"net/http"
	"strconv"
	"time"
)

const (
	xRateLimit          = "X-Rate-Limit-Limit"
	xRateLimitRemaining = "X-Rate-Limit-Remaining"
	xRateLimitRest      = "X-Rate-Limit-Limit"
)

var (
	ErrToManyRequests = errors.New("Too Many Requests")
)

// Bucket interface implements the leaky-bucket algorithm and is used by
// the Transport to rate limit out going requests.
type Bucket interface {
	// Consume consumes from the bucket and returns the bucket
	// state. In the case the rate limit is exceeded Consume
	// must return ErrTooManyRequests.
	//
	// Implementations of Consume are required to be thread safe.
	Consume(int) (State, error)
}

// State represents a bucket's state.
type State struct {
	Capacity int
	Space    int
	Reset    time.Time
}

func (s State) setXRateHeaders(r *http.Response) {
	r.Header.Set(xRateLimit, strconv.Itoa(s.Capacity))
	r.Header.Set(xRateLimitRemaining, strconv.Itoa(s.Space))
	remaining := int(time.Now().Sub(s.Reset).Seconds())
	r.Header.Set(xRateLimitRest, strconv.Itoa(remaining))
}

// Transport is an implementation of http.RoundTripper that will rate limit
// outgoing requests using the leaky-bucket algorithm, avoiding over-limit
// network requests, instead transprently returning a 429 / Too Many Requests.
type Transport struct {
	Transport http.RoundTripper
	Bucket    Bucket
}

// NewTransport returns a new Transport with the provided
// Bucket implementation.
func NewTransport(b Bucket) *Transport {
	return &Transport{Bucket: b}
}

// Client returns an *http.Client that limits requests.
func (t *Transport) Client() *http.Client {
	return &http.Client{Transport: t}
}

// RoundTrip takes a Request and returns a Response
//
// If the request exceeds the rate limit set, then a 429 error will be returned
// without connecting to the server.
func (t *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	transport := t.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}

	state, err := t.Bucket.Consume(1)
	if err != nil {
		if errors.Is(err, ErrToManyRequests) {
			resp := http.Response{
				Status:     "Too Many Requests",
				StatusCode: 429,
			}
			// TODO: consider setting end to end headers.
			resp.Header = make(http.Header)
			state.setXRateHeaders(&resp)
			return &resp, nil
		} else {
			return nil, err
		}
	}

	resp, err := transport.RoundTrip(req)
	if err != nil {
		return nil, err
	}

	// Assume our bucket is more accurate than third party.
	state.setXRateHeaders(resp)

	return resp, nil
}
