package redis

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/tjcain/limit"
)

const (
	localRedis    = "localhost:6379"
	defaultAppKey = "test-app-key"
)

var defaultTestCfg = Config{
	RedisAddr:     localRedis,
	AppKey:        defaultAppKey,
	RequestLimit:  2,
	LimitDuration: time.Second,
}

func Test_Bucket_Sync(t *testing.T) {
	client, url, teardown := setup(defaultTestCfg)
	defer teardown()

	req, _ := http.NewRequest("GET", url, nil)

	for i := 0; i < defaultTestCfg.RequestLimit*2; i++ {
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}

		if i >= defaultTestCfg.RequestLimit {
			if resp.StatusCode != 429 {
				t.Errorf("expected status 429, got %d", resp.StatusCode)
			}
		} else {
			if resp.StatusCode != 200 {
				t.Errorf("expected status 200, got %d", resp.StatusCode)
			}
		}
	}
}

// TODO: add tests for concurrency (currently leveraging go-redis implementation of
// concurrent txns).

func setup(cfg Config) (*http.Client, string, func()) {
	bucket, err := NewBucket(cfg)
	if err != nil {
		panic(err)
	}

	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte("OK"))
		}))

	transport := limit.NewTransport(bucket)
	client := http.Client{Transport: transport}

	return &client, server.URL, func() {
		bucket.rdb.FlushAll()
	}
}
