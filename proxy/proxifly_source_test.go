package proxy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

const sampleProxies = `[
	{"protocol":"socks5","ip":"1.2.3.4","port":1080,"geolocation":{"country":"US"}},
	{"protocol":"socks5","ip":"5.6.7.8","port":1081,"geolocation":{"country":"DE"}},
	{"protocol":"socks5","ip":"9.9.9.9","port":1082,"geolocation":{"country":"RU"}},
	{"protocol":"http","ip":"7.7.7.7","port":8080,"geolocation":{"country":"US"}},
	{"protocol":"socks5","ip":"","port":1083,"geolocation":{"country":"US"}},
	{"protocol":"socks5","ip":"4.4.4.4","port":0,"geolocation":{"country":"US"}}
]`

func TestProxiflyParseList(t *testing.T) {
	src := NewProxiflySource("http://example.invalid/list")
	got, err := src.parseList([]byte(sampleProxies))
	if err != nil {
		t.Fatalf("parseList() error: %v", err)
	}

	want := []string{"1.2.3.4:1080", "5.6.7.8:1081"}
	if len(got) != len(want) {
		t.Fatalf("got %d proxies, want %d: %v", len(got), len(want), got)
	}
	for i, w := range want {
		if got[i] != w {
			t.Fatalf("proxies[%d] = %q, want %q", i, got[i], w)
		}
	}
}

func TestProxiflyCustomFilters(t *testing.T) {
	src := NewProxiflySource("http://example.invalid/list",
		WithProxiflyProtocols("http"),
		WithProxiflyExcludeCountries(),
	)
	got, err := src.parseList([]byte(sampleProxies))
	if err != nil {
		t.Fatalf("parseList() error: %v", err)
	}
	want := []string{"7.7.7.7:8080"}
	if len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("parseList() = %v, want %v", got, want)
	}
}

func TestProxiflyParseListError(t *testing.T) {
	src := NewProxiflySource("http://example.invalid/list")

	if _, err := src.parseList([]byte("not json{")); err == nil {
		t.Fatal("expected error on malformed JSON")
	}
	if _, err := src.parseList([]byte(`[{"protocol":"http","ip":"1.1.1.1","port":8080}]`)); err == nil {
		t.Fatal("expected error when no usable proxies")
	}
}

func TestFetchWithRetryErrorOnBadStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	client := &http.Client{Timeout: 10 * time.Second}
	if _, err := fetchWithRetry(context.Background(), client, srv.URL); err == nil {
		t.Fatal("expected error on 500 response")
	}
}

func TestFetchWithRetryServesBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`[{"protocol":"socks5","ip":"1.2.3.4","port":1080}]`))
	}))
	defer srv.Close()

	src := NewProxiflySource(srv.URL)
	got, err := src.Fetch(context.Background())
	if err != nil {
		t.Fatalf("Fetch() error: %v", err)
	}
	if len(got) != 1 || got[0] != "1.2.3.4:1080" {
		t.Fatalf("Fetch() = %v, want [1.2.3.4:1080]", got)
	}

	if src.client.Timeout != 30*time.Second {
		t.Fatalf("default client timeout = %v, want 30s", src.client.Timeout)
	}
}
