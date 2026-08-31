package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/avast/retry-go/v5"
	"github.com/rs/zerolog/log"
)

// DefaultProxiflySourceURL is the default mirror of the proxifly free-proxy-list.
const DefaultProxiflySourceURL = "https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/all/data.json"

// ProxiflySource is a [Source] that fetches the proxifly free-proxy-list JSON.
// It knows the proxifly schema and applies the protocol/country filters, so the
// rest of the package deals only with "ip:port" strings.
type ProxiflySource struct {
	url              string
	protocols        []string
	excludeCountries []string
	client           *http.Client
}

// ProxiflyOption configures a ProxiflySource.
type ProxiflyOption func(*ProxiflySource)

// WithProxiflyProtocols restricts the source to the given proxy protocols
// (e.g. "socks5"). Defaults to ["socks5"].
func WithProxiflyProtocols(protocols ...string) ProxiflyOption {
	return func(s *ProxiflySource) { s.protocols = protocols }
}

// WithProxiflyExcludeCountries excludes proxies geolocated in the given
// countries (ISO 3166-1 alpha-2, e.g. "RU"). Defaults to ["RU"].
func WithProxiflyExcludeCountries(countries ...string) ProxiflyOption {
	return func(s *ProxiflySource) { s.excludeCountries = countries }
}

// WithProxiflyClient overrides the HTTP client used to fetch the list.
// Defaults to a client with a 30s timeout.
func WithProxiflyClient(c *http.Client) ProxiflyOption {
	return func(s *ProxiflySource) { s.client = c }
}

// NewProxiflySource returns a ProxiflySource. Unless overridden by options, it
// keeps only socks5 proxies that are not geolocated in Russia.
func NewProxiflySource(url string, opts ...ProxiflyOption) *ProxiflySource {
	s := &ProxiflySource{
		url:              url,
		protocols:        []string{"socks5"},
		excludeCountries: []string{"RU"},
		client:           &http.Client{Timeout: 30 * time.Second},
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Fetch downloads the proxy list, filters it and returns the usable "ip:port"
// addresses. An error is returned when the list cannot be fetched or contains
// no usable proxies.
func (s *ProxiflySource) Fetch(ctx context.Context) ([]string, error) {
	body, err := fetchWithRetry(ctx, s.client, s.url)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch proxy list: %w", err)
	}
	return s.parseList(body)
}

// proxiflyEntry is a single record of the proxifly free-proxy-list JSON.
type proxiflyEntry struct {
	Protocol    string `json:"protocol"`
	IP          string `json:"ip"`
	Port        int    `json:"port"`
	Geolocation struct {
		Country string `json:"country"`
	} `json:"geolocation"`
}

func (s *ProxiflySource) parseList(data []byte) ([]string, error) {
	var entries []proxiflyEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, fmt.Errorf("failed to parse proxy list: %w", err)
	}

	excluded := make(map[string]bool, len(s.excludeCountries))
	for _, c := range s.excludeCountries {
		excluded[c] = true
	}
	allowed := make(map[string]bool, len(s.protocols))
	for _, p := range s.protocols {
		allowed[p] = true
	}

	var proxies []string
	for _, e := range entries {
		if allowed[e.Protocol] && e.IP != "" && e.Port > 0 && !excluded[e.Geolocation.Country] {
			proxies = append(proxies, fmt.Sprintf("%s:%d", e.IP, e.Port))
		}
	}
	if len(proxies) == 0 {
		return nil, fmt.Errorf("no usable proxies in list")
	}
	log.Debug().Int("count", len(proxies)).Msg("Loaded proxies")
	return proxies, nil
}

const fetchAttempts = 5

func fetchWithRetry(ctx context.Context, client *http.Client, url string) ([]byte, error) {
	var body []byte
	err := retry.New(
		retry.Attempts(fetchAttempts),
		retry.Delay(100*time.Millisecond),
		retry.DelayType(retry.BackOffDelay),
		retry.Context(ctx),
		retry.OnRetry(func(attempt uint, err error) {
			log.Debug().Uint("attempt", attempt).Err(err).Msg("Retrying proxy source fetch...")
		}),
	).Do(func() error {
		resp, err := client.Get(url)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("unexpected status %d", resp.StatusCode)
		}
		body, err = io.ReadAll(resp.Body)
		return err
	})
	return body, err
}
