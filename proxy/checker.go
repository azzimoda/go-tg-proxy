package proxy

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-telegram/bot"

	"github.com/azzimoda/go-tg-proxy/proxyutil"
)

var (
	// ErrEmptyProxy is returned when a proxy address is empty.
	ErrEmptyProxy = errors.New("empty proxy")
	// ErrProxyUnavailable is returned when a proxy is unreachable.
	ErrProxyUnavailable = errors.New("proxy unavailable")
)

// Checker checks a single proxy and reports its round-trip latency.
type Checker interface {
	CheckLatency(ctx context.Context, proxy string) (time.Duration, error)
}

// DefaultCheckTimeout bounds a single proxy probe.
const DefaultCheckTimeout = 5 * time.Second

// TelegramChecker verifies a proxy end-to-end against the Telegram Bot API.
// The API call uses a fake token and the proxy is considered usable when the
// API answers "not found" — i.e. the proxy reaches Telegram.
type TelegramChecker struct{}

// CheckLatency checks the availability of a proxy and returns its round-trip
// latency.
func (TelegramChecker) CheckLatency(ctx context.Context, proxy string) (time.Duration, error) {
	if proxy == "" {
		return 0, ErrEmptyProxy
	}

	start := time.Now()

	httpClient, err := proxyutil.NewHTTPProxyClient(proxy)
	if err != nil {
		// Proxy is unavailable
		return 0, fmt.Errorf("%w: %w", ErrProxyUnavailable, err)
	}
	opts := []bot.Option{
		bot.WithHTTPClient(DefaultCheckTimeout, httpClient),
		bot.WithCheckInitTimeout(DefaultCheckTimeout),
	}
	_, err = bot.New("faketoken", opts...)

	if err != nil && errors.Is(err, bot.ErrorNotFound) {
		// Telegram API is available, then the proxy is available
		return time.Since(start), nil
	}
	return 0, ErrProxyUnavailable
}
