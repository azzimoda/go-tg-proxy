// Package proxy provides a latency-checked pool of SOCKS5 proxies together
// with helpers for building Telegram bot HTTP clients through them.
package proxy

import "context"

// Source fetches a list of usable proxies as "ip:port" strings. Implementations
// are responsible for parsing and filtering the raw data (protocol, country,
// ...) and for returning an error when the list cannot be fetched or contains
// nothing usable, so the caller can keep the previously known proxies.
type Source interface {
	Fetch(ctx context.Context) ([]string, error)
}
