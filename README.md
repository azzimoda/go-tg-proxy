# go-tg-proxy

Reusable building blocks for running `go-telegram/bot` bots over SOCKS5 proxies:
a latency-checked proxy pool with a pluggable source, and a bot lifecycle
manager that restarts the bot on a new proxy when the connection degrades.

Built for Telegram bots that run from restricted environments (e.g. Russia),
where direct access to `api.telegram.org` is blocked and proxy rotation is a
must. Extracted from [raspishika-gx](https://github.com/azzimoda/raspishika-gx).

## Packages

| Package | Purpose |
|---|---|
| `proxyutil` | `NewHTTPProxyClient` — an `*http.Client` dialing through a SOCKS5 proxy (context-aware). |
| `proxy` | `Source` interface + `ProxiflySource` (default provider), in-memory `Repository`, latency `Checker` + `TelegramChecker`, and a `Service` that keeps a warm pool of verified proxies. |
| `botservice` | `BotService` — starts/stops/restarts a `*bot.Bot`, picking the best available proxy, with a watchdog that rotates the proxy when Telegram stops delivering updates. |

## Quick start

```go
source := proxy.NewProxiflySource(proxy.DefaultProxiflySourceURL)
proxies := proxy.NewService(source)
proxies.StartPoolLoop()
defer proxies.Stop()

proxyAddr, err := proxies.FirstAvailable(ctx)
if err != nil {
    return err
}

httpClient, err := proxyutil.NewHTTPProxyClient(proxyAddr)
if err != nil {
    return err
}
b, err := bot.New(token,
    bot.WithHTTPClient(10*time.Second, httpClient),
    bot.WithCheckInitTimeout(10*time.Second),
)
if err != nil {
    return err
}
```

Or let `botservice` own the whole lifecycle:

```go
bs := botservice.NewBotService(func(proxy string, onActivity func()) (*bot.Bot, error) {
    httpClient, err := proxyutil.NewHTTPProxyClient(proxy)
    if err != nil {
        return nil, err
    }
    return bot.New(token,
        bot.WithHTTPClient(10*time.Second, httpClient),
        bot.WithCheckInitTimeout(10*time.Second),
        bot.WithMiddlewares(func(next bot.HandlerFunc) bot.HandlerFunc {
            return func(ctx context.Context, b *bot.Bot, u *models.Update) { onActivity(); next(ctx, b, u) }
        }),
    )
}, proxies)
bs.Start(ctx)
```

## Configuration

- The proxy **source** is injected via the `proxy.Source` interface — swap
  `ProxiflySource` for any provider that returns `ip:port` strings.
- `ProxiflySource` defaults to keeping only non-Russian `socks5` proxies from
  the proxifly jsdelivr mirror. Tune it with options:

```go
proxy.NewProxiflySource(proxy.DefaultProxiflySourceURL,
    proxy.WithProxiflyProtocols("socks5", "socks4"),
    proxy.WithProxiflyExcludeCountries("RU", "CN"),
    proxy.WithProxiflyClient(&http.Client{Timeout: time.Minute}),
)
```

- `proxy.NewService(source, ...)` accepts `WithChecker`, `WithRepository`,
  `WithPoolSize`, `WithFinderWorkers`, `WithPoolRecheck`, `WithCacheTTL`.

## How proxies are verified

`TelegramChecker` probes `api.telegram.org` end-to-end with a fake token: a
proxy is usable when the API replies "not found" (i.e. the request actually
reached Telegram). The probe measures round-trip latency, so the pool always
prefers the fastest live proxy.

`BotService` additionally watches for a stalled connection: if Telegram sends
no updates for over 3 minutes, or the API becomes unreachable, it restarts the
bot on the next best proxy.

## License

MIT.