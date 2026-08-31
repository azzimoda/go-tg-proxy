package proxy

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

// ErrNoAvailableProxy is returned when no usable proxy can be found.
var ErrNoAvailableProxy = errors.New("no available proxy")

const (
	defaultFinderWorkers = 128
	defaultPoolSize      = 3
	defaultPoolRecheck   = 30 * time.Second
	defaultCacheTTL      = 1 * time.Hour
)

// Option configures a Service.
type Option func(*Service)

// WithRepository overrides the proxy store. Defaults to an in-memory
// repository.
func WithRepository(repo Repository) Option {
	return func(s *Service) { s.repo = repo }
}

// WithChecker overrides the proxy availability checker. Defaults to
// [TelegramChecker].
func WithChecker(c Checker) Option {
	return func(s *Service) { s.checker = c }
}

// WithCacheTTL sets how long a fetched proxy list is considered actual.
// Defaults to 1 hour.
func WithCacheTTL(d time.Duration) Option {
	return func(s *Service) { s.cacheTTL = d }
}

// WithPoolSize sets the number of proxies kept warm in the pool.
// Defaults to 3.
func WithPoolSize(n int) Option {
	return func(s *Service) { s.poolSize = n }
}

// WithFinderWorkers sets the concurrency of latency scans. Defaults to 128.
func WithFinderWorkers(n int) Option {
	return func(s *Service) { s.finderWorkers = n }
}

// WithPoolRecheck sets how often the warm pool is revalidated in the
// background. Defaults to 30 seconds.
func WithPoolRecheck(d time.Duration) Option {
	return func(s *Service) { s.poolRecheck = d }
}

// Service keeps a warm pool of latency-checked proxies fetched from a [Source]
// and hands out the best available one on demand.
type Service struct {
	source  Source
	repo    Repository
	checker Checker

	cacheTTL      time.Duration
	finderWorkers int
	poolSize      int
	poolRecheck   time.Duration

	muUpdate sync.Mutex // serializes source updates

	poolMu sync.Mutex
	pool   []pooledProxy

	startPoolOnce sync.Once
	stopPool      context.CancelFunc
}

type pooledProxy struct {
	proxy   string
	latency time.Duration
	checked time.Time
}

// NewService returns a Service that fetches proxies from source.
func NewService(source Source, opts ...Option) *Service {
	s := &Service{
		source:        source,
		repo:          NewMemoryRepository(),
		checker:       TelegramChecker{},
		cacheTTL:      defaultCacheTTL,
		finderWorkers: defaultFinderWorkers,
		poolSize:      defaultPoolSize,
		poolRecheck:   defaultPoolRecheck,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// StartPoolLoop starts the background goroutine that keeps the pool of verified
// proxies warm. It is started lazily on the first FirstAvailable call as well.
func (s *Service) StartPoolLoop() {
	s.startPoolOnce.Do(func() {
		ctx, cancel := context.WithCancel(context.Background())
		s.stopPool = cancel
		go s.poolLoop(ctx)
	})
}

func (s *Service) Stop() {
	if s.stopPool != nil {
		s.stopPool()
	}
}

// FirstAvailable returns the best available proxy from the warm pool.
//
// Falls back to a full scan of the list when the pool is empty.
func (s *Service) FirstAvailable(ctx context.Context) (string, error) {
	s.StartPoolLoop()

	if err := s.UpdateProxies(); err != nil {
		return "", fmt.Errorf("failed to update proxies: %w", err)
	}

	s.poolMu.Lock()
	pool := append([]pooledProxy(nil), s.pool...)
	s.poolMu.Unlock()

	if len(pool) == 0 {
		s.revalidatePool(ctx)
		s.poolMu.Lock()
		pool = append([]pooledProxy(nil), s.pool...)
		s.poolMu.Unlock()
	}

	for _, pp := range pool {
		if lat, err := s.checker.CheckLatency(ctx, pp.proxy); err == nil {
			log.Debug().Str("proxy", pp.proxy).Dur("latency", lat).Msg("Found available proxy")
			return pp.proxy, nil
		}
	}

	// Pool exhausted: refresh it and try once more before giving up.
	s.revalidatePool(ctx)
	s.poolMu.Lock()
	pool = append([]pooledProxy(nil), s.pool...)
	s.poolMu.Unlock()
	for _, pp := range pool {
		if lat, err := s.checker.CheckLatency(ctx, pp.proxy); err == nil {
			log.Debug().Str("proxy", pp.proxy).Dur("latency", lat).Msg("Found available proxy after pool refresh")
			return pp.proxy, nil
		}
	}

	log.Debug().Msg("No available proxy found")
	return "", ErrNoAvailableProxy
}

func (s *Service) poolLoop(ctx context.Context) {
	ticker := time.NewTicker(s.poolRecheck)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.revalidatePool(ctx)
		}
	}
}

// revalidatePool re-checks the current pool members, drops dead ones and tops
// the pool back up to the configured size selecting candidates by lowest
// latency.
func (s *Service) revalidatePool(ctx context.Context) {
	s.poolMu.Lock()
	current := append([]pooledProxy(nil), s.pool...)
	s.poolMu.Unlock()

	valid := make([]pooledProxy, 0, s.poolSize)
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, pp := range current {
		wg.Add(1)
		go func(pp pooledProxy) {
			defer wg.Done()
			if lat, err := s.checker.CheckLatency(ctx, pp.proxy); err == nil {
				mu.Lock()
				valid = append(valid, pooledProxy{proxy: pp.proxy, latency: lat, checked: time.Now()})
				mu.Unlock()
			} else {
				log.Warn().Str("proxy", pp.proxy).Err(err).Msg("Pool proxy dropped")
			}
		}(pp)
	}
	wg.Wait()

	// Everything died: refresh the source list before trying to top up.
	if len(valid) == 0 {
		if err := s.UpdateProxies(); err != nil {
			log.Error().Err(err).Msg("Failed to update proxies for pool refresh")
		}
	}

	if need := s.poolSize - len(valid); need > 0 {
		proxies := s.repo.All()
		seen := make(map[string]bool, len(valid))
		for _, pp := range valid {
			seen[pp.proxy] = true
		}
		candidates := make([]string, 0, len(proxies))
		for _, p := range proxies {
			if !seen[p] {
				candidates = append(candidates, p)
			}
		}
		if len(candidates) > 0 {
			for _, c := range findBestAsync(candidates, s.finderWorkers, need,
				func(ctx context.Context, p string) (time.Duration, bool) {
					lat, err := s.checker.CheckLatency(ctx, p)
					return lat, err == nil
				}) {
				valid = append(valid, pooledProxy{proxy: c.proxy, latency: c.latency, checked: time.Now()})
			}
		}
	}

	sort.Slice(valid, func(i, j int) bool { return valid[i].latency < valid[j].latency })

	s.poolMu.Lock()
	s.pool = valid
	s.poolMu.Unlock()
	log.Debug().Int("poolSize", len(valid)).Msg("Proxy pool revalidated")
}

// UpdateProxies synchronously updates the proxy list from the source URL.
// When the list cannot be fetched or parsed, the previous list is kept and an
// error is returned. The update is skipped entirely when the last successful
// one is younger than the configured cache TTL.
func (s *Service) UpdateProxies() error {
	s.muUpdate.Lock()
	defer s.muUpdate.Unlock()

	if updatedAt := s.repo.UpdatedAt(); time.Since(updatedAt) <= s.cacheTTL {
		log.Trace().Time("updatedAt", updatedAt).Msg("Proxies are actual")
		return nil
	}

	log.Debug().Msg("Updating proxies...")
	proxies, err := s.source.Fetch(context.Background())
	if err != nil {
		return fmt.Errorf("failed to update proxies: %w", err)
	}
	s.repo.Replace(proxies)
	all := s.repo.All()
	log.Debug().Int("count", len(all)).Msg("Proxies updated")

	return nil
}

// NextChecked returns the next proxy from the list that is checked to be available.
//
// Returns [ErrNoAvailableProxy] if no proxy is available.
func (s *Service) NextChecked(ctx context.Context) (string, error) {
	idxStart, proxy := s.repo.Next()
	idx := idxStart
	for {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		default:
		}

		if err := s.Check(ctx, proxy); err == nil {
			return proxy, nil
		}
		idx, proxy = s.repo.Next()
		if idx == idxStart {
			// No more proxies to check, return error
			return "", ErrNoAvailableProxy
		}
	}
}

// Next returns the next proxy from the list
func (s *Service) Next() string {
	_, proxy := s.repo.Next()
	return proxy
}

// Check checks the availability of a proxy and returns an error if it is unavailable.
//
// Returns [ErrProxyUnavailable] if the proxy is unavailable, or [ErrEmptyProxy] if the proxy is empty.
func (s *Service) Check(ctx context.Context, proxy string) error {
	_, err := s.checker.CheckLatency(ctx, proxy)
	return err
}

// HealthCheck performs a health check on the proxy service, ensuring the
// proxy source is reachable and returns at least one proxy.
func (s *Service) HealthCheck() error {
	if err := s.UpdateProxies(); err != nil {
		return err
	}
	if len(s.repo.All()) == 0 {
		return fmt.Errorf("no proxies available")
	}
	return nil
}

type proxyCandidate struct {
	proxy   string
	latency time.Duration
}

// findBestAsync concurrently checks candidates and returns up to n best ones
// ordered by latency.
func findBestAsync(items []string, maxConcurrent, n int, check func(context.Context, string) (time.Duration, bool)) []proxyCandidate {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	semaphore := make(chan struct{}, maxConcurrent)
	var mu sync.Mutex
	results := make([]proxyCandidate, 0)
	var wg sync.WaitGroup

	for _, item := range items {
		wg.Add(1)
		semaphore <- struct{}{}

		go func(item string) {
			defer wg.Done()
			defer func() { <-semaphore }()

			if lat, ok := check(ctx, item); ok {
				mu.Lock()
				results = append(results, proxyCandidate{proxy: item, latency: lat})
				mu.Unlock()
			}
		}(item)
	}
	wg.Wait()

	sort.Slice(results, func(i, j int) bool { return results[i].latency < results[j].latency })
	if n > 0 && len(results) > n {
		results = results[:n]
	}
	return results
}
