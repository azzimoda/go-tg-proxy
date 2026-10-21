package proxy

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// stubSource always returns an empty list. It is never actually consulted while
// a freshly seeded repository is within the cache TTL.
type stubSource struct {
	calls int
}

func (s *stubSource) Fetch(context.Context) ([]string, error) {
	s.calls++
	return nil, nil
}

type fakeProxyChecker struct {
	mu      sync.Mutex
	latency map[string]time.Duration
	calls   int
}

func (f *fakeProxyChecker) CheckLatency(_ context.Context, proxy string) (time.Duration, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	lat, ok := f.latency[proxy]
	if !ok {
		return 0, ErrProxyUnavailable
	}
	return lat, nil
}

func (f *fakeProxyChecker) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func newTestService(t *testing.T, fake *fakeProxyChecker, proxies []string) *Service {
	t.Helper()
	repo := NewMemoryRepository()
	if len(proxies) > 0 {
		repo.Replace(proxies)
	}
	s := NewService(&stubSource{}, WithRepository(repo), WithChecker(fake))
	t.Cleanup(s.Stop)
	return s
}

func TestFirstAvailablePicksFastestFromScan(t *testing.T) {
	fake := &fakeProxyChecker{latency: map[string]time.Duration{
		"1.2.3.4:1080": 10 * time.Millisecond,
		"5.6.7.8:1081": 20 * time.Millisecond,
		"9.9.9.9:1082": 30 * time.Millisecond,
	}}
	s := newTestService(t, fake, []string{"1.2.3.4:1080", "5.6.7.8:1081", "9.9.9.9:1082"})

	got, err := s.FirstAvailable(context.Background())
	if err != nil {
		t.Fatalf("FirstAvailable() error: %v", err)
	}
	if got != "1.2.3.4:1080" {
		t.Fatalf("FirstAvailable() = %q, want fastest proxy", got)
	}
}

func TestFirstAvailableSkipsDeadPoolMembers(t *testing.T) {
	fake := &fakeProxyChecker{latency: map[string]time.Duration{
		"1.2.3.4:1080": 10 * time.Millisecond,
		"9.9.9.9:1082": 30 * time.Millisecond,
	}}
	s := newTestService(t, fake, []string{"1.2.3.4:1080", "5.6.7.8:1081", "9.9.9.9:1082"})

	s.poolMu.Lock()
	s.pool = []pooledProxy{
		{proxy: "1.2.3.4:1080", latency: 10 * time.Millisecond},
		{proxy: "5.6.7.8:1081", latency: 20 * time.Millisecond}, // dead
		{proxy: "9.9.9.9:1082", latency: 30 * time.Millisecond},
	}
	s.poolMu.Unlock()

	got, err := s.FirstAvailable(context.Background())
	if err != nil {
		t.Fatalf("FirstAvailable() error: %v", err)
	}
	if got != "1.2.3.4:1080" {
		t.Fatalf("FirstAvailable() = %q, want fastest live pool proxy", got)
	}
	if calls := fake.callCount(); calls > len(s.pool) {
		t.Fatalf("FirstAvailable() did %d checks, want <= pool size %d", calls, len(s.pool))
	}
}

func TestRevalidatePoolDropsDead(t *testing.T) {
	fake := &fakeProxyChecker{latency: map[string]time.Duration{
		"1.2.3.4:1080": 10 * time.Millisecond,
		"5.6.7.8:1081": 20 * time.Millisecond,
	}}
	s := newTestService(t, fake, []string{"1.2.3.4:1080", "5.6.7.8:1081", "9.9.9.9:1082"})

	s.poolMu.Lock()
	s.pool = []pooledProxy{
		{proxy: "1.2.3.4:1080", latency: 10 * time.Millisecond},
		{proxy: "5.6.7.8:1081", latency: 20 * time.Millisecond},
		{proxy: "9.9.9.9:1082", latency: 30 * time.Millisecond}, // dead
	}
	s.poolMu.Unlock()

	s.revalidatePool(context.Background())

	s.poolMu.Lock()
	defer s.poolMu.Unlock()
	for _, pp := range s.pool {
		if pp.proxy == "9.9.9.9:1082" {
			t.Fatal("dead proxy was not dropped from the pool")
		}
	}
}

func TestFirstAvailableNoProxies(t *testing.T) {
	fake := &fakeProxyChecker{latency: map[string]time.Duration{}}
	s := newTestService(t, fake, nil)

	_, err := s.FirstAvailable(context.Background())
	if !errors.Is(err, ErrNoAvailableProxy) {
		t.Fatalf("FirstAvailable() error = %v, want ErrNoAvailableProxy", err)
	}
}

func TestFindBestAsyncStopsAfterNeedFound(t *testing.T) {
	working := []string{"10.0.0.1000:1080", "10.0.0.1001:1081"}
	dead := make([]string, 0, 1000)
	for i := 0; i < 1000; i++ {
		dead = append(dead, fmt.Sprintf("10.0.0.%d:1080", i))
	}
	candidates := append(append(working, dead...), "10.0.0.2000:1080")

	var mu sync.Mutex
	checked := 0
	check := func(_ context.Context, p string) (time.Duration, bool) {
		for _, w := range working {
			if p == w {
				return time.Millisecond, true
			}
		}
		mu.Lock()
		checked++
		mu.Unlock()
		return 0, false
	}

	got := findBestAsync(candidates, 4, 2, 0, check)
	if len(got) != 2 {
		t.Fatalf("findBestAsync() returned %d candidates, want 2", len(got))
	}
	mu.Lock()
	defer mu.Unlock()
	if checked > 32 {
		t.Fatalf("findBestAsync() checked %d dead candidates, want early exit after n found", checked)
	}
}

func TestFindBestAsyncAlwaysChecksPriorityCandidate(t *testing.T) {
	var mu sync.Mutex
	checked := map[string]bool{}
	check := func(_ context.Context, p string) (time.Duration, bool) {
		mu.Lock()
		checked[p] = true
		mu.Unlock()
		if p == "priority" {
			// Slow, so the fast candidates reach n first: the scan must still
			// wait for the priority candidate instead of cancelling it.
			time.Sleep(20 * time.Millisecond)
		}
		return time.Millisecond, true
	}

	got := findBestAsync([]string{"priority", "a", "b", "c"}, 4, 1, 1, check)

	mu.Lock()
	defer mu.Unlock()
	if !checked["priority"] {
		t.Fatal("findBestAsync() skipped the priority candidate")
	}
	if len(got) != 1 {
		t.Fatalf("findBestAsync() returned %d candidates, want 1", len(got))
	}
}

func TestUpdateProxiesHitsSourceAndReplaces(t *testing.T) {
	fake := &fakeProxyChecker{latency: map[string]time.Duration{}}
	repo := NewMemoryRepository()
	src := &stubSource{}
	s := NewService(src, WithRepository(repo), WithChecker(fake), WithCacheTTL(time.Nanosecond))
	t.Cleanup(s.Stop)

	if err := s.UpdateProxies(); err != nil {
		t.Fatalf("UpdateProxies() error: %v", err)
	}
	if src.calls != 1 {
		t.Fatalf("source consulted %d times, want 1", src.calls)
	}
	if len(s.repo.All()) != 0 {
		t.Fatalf("repo = %v, want empty after empty source", s.repo.All())
	}
}
