package proxy

import (
	"sync"
	"time"
)

// Repository stores an ordered proxy list in memory.
type Repository interface {
	// Replace atomically replaces the list with the given proxies.
	Replace(proxies []string)
	// All returns all proxies in the list.
	All() []string
	// Next returns the next proxy in round-robin rotation, wrapping around.
	Next() (idx int, proxy string)
	// UpdatedAt returns the time of the last successful Replace.
	UpdatedAt() time.Time
}

// NewMemoryRepository returns a thread-safe in-memory Repository.
func NewMemoryRepository() Repository {
	return &memoryRepository{proxies: make([]string, 0)}
}

type memoryRepository struct {
	mu        sync.RWMutex
	proxies   []string
	updatedAt time.Time
	current   int
}

func (r *memoryRepository) Replace(proxies []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.proxies = proxies
	r.current = 0
	r.updatedAt = time.Now()
}

func (r *memoryRepository) All() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return append([]string(nil), r.proxies...)
}

func (r *memoryRepository) Next() (int, string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.proxies) == 0 {
		return 0, ""
	}
	if r.current >= len(r.proxies) {
		r.current = 0
	}

	idx := r.current
	proxy := r.proxies[idx]
	r.current++

	return idx, proxy
}

func (r *memoryRepository) UpdatedAt() time.Time {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.updatedAt
}
