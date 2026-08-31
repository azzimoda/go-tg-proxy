package proxy

import (
	"testing"
	"time"
)

func TestReplaceAndAll(t *testing.T) {
	r := NewMemoryRepository()
	if !r.UpdatedAt().IsZero() {
		t.Fatal("UpdatedAt should be zero for a fresh repo")
	}
	if idx, proxy := r.Next(); proxy != "" || idx != 0 {
		t.Fatalf("empty repo Next() = (%d, %q), want (0, \"\")", idx, proxy)
	}

	proxies := []string{"1.2.3.4:1080", "5.6.7.8:1081"}
	r.Replace(proxies)

	all := r.All()
	if len(all) != len(proxies) {
		t.Fatalf("got %d proxies, want %d: %v", len(all), len(proxies), all)
	}
	for i, p := range proxies {
		if all[i] != p {
			t.Fatalf("proxies[%d] = %q, want %q", i, all[i], p)
		}
	}

	if time.Since(r.UpdatedAt()) > time.Second {
		t.Fatalf("UpdatedAt too old: %v", time.Since(r.UpdatedAt()))
	}

	// Replacing with a new list drops the old one.
	r.Replace([]string{"9.9.9.9:1082"})
	if all := r.All(); len(all) != 1 || all[0] != "9.9.9.9:1082" {
		t.Fatalf("after Replace() = %v, want [9.9.9.9:1082]", all)
	}
}

func TestNextWrapAround(t *testing.T) {
	r := NewMemoryRepository()
	proxies := []string{"1.2.3.4:1080", "5.6.7.8:1081", "9.9.9.9:1082"}
	r.Replace(proxies)

	seen := make(map[string]bool)
	for i := 0; i < len(proxies); i++ {
		_, proxy := r.Next()
		if proxy == "" || seen[proxy] {
			t.Fatalf("unexpected proxy on iteration %d: %q", i, proxy)
		}
		seen[proxy] = true
	}

	// Wraps around and revisits the first proxy.
	first := r.All()[0]
	idx, proxy := r.Next()
	if proxy != first {
		t.Fatalf("after wrap-around Next() = (%d, %q), want first proxy %q", idx, proxy, first)
	}
}
