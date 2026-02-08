package mapbox

import (
	"context"
	"fmt"
	"sync"

	"github.com/couchcryptid/storm-data-etl-service/internal/domain"
)

// CachedGeocoder wraps a Geocoder with an in-memory LRU cache.
type CachedGeocoder struct {
	inner domain.Geocoder
	cache *lruCache
}

// NewCachedGeocoder creates a cache decorator around a geocoder.
func NewCachedGeocoder(inner domain.Geocoder, maxEntries int) *CachedGeocoder {
	return &CachedGeocoder{
		inner: inner,
		cache: newLRUCache(maxEntries),
	}
}

func (c *CachedGeocoder) ForwardGeocode(ctx context.Context, name, state string) (domain.GeocodingResult, error) {
	key := fmt.Sprintf("fwd:%s|%s", name, state)
	if result, ok := c.cache.get(key); ok {
		return result, nil
	}
	result, err := c.inner.ForwardGeocode(ctx, name, state)
	if err != nil {
		return result, err
	}
	// Only cache non-empty results so transient "not found" responses can be retried.
	if result.FormattedAddress != "" {
		c.cache.put(key, result)
	}
	return result, nil
}

func (c *CachedGeocoder) ReverseGeocode(ctx context.Context, lat, lon float64) (domain.GeocodingResult, error) {
	key := fmt.Sprintf("rev:%.6f,%.6f", lat, lon)
	if result, ok := c.cache.get(key); ok {
		return result, nil
	}
	result, err := c.inner.ReverseGeocode(ctx, lat, lon)
	if err != nil {
		return result, err
	}
	if result.FormattedAddress != "" {
		c.cache.put(key, result)
	}
	return result, nil
}

// lruCache is a simple thread-safe LRU cache for GeocodingResults.
type lruCache struct {
	maxEntries int
	mu         sync.Mutex
	entries    map[string]*entry
	head       *entry // most recently used
	tail       *entry // least recently used
}

type entry struct {
	key   string
	value domain.GeocodingResult
	prev  *entry
	next  *entry
}

func newLRUCache(maxEntries int) *lruCache {
	return &lruCache{
		maxEntries: maxEntries,
		entries:    make(map[string]*entry),
	}
}

func (c *lruCache) get(key string) (domain.GeocodingResult, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	e, ok := c.entries[key]
	if !ok {
		return domain.GeocodingResult{}, false
	}
	c.moveToFront(e)
	return e.value, true
}

func (c *lruCache) put(key string, value domain.GeocodingResult) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if e, ok := c.entries[key]; ok {
		e.value = value
		c.moveToFront(e)
		return
	}

	e := &entry{key: key, value: value}
	c.entries[key] = e
	c.addToFront(e)

	if len(c.entries) > c.maxEntries {
		c.evictTail()
	}
}

func (c *lruCache) moveToFront(e *entry) {
	if e == c.head {
		return
	}
	c.remove(e)
	c.addToFront(e)
}

func (c *lruCache) addToFront(e *entry) {
	e.next = c.head
	e.prev = nil
	if c.head != nil {
		c.head.prev = e
	}
	c.head = e
	if c.tail == nil {
		c.tail = e
	}
}

func (c *lruCache) remove(e *entry) {
	if e.prev != nil {
		e.prev.next = e.next
	} else {
		c.head = e.next
	}
	if e.next != nil {
		e.next.prev = e.prev
	} else {
		c.tail = e.prev
	}
}

func (c *lruCache) evictTail() {
	if c.tail == nil {
		return
	}
	delete(c.entries, c.tail.key)
	c.remove(c.tail)
}
