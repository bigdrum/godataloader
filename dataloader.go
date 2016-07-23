package dataloader

import "sync"

// DataLoader is threadsafe map for loading data, and handles batching/dedupping.
// It never expires. It is inspired by github.com/facebook/dataloader.
type DataLoader struct {
	mu        sync.RWMutex
	cache     map[interface{}]Value
	pending   map[interface{}]struct{}
	fetchDone *Notification

	batchLoader func(keys []interface{}) []Value
	sch         *Scheduler
}

// New creates a new dataloader.
func New(sch *Scheduler, batchLoader func(keys []interface{}) []Value) *DataLoader {
	return &DataLoader{
		cache:       make(map[interface{}]Value),
		pending:     make(map[interface{}]struct{}),
		batchLoader: batchLoader,
		sch:         sch,
	}
}

// NaiveBatch is convenient helper to convert a single fetch to a multi-fetch.
func NaiveBatch(f func(interface{}) Value) func(keys []interface{}) []Value {
	return func(keys []interface{}) []Value {
		values := make([]Value, len(keys))
		for i, k := range keys {
			values[i] = f(k)
		}
		return values
	}
}

// Value wraps the value and error.
type Value struct {
	V   interface{}
	Err error
}

// Unbox is a helper function to unbox the value.
func (v Value) Unbox() (interface{}, error) {
	return v.V, v.Err
}

// NewValue creates a new value.
func NewValue(v interface{}, err error) Value {
	return Value{V: v, Err: err}
}

func (dl *DataLoader) scheduleFetch() *Notification {
	if dl.fetchDone != nil {
		return dl.fetchDone
	}
	n := NewNotification(dl.sch)
	dl.fetchDone = n
	dl.sch.SpawnLow(func() {
		dl.fetchPending()
		n.Notify()
	})
	return n
}

func (dl *DataLoader) fetchPending() {
	dl.mu.Lock()
	defer dl.mu.Unlock()

	keys := make([]interface{}, 0, len(dl.pending))
	for key := range dl.pending {
		if _, ok := dl.cache[key]; ok {
			continue
		}
		keys = append(keys, key)
	}
	if len(keys) == 0 {
		return
	}
	// TODO: Handle panic here?
	// TODO: The locking can be optimized here.
	values := dl.batchLoader(keys)
	for i, v := range values {
		dl.cache[keys[i]] = v
	}
	dl.pending = make(map[interface{}]struct{})
	dl.fetchDone = nil
}

// Load loads a single value.
func (dl *DataLoader) Load(key interface{}) Value {
	return dl.LoadMany([]interface{}{
		key,
	})[0]
}

// LoadMany loads multiple values.
func (dl *DataLoader) LoadMany(keys []interface{}) []Value {
	values := make([]Value, len(keys))
	var keysToFetch []interface{}
	var keysToFetchIndex []int

	func() {
		dl.mu.RLock()
		defer dl.mu.RUnlock()
		for i, key := range keys {
			v, ok := dl.cache[key]
			if ok {
				values[i] = v
				continue
			}
			keysToFetch = append(keysToFetch, key)
			keysToFetchIndex = append(keysToFetchIndex, i)
		}
	}()

	if len(keysToFetch) > 0 {
		var n *Notification
		func() {
			dl.mu.Lock()
			defer dl.mu.Unlock()
			for i := 0; i < len(keysToFetch); i++ {
				key := keysToFetch[i]
				v, ok := dl.cache[key]
				if ok {
					values[keysToFetchIndex[i]] = v
					keysToFetch[i] = keysToFetch[len(keysToFetch)-1]
					keysToFetch = keysToFetch[:len(keysToFetch)-1]
					keysToFetchIndex[i] = keysToFetchIndex[len(keysToFetchIndex)-1]
					keysToFetchIndex = keysToFetchIndex[:len(keysToFetchIndex)-1]
					continue
				}
				dl.pending[key] = struct{}{}
			}
			n = dl.scheduleFetch()
		}()
		if len(keysToFetch) > 0 {
			n.Wait()
			for vsi, vi := range keysToFetchIndex {
				values[vi] = dl.cache[keysToFetch[vsi]]
			}
		}
	}
	return values
}

// Prime put a single value into the cache. No-op if the value already exists.
func (dl *DataLoader) Prime(key interface{}, v Value) {
	dl.mu.Lock()
	defer dl.mu.Unlock()
	if _, ok := dl.cache[key]; ok {
		// If you want to override, call Clear first.
		return
	}
	dl.cache[key] = v
}

// Clear removes a single value from the cache.
func (dl *DataLoader) Clear(key interface{}) {
	dl.mu.Lock()
	defer dl.mu.Unlock()
	delete(dl.cache, key)
}

// ClearAll removes all values from the cache.
func (dl *DataLoader) ClearAll() {
	dl.mu.Lock()
	defer dl.mu.Unlock()
	dl.cache = make(map[interface{}]Value)
}
