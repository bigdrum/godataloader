package dataloader

import (
	"sync"
)

// DataLoader is threadsafe map for loading data, and handles batching/dedupping.
// It never expires. It is inspired by github.com/facebook/dataloader.
type DataLoader struct {
	mu        sync.RWMutex
	cache     map[interface{}]Value
	pending   map[interface{}]interface{}
	fetchDone *Notification

	batchLoader func(keys []interface{}) []Value
	sch         *Scheduler
}

// New creates a new dataloader.
func New(sch *Scheduler, batchLoader func(keys []interface{}) []Value) *DataLoader {
	return &DataLoader{
		cache:       make(map[interface{}]Value),
		pending:     make(map[interface{}]interface{}),
		batchLoader: batchLoader,
		sch:         sch,
	}
}

// Parallel is convenient helper to convert a single fetch to a multi-fetch that execute
// the individual single fetch in parallel.
func Parallel(f func(interface{}) Value) func(keys []interface{}) []Value {
	return func(keys []interface{}) []Value {
		if len(keys) == 1 {
			return []Value{f(keys[0])}
		}
		values := make([]Value, len(keys))
		var wg sync.WaitGroup
		for i := range keys {
			wg.Add(1)
			i := i
			go func() {
				defer wg.Done()
				values[i] = f(keys[i])
			}()
		}
		wg.Wait()
		return values
	}
}

// Serial is convenient helper to convert a single fetch to a multi-fetch that execute
// the individual single fetch serially.
func Serial(f func(interface{}) Value) func(keys []interface{}) []Value {
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
	// Must be called with dl.mu locked.
	if dl.fetchDone != nil {
		return dl.fetchDone
	}
	if dl.sch == nil {
		dl.mu.Unlock()
		dl.fetchPending()
		dl.mu.Lock()
		return nil
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
	defer func() {
		dl.pending = make(map[interface{}]interface{})
		dl.fetchDone = nil
		dl.mu.Unlock()
	}()

	keys := make([]interface{}, 0, len(dl.pending))
	mkeys := make([]interface{}, 0, len(dl.pending))

	for mkey, key := range dl.pending {
		if _, ok := dl.cache[mkey]; ok {
			continue
		}
		mkeys = append(mkeys, mkey)
		keys = append(keys, key)
	}
	if len(keys) == 0 {
		return
	}
	// TODO: Handle panic here?
	// TODO: The locking can be optimized here.
	values := dl.batchLoader(keys)
	for i, v := range values {
		dl.cache[mkeys[i]] = v
	}
}

// Load loads a single value.
func (dl *DataLoader) Load(key interface{}) Value {
	return dl.LoadMany([]interface{}{
		key,
	})[0]
}

type MapKeyer interface {
	MapKey() interface{}
}

func getMapKey(key interface{}) interface{} {
	if v, ok := key.(MapKeyer); ok {
		return v.MapKey()
	}
	return key
}

// LoadMany loads multiple values.
func (dl *DataLoader) LoadMany(keys []interface{}) []Value {
	values := make([]Value, len(keys))
	var keysToFetch []interface{}
	var mkeysToFetch []interface{}
	var keysToFetchIndex []int

	func() {
		dl.mu.RLock()
		defer dl.mu.RUnlock()
		for i, key := range keys {
			mkey := getMapKey(key)
			v, ok := dl.cache[mkey]
			if ok {
				values[i] = v
				continue
			}
			keysToFetch = append(keysToFetch, key)
			mkeysToFetch = append(mkeysToFetch, mkey)
			keysToFetchIndex = append(keysToFetchIndex, i)
		}
	}()

	if len(keysToFetch) > 0 {
		n := func() *Notification {
			dl.mu.Lock()
			defer dl.mu.Unlock()
			for i := 0; i < len(keysToFetch); i++ {
				key := keysToFetch[i]
				mkey := mkeysToFetch[i]
				v, ok := dl.cache[mkey]
				if ok {
					values[keysToFetchIndex[i]] = v
					keysToFetch[i] = keysToFetch[len(keysToFetch)-1]
					keysToFetch = keysToFetch[:len(keysToFetch)-1]
					keysToFetchIndex[i] = keysToFetchIndex[len(keysToFetchIndex)-1]
					keysToFetchIndex = keysToFetchIndex[:len(keysToFetchIndex)-1]
					continue
				}
				dl.pending[mkey] = key
			}
			return dl.scheduleFetch()
		}()
		if len(keysToFetch) > 0 {
			if n != nil {
				n.Wait()
			}
			for vsi, vi := range keysToFetchIndex {
				values[vi] = dl.cache[mkeysToFetch[vsi]]
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
