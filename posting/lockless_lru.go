/*
Copyright 2013 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Modified by Dgraph Labs, Inc.

// Package lru implements an LRU cache.
package posting

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/glog"

	"github.com/dgraph-io/dgraph/x"
)

// listCache is an LRU cache.
type listCache struct {
	ctx context.Context
	// MaxSize is the maximum size of cache before an item is evicted.
	MaxSize uint64 // should be changed with atomic to allow concurrent access

	CurSize uint64 // should be changed with atomic to allow concurrent access
	Evicts  uint64
	cache   sync.Map
	//[string]*list.Element
}

type CacheStats struct {
	Length    int
	Size      uint64
	NumEvicts uint64
}

type entry struct {
	key  string
	pl   *List
	size uint64
}

// New creates a new Cache.
func newListCache(maxSize uint64) *listCache {
	lc := &listCache{
		ctx:     context.Background(),
		MaxSize: maxSize,
	}
	go lc.evictLoop()
	return lc
}

func (c *listCache) evictLoop() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for range ticker.C {
		c.evict()
	}
}

// evict keys randomly for a maximum of 10ms
func (c *listCache) evict() {
	// Only allow evictions for 10ms out of a second.
	deadline := time.Now().Add(10 * time.Millisecond)

	// since eviction does not need to be 100% accurate
	// it's ok to evict a little bit more or less
	// hence we use local variables to estimate the cache size
	// instead of loading c.CurSize using atomic operations repeatedly
	curSize := atomic.LoadUint64(&c.CurSize)
	originalSize := curSize
	maxSize := atomic.LoadUint64(&c.MaxSize)
	glog.Infof("before eviction, CurSize %d maxSize %d", curSize, maxSize)

	c.cache.Range(func(key, value interface{}) bool {
		if !time.Now().Before(deadline) {
			glog.Infof("deadline passed")
			// stop the eviction if deadline has passed
			return false
		}
		if curSize <= maxSize {
			glog.Infof("cache size under maxSize")
			// no need to evict any more
			return false
		}

		pl := value.(*List)
		if !pl.SetForDeletion() {
			// If the posting list has pending mutations, SetForDeletion would
			// return false, and hence would be skipped.
			return true
		}

		// evict the current key
		c.cache.Delete(key)
		c.Evicts++
		curSize -= uint64(pl.EstimatedSize())
		return true
	})

	// put the size change back to CurSize
	atomic.AddUint64(&c.CurSize, curSize-originalSize)
	curSize = atomic.LoadUint64(&c.CurSize)
	maxSize = atomic.LoadUint64(&c.MaxSize)
	glog.Infof("done eviction, CurSize %d maxSize %d", curSize, maxSize)
}

func (c *listCache) UpdateMaxSize(size uint64) uint64 {
	if size == 0 {
		size = c.CurSize
	} /*
		if size < (50 << 20) {
			size = 50 << 20
		}*/
	atomic.StoreUint64(&c.MaxSize, size)
	//c.MaxSize = size
	x.LcacheCapacity.Set(int64(size))
	return c.MaxSize
}

// Add adds a value to the cache.
func (c *listCache) PutIfMissing(key string, pl *List) *List {
	res, _ := c.cache.LoadOrStore(key, pl)

	// update the current size
	size := uint64(pl.EstimatedSize())
	if size < 100 {
		size = 100
	}

	atomic.AddUint64(&c.CurSize, size)
	return res.(*List)
}

// Get looks up a key's value from the cache.
func (c *listCache) Get(key string) (pl *List) {
	res, ok := c.cache.Load(key)
	if ok {
		return res.(*List)
	}
	return nil
}

// Len returns the number of items in the cache.
func (c *listCache) Stats() CacheStats {

	return CacheStats{
		Size:      c.CurSize,
		NumEvicts: c.Evicts,
	}
}

func (c *listCache) Reset() {
	//c.ll = list.New()
	c.cache = sync.Map{} //make(map[string]*list.Element)
	c.CurSize = 0
}

// Doesn't sync to disk, call this function only when you are deleting the pls.
func (c *listCache) clear(remove func(key []byte) bool) {
	c.cache.Range(func(key, value interface{}) bool {
		remove(key.([]byte))
		return true
	})
}
