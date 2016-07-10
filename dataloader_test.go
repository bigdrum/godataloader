package dataloader_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/bigdrum/godataloader"
)

func newTestLoader() func(key string) string {
	var mu sync.Mutex
	counter := 0

	dl := dataloader.New(func(keys []interface{}) []dataloader.Value {
		mu.Lock()
		defer mu.Unlock()
		counter++

		result := make([]dataloader.Value, 0, len(keys))
		for _, key := range keys {
			result = append(result, dataloader.NewValue(fmt.Sprintf("#%d\t%v", counter, key), nil))
		}
		return result
	})
	return func(key string) string {
		return dl.Load(key).V.(string)
	}
}

func TestBasic(t *testing.T) {
	load := newTestLoader()
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v := load(fmt.Sprint("key", "\t", i))
			fmt.Println(i, v)
			v = load(fmt.Sprint("key", "\t", v))
			fmt.Println(i, v)
		}(i)
	}
	wg.Wait()
	t.Error(1)
}
