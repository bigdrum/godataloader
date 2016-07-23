package dataloader_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/bigdrum/godataloader"
)

type stat struct {
	counter int
}

func newTestLoader(sch *dataloader.Scheduler) (func(key string) string, *stat) {
	var mu sync.Mutex
	stat := &stat{}

	dl := dataloader.New(sch, func(keys []interface{}) []dataloader.Value {
		mu.Lock()
		defer mu.Unlock()
		stat.counter++

		result := make([]dataloader.Value, 0, len(keys))
		for _, key := range keys {
			result = append(result, dataloader.NewValue(fmt.Sprintf("#%d\t%v", stat.counter, key), nil))
		}
		return result
	})
	return func(key string) string {
		return dl.Load(key).V.(string)
	}, stat
}

func TestWithScheduler(t *testing.T) {
	var outstat *stat
	dataloader.RunWithScheduler(func(sch *dataloader.Scheduler) {
		var load func(key string) string
		load, outstat = newTestLoader(sch)
		for i := 0; i < 20; i++ {
			i := i
			sch.Spawn(func() {
				sch.Spawn(func() {
					v := load(fmt.Sprint("key", "\t", i))
					t.Log(i, "spawn", v)
					v = load(fmt.Sprint("key", "\t", v))
					t.Log(i, "spawn", v)

				})
				v := load(fmt.Sprint("key", "\t", i))
				t.Log(i, v)
				v = load(fmt.Sprint("key", "\t", v))
				t.Log(i, v)
			})
		}
	})
	if outstat.counter != 2 {
		t.Error("expect load twice, but loaded: ", outstat.counter)
	}
}
