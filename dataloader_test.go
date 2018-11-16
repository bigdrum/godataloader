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

type control struct {
	load     func(key string) string
	loadMany func(keys []string) []string
	stat     *stat
	prime    func(key, value string)
}

func newTestLoader(sch *dataloader.Scheduler) control {
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
	return control{
		load: func(key string) string {
			return dl.Load(key).V.(string)
		},
		loadMany: func(keys []string) []string {
			var ikeys []interface{}
			for _, k := range keys {
				ikeys = append(ikeys, k)
			}
			vs := dl.LoadMany(ikeys)
			var ret []string
			for _, v := range vs {
				ret = append(ret, v.V.(string))
			}
			return ret
		},
		stat: stat,
		prime: func(key, value string) {
			dl.Prime(key, dataloader.NewValue(value, nil))
		},
	}
}

func TestWithScheduler(t *testing.T) {
	var outstat *stat
	dataloader.RunWithScheduler(func(sch *dataloader.Scheduler) {
		ctrl := newTestLoader(sch)
		outstat = ctrl.stat
		for i := 0; i < 20; i++ {
			i := i
			sch.Spawn(func() {
				key1 := fmt.Sprint("key", "\t", i)
				sch.Spawn(func() {
					if i != 0 {
						return
					}
					t.Log(i, "start0")
					ctrl.prime(fmt.Sprint("key", "\t", 0), "primed_key1_value")
					t.Log(i, "prime")
				})
				sch.Spawn(func() {
					t.Log(i, "start")
					v := ctrl.load(key1)
					t.Log(i, "spawn", v)
					v = ctrl.load(fmt.Sprint("key", "\t", v))
					t.Log(i, "spawn", v)

				})
				sch.Spawn(func() {
					t.Log(i, "start2")
					v := ctrl.load(key1)
					t.Log(i, "spawn2", v)
					v = ctrl.load(fmt.Sprint("key", "\t", v))
					t.Log(i, "spawn2", v)

				})
				sch.Spawn(func() {
					t.Log(i, "start3")
					ctrl.load("meh")

				})
				v := ctrl.load(key1)
				t.Log(i, "root", v)
				v = ctrl.load(fmt.Sprint("key", "\t", v))
				t.Log(i, "root", v)
			})
		}
	})
	if outstat.counter != 2 {
		t.Error("expect load twice, but loaded: ", outstat.counter)
	}
}

func TestBoundaryCase(t *testing.T) {
	dataloader.RunWithScheduler(func(sch *dataloader.Scheduler) {
		ctrl := newTestLoader(sch)
		sch.Spawn(func() {
			key1 := "key1"
			wg := dataloader.NewWaitGroup(sch)
			wg.Add(1)
			sch.Spawn(func() {
				defer wg.Done()
				t.Log("start")
				ctrl.prime(key1, "primed_key1_value")
				t.Log("prime")
			})
			wg.Add(1)
			sch.Spawn(func() {
				defer wg.Done()
				t.Log("start1")
				v := ctrl.load(key1)
				t.Log("loaded1", v)
				v = ctrl.load("key2")
				t.Log("loaded1 2", v)

			})
			t.Log("start root")
			v := ctrl.load(key1)
			t.Log("loaded root", v)
			wg.Wait()
		})
	})
}

func TestNoScheduler(t *testing.T) {
	dl := newTestLoader(nil)
	v := dl.load("key1")
	if v != "#1\tkey1" {
		t.Error(v)
	}
	v = dl.load("key1")
	if v != "#1\tkey1" {
		t.Error(v)
	}
	vs := fmt.Sprint(dl.loadMany([]string{"key1", "key2", "key3"}))
	if vs != "[#1\tkey1 #2\tkey2 #2\tkey3]" {
		t.Error(fmt.Sprintf("%#v", vs))
	}
}
