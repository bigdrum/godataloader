package dataloader_test

import (
	"runtime/debug"
	"testing"

	"github.com/bigdrum/godataloader"
)

func TestScheduler(t *testing.T) {
	dataloader.RunWithScheduler(func(sch *dataloader.Scheduler) {
		n := dataloader.NewNotification(sch)
		k := 0
		expectedK := 0
		waitForK := func() {
			k++
			n.Wait()
		}
		sch.SpawnLow(func() {
			t.Log("Run idle")
			k += 100
			expectedK = 102
			n.Notify()
			sch.Spawn(func() {
				t.Log("6")
			})
		})
		sch.Spawn(func() {
			t.Log("1")
			sch.Spawn(func() {
				t.Log("3")
			})
			waitForK()
			if k != expectedK {
				t.Error(k, expectedK)
			}
			t.Log("4")
		})
		sch.Spawn(func() {
			t.Log("2")
			waitForK()
			if k != expectedK {
				t.Error(k, expectedK)
			}
			t.Log("5")
		})
	})
}

func TestManySpawn(t *testing.T) {
	// A test to avoid us doing recursion too much.
	debug.SetMaxStack(4096)
	dataloader.RunWithScheduler(func(sch *dataloader.Scheduler) {
		for i := 0; i < 10000; i++ {
			sch.Spawn(func() {})
		}
	})
}
