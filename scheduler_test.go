package dataloader_test

import (
	"testing"

	"github.com/bigdrum/godataloader"
)

func TestScheduler(t *testing.T) {
	dataloader.RunWithScheduler(func(sch *dataloader.Scheduler) {
		n := dataloader.NewNotification(sch)
		waitForK := func() {
			n.Wait()
		}
		sch.SpawnLow(func() {
			t.Log("Run idle")
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
			t.Log("4")
		})
		sch.Spawn(func() {
			t.Log("2")
			waitForK()
			t.Log("5")
		})
	})
}
