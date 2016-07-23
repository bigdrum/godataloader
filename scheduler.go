package dataloader

import "sync"

// Scheduler provides a custom way to run goroutines with a specific execution order,
// with two priorities.
// Use "RunWithScheduler" to start a root goroutine. This goroutinecan spawn new ones with
// Spaw/SpawnLow with normal/low priorities respectively. All goroutine managed by the
// schedule must be spawned in this way. RunWithScheduler returns when all spawned goroutines
// finish.
// At a given time, one and only one single goroutine is active in execution. Initially,
// the root goroutine is active. The scheduling (switching the active goroutine) happens
// when the current active goroutine becomes inactive, i.e. it finishes, or blocked by
// Notification.Wait Note that the normal blocking in Go, such as waiting for mutex, waitgroup,
// channel etc doesn't trigger the scheduling here. Waiting for a mutex in a spawned
// goroutine to be unlocked by another one is likely to cause deadlock, so only use Notification.Wait
// to coordinate the execution.
// The scheduler will not schedule goroutines with low priority until all goroutines with normal
// priority are inactive. For goroutines with the same priority, they are scheduled in a FILO
// manner (And this is an arbitrary choice since a stack is easier to implement than a queue).
//
// This is a very simple scheduler, that provides just enough feature for dataloader, that it
// collects data request with normal priority, and fetch data in low priority, so as to ensure
// more requests are collected before making a batched fetch.
// It would be interesting to extend the scheduler to support features beyond that:
// * Support "multi-slot", i.e. multiple goroutine can be active at a given time.
// * Support richer inter goroutine communication feature, such as channel, select, mutex.
// * Support pluggable scheduling algorithm.
//
// All functions must be called from the spawned go-routines. It is not thread-safe to
// call the functions from an "external" goroutine.
type Scheduler struct {
	// They actually act as stacks.
	normalQ []func()
	lowQ    []func()
	running sync.WaitGroup
}

func RunWithScheduler(f func(sch *Scheduler)) {
	sch := &Scheduler{}
	sch.Spawn(func() {
		f(sch)
	})
	sch.schedule()
	sch.running.Wait()
}

func (sch *Scheduler) schedule() {
	q := &sch.normalQ
	if len(*q) == 0 {
		q = &sch.lowQ
		if len(*q) == 0 {
			return
		}
	}

	newlen := len(*q) - 1
	// FILO.
	f := (*q)[newlen]
	*q = (*q)[:newlen]
	go f()
}

func (sch *Scheduler) Spawn(f func()) {
	sch.spawnAt(&sch.normalQ, f)
}

func (sch *Scheduler) SpawnLow(f func()) {
	sch.spawnAt(&sch.lowQ, f)
}

func (sch *Scheduler) spawnAt(q *[]func(), f func()) {
	sch.running.Add(1)
	*q = append(*q, func() {
		f()
		sch.running.Done()
		sch.schedule()
	})
}

type Notification struct {
	q   []func()
	sch *Scheduler
}

func NewNotification(sch *Scheduler) *Notification {
	return &Notification{sch: sch}
}

func (n *Notification) Notify() {
	for _, f := range n.q {
		f()
	}
}

func (n *Notification) Wait() {
	var wg sync.WaitGroup
	wg.Add(1)
	n.q = append(n.q, func() {
		n.sch.normalQ = append(n.sch.normalQ, func() {
			wg.Done()
		})
	})
	n.sch.schedule()
	wg.Wait()
}
