package dataloader

import (
	"sync"
)

// Scheduler provides a custom way to run tasks (arbitrary functions) with a specific
// execution order, with two priorities.
//
// Use "RunWithScheduler" to start a root task. This taskcan spawn new ones with
// Spaw/SpawnLow with normal/low priorities respectively. All task managed by the
// schedule must be spawned in this way. RunWithScheduler returns when all spawned tasks
// finish.
//
// At a given time, one and only one single task is active in execution. Initially,
// the root task is active. The scheduling (switching the active task) happens
// when the current active task becomes inactive, i.e. it finishes, or blocked by
// Notification.Wait Note that the normal blocking in Go, such as waiting for mutex, waitgroup,
// channel etc doesn't trigger the scheduling here. Waiting for a mutex in a spawned
// task to be unlocked by another one is likely to cause deadlock, so only use Notification.Wait
// to coordinate the execution.
//
// The scheduler will not schedule tasks with low priority until all tasks with normal
// priority are inactive. For tasks with the same priority, they are scheduled in a FILO
// manner (And this is an arbitrary choice since a stack is easier to implement than a queue).
//
// This is a very simple scheduler, that provides just enough feature for dataloader: it
// collects data request with normal priority, and fetch data in low priority, so as to ensure
// more requests are collected before making the single batched call to remote service.
//
// It would be interesting to extend the scheduler to support features beyond that:
// * Support "multi-slot", i.e. multiple task can be active at a given time.
// * Support richer inter task communication feature, such as channel, select, mutex.
// * Support pluggable scheduling algorithm.
// * Use context.Context to pass around scheduler, opt-in.
//
// All functions must be called from the spawned tasks. It is not thread-safe to
// call the functions from an "external" goroutine.
//
// Does it create new goroutines?
//
// Each time when the task is yield (Notification.Wait), a new goroutine is created. Spawn
// doesn't create new goroutines. In theory, the new goroutine is not necessary. But
// we use this approach to limit the stack size (probably not necessary, we should benchmark which
// way is better.)
type Scheduler struct {
	// They actually act as stacks.
	normalQ []schedulable
	lowQ    []schedulable
}

type schedulable struct {
	action   func()
	pickNext bool
}

// RunWithScheduler starts a root task and wait for it and its subtasks to finish.
func RunWithScheduler(f func(sch *Scheduler)) {
	sch := &Scheduler{}
	sch.Spawn(func() {
		f(sch)
	})
	sch.schedule()
}

func (sch *Scheduler) schedule() {
	for {
		q := &sch.normalQ
		if len(*q) == 0 {
			q = &sch.lowQ
			if len(*q) == 0 {
				return
			}
		}

		newlen := len(*q) - 1
		// LIFO.
		s := (*q)[newlen]
		*q = (*q)[:newlen]
		s.action()
		if !s.pickNext {
			return
		}
	}
}

// Spawn enqueue a task to be executed with normal priority.
func (sch *Scheduler) Spawn(f func()) {
	sch.spawnAt(&sch.normalQ, f)
}

// SpawnLow enqueue a task to be executed with lower than normal priority.
func (sch *Scheduler) SpawnLow(f func()) {
	sch.spawnAt(&sch.lowQ, f)
}

func (sch *Scheduler) spawnAt(q *[]schedulable, f func()) {
	*q = append(*q, schedulable{func() {
		f()
	}, true})
}

// Notification provides a way to allow a task to wait for a event to happen.
type Notification struct {
	q        []*sync.WaitGroup
	sch      *Scheduler
	notified bool
}

// NewNotification creates a new notification.
func NewNotification(sch *Scheduler) *Notification {
	return &Notification{sch: sch}
}

// Notify wakes up other tasks that waited for the notification.
func (n *Notification) Notify() {
	n.notified = true
	for i := range n.q {
		wg := n.q[i]
		n.sch.normalQ = append(n.sch.normalQ, schedulable{func() {
			wg.Done()
		}, false})
	}
}

// Wait stops the current exeuction of the task, until notification is notified.
func (n *Notification) Wait() {
	if n.notified {
		return
	}
	var wg sync.WaitGroup
	wg.Add(1)
	n.q = append(n.q, &wg)
	go n.sch.schedule()
	wg.Wait()
}

// WaitGroup is like sync.WaitGroup but for scheduler.
type WaitGroup struct {
	// No lock is needed since Schduler is currently single threaded.
	n         *Notification
	numToWait int
}

func NewWaitGroup(sch *Scheduler) *WaitGroup {
	return &WaitGroup{n: NewNotification(sch)}
}

func (w *WaitGroup) Add(i int) {
	w.numToWait += i
}

func (w *WaitGroup) Done() {
	w.numToWait--
	if w.numToWait < 0 {
		panic("negative waitgroup")
	}
	if w.numToWait == 0 {
		w.n.Notify()
	}
}

func (w *WaitGroup) Wait() {
	if w.numToWait == 0 {
		return
	}
	w.n.Wait()
}
