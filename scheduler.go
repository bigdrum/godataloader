package dataloader

import (
	"fmt"
	"sync"
)

type Scheduler struct {
	normalQ []func()
	lowQ    []func()
	running sync.WaitGroup
}

func NewScheduler() *Scheduler {
	return &Scheduler{}
}

func (sch *Scheduler) Run(f func()) {
	sch.Spawn(f)
	sch.schedule()
	sch.running.Wait()
}

func (sch *Scheduler) schedule() {
	q := &sch.normalQ
	if len(*q) == 0 {
		fmt.Println("normalQ empty")
		q = &sch.lowQ
	}
	if len(*q) == 0 {
		fmt.Println("allQ empty")
		return
	}
	f := (*q)[0]
	*q = (*q)[1:]
	go f()
}

func (sch *Scheduler) Spawn(f func()) {
	sch.running.Add(1)
	sch.normalQ = append(sch.normalQ, func() {
		f()
		sch.running.Done()
		sch.schedule()
	})
}

func (sch *Scheduler) NextIdle(f func()) {
	sch.running.Add(1)
	sch.lowQ = append(sch.lowQ, func() {
		defer sch.running.Done()
		f()
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
