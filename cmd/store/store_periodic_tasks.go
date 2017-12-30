package store

import "time"

type PeriodicTask interface {
	EverySecond()
}

func (ss *storeServer) startPeriodTasks() {
	for range time.Tick(time.Second) {
		for _, t := range ss.periodTasks {
			t.EverySecond()
		}
	}
}

func (ss *storeServer) RegisterPeriodicTask(task PeriodicTask) {
	// TODO need to add a lock here
	ss.periodTasks = append(ss.periodTasks, task)
}

func (ss *storeServer) UnregisterPeriodicTask(task PeriodicTask) {
	var t []PeriodicTask
	for _, p := range ss.periodTasks {
		if p != task {
			x := p
			t = append(t, x)
		}
	}
	ss.periodTasks = t
}
