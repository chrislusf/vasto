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
	found := -1
	for k, p := range ss.periodTasks {
		if p == task {
			found = k
		}
	}
	if found < 0 {
		return
	}
	copy(ss.periodTasks[found:], ss.periodTasks[found+1:])
	ss.periodTasks[len(ss.periodTasks)-1] = nil // or the zero value of T
	ss.periodTasks = ss.periodTasks[:len(ss.periodTasks)-1]
}
