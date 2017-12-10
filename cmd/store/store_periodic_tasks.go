package store

import "time"

type PeriodicTask interface {
	EverySecond()
	Keyspace() string
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

func (ss *storeServer) UnregisterPeriodicTask(keyspace string) {
	var t []PeriodicTask
	for _, p := range ss.periodTasks {
		if p.Keyspace() != keyspace {
			x := p
			t = append(t, x)
		}
	}
	ss.periodTasks = t
}
