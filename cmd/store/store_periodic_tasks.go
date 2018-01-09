package store

import (
	"time"
	"log"
)

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

	log.Printf("RegisterPeriodicTask: %+v", task)

	found := false
	for _, t := range ss.periodTasks {
		if t == task {
			found = true
		}
	}
	if found {
		log.Printf("RegisterPeriodicTask already exists!: %+v", task)
		return
	}

	ss.periodTasks = append(ss.periodTasks, task)
}

func (ss *storeServer) UnregisterPeriodicTask(task PeriodicTask) {
	log.Printf("UnregisterPeriodicTask: %+v", task)
	var t []PeriodicTask
	for _, p := range ss.periodTasks {
		if p != task {
			x := p
			t = append(t, x)
		}
	}
	ss.periodTasks = t
}
