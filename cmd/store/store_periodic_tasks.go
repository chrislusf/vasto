package store

import (
	"github.com/chrislusf/glog"
	"time"
)

type periodicTask interface {
	EverySecond()
}

func (ss *storeServer) startPeriodTasks() {
	for range time.Tick(time.Second) {
		for _, t := range ss.periodTasks {
			t.EverySecond()
		}
	}
}

func (ss *storeServer) RegisterPeriodicTask(task periodicTask) {

	glog.V(3).Infof("RegisterPeriodicTask: %+v", task)

	found := false
	for _, t := range ss.periodTasks {
		if t == task {
			found = true
		}
	}
	if found {
		glog.V(3).Infof("RegisterPeriodicTask already exists!: %+v", task)
		return
	}

	ss.periodTasks = append(ss.periodTasks, task)
}

func (ss *storeServer) UnregisterPeriodicTask(task periodicTask) {
	glog.V(3).Infof("UnregisterPeriodicTask: %+v", task)
	var t []periodicTask
	for _, p := range ss.periodTasks {
		if p != task {
			x := p
			t = append(t, x)
		}
	}
	ss.periodTasks = t
}
