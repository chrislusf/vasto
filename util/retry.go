package util

import (
	"context"
	"math/rand"
	"time"
	"github.com/golang/glog"
)

func Retry(fn func() error) error {
	return timeDelayedRetry(fn, time.Second, 3*time.Second)
}

func RetryForever(ctx context.Context, name string, fn func() error, waitTimes time.Duration) {
	RetryUntil(ctx, name, fn, waitTimes)
}

func RetryUntil(ctx context.Context, name string, fn func() error, waitTimes time.Duration) {

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for {
		err := fn()
		if err != nil {
			glog.Errorf("%s failed: %v", name, err)
		}

		time.Sleep(time.Duration((r.Float64() + 1) * float64(waitTimes)))

		select {
		case <-ctx.Done():
			glog.V(1).Infof("%s has finished", name)
			return
		default:
			glog.V(2).Infof("%s retrying...", name)
		}
	}
}

func timeDelayedRetry(fn func() error, waitTimes ...time.Duration) error {

	err := fn()
	if err == nil {
		return nil
	}

	glog.V(2).Infof("Retrying after failure: %v", err)

	var i int
	var t time.Duration

	for i, t = range waitTimes {
		err = fn()
		if err == nil {
			break
		}
		glog.V(2).Infof("Failed %d time due to %v", i+1, err)
		time.Sleep(t)
	}

	return err
}
