package util

import (
	"context"
	"github.com/chrislusf/glog"
	"math/rand"
	"time"
)

// RetryForever keeps retrying the fn unless the context is cancelled.
func RetryForever(ctx context.Context, name string, fn func() error, waitTimes time.Duration) {

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	hasFailedBefore := false
	for {
		err := fn()
		if err != nil {
			if hasFailedBefore {
				glog.Errorf("%s still failed: %v", name, err)
			} else {
				hasFailedBefore = true
				glog.Errorf("%s failed: %v", name, err)
			}
		} else {
			if hasFailedBefore {
				hasFailedBefore = false
				glog.V(0).Infof("%s recovered", name)
			} else {
				glog.V(0).Infof("%s succeeded", name)
			}
		}

		time.Sleep(time.Duration((r.Float64() + 1) * float64(waitTimes)))

		select {
		case <-ctx.Done():
			glog.V(9).Infof("%s has finished", name)
			return
		default:
			glog.V(2).Infof("%s retrying...", name)
		}
	}
}
