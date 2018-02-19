package util

import (
	"context"
	"math/rand"
	"time"
	"github.com/chrislusf/glog"
)

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
				glog.V(0).Infof("%s recovered", name)
			} else {
				glog.V(0).Infof("%s succeeded", name)
			}
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
