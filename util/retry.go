package util

import (
	"log"
	"math/rand"
	"time"
	"context"
)

func Retry(fn func() error) error {
	return timeDelayedRetry(fn, time.Second, 3*time.Second)
}

func RetryForever(ctx context.Context, name string, fn func() error, waitTimes time.Duration) {
	RetryUntil(ctx, name, nil, fn, waitTimes)
}

func RetryUntil(ctx context.Context, name string, conditionFn func() bool, fn func() error, waitTimes time.Duration) {

	if conditionFn != nil && !conditionFn() {
		return
	}

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for {
		err := fn()
		if err != nil {
			log.Printf("%s failed: %v", name, err)
		}

		if conditionFn != nil && !conditionFn() {
			break
		}

		time.Sleep(time.Duration((r.Float64() + 1) * float64(waitTimes)))

		select {
		case <-ctx.Done():
			log.Printf("%s has finished", name)
			return
		default:
			log.Printf("%s retrying...", name)
		}
	}
}

func timeDelayedRetry(fn func() error, waitTimes ...time.Duration) error {

	err := fn()
	if err == nil {
		return nil
	}

	log.Printf("Retrying after failure: %v", err)

	var i int
	var t time.Duration

	for i, t = range waitTimes {
		err = fn()
		if err == nil {
			break
		}
		log.Printf("Failed %d time due to %v", i+1, err)
		time.Sleep(t)
	}

	return err
}
