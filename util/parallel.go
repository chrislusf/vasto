package util

import (
	"sync"
)

// Parallel executes multiple actions concurrently. If error happens, one of the errors is returned.
func Parallel(actions ...func() error) error {
	var wg sync.WaitGroup

	actionErrors := make([]error, len(actions))

	for actionIndex, action := range actions {
		wg.Add(1)
		go func(actionIndex int, action func() error) {
			defer wg.Done()
			actionErrors[actionIndex] = action()
		}(actionIndex, action)
	}

	wg.Wait()

	for _, actionError := range actionErrors {
		if actionError != nil {
			return actionError
		}
	}
	return nil
}
