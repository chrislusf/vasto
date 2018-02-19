package util

import (
	"testing"
	"io"
)

func TestParallel(t *testing.T) {

	err := Parallel(func() error {
		return io.EOF
	}, func() error {
		return nil
	},
	)

	if err != io.EOF {
		t.Errorf("unexpected err: %v", err)
	}

	err = Parallel(func() error {
		return nil
	}, func() error {
		return nil
	},
	)

	if err != nil {
		t.Errorf("unexpected err: %v", err)
	}

}
