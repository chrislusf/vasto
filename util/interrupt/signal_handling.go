// +build darwin freebsd netbsd openbsd
// +build !plan9,!windows,!linux

package interrupt

import (
	"os"
	"os/signal"
	"syscall"
)

// OnInterrupt executes a function when the process is interrupted.
func OnInterrupt(fn func(), onExitFunc func()) {
	// deal with control+c,etc
	signalChan := make(chan os.Signal, 1)
	// controlling terminal close, daemon not exit
	signal.Ignore(syscall.SIGHUP)
	signal.Notify(signalChan,
		os.Interrupt,
		os.Kill,
		syscall.SIGALRM,
		// syscall.SIGHUP,
		syscall.SIGINFO, // this causes windows to fail
		syscall.SIGINT,
		syscall.SIGTERM,
		// syscall.SIGQUIT, // Quit from keyboard, "kill -3"
	)
	go func() {
		for sig := range signalChan {
			fn()
			if sig != syscall.SIGINFO {
				if onExitFunc != nil {
					onExitFunc()
				}
				os.Exit(0)
			}
		}
	}()
}
