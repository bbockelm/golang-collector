//go:build unix

package cycle

import (
	"syscall"
	"time"
)

// processCPUTime returns the calling process's accumulated CPU time
// (user+system) so far, via getrusage(RUSAGE_SELF). A cycle records the delta
// across its run. This is whole-process (all goroutines), matching the C++
// negotiator's rusage sampling in spirit but not its single-threaded exactness.
func processCPUTime() time.Duration {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	return timevalDuration(ru.Utime) + timevalDuration(ru.Stime)
}

func timevalDuration(tv syscall.Timeval) time.Duration {
	return time.Duration(tv.Sec)*time.Second + time.Duration(tv.Usec)*time.Microsecond
}
