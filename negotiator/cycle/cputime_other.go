//go:build !unix

package cycle

import "time"

// processCPUTime is a no-op on platforms without getrusage; CpuTime stays 0.
func processCPUTime() time.Duration { return 0 }
