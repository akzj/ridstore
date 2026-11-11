package engine

import "time"

// clockStep returns the smallest interval the host's monotonic clock can
// resolve: the first non-zero time.Since it produces. Durations shorter than
// this step are recorded as zero by any measurement, which is why stage timing
// assertions compare against it instead of demanding a non-zero value.
func clockStep() time.Duration {
	start := time.Now()
	for {
		if elapsed := time.Since(start); elapsed > 0 {
			return elapsed
		}
	}
}
