package main

import (
	"testing"
	"time"

	"github.com/bbockelm/golang-collector/store"
	"github.com/bbockelm/golang-htcondor/config"
)

// TestQueuePolicyDefaults checks that an unconfigured collector gets the deep update
// queue with its shipped bounds -- the protection has to be on by default, since the
// pools that need it are the ones nobody has tuned -- and in particular that ingest is
// buffered rather than direct, the mode with no detection window.
func TestQueuePolicyDefaults(t *testing.T) {
	got := queuePolicy(config.NewEmpty())
	if got != store.DefaultQueuePolicy {
		t.Errorf("unconfigured policy = %+v, want %+v", got, store.DefaultQueuePolicy)
	}
}

// TestQueuePolicyOverrides checks each knob, including the two settings an operator
// could not otherwise express: 0 for "no cap" (which QueuePolicy spells as a negative,
// since 0 there means "take the default"), and a 0 stall threshold for "always queue".
func TestQueuePolicyOverrides(t *testing.T) {
	cfg := config.NewEmpty()
	cfg.Set("COLLECTOR_QUEUE_STALL_MS", "250")
	cfg.Set("COLLECTOR_QUEUE_CLEAR_S", "60")
	cfg.Set("COLLECTOR_QUEUE_MAX_ADS", "5000")
	cfg.Set("COLLECTOR_QUEUE_MAX_MB", "64")
	cfg.Set("COLLECTOR_QUEUE_MAX_INVALIDATES", "128")
	cfg.Set("COLLECTOR_FLUSH_TIMEOUT_S", "30")
	cfg.Set("COLLECTOR_INGEST_DIRECT", "True")
	got := queuePolicy(cfg)
	want := store.QueuePolicy{
		StallThreshold:     250 * time.Millisecond,
		StallClear:         time.Minute,
		MaxAds:             5000,
		MaxBytes:           64 << 20,
		MaxInvalidates:     128,
		FlushTimeout:       30 * time.Second,
		DirectUntilStalled: true,
	}
	if got != want {
		t.Errorf("policy = %+v, want %+v", got, want)
	}

	cfg = config.NewEmpty()
	cfg.Set("COLLECTOR_QUEUE_STALL_MS", "0")
	cfg.Set("COLLECTOR_QUEUE_MAX_ADS", "0")
	cfg.Set("COLLECTOR_QUEUE_MAX_MB", "0")
	cfg.Set("COLLECTOR_FLUSH_TIMEOUT_S", "0")
	got = queuePolicy(cfg)
	if got.StallThreshold != 0 {
		t.Errorf("STALL_MS=0 gave %v, want 0 (queue permanently engaged)", got.StallThreshold)
	}
	if got.MaxAds >= 0 || got.MaxBytes >= 0 || got.FlushTimeout >= 0 {
		t.Errorf("a configured 0 must mean no bound, got ads=%d bytes=%d flush=%v",
			got.MaxAds, got.MaxBytes, got.FlushTimeout)
	}
}
