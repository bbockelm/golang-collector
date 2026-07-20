package server

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/PelicanPlatform/classad/dbrpc"
)

// TestIsCanceled guards the shutdown-vs-rejection distinction in updateHandler: a
// context cancellation/deadline (the collector shutting down, or the peer going
// away) must be recognized so the handler ends cleanly instead of logging every
// in-flight update as a "rejected ad" -- while a genuine rejection (a server error)
// is NOT a cancellation and still logs + skips.
func TestIsCanceled(t *testing.T) {
	if !isCanceled(context.Canceled) {
		t.Error("context.Canceled must be recognized as a cancellation")
	}
	if !isCanceled(context.DeadlineExceeded) {
		t.Error("context.DeadlineExceeded must be recognized as a cancellation")
	}
	if !isCanceled(fmt.Errorf("collector: ad database unavailable: %w", context.Canceled)) {
		t.Error("a wrapped context.Canceled must be recognized")
	}
	if isCanceled(errors.New("connection reset")) {
		t.Error("a plain transport error is not a cancellation")
	}
	if isCanceled(&dbrpc.ServerError{Msg: "parse error: bad value"}) {
		t.Error("a server rejection is not a cancellation")
	}
}
