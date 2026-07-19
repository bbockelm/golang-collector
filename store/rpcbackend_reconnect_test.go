package store

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
)

// dialController is a controllable dial for RPCBackend tests: it counts attempts and
// can be toggled between failing (simulating a down database) and connecting to an
// in-process dbrpc server.
type dialController struct {
	srv      *dbrpc.Server
	attempts atomic.Int32
	mu       sync.Mutex
	fail     bool
}

func newDialController(t *testing.T) *dialController {
	t.Helper()
	cat, err := db.OpenCatalog(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cat.Close() })
	return &dialController{srv: dbrpc.NewServerCatalog(cat)}
}

func (dc *dialController) setFail(f bool) {
	dc.mu.Lock()
	dc.fail = f
	dc.mu.Unlock()
}

func (dc *dialController) dial(context.Context) (dbrpc.MsgConn, error) {
	dc.attempts.Add(1)
	dc.mu.Lock()
	fail := dc.fail
	dc.mu.Unlock()
	if fail {
		return nil, errors.New("dial: connection refused")
	}
	sc, cc := net.Pipe()
	go func() { _ = dc.srv.ServeConnOpts(dbrpc.NewStreamConn(sc), dbrpc.ServeOptions{IncludePrivate: true}) }()
	return dbrpc.NewStreamConn(cc), nil
}

func fastPolicy() RetryPolicy {
	return RetryPolicy{Initial: 10 * time.Millisecond, Max: 40 * time.Millisecond, Multiplier: 2, Jitter: 0.1, MaxElapsed: 500 * time.Millisecond}
}

// TestRPCBackendRetriesThroughOutage: an operation issued while the database is down
// rides out the outage -- backing off and reconnecting -- and completes once the
// database comes back, instead of failing immediately.
func TestRPCBackendRetriesThroughOutage(t *testing.T) {
	dc := newDialController(t)
	dc.setFail(true) // database down
	b := NewRPCBackend(context.Background(), dc.dial, fastPolicy())
	defer b.Close()

	done := make(chan error, 1)
	go func() {
		done <- b.UpdateOldText(context.Background(), StartdAd, `Name = "slot1@x"`+"\n"+`State = "Idle"`)
	}()

	// Bring the database back after a few backoff rounds.
	time.Sleep(60 * time.Millisecond)
	dc.setFail(false)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("update should have succeeded after the database recovered, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("update did not complete after the database recovered")
	}
	// The ad landed.
	if n, err := b.Len(context.Background(), StartdAd); err != nil || n != 1 {
		t.Fatalf("Len = %d (err=%v) after recovery, want 1", n, err)
	}
}

// TestRPCBackendGivesUpAfterBudget: a permanently-down database makes an operation
// fail with an error after the retry budget, rather than hanging forever or failing
// instantly.
func TestRPCBackendGivesUpAfterBudget(t *testing.T) {
	dc := newDialController(t)
	dc.setFail(true)
	b := NewRPCBackend(context.Background(), dc.dial, fastPolicy())
	defer b.Close()

	start := time.Now()
	err := b.UpdateOldText(context.Background(), StartdAd, `Name = "slot1@x"`+"\n"+`State = "Idle"`)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("update should fail when the database is permanently down")
	}
	if elapsed < 400*time.Millisecond {
		t.Fatalf("gave up too early (%s); should retry until ~the 500ms budget", elapsed)
	}
	if elapsed > 3*time.Second {
		t.Fatalf("took too long to give up (%s); budget should bound it", elapsed)
	}
}

// TestRPCBackendSingleFlightDial: many concurrent operations against a down database
// share reconnect attempts (single-flight) rather than each dialing -- the
// reconnect-storm guard. The number of dials must be far below the number of
// operations.
func TestRPCBackendSingleFlightDial(t *testing.T) {
	dc := newDialController(t)
	dc.setFail(true)
	b := NewRPCBackend(context.Background(), dc.dial, fastPolicy())
	defer b.Close()

	const ops = 50
	var wg sync.WaitGroup
	wg.Add(ops)
	for i := 0; i < ops; i++ {
		go func() {
			defer wg.Done()
			_ = b.UpdateOldText(context.Background(), StartdAd, `Name = "slot@x"`+"\n"+`State = "Idle"`)
		}()
	}
	wg.Wait()

	dials := dc.attempts.Load()
	if int(dials) >= ops {
		t.Fatalf("dial attempts = %d for %d concurrent ops; single-flight should make it far fewer (a reconnect storm otherwise)", dials, ops)
	}
	t.Logf("single-flight: %d dial attempts served %d concurrent operations", dials, ops)
}
