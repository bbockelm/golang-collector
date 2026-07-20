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
	gateNext bool            // the next dial returns a wedged connection
	gated    []*blockingConn // wedged connections handed out, released at cleanup
}

func newDialController(t *testing.T) *dialController {
	t.Helper()
	cat, err := db.OpenCatalog(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cat.Close() })
	dc := &dialController{srv: dbrpc.NewServerCatalog(cat)}
	t.Cleanup(dc.releaseGated)
	return dc
}

func (dc *dialController) setFail(f bool) {
	dc.mu.Lock()
	dc.fail = f
	dc.mu.Unlock()
}

// setGateNext makes the next dial hand out a connection whose reader is wedged (never
// returns a frame), simulating a connection whose dbrpc demux loop is stalled behind a
// slow stream consumer.
func (dc *dialController) setGateNext(g bool) {
	dc.mu.Lock()
	dc.gateNext = g
	dc.mu.Unlock()
}

func (dc *dialController) releaseGated() {
	dc.mu.Lock()
	gated := dc.gated
	dc.gated = nil
	dc.mu.Unlock()
	for _, g := range gated {
		g.release()
	}
}

func (dc *dialController) dial(context.Context) (dbrpc.MsgConn, error) {
	dc.attempts.Add(1)
	dc.mu.Lock()
	fail := dc.fail
	gate := dc.gateNext
	dc.gateNext = false
	dc.mu.Unlock()
	if fail {
		return nil, errors.New("dial: connection refused")
	}
	sc, cc := net.Pipe()
	go func() { _ = dc.srv.ServeConnOpts(dbrpc.NewStreamConn(sc), dbrpc.ServeOptions{IncludePrivate: true}) }()
	conn := dbrpc.NewStreamConn(cc)
	if gate {
		bc := &blockingConn{MsgConn: conn, released: make(chan struct{})}
		dc.mu.Lock()
		dc.gated = append(dc.gated, bc)
		dc.mu.Unlock()
		return bc, nil
	}
	return conn, nil
}

// blockingConn wraps a MsgConn whose ReadMsg blocks until released -- so the dbrpc
// client's reader goroutine wedges on it exactly as it would behind a stream consumer
// that has fallen too far behind. Everything else delegates.
type blockingConn struct {
	dbrpc.MsgConn
	once     sync.Once
	released chan struct{}
}

func (b *blockingConn) ReadMsg() ([]byte, error) {
	<-b.released
	return nil, errors.New("blockingConn: released")
}

// release unblocks the wedged reader (idempotent).
func (b *blockingConn) release() { b.once.Do(func() { close(b.released) }) }

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

	// Without single-flight, each op would dial on nearly every retry round -- ~50 ops
	// x ~14 rounds over the 500ms budget ~= hundreds of dials. Single-flight collapses
	// that to roughly budget/backoff windows (a few dozen); the gate leaks a couple of
	// extra dials per window under scheduler contention, so bound generously (ops*3)
	// while still catching a real storm by an order of magnitude.
	dials := dc.attempts.Load()
	if int(dials) >= ops*3 {
		t.Fatalf("dial attempts = %d for %d concurrent ops; single-flight should make it far fewer (a reconnect storm otherwise)", dials, ops)
	}
	t.Logf("single-flight: %d dial attempts served %d concurrent operations", dials, ops)
}

// TestReadStallDoesNotBlockWrites is the regression guard for the read/write
// connection split: a wedged read connection (its dbrpc demux reader stalled, as when
// a streaming query's consumer falls behind) must NOT stall write transactions, which
// ride a separate connection. Were reads and writes multiplexed on one connection, the
// wedged reader would starve the write's commit response and this test would hang.
func TestReadStallDoesNotBlockWrites(t *testing.T) {
	dc := newDialController(t)
	// One read lane so the wedged dial deterministically lands on it.
	b := NewRPCBackendPool(context.Background(), dc.dial, fastPolicy(), 1)
	defer b.Close()
	ctx := context.Background()

	// Bring the write lane up on a healthy connection.
	if err := b.UpdateOldText(ctx, StartdAd, `Name = "w1"`+"\n"+`State = "Idle"`); err != nil {
		t.Fatalf("initial write: %v", err)
	}

	// The read lane's next dial gets a wedged connection; a read on it must hang.
	dc.setGateNext(true)
	readDone := make(chan struct{})
	go func() { _, _ = b.Len(ctx, StartdAd); close(readDone) }()
	select {
	case <-readDone:
		t.Fatal("read unexpectedly completed on a wedged connection")
	case <-time.After(150 * time.Millisecond):
	}

	// The write lane is a different connection, so a write completes promptly despite
	// the wedged read.
	writeDone := make(chan error, 1)
	go func() {
		writeDone <- b.UpdateOldText(ctx, StartdAd, `Name = "w2"`+"\n"+`State = "Idle"`)
	}()
	select {
	case err := <-writeDone:
		if err != nil {
			t.Fatalf("write failed while a read connection was wedged: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("write stalled behind a wedged read connection -- read and write lanes are not isolated")
	}

	// The wedged read is still stuck (released only at cleanup); confirm the write
	// really did overtake it rather than both having completed.
	select {
	case <-readDone:
		t.Fatal("read should still be wedged")
	default:
	}
}

// TestReadWriteUseSeparateConnections asserts, white-box, that a read and a write land
// on distinct dbrpc client connections -- the structural invariant behind the stall
// fix.
func TestReadWriteUseSeparateConnections(t *testing.T) {
	dc := newDialController(t)
	b := NewRPCBackendPool(context.Background(), dc.dial, fastPolicy(), 2)
	defer b.Close()
	ctx := context.Background()

	if err := b.UpdateOldText(ctx, StartdAd, `Name = "x"`+"\n"+`State = "Idle"`); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := b.Len(ctx, StartdAd); err != nil {
		t.Fatalf("read: %v", err)
	}

	wc := b.write.client
	if wc == nil {
		t.Fatal("write lane has no connection after a write")
	}
	for i, r := range b.reads {
		if r.client != nil && r.client == wc {
			t.Fatalf("read lane %d shares the write lane's connection; reads and writes must be isolated", i)
		}
	}
}
