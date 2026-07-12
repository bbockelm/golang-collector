package cycle

import (
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/classad"

	"github.com/bbockelm/golang-collector/negotiator/negtest"
)

// slowProxy is a byte-forwarding TCP proxy that sleeps before relaying each
// chunk, simulating a slow schedd link.
type slowProxy struct {
	ln    net.Listener
	delay time.Duration

	mu    sync.Mutex
	conns []net.Conn
}

func startSlowProxy(t *testing.T, target string, delay time.Duration) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("proxy listen: %v", err)
	}
	p := &slowProxy{ln: ln, delay: delay}
	go p.serve(target)
	t.Cleanup(p.stop)
	return ln.Addr().String()
}

func (p *slowProxy) serve(target string) {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		d, err := net.Dial("tcp", target)
		if err != nil {
			_ = c.Close()
			continue
		}
		p.mu.Lock()
		p.conns = append(p.conns, c, d)
		p.mu.Unlock()
		go p.copy(d, c)
		go p.copy(c, d)
	}
}

func (p *slowProxy) copy(dst, src net.Conn) {
	buf := make([]byte, 512)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			time.Sleep(p.delay)
			if _, werr := dst.Write(buf[:n]); werr != nil {
				break
			}
		}
		if err != nil {
			break
		}
	}
	_ = dst.Close()
	_ = src.Close()
}

func (p *slowProxy) stop() {
	_ = p.ln.Close()
	p.mu.Lock()
	for _, c := range p.conns {
		_ = c.Close()
	}
	p.mu.Unlock()
}

// TestTimeBudgetPerSubmitter: a slow schedd link plus a small RRL batch size
// forces blocking fetches inside the negotiation loop; MaxTimePerSubmitter
// trips cleanly (the C++ between-requests deadline check), the cycle
// completes, and the submitter stops early.
func TestTimeBudgetPerSubmitter(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode")
	}
	ctx := testCtx(t)

	const totalGroups = 40
	var ads []*classad.ClassAd
	for i := 1; i <= 8; i++ {
		name := fmt.Sprintf("tb%d@ep", i)
		ads = append(ads, machineAd(t, name, 1), pvtAd(name, claimForSlot(name)))
	}
	var groups []negtest.Group
	for g := 0; g < totalGroups; g++ {
		groups = append(groups, group(t, 60+g, 600+g, 1, 1, ""))
	}
	sched := startSchedd(t, ctx, [][]negtest.Group{groups})
	proxyAddr := startSlowProxy(t, sched.Addr(), 15*time.Millisecond)
	ads = append(ads, submitterAd("slowpoke@pool", "schedd_s", proxyAddr, totalGroups))

	st := seedStore(t, ads...)
	cfg := DefaultConfig()
	cfg.CompatMode = true
	cfg.RequestListSize = 2 // force fetches inside the budgeted loop
	cfg.MaxTimePerSubmitter = 250 * time.Millisecond
	cyc, err := New(embeddedSource(t, st), newAccountant(t), newCountingFactory(newFactory()), cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	start := time.Now()
	stats, err := cyc.Run(ctx)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 20*time.Second {
		t.Fatalf("cycle took %v; budget did not trip", elapsed)
	}

	// The budget stopped the round before every request was serviced.
	if handled := stats.Matches + stats.Rejections; handled >= totalGroups {
		t.Errorf("handled %d requests (matches=%d rejections=%d), want < %d (early stop)",
			handled, stats.Matches, stats.Rejections, totalGroups)
	}
	if stats.Matches == 0 {
		t.Errorf("no matches at all before the budget tripped")
	}
}
