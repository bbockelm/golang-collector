package server

import (
	"context"
	"log/slog"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/bbockelm/cedar/commands"
	"github.com/bbockelm/cedar/security"
	htcondor "github.com/bbockelm/golang-htcondor"
)

// viewQueueDepth bounds how many ads may be queued for a single view collector
// before new forwards are dropped. Forwarding is best-effort (a view collector is
// a secondary, aggregating sink); the primary store must never block on a slow or
// unreachable view host, so we drop-and-log rather than apply backpressure.
const viewQueueDepth = 4096

// Forwarder relays ad updates and invalidations on to one or more CONDOR_VIEW_HOST
// collectors, exactly like the C++ collector's view forwarding. Each view host has
// its own bounded queue and worker goroutine, so a slow host cannot block the
// others or the primary store, and per-host ordering is preserved.
//
// A nil *Forwarder is valid and forwards nothing, so callers with no view hosts
// need no special-casing.
type Forwarder struct {
	hosts []*viewHost
}

type viewHost struct {
	addr string
	col  *htcondor.Collector
	ctx  context.Context
	ch   chan forwardItem
}

type forwardItem struct {
	cmd int
	ad  *classad.ClassAd
}

// NewForwarder builds a Forwarder that relays to each address in addrs, securing
// each connection with sec. It returns nil if addrs is empty (nothing to forward
// to). The caller is responsible for excluding this collector's own address to
// avoid a forwarding loop.
func NewForwarder(addrs []string, sec *security.SecurityConfig) *Forwarder {
	if len(addrs) == 0 {
		return nil
	}
	f := &Forwarder{}
	for _, addr := range addrs {
		vh := &viewHost{
			addr: addr,
			col:  htcondor.NewCollector(addr),
			ctx:  htcondor.WithSecurityConfig(context.Background(), sec),
			ch:   make(chan forwardItem, viewQueueDepth),
		}
		go vh.run()
		f.hosts = append(f.hosts, vh)
	}
	return f
}

// forwardText parses a raw old-ClassAd wire body and enqueues it for every view
// host under command cmd. Parsing happens once here (view forwarding is opt-in, so
// the streaming store path stays parse-free when no view host is configured).
func (f *Forwarder) forwardText(cmd int, text string) {
	if f == nil {
		return
	}
	ad, err := classad.ParseOld(text)
	if err != nil {
		slog.Warn("view forward: could not parse ad", "err", err)
		return
	}
	f.forward(cmd, ad)
}

// forward enqueues ad for every view host under command cmd, dropping (with a log)
// for any host whose queue is full.
func (f *Forwarder) forward(cmd int, ad *classad.ClassAd) {
	if f == nil {
		return
	}
	for _, vh := range f.hosts {
		select {
		case vh.ch <- forwardItem{cmd: cmd, ad: ad}:
		default:
			slog.Warn("view forward: queue full, dropping ad", "host", vh.addr)
		}
	}
}

// run drains a view host's queue, advertising each item with its original command.
func (vh *viewHost) run() {
	for item := range vh.ch {
		if err := vh.col.Advertise(vh.ctx, item.ad, &htcondor.AdvertiseOptions{Command: commands.CommandType(item.cmd)}); err != nil {
			slog.Warn("view forward: advertise failed", "host", vh.addr, "err", err)
		}
	}
}
