package protocol

import (
	"sync"

	"github.com/PelicanPlatform/classad/classad"

	"github.com/bbockelm/cedar/client"
	"github.com/bbockelm/cedar/security"
	"github.com/bbockelm/cedar/stream"

	"github.com/bbockelm/golang-collector/negotiator"
)

// Factory mints ScheddSessions over a cache of warm cedar sockets, one per
// (schedd address, submitter tag). It is the negotiator.SessionFactory
// implementation: the cycle asks for a Session, drives one NEGOTIATE round, and
// on End the socket is returned here for the next cycle to reuse without a
// re-handshake (design doc section 5).
//
// Factory is safe for concurrent use: the cycle's RRL prefetch opens sessions to
// many schedds in parallel.
type Factory struct {
	sec            *security.SecurityConfig
	negotiatorName string
	// legacy forces one-at-a-time SEND_JOB_INFO fetching for pre-8.3.0 schedds
	// (USE_RESOURCE_REQUEST_COUNTS off); default false uses the batched RRL.
	legacy bool

	mu   sync.Mutex
	warm map[cacheKey]*warmConn
}

type cacheKey struct {
	addr string
	tag  string
}

// warmConn is a live, authenticated, encrypted cedar session held open between
// negotiation cycles. The next round writes a bare NEGOTIATE command int on it.
type warmConn struct {
	client *client.HTCondorClient
	stream *stream.Stream
}

func (w *warmConn) close() {
	if w != nil && w.client != nil {
		_ = w.client.Close()
	}
}

// Option configures a Factory.
type Option func(*Factory)

// WithNegotiatorName sets the NegotiatorName stamped in the NEGOTIATE header
// when the per-session header does not carry one.
func WithNegotiatorName(name string) Option { return func(f *Factory) { f.negotiatorName = name } }

// WithLegacyFetch selects one-at-a-time SEND_JOB_INFO fetching instead of the
// batched SEND_RESOURCE_REQUEST_LIST (for schedds older than 8.3.0).
func WithLegacyFetch() Option { return func(f *Factory) { f.legacy = true } }

// NewFactory builds a session factory. sec is the NEGOTIATOR-level security
// config used to dial schedds (the match-password session in production); the
// factory copies it per dial and stamps Command=NEGOTIATE, never mutating the
// caller's config.
func NewFactory(sec *security.SecurityConfig, opts ...Option) *Factory {
	f := &Factory{
		sec:  sec,
		warm: make(map[cacheKey]*warmConn),
	}
	for _, o := range opts {
		o(f)
	}
	return f
}

// Session returns a ScheddSession for one submitter/schedd pair. The socket is
// bound lazily in Begin (reused from the cache or freshly dialed).
func (f *Factory) Session(submitter, scheddName, scheddAddr string, submitterAd *classad.ClassAd) negotiator.ScheddSession {
	return &session{
		f:           f,
		submitter:   submitter,
		scheddName:  scheddName,
		scheddAddr:  scheddAddr,
		submitterAd: submitterAd,
		legacy:      f.legacy,
	}
}

// take removes and returns a warm session for key, or nil if none is cached.
func (f *Factory) take(key cacheKey) *warmConn {
	f.mu.Lock()
	defer f.mu.Unlock()
	wc, ok := f.warm[key]
	if !ok {
		return nil
	}
	delete(f.warm, key)
	return wc
}

// put returns a warm session to the cache. Any conn already cached under key
// (there should be none while a session holds it) is closed and replaced.
func (f *Factory) put(key cacheKey, wc *warmConn) {
	if wc == nil {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if old, ok := f.warm[key]; ok && old != wc {
		old.close()
	}
	f.warm[key] = wc
}

// evict drops (and closes) any cached conn for key.
func (f *Factory) evict(key cacheKey) {
	f.mu.Lock()
	wc := f.warm[key]
	delete(f.warm, key)
	f.mu.Unlock()
	wc.close()
}

// CloseAll tears down every cached warm session (daemon shutdown).
func (f *Factory) CloseAll() {
	f.mu.Lock()
	conns := f.warm
	f.warm = make(map[cacheKey]*warmConn)
	f.mu.Unlock()
	for _, wc := range conns {
		wc.close()
	}
}
