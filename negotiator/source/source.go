package source

import (
	"fmt"
	"log/slog"

	"github.com/bbockelm/cedar/security"

	"github.com/bbockelm/golang-collector/store"
)

// Config configures an AdSource (embedded or remote). The constraint fields
// apply in both modes; the CollectorAddr/Security fields are used only by the
// remote source.
type Config struct {
	// SlotConstraint is NEGOTIATOR_SLOT_CONSTRAINT: a ClassAd expression that a
	// machine ad must satisfy to enter the snapshot. Empty means no constraint.
	// It is applied at query time (store index / collector query) in both modes.
	SlotConstraint string
	// SubmitterConstraint is NEGOTIATOR_SUBMITTER_CONSTRAINT, applied to
	// submitter ads the same way.
	SubmitterConstraint string

	// SlotWeightExpr is SLOT_WEIGHT (default "Cpus"): the cost expression the
	// negotiator defaults a slot's weight to when the ad carries no usable
	// SlotWeight. It lets an operator weight matchmaking cost and fair-share
	// usage by something other than CPUs (e.g. "Cpus + Memory/1024"). A slot that
	// already advertises its own SlotWeight is left untouched.
	SlotWeightExpr string

	// CollectorAddr is the collector address (host:port / sinful, or a
	// comma-separated list) the remote source queries. Required for the remote
	// source; ignored by the embedded source.
	CollectorAddr string
	// Security is the CEDAR client security policy the remote source uses for
	// its collector queries and publishes. Required for the remote source;
	// ignored by the embedded source. The private-ad query needs NEGOTIATOR
	// authorization at the collector.
	Security *security.SecurityConfig

	// Logger for operational logging (default slog.Default()).
	Logger *slog.Logger
}

func (c Config) logger() *slog.Logger {
	if c.Logger != nil {
		return c.Logger
	}
	return slog.Default()
}

// NewEmbedded constructs an EmbeddedSource reading st directly. It validates
// the constraint expressions up front so a bad constraint fails at
// construction rather than mid-cycle.
func NewEmbedded(st *store.Store, cfg Config) (*EmbeddedSource, error) {
	if st == nil {
		return nil, fmt.Errorf("source: embedded source requires a non-nil store")
	}
	slotQ, err := compileConstraint(cfg.SlotConstraint)
	if err != nil {
		return nil, fmt.Errorf("source: NEGOTIATOR_SLOT_CONSTRAINT %q: %w", cfg.SlotConstraint, err)
	}
	subQ, err := compileConstraint(cfg.SubmitterConstraint)
	if err != nil {
		return nil, fmt.Errorf("source: NEGOTIATOR_SUBMITTER_CONSTRAINT %q: %w", cfg.SubmitterConstraint, err)
	}
	return &EmbeddedSource{
		store: st,
		cfg:   cfg,
		log:   cfg.logger(),
		slotQ: slotQ,
		subQ:  subQ,
		defaultWeight: ParseSlotWeight(cfg.SlotWeightExpr),
	}, nil
}

// NewRemote constructs a RemoteSource querying the collector at
// cfg.CollectorAddr with cfg.Security. It validates the constraints and the
// required remote fields.
func NewRemote(cfg Config) (*RemoteSource, error) {
	if cfg.CollectorAddr == "" {
		return nil, fmt.Errorf("source: remote source requires a CollectorAddr")
	}
	if cfg.Security == nil {
		return nil, fmt.Errorf("source: remote source requires a Security config")
	}
	// Validate the constraints parse (they are pushed to the collector as
	// strings, but a parse error should surface here, not on the wire).
	if _, err := compileConstraint(cfg.SlotConstraint); err != nil {
		return nil, fmt.Errorf("source: NEGOTIATOR_SLOT_CONSTRAINT %q: %w", cfg.SlotConstraint, err)
	}
	if _, err := compileConstraint(cfg.SubmitterConstraint); err != nil {
		return nil, fmt.Errorf("source: NEGOTIATOR_SUBMITTER_CONSTRAINT %q: %w", cfg.SubmitterConstraint, err)
	}
	return &RemoteSource{
		cfg: cfg,
		log: cfg.logger(),
		defaultWeight: ParseSlotWeight(cfg.SlotWeightExpr),
	}, nil
}
