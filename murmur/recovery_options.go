package murmur

import (
	"time"
)

// RecoveryOptions configures the optional disposable recovery cache: sorted,
// compressed, encrypted row runs rebuilt from the authoritative Spool
// database to accelerate restarts. A nil *RecoveryOptions disables the
// feature entirely; callers that construct Options manually keep the
// legacy behavior without changes.
type RecoveryOptions struct {
	// Enabled turns the recovery cache on. When false (or when the
	// whole options pointer is nil) open follows the legacy
	// RecoverCurrent path and never builds recovery files.
	Enabled bool
	// AutoBuild regenerates the cache in the background after open
	// when it is missing or stale.
	AutoBuild bool
	// BuildInterval caps how often automatic rebuilds may start.
	// Zero disables periodic rebuilds.
	BuildInterval time.Duration
	// TargetRunBytes is the soft size target for one sorted row run
	// file. Large tables split into several runs.
	TargetRunBytes int64
	// TargetBlockBytes is the soft uncompressed plaintext target for
	// one row block inside a run.
	TargetBlockBytes int
	// MaxBuildMemory bounds builder memory; overflow spills to
	// encrypted temporary runs.
	MaxBuildMemory int64
	// RetainGenerations is how many published cache generations to
	// keep across replacement. At least one is always retained.
	RetainGenerations int
}

// DefaultRecoveryOptions returns validation-passing recovery settings with
// the feature disabled. Flip Enabled (and usually AutoBuild) to opt in.
func DefaultRecoveryOptions() RecoveryOptions {
	return RecoveryOptions{
		Enabled:           false,
		AutoBuild:         true,
		BuildInterval:     time.Hour,
		TargetRunBytes:    256 << 20,
		TargetBlockBytes:  256 << 10,
		MaxBuildMemory:    256 << 20,
		RetainGenerations: 2,
	}
}

// IsEnabled reports whether the recovery cache is active. Nil-safe so
// zero-value Options keep the legacy path.
func (r *RecoveryOptions) IsEnabled() bool {
	return r != nil && r.Enabled
}

// WithDefaults fills unset numeric fields from DefaultRecoveryOptions,
// preserving explicitly set values. The receiver is not modified.
func (r RecoveryOptions) WithDefaults() RecoveryOptions {
	def := DefaultRecoveryOptions()
	if r.BuildInterval == 0 {
		r.BuildInterval = def.BuildInterval
	}
	if r.TargetRunBytes == 0 {
		r.TargetRunBytes = def.TargetRunBytes
	}
	if r.TargetBlockBytes == 0 {
		r.TargetBlockBytes = def.TargetBlockBytes
	}
	if r.MaxBuildMemory == 0 {
		r.MaxBuildMemory = def.MaxBuildMemory
	}
	if r.RetainGenerations == 0 {
		r.RetainGenerations = def.RetainGenerations
	}
	return r
}

// Validate checks recovery bounds. A nil receiver is valid (disabled).
func (r *RecoveryOptions) Validate() error {
	if r == nil {
		return nil
	}
	if r.BuildInterval < 0 {
		return invalidErr("invalid recovery build interval")
	}
	if r.TargetRunBytes <= 0 || r.TargetRunBytes > 16<<30 {
		return invalidErr("invalid recovery run size")
	}
	if r.TargetBlockBytes <= 0 || r.TargetBlockBytes > 64<<20 {
		return invalidErr("invalid recovery block size")
	}
	if r.MaxBuildMemory <= 0 || r.MaxBuildMemory > 64<<30 {
		return invalidErr("invalid recovery memory bound")
	}
	if r.RetainGenerations <= 0 || r.RetainGenerations > 16 {
		return invalidErr("invalid recovery generation retention")
	}
	return nil
}
