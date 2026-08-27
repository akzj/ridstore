// Package migration provides read-only format inspection and upgrade planning.
// Execution is intentionally absent until a concrete future format is frozen.
package migration

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/bootstrap"
	"github.com/akzj/ridstore/internal/filelock"
	"github.com/akzj/ridstore/internal/storecatalog"
	"github.com/akzj/ridstore/internal/verifier"
)

type Version struct {
	Major uint16 `json:"major"`
	Minor uint16 `json:"minor"`
}

func (v Version) String() string { return fmt.Sprintf("%d.%d", v.Major, v.Minor) }

func CurrentVersion() Version {
	return Version{Major: storecatalog.FormatMajor, Minor: storecatalog.FormatMinor}
}

type Step struct {
	Name string  `json:"name"`
	From Version `json:"from"`
	To   Version `json:"to"`
}

type Registry struct{ bySource map[Version]Step }

func NewRegistry(steps []Step) (Registry, error) {
	registry := Registry{bySource: make(map[Version]Step, len(steps))}
	names := make(map[string]struct{}, len(steps))
	for _, step := range steps {
		if step.Name == "" || step.From.Major == 0 || step.To.Major == 0 || !versionLess(step.From, step.To) {
			return Registry{}, fmt.Errorf("invalid migration step: %w", base.ErrInvalidConfig)
		}
		if _, duplicate := registry.bySource[step.From]; duplicate {
			return Registry{}, fmt.Errorf("duplicate migration source %s: %w", step.From, base.ErrInvalidConfig)
		}
		if _, duplicate := names[step.Name]; duplicate {
			return Registry{}, fmt.Errorf("duplicate migration name %q: %w", step.Name, base.ErrInvalidConfig)
		}
		registry.bySource[step.From], names[step.Name] = step, struct{}{}
	}
	return registry, nil
}

func (r Registry) Path(from, to Version) ([]Step, bool) {
	if from == to {
		return []Step{}, true
	}
	path := make([]Step, 0, len(r.bySource))
	for current := from; current != to; {
		step, ok := r.bySource[current]
		if !ok || !versionLess(current, step.To) || versionLess(to, step.To) {
			return nil, false
		}
		path = append(path, step)
		current = step.To
		if len(path) > len(r.bySource) {
			return nil, false
		}
	}
	return path, true
}

type Plan struct {
	Directory          string  `json:"directory"`
	StoreUUID          string  `json:"store_uuid,omitempty"`
	ManifestGeneration uint64  `json:"manifest_generation,omitempty"`
	From               Version `json:"from"`
	To                 Version `json:"to"`
	MigrationRequired  bool    `json:"migration_required"`
	Supported          bool    `json:"supported"`
	VerifiedCurrent    bool    `json:"verified_current"`
	Steps              []Step  `json:"steps"`
}

var defaultRegistry, _ = NewRegistry(nil)

// Inspect takes the Store's exclusive lease and never modifies it. Current
// stores must pass exact verification; unknown versions only receive a plan.
func Inspect(ctx context.Context, root string) (plan Plan, resultErr error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return plan, err
	}
	if root == "" {
		return plan, base.ErrInvalidConfig
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return plan, err
	}
	plan.Directory, plan.To = abs, CurrentVersion()
	lease, err := filelock.AcquireExisting(abs)
	if err != nil {
		return plan, err
	}
	defer func() { resultErr = errors.Join(resultErr, lease.Close()) }()
	if found, err := bootstrap.RecoveryArtifacts(abs); err != nil {
		return plan, err
	} else if found {
		return plan, base.ErrRecoveryRequired
	}
	header, err := storecatalog.InspectLatestHeader(abs)
	if err != nil {
		return plan, classify(err)
	}
	plan.StoreUUID = hex.EncodeToString(header.StoreUUID[:])
	plan.ManifestGeneration = header.Generation
	plan.From = Version{Major: header.FormatMajor, Minor: header.FormatMinor}
	plan.MigrationRequired = plan.From != plan.To
	plan.Steps, plan.Supported = defaultRegistry.Path(plan.From, plan.To)
	if !plan.Supported {
		return plan, fmt.Errorf("no migration path from %s to %s: %w", plan.From, plan.To, base.ErrUnsupported)
	}
	if !plan.MigrationRequired {
		report, err := verifier.VerifyHeld(ctx, abs, verifier.Config{MappingCacheBytes: 256 << 20, MaxLiveIDs: 1 << 20, MaxReplayStatuses: 1 << 20})
		if err != nil {
			return plan, classify(err)
		}
		if report.Stage != verifier.StageExact || report.StoreID != [16]byte(header.StoreUUID) || report.ManifestGeneration != header.Generation {
			return plan, base.ErrCorrupt
		}
		plan.VerifiedCurrent = true
	}
	return plan, nil
}

func classify(err error) error {
	switch {
	case errors.Is(err, storecatalog.ErrUnsupported):
		return errors.Join(base.ErrUnsupported, err)
	case errors.Is(err, storecatalog.ErrRecoveryRequired):
		return errors.Join(base.ErrRecoveryRequired, err)
	case errors.Is(err, storecatalog.ErrCorrupt), errors.Is(err, storecatalog.ErrInvalid):
		return errors.Join(base.ErrCorrupt, err)
	default:
		return err
	}
}

func versionLess(left, right Version) bool {
	return left.Major < right.Major || left.Major == right.Major && left.Minor < right.Minor
}
