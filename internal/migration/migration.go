package migration

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/filelock"
	storeformat "github.com/akzj/ridstore/internal/format"
	"github.com/akzj/ridstore/internal/initialize"
	"github.com/akzj/ridstore/internal/manifest"
	"github.com/akzj/ridstore/internal/verify"
)

type Version struct {
	Major uint16 `json:"major"`
	Minor uint16 `json:"minor"`
}

func (v Version) String() string { return fmt.Sprintf("%d.%d", v.Major, v.Minor) }

func CurrentVersion() Version {
	return Version{Major: storeformat.FormatMajorVersion, Minor: storeformat.FormatMinorVersion}
}

type Step struct {
	Name string  `json:"name"`
	From Version `json:"from"`
	To   Version `json:"to"`
}

type Registry struct {
	bySource map[Version]Step
}

func NewRegistry(steps []Step) (Registry, error) {
	registry := Registry{bySource: make(map[Version]Step, len(steps))}
	for _, step := range steps {
		if step.Name == "" || step.From.Major == 0 || step.To.Major == 0 || !versionLess(step.From, step.To) {
			return Registry{}, fmt.Errorf("invalid migration step: %w", base.ErrInvalidConfig)
		}
		if _, duplicate := registry.bySource[step.From]; duplicate {
			return Registry{}, fmt.Errorf("duplicate migration source %s: %w", step.From, base.ErrInvalidConfig)
		}
		registry.bySource[step.From] = step
	}
	return registry, nil
}

func (r Registry) Path(from, to Version) ([]Step, bool) {
	if from == to {
		return nil, true
	}
	seen := make(map[Version]struct{}, len(r.bySource)+1)
	var path []Step
	for current := from; current != to; {
		if _, cycle := seen[current]; cycle {
			return nil, false
		}
		seen[current] = struct{}{}
		step, ok := r.bySource[current]
		if !ok {
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
	Steps              []Step  `json:"steps,omitempty"`
}

var defaultRegistry = Registry{bySource: map[Version]Step{}}

// Inspect acquires the offline Store lease, reads the raw Manifest header, and
// returns the compiled migration path to the current format. It never modifies
// the Store. With no registered v1 migrations, non-current formats fail closed.
func Inspect(ctx context.Context, root string) (plan Plan, resultErr error) {
	if err := ctx.Err(); err != nil {
		return plan, err
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return plan, err
	}
	plan.Directory, plan.To = abs, CurrentVersion()
	if _, err := os.Lstat(filepath.Join(abs, initialize.RestoringMarkerFileName)); err == nil {
		return plan, base.ErrRecoveryRequired
	} else if !errors.Is(err, os.ErrNotExist) {
		return plan, err
	}
	lease, err := filelock.AcquireExisting(abs)
	if err != nil {
		return plan, err
	}
	defer func() { resultErr = errors.Join(resultErr, lease.Close()) }()
	name, err := manifest.ReadCurrentName(abs)
	if err != nil {
		return plan, err
	}
	generation, err := manifest.ParseManifestFileName(name)
	if err != nil {
		return plan, err
	}
	header, err := readManifestHeader(filepath.Join(abs, manifest.ManifestDirName, name))
	if err != nil {
		return plan, err
	}
	if header.Generation != generation {
		return plan, fmt.Errorf("migration Manifest generation mismatch: %w", base.ErrCorrupt)
	}
	plan.StoreUUID = hex.EncodeToString(header.StoreUUID[:])
	plan.ManifestGeneration = header.Generation
	plan.From = Version{Major: header.MajorVersion, Minor: header.MinorVersion}
	plan.MigrationRequired = plan.From != plan.To
	plan.Steps, plan.Supported = defaultRegistry.Path(plan.From, plan.To)
	if !plan.Supported {
		return plan, fmt.Errorf("no migration path from %s to %s: %w", plan.From, plan.To, base.ErrUnsupported)
	}
	if !plan.MigrationRequired {
		report, err := verify.RunUnderLease(ctx, abs)
		if err != nil || !report.Clean || report.StoreUUID != plan.StoreUUID || report.ManifestGeneration != plan.ManifestGeneration {
			if err == nil {
				err = base.ErrCorrupt
			}
			return plan, err
		}
		plan.VerifiedCurrent = true
	}
	return plan, nil
}

func readManifestHeader(path string) (header storeformat.ContainerHeader, resultErr error) {
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return header, err
	}
	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		_ = syscall.Close(fd)
		return header, fmt.Errorf("open migration Manifest")
	}
	defer func() { resultErr = errors.Join(resultErr, file.Close()) }()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() < storeformat.ContainerHeaderSize || info.Size() > storeformat.MaxManifestPayloadSize+storeformat.ContainerHeaderSize {
		if err == nil {
			err = base.ErrCorrupt
		}
		return header, err
	}
	data := make([]byte, storeformat.ContainerHeaderSize)
	if _, err := file.ReadAt(data, 0); err != nil {
		return header, err
	}
	return storeformat.InspectContainerHeader(data, storeformat.ManifestMagic, uint64(info.Size()), storeformat.MaxManifestPayloadSize)
}

func versionLess(left, right Version) bool {
	return left.Major < right.Major || left.Major == right.Major && left.Minor < right.Minor
}
