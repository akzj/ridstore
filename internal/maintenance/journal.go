package maintenance

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/failpoint"
	storeformat "github.com/akzj/ridstore/internal/format"
)

const journalName = "MAINTENANCE"

const (
	PointBeforeTempRemove    failpoint.Point = "maintenance.before-temp-remove"
	PointBeforeWrite         failpoint.Point = "maintenance.before-write"
	PointBeforeFileSync      failpoint.Point = "maintenance.before-file-sync"
	PointBeforeRename        failpoint.Point = "maintenance.before-rename"
	PointBeforeDirSync       failpoint.Point = "maintenance.before-dir-sync"
	PointBeforeRemove        failpoint.Point = "maintenance.before-remove"
	PointBeforeRemoveTemp    failpoint.Point = "maintenance.before-remove-temp"
	PointBeforeRemoveDirSync failpoint.Point = "maintenance.before-remove-dir-sync"
)

func Install(root string, journal storeformat.MaintenanceJournal) error {
	return InstallWithHook(root, journal, nil)
}

func InstallWithHook(root string, journal storeformat.MaintenanceJournal, hook failpoint.Hook) error {
	if current, found, err := Load(root); err != nil {
		return err
	} else if found {
		if err := storeformat.ValidateMaintenanceTransition(current, journal); err != nil {
			if current.Generation != journal.Generation || current.StoreUUID != journal.StoreUUID || current.OperationID != journal.OperationID ||
				current.OperationType != journal.OperationType || current.OldManifestGeneration != journal.OldManifestGeneration {
				return fmt.Errorf("another maintenance operation is active: %w", base.ErrConflict)
			}
			return fmt.Errorf("maintenance journal transition: %w", err)
		}
	}
	encoded, err := storeformat.EncodeMaintenanceJournal(journal)
	if err != nil {
		return err
	}
	dir := filepath.Join(root, "journal")
	temp, final := filepath.Join(dir, ".MAINTENANCE.tmp"), filepath.Join(dir, journalName)
	if err := failpoint.Hit(hook, PointBeforeTempRemove); err != nil {
		return err
	}
	if err := os.Remove(temp); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	file, err := os.OpenFile(temp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	if err := failpoint.Hit(hook, PointBeforeWrite); err != nil {
		return errors.Join(err, file.Close())
	}
	if n, err := file.Write(encoded); err != nil || n != len(encoded) {
		if err == nil {
			err = io.ErrShortWrite
		}
		return errors.Join(err, file.Close())
	}
	if err := failpoint.Hit(hook, PointBeforeFileSync); err != nil {
		return errors.Join(err, file.Close())
	}
	if err := file.Sync(); err != nil {
		return errors.Join(err, file.Close())
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := failpoint.Hit(hook, PointBeforeRename); err != nil {
		return err
	}
	if err := os.Rename(temp, final); err != nil {
		return err
	}
	if err := failpoint.Hit(hook, PointBeforeDirSync); err != nil {
		return err
	}
	return SyncDirectory(dir)
}

func Load(root string) (storeformat.MaintenanceJournal, bool, error) {
	dir := filepath.Join(root, "journal")
	// A temp file is never authoritative, whether or not the previous final
	// journal exists. Remove only a regular file and make that cleanup durable
	// before interpreting the published state.
	if err := cleanupOrphanTemp(dir); err != nil {
		return storeformat.MaintenanceJournal{}, false, err
	}
	data, err := os.ReadFile(filepath.Join(dir, journalName))
	if errors.Is(err, os.ErrNotExist) {
		return storeformat.MaintenanceJournal{}, false, nil
	}
	if err != nil {
		return storeformat.MaintenanceJournal{}, false, err
	}
	journal, err := storeformat.DecodeMaintenanceJournal(data)
	return journal, err == nil, err
}

func Remove(root string) error {
	return RemoveWithHook(root, nil)
}

func RemoveWithHook(root string, hook failpoint.Hook) error {
	dir := filepath.Join(root, "journal")
	if err := failpoint.Hit(hook, PointBeforeRemove); err != nil {
		return err
	}
	if err := os.Remove(filepath.Join(dir, journalName)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := failpoint.Hit(hook, PointBeforeRemoveTemp); err != nil {
		return err
	}
	if err := os.Remove(filepath.Join(dir, ".MAINTENANCE.tmp")); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := failpoint.Hit(hook, PointBeforeRemoveDirSync); err != nil {
		return err
	}
	return SyncDirectory(dir)
}

func cleanupOrphanTemp(dir string) error {
	temp := filepath.Join(dir, ".MAINTENANCE.tmp")
	info, err := os.Lstat(temp)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("maintenance temp is not a regular file: %w", base.ErrCorrupt)
	}
	if err := os.Remove(temp); err != nil {
		return err
	}
	return SyncDirectory(dir)
}

func SyncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}
