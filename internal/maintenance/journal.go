package maintenance

import (
	"errors"
	"os"
	"path/filepath"

	storeformat "github.com/akzj/ridstore/internal/format"
)

const journalName = "MAINTENANCE"

func Install(root string, journal storeformat.MaintenanceJournal) error {
	encoded, err := storeformat.EncodeMaintenanceJournal(journal)
	if err != nil {
		return err
	}
	dir := filepath.Join(root, "journal")
	temp, final := filepath.Join(dir, ".MAINTENANCE.tmp"), filepath.Join(dir, journalName)
	if err := os.Remove(temp); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	file, err := os.OpenFile(temp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	if _, err := file.Write(encoded); err != nil {
		return errors.Join(err, file.Close())
	}
	if err := file.Sync(); err != nil {
		return errors.Join(err, file.Close())
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(temp, final); err != nil {
		return err
	}
	return SyncDirectory(dir)
}

func Load(root string) (storeformat.MaintenanceJournal, bool, error) {
	data, err := os.ReadFile(filepath.Join(root, "journal", journalName))
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
	dir := filepath.Join(root, "journal")
	if err := os.Remove(filepath.Join(dir, journalName)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.Remove(filepath.Join(dir, ".MAINTENANCE.tmp")); err != nil && !errors.Is(err, os.ErrNotExist) {
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
