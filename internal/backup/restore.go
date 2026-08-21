package backup

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/failpoint"
	"github.com/akzj/ridstore/internal/filelock"
	storeformat "github.com/akzj/ridstore/internal/format"
	"github.com/akzj/ridstore/internal/initialize"
	"github.com/akzj/ridstore/internal/manifest"
	"github.com/akzj/ridstore/internal/verify"
)

const restorePayloadDirName = ".payload"

type RestoreOptions struct {
	PreserveUUID bool
	Hook         failpoint.Hook
}

const (
	PointRestorePrepared         failpoint.Point = "restore.prepared"
	PointRestoreFilesCopied      failpoint.Point = "restore.files-copied"
	PointRestoreUUIDRewritten    failpoint.Point = "restore.uuid-rewritten"
	PointRestorePayloadVerified  failpoint.Point = "restore.payload-verified"
	PointRestorePayloadPublished failpoint.Point = "restore.payload-published"
	PointRestoreLayoutVerified   failpoint.Point = "restore.layout-verified"
	PointRestoreMarkerRemoved    failpoint.Point = "restore.marker-removed"
	PointRestorePublished        failpoint.Point = "restore.published"
)

type RestoreReport struct {
	Destination        string `json:"destination"`
	SourceStoreUUID    string `json:"source_store_uuid"`
	RestoredStoreUUID  string `json:"restored_store_uuid"`
	PreservedUUID      bool   `json:"preserved_uuid"`
	ManifestGeneration uint64 `json:"manifest_generation"`
	Files              uint64 `json:"files"`
	Bytes              uint64 `json:"bytes"`
}

// Restore validates an artifact and publishes a verified Store into a new
// destination directory. It never merges with or overwrites an existing path.
func Restore(ctx context.Context, artifact, destination string, options RestoreOptions) (report RestoreReport, resultErr error) {
	if err := ctx.Err(); err != nil {
		return report, err
	}
	artifactAbs, err := filepath.Abs(artifact)
	if err != nil {
		return report, err
	}
	destinationAbs, err := filepath.Abs(destination)
	if err != nil {
		return report, err
	}
	if artifactAbs == destinationAbs || isWithin(artifactAbs, destinationAbs) {
		return report, fmt.Errorf("restore destination must be outside backup artifact: %w", base.ErrInvalidConfig)
	}
	metadata, err := Inspect(ctx, artifactAbs)
	if err != nil {
		return report, err
	}
	if err := createRestoreRoot(destinationAbs); err != nil {
		return report, err
	}
	if err := failpoint.Hit(options.Hook, PointRestorePrepared); err != nil {
		return report, err
	}
	payloadRoot := filepath.Join(destinationAbs, restorePayloadDirName)
	for _, name := range []string{"manifests", "data", "mapping", "journal", "trash", "tmp"} {
		if err := os.Mkdir(filepath.Join(payloadRoot, name), 0o700); err != nil {
			return report, err
		}
	}
	if err := writeNewSynced(filepath.Join(payloadRoot, filelock.FileName), []byte{}, 0o600); err != nil {
		return report, err
	}
	for _, entry := range metadata.Files {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		size, digest, err := copyRegularFile(ctx, filepath.Join(artifactAbs, payloadDirName, entry.Path), filepath.Join(payloadRoot, entry.Path))
		if err != nil {
			return report, err
		}
		if size != entry.Size || digest != entry.SHA256 {
			return report, fmt.Errorf("backup changed while restoring %s: %w", entry.Path, base.ErrCorrupt)
		}
		if err := add(&report.Bytes, size); err != nil {
			return report, err
		}
	}
	if err := failpoint.Hit(options.Hook, PointRestoreFilesCopied); err != nil {
		return report, err
	}
	sourceUUID, err := parseUUID(metadata.StoreUUID)
	if err != nil {
		return report, err
	}
	restoredUUID := sourceUUID
	if !options.PreserveUUID {
		for restoredUUID == sourceUUID {
			restoredUUID, err = newStoreUUID()
			if err != nil {
				return report, err
			}
		}
		if err := rewriteStoreUUID(payloadRoot, metadata.Files, sourceUUID, restoredUUID); err != nil {
			return report, err
		}
	}
	if err := failpoint.Hit(options.Hook, PointRestoreUUIDRewritten); err != nil {
		return report, err
	}
	for _, dir := range []string{
		filepath.Join(payloadRoot, "manifests"), filepath.Join(payloadRoot, "data"), filepath.Join(payloadRoot, "mapping"),
		filepath.Join(payloadRoot, "journal"), filepath.Join(payloadRoot, "trash"), filepath.Join(payloadRoot, "tmp"), payloadRoot, destinationAbs,
	} {
		if err := syncDirectory(dir); err != nil {
			return report, err
		}
	}
	lease, err := filelock.AcquireExisting(payloadRoot)
	if err != nil {
		return report, err
	}
	leaseClosed := false
	defer func() {
		if !leaseClosed {
			resultErr = errors.Join(resultErr, lease.Close())
		}
	}()
	prePublish, err := verify.RunUnderLease(ctx, payloadRoot)
	if err != nil || !prePublish.Clean || prePublish.StoreUUID != hex.EncodeToString(restoredUUID[:]) {
		if err == nil {
			err = base.ErrCorrupt
		}
		return report, err
	}
	if err := failpoint.Hit(options.Hook, PointRestorePayloadVerified); err != nil {
		return report, err
	}
	if err := publishRestorePayload(payloadRoot, destinationAbs); err != nil {
		return report, err
	}
	if err := failpoint.Hit(options.Hook, PointRestorePayloadPublished); err != nil {
		return report, err
	}
	postPublish, err := verify.RunRestoringUnderLease(ctx, destinationAbs)
	if err != nil || !postPublish.Clean || postPublish.StoreUUID != hex.EncodeToString(restoredUUID[:]) {
		if err == nil {
			err = base.ErrCorrupt
		}
		return report, err
	}
	if err := failpoint.Hit(options.Hook, PointRestoreLayoutVerified); err != nil {
		return report, err
	}
	if err := os.Remove(filepath.Join(destinationAbs, initialize.RestoringMarkerFileName)); err != nil {
		return report, err
	}
	if err := failpoint.Hit(options.Hook, PointRestoreMarkerRemoved); err != nil {
		restoreErr := writeNewSynced(filepath.Join(destinationAbs, initialize.RestoringMarkerFileName), []byte("ridstore restore incomplete\n"), 0o600)
		if restoreErr == nil {
			restoreErr = syncDirectory(destinationAbs)
		}
		return report, errors.Join(err, restoreErr)
	}
	if err := syncDirectory(destinationAbs); err != nil {
		restoreErr := writeNewSynced(filepath.Join(destinationAbs, initialize.RestoringMarkerFileName), []byte("ridstore restore incomplete\n"), 0o600)
		if restoreErr == nil {
			restoreErr = syncDirectory(destinationAbs)
		}
		return report, errors.Join(err, restoreErr)
	}
	if err := failpoint.Hit(options.Hook, PointRestorePublished); err != nil {
		return report, err
	}
	if err := lease.Close(); err != nil {
		leaseClosed = true
		return report, err
	}
	leaseClosed = true
	report.Destination = destinationAbs
	report.SourceStoreUUID = metadata.StoreUUID
	report.RestoredStoreUUID = hex.EncodeToString(restoredUUID[:])
	report.PreservedUUID = options.PreserveUUID
	report.ManifestGeneration = metadata.ManifestGeneration
	report.Files = uint64(len(metadata.Files))
	return report, nil
}

func createRestoreRoot(root string) error {
	parent := filepath.Dir(root)
	info, err := os.Lstat(parent)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("restore parent is not a real directory: %w", base.ErrInvalidConfig)
	}
	if err := os.Mkdir(root, 0o700); err != nil {
		return err
	}
	if err := writeNewSynced(filepath.Join(root, initialize.RestoringMarkerFileName), []byte("ridstore restore incomplete\n"), 0o600); err != nil {
		return errors.Join(err, os.Remove(root))
	}
	if err := syncDirectory(root); err != nil {
		return err
	}
	if err := syncDirectory(parent); err != nil {
		return err
	}
	if err := os.Mkdir(filepath.Join(root, restorePayloadDirName), 0o700); err != nil {
		return err
	}
	return nil
}

func publishRestorePayload(payloadRoot, destination string) error {
	entries := []string{"manifests", "data", "mapping", "journal", "trash", "tmp", manifest.CurrentFileName, filelock.FileName}
	for _, name := range entries {
		if err := os.Rename(filepath.Join(payloadRoot, name), filepath.Join(destination, name)); err != nil {
			return err
		}
	}
	if err := os.Remove(payloadRoot); err != nil {
		return err
	}
	return syncDirectory(destination)
}

func rewriteStoreUUID(root string, files []FileEntry, oldUUID, newUUID base.StoreUUID) error {
	current, err := manifest.LoadCurrent(root)
	if err != nil {
		return err
	}
	if current.StoreUUID != oldUUID {
		return fmt.Errorf("restore source UUID changed: %w", base.ErrCorrupt)
	}
	for _, entry := range files {
		if !stringsHasDirectory(entry.Path, "data") && !stringsHasDirectory(entry.Path, "mapping") {
			continue
		}
		if err := rewriteSegmentHeader(filepath.Join(root, entry.Path), oldUUID, newUUID); err != nil {
			return err
		}
	}
	current.StoreUUID = newUUID
	encoded, err := storeformat.EncodeManifest(current)
	if err != nil {
		return err
	}
	manifestPath := filepath.Join(root, manifest.ManifestDirName, manifest.ManifestFileName(current.Generation))
	if err := replaceSynced(manifestPath, encoded); err != nil {
		return err
	}
	return syncDirectory(filepath.Dir(manifestPath))
}

func rewriteSegmentHeader(path string, oldUUID, newUUID base.StoreUUID) (resultErr error) {
	fd, err := syscall.Open(path, syscall.O_RDWR|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		_ = syscall.Close(fd)
		return fmt.Errorf("open restored segment %s", path)
	}
	defer func() { resultErr = errors.Join(resultErr, file.Close()) }()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() < storeformat.SegmentHeaderSize {
		if err == nil {
			err = base.ErrCorrupt
		}
		return err
	}
	headerBytes := make([]byte, storeformat.SegmentHeaderSize)
	if _, err := file.ReadAt(headerBytes, 0); err != nil {
		return err
	}
	header, err := storeformat.DecodeSegmentHeader(headerBytes)
	if err != nil {
		return err
	}
	if header.StoreUUID != oldUUID {
		return fmt.Errorf("restored segment source UUID mismatch: %w", base.ErrCorrupt)
	}
	header.StoreUUID = newUUID
	encoded, err := storeformat.EncodeSegmentHeader(header)
	if err != nil {
		return err
	}
	if n, err := file.WriteAt(encoded[:], 0); err != nil {
		return err
	} else if n != len(encoded) {
		return io.ErrShortWrite
	}
	return file.Sync()
}

func replaceSynced(path string, data []byte) (resultErr error) {
	temp := path + ".restore"
	file, err := os.OpenFile(temp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	closed := false
	defer func() {
		if !closed {
			resultErr = errors.Join(resultErr, file.Close())
		}
		if resultErr != nil {
			resultErr = errors.Join(resultErr, removeIfExists(temp))
		}
	}()
	if n, err := file.Write(data); err != nil {
		return err
	} else if n != len(data) {
		return io.ErrShortWrite
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	closed = true
	if err := os.Rename(temp, path); err != nil {
		return err
	}
	return nil
}

func parseUUID(encoded string) (base.StoreUUID, error) {
	var uuid base.StoreUUID
	data, err := hex.DecodeString(encoded)
	if err != nil || len(data) != len(uuid) {
		return uuid, fmt.Errorf("backup StoreUUID: %w", base.ErrCorrupt)
	}
	copy(uuid[:], data)
	if uuid == (base.StoreUUID{}) {
		return base.StoreUUID{}, fmt.Errorf("backup zero StoreUUID: %w", base.ErrCorrupt)
	}
	return uuid, nil
}

func newStoreUUID() (base.StoreUUID, error) {
	for {
		var uuid base.StoreUUID
		if _, err := io.ReadFull(rand.Reader, uuid[:]); err != nil {
			return base.StoreUUID{}, err
		}
		if uuid != (base.StoreUUID{}) {
			return uuid, nil
		}
	}
}

func stringsHasDirectory(path, directory string) bool {
	prefix := directory + string(filepath.Separator)
	return len(path) > len(prefix) && path[:len(prefix)] == prefix
}
