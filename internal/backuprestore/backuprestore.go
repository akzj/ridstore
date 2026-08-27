package backuprestore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/filelock"
	"github.com/akzj/ridstore/internal/storecatalog"
	"github.com/akzj/ridstore/internal/verifier"
)

type Config struct {
	SourceDir string
	DestDir   string
	Verify    verifier.Config
	Hook      FaultHook
}

type FaultPoint string

type FaultHook func(FaultPoint) error

const (
	FaultBackupBeforeStaging      FaultPoint = "backup.before-staging"
	FaultBackupAfterIncomplete    FaultPoint = "backup.after-incomplete"
	FaultBackupBeforeFileCreate   FaultPoint = "backup.before-file-create"
	FaultBackupBeforeFileWrite    FaultPoint = "backup.before-file-write"
	FaultBackupBeforeFileSync     FaultPoint = "backup.before-file-sync"
	FaultBackupAfterPayload       FaultPoint = "backup.after-payload"
	FaultBackupAfterVerify        FaultPoint = "backup.after-verify"
	FaultBackupBeforeMetadata     FaultPoint = "backup.before-metadata"
	FaultBackupAfterMetadata      FaultPoint = "backup.after-metadata"
	FaultBackupBeforeMarkerRemove FaultPoint = "backup.before-marker-remove"
	FaultBackupBeforePublish      FaultPoint = "backup.before-publish"
	FaultBackupAfterPublish       FaultPoint = "backup.after-publish"

	FaultRestoreBeforeStaging      FaultPoint = "restore.before-staging"
	FaultRestoreBeforeFileCreate   FaultPoint = "restore.before-file-create"
	FaultRestoreBeforeFileWrite    FaultPoint = "restore.before-file-write"
	FaultRestoreBeforeFileSync     FaultPoint = "restore.before-file-sync"
	FaultRestoreAfterPayload       FaultPoint = "restore.after-payload"
	FaultRestoreAfterVerify        FaultPoint = "restore.after-verify"
	FaultRestoreBeforeMarkerRemove FaultPoint = "restore.before-marker-remove"
	FaultRestoreBeforePublish      FaultPoint = "restore.before-publish"
	FaultRestoreAfterPublish       FaultPoint = "restore.after-publish"
)

type Report struct {
	StoreID            [16]byte
	ManifestGeneration uint64
	Files              uint64
	Bytes              uint64
}

func Backup(ctx context.Context, config Config) (report Report, resultErr error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return report, err
	}
	if err := validateDistinctPaths(config.SourceDir, config.DestDir); err != nil {
		return report, err
	}
	if !publicationSupported() {
		return report, base.ErrUnsupported
	}
	if err := requireAbsent(config.DestDir); err != nil {
		return report, err
	}
	if err := requireRealDirectory(filepath.Dir(config.DestDir)); err != nil {
		return report, err
	}
	lease, err := filelock.AcquireExisting(config.SourceDir)
	if err != nil {
		return report, err
	}
	defer func() { resultErr = errors.Join(resultErr, lease.Close()) }()
	verified, err := verifier.VerifyHeld(ctx, config.SourceDir, config.Verify)
	if err != nil {
		return report, err
	}
	manifest, err := storecatalog.LoadStrict(config.SourceDir)
	if err != nil {
		return report, classify(err)
	}
	if verified.Stage != verifier.StageExact || verified.ManifestGeneration != manifest.Generation || verified.StoreID != manifest.StoreUUID {
		return report, base.ErrCorrupt
	}
	paths := manifestPaths(manifest)
	if err := hit(config.Hook, FaultBackupBeforeStaging); err != nil {
		return report, err
	}
	staging, err := makeStaging(config.DestDir, "backup")
	if err != nil {
		return report, err
	}
	published := false
	defer func() {
		if !published {
			resultErr = errors.Join(resultErr, os.RemoveAll(staging))
		}
	}()
	if err := createBackupLayout(staging); err != nil {
		return report, err
	}
	if err := hit(config.Hook, FaultBackupAfterIncomplete); err != nil {
		return report, err
	}
	entries := make([]Entry, 0, len(paths))
	for _, relative := range paths {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		entry, err := copyAndHash(ctx, filepath.Join(config.SourceDir, relative), filepath.Join(staging, PayloadDirectory, relative), relative, config.Hook,
			FaultBackupBeforeFileCreate, FaultBackupBeforeFileWrite, FaultBackupBeforeFileSync)
		if err != nil {
			return report, err
		}
		entries = append(entries, entry)
		report.Bytes += entry.Size
	}
	if err := hit(config.Hook, FaultBackupAfterPayload); err != nil {
		return report, err
	}
	payload := filepath.Join(staging, PayloadDirectory)
	if err := createLock(payload); err != nil {
		return report, err
	}
	payloadReport, err := verifier.Verify(ctx, payload, config.Verify)
	removeErr := removeAndSync(filepath.Join(payload, filelock.FileName), payload)
	if err != nil || removeErr != nil {
		return report, errors.Join(err, removeErr)
	}
	if payloadReport.Stage != verifier.StageExact || payloadReport.ManifestGeneration != manifest.Generation || payloadReport.StoreID != manifest.StoreUUID {
		return report, base.ErrCorrupt
	}
	if err := hit(config.Hook, FaultBackupAfterVerify); err != nil {
		return report, err
	}
	metadata := Metadata{
		StoreID: manifest.StoreUUID, RecordLogID: manifest.RecordLogID,
		ManifestGeneration: manifest.Generation, CreatedUnixNano: time.Now().UTC().UnixNano(), Entries: entries,
	}
	encoded, err := EncodeMetadata(metadata)
	if err != nil {
		return report, classify(err)
	}
	if err := hit(config.Hook, FaultBackupBeforeMetadata); err != nil {
		return report, err
	}
	if err := writeSync(filepath.Join(staging, MetadataName), encoded, 0o600); err != nil {
		return report, err
	}
	if err := hit(config.Hook, FaultBackupAfterMetadata); err != nil {
		return report, err
	}
	if err := syncBackupTree(staging); err != nil {
		return report, err
	}
	if err := hit(config.Hook, FaultBackupBeforeMarkerRemove); err != nil {
		return report, err
	}
	if err := removeAndSync(filepath.Join(staging, IncompleteName), staging); err != nil {
		return report, err
	}
	if err := publish(staging, config.DestDir, config.Hook, FaultBackupBeforePublish, FaultBackupAfterPublish); err != nil {
		return report, err
	}
	published = true
	report.StoreID = manifest.StoreUUID
	report.ManifestGeneration = manifest.Generation
	report.Files = uint64(len(entries))
	return report, nil
}

func Restore(ctx context.Context, config Config) (report Report, resultErr error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return report, err
	}
	if err := validateDistinctPaths(config.SourceDir, config.DestDir); err != nil {
		return report, err
	}
	if !publicationSupported() {
		return report, base.ErrUnsupported
	}
	if err := requireAbsent(config.DestDir); err != nil {
		return report, err
	}
	if err := requireRealDirectory(config.SourceDir); err != nil {
		return report, err
	}
	if err := requireRealDirectory(filepath.Dir(config.DestDir)); err != nil {
		return report, err
	}
	metadata, err := readMetadata(config.SourceDir)
	if err != nil {
		return report, classify(err)
	}
	if err := validateArtifact(ctx, config.SourceDir, metadata); err != nil {
		return report, classify(err)
	}
	if err := hit(config.Hook, FaultRestoreBeforeStaging); err != nil {
		return report, err
	}
	staging, err := makeStaging(config.DestDir, "restore")
	if err != nil {
		return report, err
	}
	published := false
	defer func() {
		if !published {
			resultErr = errors.Join(resultErr, os.RemoveAll(staging))
		}
	}()
	if err := createStoreLayout(staging); err != nil {
		return report, err
	}
	for _, entry := range metadata.Entries {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		got, err := copyAndHash(ctx, filepath.Join(config.SourceDir, PayloadDirectory, entry.Path), filepath.Join(staging, entry.Path), entry.Path, config.Hook,
			FaultRestoreBeforeFileCreate, FaultRestoreBeforeFileWrite, FaultRestoreBeforeFileSync)
		if err != nil {
			return report, err
		}
		if got.Size != entry.Size || got.SHA256 != entry.SHA256 {
			return report, base.ErrCorrupt
		}
		report.Bytes += entry.Size
	}
	if err := hit(config.Hook, FaultRestoreAfterPayload); err != nil {
		return report, err
	}
	if err := createLock(staging); err != nil {
		return report, err
	}
	if err := syncStoreTree(staging); err != nil {
		return report, err
	}
	verified, err := verifier.Verify(ctx, staging, config.Verify)
	if err != nil {
		return report, err
	}
	if verified.Stage != verifier.StageExact || verified.StoreID != metadata.StoreID || verified.ManifestGeneration != metadata.ManifestGeneration {
		return report, base.ErrCorrupt
	}
	if err := hit(config.Hook, FaultRestoreAfterVerify); err != nil {
		return report, err
	}
	if err := hit(config.Hook, FaultRestoreBeforeMarkerRemove); err != nil {
		return report, err
	}
	if err := removeAndSync(filepath.Join(staging, RestoreIncompleteName), staging); err != nil {
		return report, err
	}
	if err := publish(staging, config.DestDir, config.Hook, FaultRestoreBeforePublish, FaultRestoreAfterPublish); err != nil {
		return report, err
	}
	published = true
	report.StoreID = metadata.StoreID
	report.ManifestGeneration = metadata.ManifestGeneration
	report.Files = uint64(len(metadata.Entries))
	return report, nil
}

func manifestPaths(manifest storecatalog.Manifest) []string {
	paths := make([]string, 0, 3+len(manifest.SealedDataSegments)+len(manifest.SealedMapSegments))
	paths = append(paths, fmt.Sprintf("MANIFEST-v2-%d", manifest.Generation&1))
	for _, segment := range manifest.SealedDataSegments {
		paths = append(paths, filepath.Join("records", fmt.Sprintf("record-%010d.sealed", segment.SegmentID)))
	}
	paths = append(paths, filepath.Join("records", fmt.Sprintf("record-%010d.active", manifest.ActiveDataSegmentID)))
	for _, segment := range manifest.SealedMapSegments {
		paths = append(paths, filepath.Join("mapping-v2", fmt.Sprintf("map-%010d.sealed", segment.SegmentID)))
	}
	paths = append(paths, filepath.Join("mapping-v2", fmt.Sprintf("map-%010d.active", manifest.ActiveMapSegmentID)))
	sort.Strings(paths)
	return paths
}

func readMetadata(root string) (Metadata, error) {
	path := filepath.Join(root, MetadataName)
	info, err := os.Lstat(path)
	if err != nil {
		return Metadata{}, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() > maxMetadataSize {
		return Metadata{}, errInvalid
	}
	file, err := os.Open(path)
	if err != nil {
		return Metadata{}, err
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !opened.Mode().IsRegular() || !os.SameFile(info, opened) {
		return Metadata{}, errors.Join(errInvalid, err)
	}
	encoded, err := io.ReadAll(io.LimitReader(file, maxMetadataSize+1))
	if err != nil {
		return Metadata{}, err
	}
	if len(encoded) > maxMetadataSize {
		return Metadata{}, errInvalid
	}
	return DecodeMetadata(encoded)
}

func validateArtifact(ctx context.Context, root string, metadata Metadata) error {
	if _, err := os.Lstat(filepath.Join(root, IncompleteName)); err == nil {
		return errInvalid
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := requireNames(root, map[string]bool{MetadataName: false, PayloadDirectory: true}); err != nil {
		return err
	}
	payload := filepath.Join(root, PayloadDirectory)
	expected := make(map[string]Entry, len(metadata.Entries))
	for _, entry := range metadata.Entries {
		expected[entry.Path] = entry
	}
	manifestCount := 0
	for path := range expected {
		parts := strings.Split(path, string(filepath.Separator))
		switch {
		case len(parts) == 1 && strings.HasPrefix(parts[0], "MANIFEST-v2-"):
			manifestCount++
		case len(parts) == 2 && (parts[0] == "records" || parts[0] == "mapping-v2"):
		default:
			return errInvalid
		}
	}
	if manifestCount != 1 {
		return errInvalid
	}
	if err := requireArtifactFiles(payload, expected); err != nil {
		return err
	}
	manifest, err := storecatalog.LoadStrict(payload)
	if err != nil {
		return err
	}
	if manifest.StoreUUID != metadata.StoreID || manifest.RecordLogID != metadata.RecordLogID || manifest.Generation != metadata.ManifestGeneration {
		return errInvalid
	}
	want := manifestPaths(manifest)
	if len(want) != len(metadata.Entries) {
		return errInvalid
	}
	for index, path := range want {
		if metadata.Entries[index].Path != path {
			return errInvalid
		}
	}
	for _, entry := range metadata.Entries {
		got, err := hashRegular(ctx, filepath.Join(payload, entry.Path), entry.Path)
		if err != nil {
			return err
		}
		if got.Size != entry.Size || got.SHA256 != entry.SHA256 {
			return errInvalid
		}
	}
	return nil
}

func requireArtifactFiles(payload string, expected map[string]Entry) error {
	entries, err := os.ReadDir(payload)
	if err != nil {
		return err
	}
	wantRoot := map[string]bool{"records": true, "mapping-v2": true}
	for path := range expected {
		if !strings.Contains(path, string(filepath.Separator)) {
			wantRoot[path] = false
		}
	}
	if err := checkEntries(entries, wantRoot); err != nil {
		return err
	}
	for _, directory := range []string{"records", "mapping-v2"} {
		want := make(map[string]bool)
		prefix := directory + string(filepath.Separator)
		for path := range expected {
			if strings.HasPrefix(path, prefix) {
				want[strings.TrimPrefix(path, prefix)] = false
			}
		}
		if err := requireNames(filepath.Join(payload, directory), want); err != nil {
			return err
		}
	}
	return nil
}

func requireNames(root string, want map[string]bool) error {
	entries, err := os.ReadDir(root)
	if err != nil {
		return err
	}
	return checkEntries(entries, want)
}

func checkEntries(entries []os.DirEntry, want map[string]bool) error {
	if len(entries) != len(want) {
		return errInvalid
	}
	for _, entry := range entries {
		wantDir, ok := want[entry.Name()]
		if !ok {
			return errInvalid
		}
		info, err := entry.Info()
		if err != nil || info.Mode()&os.ModeSymlink != 0 || info.IsDir() != wantDir || !wantDir && !info.Mode().IsRegular() {
			return errInvalid
		}
	}
	return nil
}

func createBackupLayout(staging string) error {
	if err := writeSync(filepath.Join(staging, IncompleteName), nil, 0o600); err != nil {
		return err
	}
	payload := filepath.Join(staging, PayloadDirectory)
	if err := os.Mkdir(payload, 0o700); err != nil {
		return err
	}
	if err := os.Mkdir(filepath.Join(payload, "records"), 0o700); err != nil {
		return err
	}
	if err := os.Mkdir(filepath.Join(payload, "mapping-v2"), 0o700); err != nil {
		return err
	}
	return syncDirectory(staging)
}

func createStoreLayout(staging string) error {
	if err := writeSync(filepath.Join(staging, RestoreIncompleteName), nil, 0o600); err != nil {
		return err
	}
	for _, directory := range []string{"records", "mapping-v2", "journal"} {
		if err := os.Mkdir(filepath.Join(staging, directory), 0o700); err != nil {
			return err
		}
	}
	return syncDirectory(staging)
}

func makeStaging(destination, operation string) (string, error) {
	parent := filepath.Dir(destination)
	staging, err := os.MkdirTemp(parent, "."+filepath.Base(destination)+"."+operation+"-")
	if err != nil {
		return "", err
	}
	if err := syncDirectory(parent); err != nil {
		return "", errors.Join(err, os.RemoveAll(staging))
	}
	return staging, nil
}

func copyAndHash(ctx context.Context, source, destination, relative string, hook FaultHook, beforeCreate, beforeWrite, beforeSync FaultPoint) (entry Entry, resultErr error) {
	info, err := os.Lstat(source)
	if err != nil {
		return entry, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return entry, errInvalid
	}
	input, err := os.Open(source)
	if err != nil {
		return entry, err
	}
	defer func() { resultErr = errors.Join(resultErr, input.Close()) }()
	opened, err := input.Stat()
	if err != nil || !opened.Mode().IsRegular() || !os.SameFile(info, opened) || opened.Size() < 0 {
		return entry, errors.Join(errInvalid, err)
	}
	if err := hit(hook, beforeCreate); err != nil {
		return entry, err
	}
	output, err := os.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return entry, err
	}
	fail := func(cause error) (Entry, error) { return Entry{}, errors.Join(cause, output.Close()) }
	hash := sha256.New()
	if err := hit(hook, beforeWrite); err != nil {
		return fail(err)
	}
	written, err := copyWithContext(ctx, io.MultiWriter(output, hash), input)
	if err != nil {
		return fail(err)
	}
	if written < 0 || uint64(written) != uint64(info.Size()) {
		return fail(errInvalid)
	}
	if err := hit(hook, beforeSync); err != nil {
		return fail(err)
	}
	if err := output.Sync(); err != nil {
		return fail(err)
	}
	if err := output.Close(); err != nil {
		return entry, err
	}
	entry = Entry{Path: relative, Size: uint64(written)}
	copy(entry.SHA256[:], hash.Sum(nil))
	return entry, nil
}

func hashRegular(ctx context.Context, path, relative string) (entry Entry, resultErr error) {
	info, err := os.Lstat(path)
	if err != nil {
		return entry, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() < 0 {
		return entry, errInvalid
	}
	file, err := os.Open(path)
	if err != nil {
		return entry, err
	}
	defer func() { resultErr = errors.Join(resultErr, file.Close()) }()
	opened, err := file.Stat()
	if err != nil || !opened.Mode().IsRegular() || !os.SameFile(info, opened) {
		return entry, errors.Join(errInvalid, err)
	}
	hash := sha256.New()
	written, err := copyWithContext(ctx, hash, file)
	if err != nil || written != info.Size() {
		return entry, errors.Join(errInvalid, err)
	}
	entry = Entry{Path: relative, Size: uint64(written)}
	copy(entry.SHA256[:], hash.Sum(nil))
	return entry, nil
}

func copyWithContext(ctx context.Context, destination io.Writer, source io.Reader) (int64, error) {
	buffer := make([]byte, 1<<20)
	var total int64
	for {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		read, readErr := source.Read(buffer)
		if read != 0 {
			written, writeErr := destination.Write(buffer[:read])
			total += int64(written)
			if writeErr != nil {
				return total, writeErr
			}
			if written != read {
				return total, io.ErrShortWrite
			}
		}
		if errors.Is(readErr, io.EOF) {
			return total, nil
		}
		if readErr != nil {
			return total, readErr
		}
	}
}

func writeSync(path string, value []byte, mode os.FileMode) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		return err
	}
	if _, err := io.Copy(file, bytes.NewReader(value)); err != nil {
		return errors.Join(err, file.Close())
	}
	return errors.Join(file.Sync(), file.Close())
}

func createLock(root string) error {
	if err := writeSync(filepath.Join(root, filelock.FileName), nil, 0o600); err != nil {
		return err
	}
	return syncDirectory(root)
}

func removeAndSync(path, parent string) error {
	if err := os.Remove(path); err != nil {
		return err
	}
	return syncDirectory(parent)
}

func syncBackupTree(root string) error {
	for _, directory := range []string{
		filepath.Join(root, PayloadDirectory, "records"),
		filepath.Join(root, PayloadDirectory, "mapping-v2"),
		filepath.Join(root, PayloadDirectory), root,
	} {
		if err := syncDirectory(directory); err != nil {
			return err
		}
	}
	return nil
}

func syncStoreTree(root string) error {
	for _, directory := range []string{filepath.Join(root, "records"), filepath.Join(root, "mapping-v2"), filepath.Join(root, "journal"), root} {
		if err := syncDirectory(directory); err != nil {
			return err
		}
	}
	return nil
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}

func publish(staging, destination string, hook FaultHook, before, after FaultPoint) error {
	if err := requireAbsent(destination); err != nil {
		return err
	}
	if err := hit(hook, before); err != nil {
		return err
	}
	if err := renameNoReplace(staging, destination); err != nil {
		return err
	}
	if err := hit(hook, after); err != nil {
		return err
	}
	return syncDirectory(filepath.Dir(destination))
}

func hit(hook FaultHook, point FaultPoint) error {
	if hook == nil {
		return nil
	}
	return hook(point)
}

func requireAbsent(path string) error {
	if _, err := os.Lstat(path); err == nil {
		return base.ErrAlreadyExists
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func requireRealDirectory(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return base.ErrInvalidConfig
	}
	return nil
}

func validateDistinctPaths(source, destination string) error {
	if source == "" || destination == "" {
		return base.ErrInvalidConfig
	}
	source, err := filepath.Abs(source)
	if err != nil {
		return errors.Join(base.ErrInvalidConfig, err)
	}
	destination, err = filepath.Abs(destination)
	if err != nil {
		return errors.Join(base.ErrInvalidConfig, err)
	}
	canonicalSource, sourceErr := filepath.EvalSymlinks(source)
	canonicalParent, parentErr := filepath.EvalSymlinks(filepath.Dir(destination))
	if sourceErr == nil && parentErr == nil {
		source = canonicalSource
		destination = filepath.Join(canonicalParent, filepath.Base(destination))
	}
	if source == destination || within(source, destination) || within(destination, source) {
		return base.ErrInvalidConfig
	}
	return nil
}

func within(parent, child string) bool {
	relative, err := filepath.Rel(parent, child)
	return err == nil && relative != "." && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

func classify(err error) error {
	switch {
	case errors.Is(err, errUnsupported), errors.Is(err, storecatalog.ErrUnsupported):
		return errors.Join(base.ErrUnsupported, err)
	case errors.Is(err, errInvalid), errors.Is(err, storecatalog.ErrCorrupt), errors.Is(err, storecatalog.ErrInvalid), errors.Is(err, storecatalog.ErrRecoveryRequired):
		return errors.Join(base.ErrCorrupt, err)
	default:
		return err
	}
}
