package backup

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/filelock"
	storeformat "github.com/akzj/ridstore/internal/format"
	"github.com/akzj/ridstore/internal/manifest"
	"github.com/akzj/ridstore/internal/segment"
	"github.com/akzj/ridstore/internal/verify"
)

const (
	metadataName    = "BACKUP.json"
	incompleteName  = "INCOMPLETE"
	payloadDirName  = "files"
	artifactFormat  = "ridstore-backup"
	artifactVersion = uint32(1)
	maxMetadataSize = int64(512 << 20)
)

type FileEntry struct {
	Path   string `json:"path"`
	Size   uint64 `json:"size"`
	SHA256 string `json:"sha256"`
}

type Metadata struct {
	Format             string      `json:"format"`
	Version            uint32      `json:"version"`
	StoreUUID          string      `json:"store_uuid"`
	ManifestGeneration uint64      `json:"manifest_generation"`
	CreatedUnixNano    int64       `json:"created_unix_nano"`
	Files              []FileEntry `json:"files"`
}

type CreateReport struct {
	Destination        string `json:"destination"`
	StoreUUID          string `json:"store_uuid"`
	ManifestGeneration uint64 `json:"manifest_generation"`
	Files              uint64 `json:"files"`
	Bytes              uint64 `json:"bytes"`
}

// Create writes a complete offline backup artifact. Source verification and
// all file copies occur while the same exclusive Store lease is held.
func Create(ctx context.Context, source, destination string) (report CreateReport, resultErr error) {
	if err := ctx.Err(); err != nil {
		return report, err
	}
	sourceAbs, err := filepath.Abs(source)
	if err != nil {
		return report, err
	}
	destinationAbs, err := filepath.Abs(destination)
	if err != nil {
		return report, err
	}
	if sourceAbs == destinationAbs || isWithin(sourceAbs, destinationAbs) {
		return report, fmt.Errorf("backup destination must be outside source Store: %w", base.ErrInvalidConfig)
	}
	lease, err := filelock.AcquireExisting(sourceAbs)
	if err != nil {
		return report, err
	}
	defer func() { resultErr = errors.Join(resultErr, lease.Close()) }()
	verified, err := verify.RunUnderLease(ctx, sourceAbs)
	if err != nil || !verified.Clean {
		if err == nil {
			err = base.ErrCorrupt
		}
		return report, err
	}
	current, err := manifest.LoadCurrent(sourceAbs)
	if err != nil {
		return report, err
	}
	paths := storePaths(current)
	if err := createArtifactRoot(destinationAbs); err != nil {
		return report, err
	}
	payloadRoot := filepath.Join(destinationAbs, payloadDirName)
	for _, name := range []string{"manifests", "data", "mapping"} {
		if err := os.Mkdir(filepath.Join(payloadRoot, name), 0o700); err != nil {
			return report, err
		}
	}
	metadata := Metadata{
		Format: artifactFormat, Version: artifactVersion,
		StoreUUID:          hex.EncodeToString(current.StoreUUID[:]),
		ManifestGeneration: current.Generation, CreatedUnixNano: time.Now().UnixNano(),
		Files: make([]FileEntry, 0, len(paths)),
	}
	for _, relative := range paths {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		size, digest, copyErr := copyRegularFile(ctx, filepath.Join(sourceAbs, relative), filepath.Join(payloadRoot, relative))
		if copyErr != nil {
			return report, copyErr
		}
		metadata.Files = append(metadata.Files, FileEntry{Path: relative, Size: size, SHA256: digest})
		if err := add(&report.Bytes, size); err != nil {
			return report, err
		}
	}
	if err := verifyCopiedPayload(ctx, payloadRoot); err != nil {
		return report, err
	}
	encoded, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return report, err
	}
	encoded = append(encoded, '\n')
	if err := writeNewSynced(filepath.Join(destinationAbs, metadataName), encoded, 0o600); err != nil {
		return report, err
	}
	for _, dir := range []string{
		filepath.Join(payloadRoot, "manifests"), filepath.Join(payloadRoot, "data"), filepath.Join(payloadRoot, "mapping"), payloadRoot, destinationAbs,
	} {
		if err := syncDirectory(dir); err != nil {
			return report, err
		}
	}
	if _, err := validateArtifact(ctx, destinationAbs, true); err != nil {
		return report, err
	}
	if err := os.Remove(filepath.Join(destinationAbs, incompleteName)); err != nil {
		return report, err
	}
	if err := syncDirectory(destinationAbs); err != nil {
		return report, err
	}
	report.Destination = destinationAbs
	report.StoreUUID = metadata.StoreUUID
	report.ManifestGeneration = metadata.ManifestGeneration
	report.Files = uint64(len(metadata.Files))
	return report, nil
}

// Inspect validates a completed artifact, including its exact payload file set
// and every SHA-256 digest.
func Inspect(ctx context.Context, root string) (Metadata, error) {
	return validateArtifact(ctx, root, false)
}

func validateArtifact(ctx context.Context, root string, allowIncomplete bool) (Metadata, error) {
	var metadata Metadata
	if err := ctx.Err(); err != nil {
		return metadata, err
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return metadata, err
	}
	info, err := os.Lstat(abs)
	if err != nil {
		return metadata, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return metadata, fmt.Errorf("backup artifact is not a real directory: %w", base.ErrInvalidConfig)
	}
	if _, err := os.Lstat(filepath.Join(abs, incompleteName)); err == nil {
		if !allowIncomplete {
			return metadata, fmt.Errorf("backup artifact is incomplete: %w", base.ErrRecoveryRequired)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return metadata, err
	} else if allowIncomplete {
		return metadata, fmt.Errorf("backup INCOMPLETE marker missing before publication: %w", base.ErrCorrupt)
	}
	if err := validateArtifactRoot(abs, allowIncomplete); err != nil {
		return metadata, err
	}
	data, err := readRegularFile(filepath.Join(abs, metadataName), maxMetadataSize)
	if err != nil {
		return metadata, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&metadata); err != nil {
		return Metadata{}, fmt.Errorf("decode backup metadata: %w", base.ErrCorrupt)
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return Metadata{}, err
	}
	if err := validateMetadata(metadata); err != nil {
		return Metadata{}, err
	}
	payloadRoot := filepath.Join(abs, payloadDirName)
	seen, err := payloadFiles(payloadRoot)
	if err != nil {
		return Metadata{}, err
	}
	if len(seen) != len(metadata.Files) {
		return Metadata{}, fmt.Errorf("backup payload file count mismatch: %w", base.ErrCorrupt)
	}
	for _, entry := range metadata.Files {
		if err := ctx.Err(); err != nil {
			return Metadata{}, err
		}
		if _, ok := seen[entry.Path]; !ok {
			return Metadata{}, fmt.Errorf("backup payload missing %s: %w", entry.Path, base.ErrCorrupt)
		}
		size, digest, err := hashRegularFile(ctx, filepath.Join(payloadRoot, entry.Path))
		if err != nil {
			return Metadata{}, err
		}
		if size != entry.Size || digest != entry.SHA256 {
			return Metadata{}, fmt.Errorf("backup payload digest mismatch %s: %w", entry.Path, base.ErrCorrupt)
		}
	}
	current, err := manifest.LoadCurrent(payloadRoot)
	if err != nil {
		return Metadata{}, err
	}
	if hex.EncodeToString(current.StoreUUID[:]) != metadata.StoreUUID || current.Generation != metadata.ManifestGeneration {
		return Metadata{}, fmt.Errorf("backup metadata identity mismatch: %w", base.ErrCorrupt)
	}
	want := storePaths(current)
	if len(want) != len(metadata.Files) {
		return Metadata{}, fmt.Errorf("backup Manifest file count mismatch: %w", base.ErrCorrupt)
	}
	for i := range want {
		if metadata.Files[i].Path != want[i] {
			return Metadata{}, fmt.Errorf("backup Manifest file set mismatch: %w", base.ErrCorrupt)
		}
	}
	return metadata, nil
}

func validateMetadata(metadata Metadata) error {
	if metadata.Format != artifactFormat || metadata.Version != artifactVersion {
		return fmt.Errorf("backup metadata header: %w", base.ErrUnsupported)
	}
	if metadata.ManifestGeneration == 0 || metadata.CreatedUnixNano <= 0 || len(metadata.Files) == 0 {
		return fmt.Errorf("backup metadata identity: %w", base.ErrCorrupt)
	}
	uuid, err := hex.DecodeString(metadata.StoreUUID)
	if err != nil || len(uuid) != len(base.StoreUUID{}) {
		return fmt.Errorf("backup metadata StoreUUID: %w", base.ErrCorrupt)
	}
	var parsed base.StoreUUID
	copy(parsed[:], uuid)
	if parsed == (base.StoreUUID{}) {
		return fmt.Errorf("backup metadata zero StoreUUID: %w", base.ErrCorrupt)
	}
	previous := ""
	for _, entry := range metadata.Files {
		if entry.Path == "" || entry.Path <= previous || !validRelativePath(entry.Path) || entry.Size == 0 {
			return fmt.Errorf("backup metadata file order/path/size: %w", base.ErrCorrupt)
		}
		digest, err := hex.DecodeString(entry.SHA256)
		if err != nil || len(digest) != sha256.Size || hex.EncodeToString(digest) != entry.SHA256 {
			return fmt.Errorf("backup metadata digest: %w", base.ErrCorrupt)
		}
		previous = entry.Path
	}
	return nil
}

func storePaths(current storeformat.Manifest) []string {
	paths := []string{
		manifest.CurrentFileName,
		filepath.Join(manifest.ManifestDirName, manifest.ManifestFileName(current.Generation)),
		filepath.Join("data", segment.ActiveDataFileName(current.ActiveDataSegmentID)),
		filepath.Join("mapping", fmt.Sprintf("MAP-%08d.active", current.ActiveMapSegmentID)),
	}
	for _, summary := range current.SealedDataSegments {
		paths = append(paths, filepath.Join("data", segment.SealedDataFileName(base.DataSegmentID(summary.FileID))))
	}
	for _, summary := range current.SealedMappingSegments {
		paths = append(paths, filepath.Join("mapping", fmt.Sprintf("MAP-%08d.seg", summary.FileID)))
	}
	sort.Strings(paths)
	return paths
}

func createArtifactRoot(root string) error {
	parent := filepath.Dir(root)
	info, err := os.Lstat(parent)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("backup parent is not a real directory: %w", base.ErrInvalidConfig)
	}
	if err := os.Mkdir(root, 0o700); err != nil {
		return err
	}
	if err := writeNewSynced(filepath.Join(root, incompleteName), []byte("ridstore backup incomplete\n"), 0o600); err != nil {
		return errors.Join(err, os.Remove(root))
	}
	if err := syncDirectory(root); err != nil {
		return err
	}
	if err := syncDirectory(parent); err != nil {
		return err
	}
	if err := os.Mkdir(filepath.Join(root, payloadDirName), 0o700); err != nil {
		return err
	}
	return nil
}

func verifyCopiedPayload(ctx context.Context, payloadRoot string) (resultErr error) {
	trash := filepath.Join(payloadRoot, "trash")
	lock := filepath.Join(payloadRoot, filelock.FileName)
	if err := os.Mkdir(trash, 0o700); err != nil {
		return err
	}
	defer func() {
		resultErr = errors.Join(resultErr, removeIfExists(lock), removeIfExists(trash))
	}()
	if err := writeNewSynced(lock, []byte{}, 0o600); err != nil {
		return err
	}
	report, err := verify.Run(ctx, payloadRoot)
	if err != nil {
		return err
	}
	if !report.Clean {
		return base.ErrCorrupt
	}
	return nil
}

func removeIfExists(path string) error {
	err := os.Remove(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func copyRegularFile(ctx context.Context, source, destination string) (size uint64, digest string, resultErr error) {
	fd, err := syscall.Open(source, syscall.O_RDONLY|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return 0, "", err
	}
	in := os.NewFile(uintptr(fd), source)
	if in == nil {
		_ = syscall.Close(fd)
		return 0, "", fmt.Errorf("open backup source %s", source)
	}
	defer func() { resultErr = errors.Join(resultErr, in.Close()) }()
	info, err := in.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 {
		if err == nil {
			err = base.ErrCorrupt
		}
		return 0, "", fmt.Errorf("backup source is not a non-empty regular file %s: %w", source, err)
	}
	out, err := os.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return 0, "", err
	}
	defer func() { resultErr = errors.Join(resultErr, out.Close()) }()
	hash := sha256.New()
	written, err := copyContext(ctx, io.MultiWriter(out, hash), in)
	if err != nil {
		return 0, "", err
	}
	if written != info.Size() {
		return 0, "", fmt.Errorf("backup source size changed during copy: %w", base.ErrCorrupt)
	}
	if err := out.Sync(); err != nil {
		return 0, "", err
	}
	return uint64(written), hex.EncodeToString(hash.Sum(nil)), nil
}

func hashRegularFile(ctx context.Context, path string) (size uint64, digest string, resultErr error) {
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return 0, "", err
	}
	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		_ = syscall.Close(fd)
		return 0, "", fmt.Errorf("open backup payload %s", path)
	}
	defer func() { resultErr = errors.Join(resultErr, file.Close()) }()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 {
		if err == nil {
			err = base.ErrCorrupt
		}
		return 0, "", err
	}
	hash := sha256.New()
	written, err := copyContext(ctx, hash, file)
	if err != nil {
		return 0, "", err
	}
	if written != info.Size() {
		return 0, "", fmt.Errorf("backup payload size changed while hashing: %w", base.ErrCorrupt)
	}
	return uint64(written), hex.EncodeToString(hash.Sum(nil)), nil
}

func copyContext(ctx context.Context, dst io.Writer, src io.Reader) (int64, error) {
	buffer := make([]byte, 1<<20)
	var total int64
	for {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		n, readErr := src.Read(buffer)
		if n != 0 {
			written, writeErr := dst.Write(buffer[:n])
			total += int64(written)
			if writeErr != nil {
				return total, writeErr
			}
			if written != n {
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

func payloadFiles(root string) (map[string]struct{}, error) {
	files := make(map[string]struct{})
	wantDirectories := map[string]struct{}{"manifests": {}, "data": {}, "mapping": {}}
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == root {
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("backup payload symlink %s: %w", path, base.ErrCorrupt)
		}
		if entry.IsDir() {
			relative, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			if _, ok := wantDirectories[relative]; !ok {
				return fmt.Errorf("unexpected backup payload directory %s: %w", relative, base.ErrCorrupt)
			}
			delete(wantDirectories, relative)
			return nil
		}
		info, err := entry.Info()
		if err != nil || !info.Mode().IsRegular() {
			return fmt.Errorf("backup payload non-regular file %s: %w", path, base.ErrCorrupt)
		}
		relative, err := filepath.Rel(root, path)
		if err != nil || !validRelativePath(relative) {
			return fmt.Errorf("backup payload invalid path %s: %w", path, base.ErrCorrupt)
		}
		files[relative] = struct{}{}
		return nil
	})
	if err == nil && len(wantDirectories) != 0 {
		return nil, fmt.Errorf("backup payload directories missing: %w", base.ErrCorrupt)
	}
	return files, err
}

func validateArtifactRoot(root string, allowIncomplete bool) error {
	want := map[string]bool{metadataName: false, payloadDirName: true}
	if allowIncomplete {
		want[incompleteName] = false
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		wantDirectory, ok := want[entry.Name()]
		if !ok {
			return fmt.Errorf("unexpected backup artifact entry %s: %w", entry.Name(), base.ErrCorrupt)
		}
		info, err := entry.Info()
		if err != nil || info.Mode()&os.ModeSymlink != 0 || info.IsDir() != wantDirectory || (!wantDirectory && !info.Mode().IsRegular()) {
			return fmt.Errorf("invalid backup artifact entry %s: %w", entry.Name(), base.ErrCorrupt)
		}
		delete(want, entry.Name())
	}
	if len(want) != 0 {
		return fmt.Errorf("backup artifact entries missing: %w", base.ErrCorrupt)
	}
	return nil
}

func validRelativePath(path string) bool {
	return path != "" && !filepath.IsAbs(path) && filepath.Clean(path) == path && path != "." && path != ".." && !strings.HasPrefix(path, ".."+string(filepath.Separator))
}

func isWithin(parent, child string) bool {
	relative, err := filepath.Rel(parent, child)
	return err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

func readRegularFile(path string, maxSize int64) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() <= 0 || info.Size() > maxSize {
		return nil, fmt.Errorf("invalid backup metadata file: %w", base.ErrCorrupt)
	}
	return os.ReadFile(path)
}

func writeNewSynced(path string, data []byte, mode os.FileMode) (resultErr error) {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, file.Close()) }()
	writer := bufio.NewWriter(file)
	if _, err := writer.Write(data); err != nil {
		return err
	}
	if err := writer.Flush(); err != nil {
		return err
	}
	return file.Sync()
}

func syncDirectory(path string) (resultErr error) {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, directory.Close()) }()
	return directory.Sync()
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return fmt.Errorf("backup metadata trailing content: %w", base.ErrCorrupt)
	}
	return nil
}

func add(dst *uint64, value uint64) error {
	next, err := base.AddUint64(*dst, value)
	if err != nil {
		return err
	}
	*dst = next
	return nil
}
