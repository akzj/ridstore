package mapstore

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/akzj/ridstore/internal/model"
)

// PromoteGeneration moves every file in an isolated generation into the
// canonical Mapping directory. It is idempotent across partial rename and
// directory-sync failures; Catalog publication must happen only after success.
func PromoteGeneration(root, stagingRoot string, generation Generation, hook FaultHook) error {
	if root == "" || stagingRoot == "" || root == stagingRoot || !validGeneration(generation) {
		return ErrInvalid
	}
	canonical, err := requireDirectory(filepath.Join(root, mappingDirectory))
	if err != nil {
		return err
	}
	staging, err := requireDirectory(filepath.Join(stagingRoot, mappingDirectory))
	if err != nil {
		return err
	}
	for _, name := range generationNames(generation) {
		source, destination := filepath.Join(staging, name), filepath.Join(canonical, name)
		sourceExists, err := regularExists(source)
		if err != nil {
			return err
		}
		destinationExists, err := regularExists(destination)
		if err != nil {
			return err
		}
		if sourceExists && destinationExists || !sourceExists && !destinationExists {
			return ErrCorrupt
		}
		if sourceExists {
			if err := hitFault(hook, FaultBeforeGCPromoteRename); err != nil {
				return err
			}
			if err := os.Rename(source, destination); err != nil {
				return err
			}
		}
	}
	if err := hitFault(hook, FaultBeforeGCPromoteSync); err != nil {
		return err
	}
	return errors.Join(syncDirectory(staging), syncDirectory(canonical))
}

// RollbackGeneration removes an unpublished generation from both staging and
// canonical locations. Segment IDs are never reused, so the marker-provided
// generation identifies the only files this operation may remove.
func RollbackGeneration(root, stagingRoot string, generation Generation, hook FaultHook) error {
	if root == "" || stagingRoot == "" || !validGeneration(generation) {
		return ErrInvalid
	}
	canonical := filepath.Join(root, mappingDirectory)
	staging := filepath.Join(stagingRoot, mappingDirectory)
	for _, dir := range []string{canonical, staging} {
		for _, name := range generationNames(generation) {
			if _, err := removeGenerationFile(filepath.Join(dir, name), hook, FaultBeforeGCRollbackRemove); err != nil {
				return err
			}
		}
		if exists, err := directoryExists(dir); err != nil {
			return err
		} else if exists {
			if err := hitFault(hook, FaultBeforeGCRollbackSync); err != nil {
				return err
			}
			if err := syncDirectory(dir); err != nil {
				return err
			}
		}
	}
	return RemoveGenerationStaging(root, stagingRoot)
}

// RetireGeneration durably moves an old published file set through trash and
// unlinks it. The new generation must already be verified and authoritative.
func RetireGeneration(root string, generation Generation, catalogGeneration uint64, hook FaultHook) error {
	if root == "" || catalogGeneration == 0 || !validGeneration(generation) {
		return ErrInvalid
	}
	canonical, err := requireDirectory(filepath.Join(root, mappingDirectory))
	if err != nil {
		return err
	}
	trash, err := ensureGenerationTrash(root)
	if err != nil {
		return err
	}
	for _, name := range generationNames(generation) {
		source := filepath.Join(canonical, name)
		destination := filepath.Join(trash, fmt.Sprintf("%s.g%d.trash", name, catalogGeneration))
		sourceExists, err := regularExists(source)
		if err != nil {
			return err
		}
		destinationExists, err := regularExists(destination)
		if err != nil {
			return err
		}
		if sourceExists && destinationExists {
			return ErrCorrupt
		}
		if sourceExists {
			if err := hitFault(hook, FaultBeforeGCRetireRename); err != nil {
				return err
			}
			if err := os.Rename(source, destination); err != nil {
				return err
			}
		}
	}
	if err := hitFault(hook, FaultBeforeGCRetireSync); err != nil {
		return err
	}
	if err := errors.Join(syncDirectory(canonical), syncDirectory(trash)); err != nil {
		return err
	}
	for _, name := range generationNames(generation) {
		path := filepath.Join(trash, fmt.Sprintf("%s.g%d.trash", name, catalogGeneration))
		if _, err := removeGenerationFile(path, hook, FaultBeforeGCTrashRemove); err != nil {
			return err
		}
	}
	if err := hitFault(hook, FaultBeforeGCTrashSync); err != nil {
		return err
	}
	return syncDirectory(trash)
}

// RemoveGenerationStaging removes only an empty, known staging hierarchy.
func RemoveGenerationStaging(root, stagingRoot string) error {
	if root == "" || stagingRoot == "" || filepath.Dir(stagingRoot) != root {
		return ErrInvalid
	}
	mapping := filepath.Join(stagingRoot, mappingDirectory)
	if err := removeEmptyDirectory(mapping); err != nil {
		return err
	}
	if err := removeEmptyDirectory(stagingRoot); err != nil {
		return err
	}
	return syncDirectory(root)
}

func validGeneration(g Generation) bool {
	if g.ActiveSegment == 0 || g.NextSegment == 0 || g.NextSegment != g.ActiveSegment+1 || g.Root != 0 && !g.Root.Valid() {
		return false
	}
	var previous uint32
	rootFound := g.Root == 0 || g.Root.SegmentID() == g.ActiveSegment
	for index, ref := range g.SealedSegments {
		if ref.SegmentID == 0 || ref.SegmentID >= g.ActiveSegment || ref.ValidEnd < SegmentHeaderSize || ref.ValidEnd%Alignment != 0 ||
			index != 0 && uint32(ref.SegmentID) != previous+1 {
			return false
		}
		if g.Root != 0 && g.Root.SegmentID() == ref.SegmentID && g.Root.Offset() < ref.ValidEnd {
			rootFound = true
		}
		previous = uint32(ref.SegmentID)
	}
	if len(g.SealedSegments) != 0 && model.MapSegmentID(previous+1) != g.ActiveSegment {
		return false
	}
	return rootFound
}

func generationNames(g Generation) []string {
	names := make([]string, 0, len(g.SealedSegments)+1)
	for _, ref := range g.SealedSegments {
		names = append(names, sealedName(ref.SegmentID))
	}
	names = append(names, activeName(g.ActiveSegment))
	sort.Strings(names)
	return names
}

func regularExists(path string) (bool, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return false, ErrCorrupt
	}
	return true, nil
}

func directoryExists(path string) (bool, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return false, ErrCorrupt
	}
	return true, nil
}

func requireDirectory(path string) (string, error) {
	exists, err := directoryExists(path)
	if err != nil {
		return "", err
	}
	if !exists {
		return "", os.ErrNotExist
	}
	return path, nil
}

func removeGenerationFile(path string, hook FaultHook, point FaultPoint) (bool, error) {
	exists, err := regularExists(path)
	if err != nil || !exists {
		return false, err
	}
	if err := hitFault(hook, point); err != nil {
		return false, err
	}
	return true, os.Remove(path)
}

func removeEmptyDirectory(path string) error {
	exists, err := directoryExists(path)
	if err != nil || !exists {
		return err
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return err
	}
	if len(entries) != 0 {
		return ErrCorrupt
	}
	return os.Remove(path)
}

func ensureGenerationTrash(root string) (string, error) {
	path := filepath.Join(root, "trash")
	if err := os.Mkdir(path, 0o700); err == nil {
		return path, syncDirectory(root)
	} else if !errors.Is(err, os.ErrExist) {
		return "", err
	}
	return requireDirectory(path)
}
