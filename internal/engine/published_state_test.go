package engine

import (
	"bytes"
	"context"
	"path/filepath"
	"reflect"
	"testing"
)

func TestCreateAndOpenInitializePublishedState(t *testing.T) {
	root := filepath.Join(t.TempDir(), "store")
	config := testCreateConfig()
	store, err := Create(context.Background(), root, config)
	if err != nil {
		t.Fatal(err)
	}
	assertPublishedStateMatchesCatalog(t, store)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(context.Background(), root, config.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	assertPublishedStateMatchesCatalog(t, reopened)
}

func TestRecordLogRotationPublishesState(t *testing.T) {
	store := newRelocationStore(t)
	initial := store.catalog.Snapshot().ActiveDataSegmentID
	for store.catalog.Snapshot().ActiveDataSegmentID == initial {
		batch, err := store.Begin(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := batch.Create(context.Background(), bytes.Repeat([]byte{'x'}, 512)); err != nil {
			t.Fatal(err)
		}
		if _, err := batch.Commit(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	assertPublishedStateMatchesCatalog(t, store)
}

func assertPublishedStateMatchesCatalog(t *testing.T, store *Store) {
	t.Helper()
	if store.publisher == nil {
		t.Fatal("PublishCoordinator was not initialized")
	}
	state := store.PublishedState()
	if state == nil {
		t.Fatal("PublishedState was not initialized")
	}
	manifest := store.catalog.Snapshot()
	if state.Generation != manifest.Generation || state.MappingRoot != manifest.MappingRoot || state.CoveredCommit != manifest.CoveredCommitSeq ||
		!reflect.DeepEqual(state.Manifest, manifest) {
		t.Fatalf("PublishedState does not match Catalog: state=%+v manifest=%+v", state, manifest)
	}
}
