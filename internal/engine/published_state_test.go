package engine

import (
	"bytes"
	"context"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/akzj/ridstore/internal/storecatalog"
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
	initial := store.core.catalog.Snapshot().ActiveDataSegmentID
	for store.core.catalog.Snapshot().ActiveDataSegmentID == initial {
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

func TestPublishedStateSnapshotDoesNotWaitForPublisherLock(t *testing.T) {
	root := filepath.Join(t.TempDir(), "store")
	store, err := Create(context.Background(), root, testCreateConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	publisher := store.core.publisher
	publisher.mu.Lock()
	defer publisher.mu.Unlock()

	done := make(chan storecatalog.Manifest, 1)
	go func() { done <- store.catalogSnapshot() }()
	select {
	case snapshot := <-done:
		if snapshot.Generation == 0 || snapshot.Generation != publisher.published.Load().Generation {
			t.Fatalf("snapshot generation=%d", snapshot.Generation)
		}
	case <-time.After(time.Second):
		t.Fatal("PublishedState snapshot waited for publisher lock")
	}
}

func assertPublishedStateMatchesCatalog(t *testing.T, store *Store) {
	t.Helper()
	if store.core.publisher == nil {
		t.Fatal("PublishCoordinator was not initialized")
	}
	state := store.PublishedState()
	if state == nil {
		t.Fatal("PublishedState was not initialized")
	}
	manifest := store.core.catalog.Snapshot()
	if state.Generation != manifest.Generation || state.MappingRoot != manifest.MappingRoot || state.CoveredCommit != manifest.CoveredCommitSeq ||
		!reflect.DeepEqual(state.Manifest, manifest) {
		t.Fatalf("PublishedState does not match Catalog: state=%+v manifest=%+v", state, manifest)
	}
}
