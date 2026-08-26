package ridstore

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
)

func TestPublicConcurrentCASCheckpointAndReopen(t *testing.T) {
	const (
		contenders = 12
		rounds     = 4
	)
	ctx := context.Background()
	config := testCreateConfig(filepath.Join(t.TempDir(), "store"))
	config.HardLimits.MaxOpenBatches = contenders + 4
	config.Runtime.StatusRetention = 256
	store, err := Create(ctx, config)
	if err != nil {
		t.Fatal(err)
	}

	initial, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	id, err := initial.Create(ctx, []byte("initial"))
	if err != nil {
		t.Fatal(err)
	}
	lastCommit, err := initial.Commit(ctx)
	if err != nil {
		t.Fatal(err)
	}
	wantValue := "initial"

	type outcome struct {
		batchID BatchID
		value   string
		commit  CommitResult
		err     error
	}
	for round := 0; round < rounds; round++ {
		observed, getErr := store.Get(ctx, id)
		if getErr != nil {
			t.Fatal(getErr)
		}
		if string(observed.Value) != wantValue {
			t.Fatalf("round %d observed=%q want=%q", round, observed.Value, wantValue)
		}

		batches := make([]*Batch, contenders)
		values := make([]string, contenders)
		for i := range batches {
			batch, beginErr := store.Begin(ctx)
			if beginErr != nil {
				t.Fatal(beginErr)
			}
			value := fmt.Sprintf("round-%d-contender-%d", round, i)
			if putErr := batch.CompareAndPut(ctx, id, observed.Token, []byte(value)); putErr != nil {
				t.Fatal(putErr)
			}
			batches[i], values[i] = batch, value
		}

		start := make(chan struct{})
		outcomes := make(chan outcome, contenders)
		checkpoint := make(chan error, 1)
		var ready sync.WaitGroup
		ready.Add(contenders + 1)
		for i, batch := range batches {
			go func(batch *Batch, value string) {
				ready.Done()
				<-start
				commit, commitErr := batch.Commit(ctx)
				outcomes <- outcome{batchID: batch.ID(), value: value, commit: commit, err: commitErr}
			}(batch, values[i])
		}
		go func() {
			ready.Done()
			<-start
			checkpoint <- store.Checkpoint(ctx)
		}()
		ready.Wait()
		close(start)

		winnerCount := 0
		for range contenders {
			result := <-outcomes
			status, statusErr := store.Status(ctx, result.batchID)
			if statusErr != nil {
				t.Fatal(statusErr)
			}
			switch {
			case result.err == nil:
				winnerCount++
				wantValue = result.value
				if result.commit.CommitSeq != lastCommit.CommitSeq+1 {
					t.Fatalf("round %d commit seq=%d want=%d", round, result.commit.CommitSeq, lastCommit.CommitSeq+1)
				}
				lastCommit = result.commit
				if status.State != BatchStateCommitted || status.CommitSeq != result.commit.CommitSeq {
					t.Fatalf("round %d winner status=%+v", round, status)
				}
			case errors.Is(result.err, ErrConflict):
				if status.State != BatchStateAborted || status.CommitSeq != 0 {
					t.Fatalf("round %d loser status=%+v", round, status)
				}
			default:
				t.Fatalf("round %d commit err=%v", round, result.err)
			}
		}
		if winnerCount != 1 {
			t.Fatalf("round %d winners=%d", round, winnerCount)
		}
		if checkpointErr := <-checkpoint; checkpointErr != nil {
			t.Fatalf("round %d checkpoint: %v", round, checkpointErr)
		}
	}

	if err := store.Checkpoint(ctx); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(ctx, OpenConfig{Dir: config.Dir, Runtime: config.Runtime})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	recovered, err := reopened.Get(ctx, id)
	if err != nil || string(recovered.Value) != wantValue {
		t.Fatalf("recovered=%q want=%q err=%v", recovered.Value, wantValue, err)
	}
}
