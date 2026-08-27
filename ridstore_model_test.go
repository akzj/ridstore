package ridstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"path/filepath"
	"sync"
	"testing"
)

// TestPublicRandomizedConcurrentModel composes the public mutation and
// maintenance state machines. Individual protocol tests cover deeper fault
// boundaries; this test checks that valid concurrent histories preserve the
// same logical records across checkpoints, both GC paths, and reopen.
func TestPublicRandomizedConcurrentModel(t *testing.T) {
	for _, seed := range []int64{1, 17, 99} {
		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) {
			runRandomizedConcurrentModel(t, seed)
		})
	}
}

type modelRecord struct {
	value   []byte
	present bool
}

type modelMutation struct {
	value   []byte
	present bool
}

type modelOutcome struct {
	batchID BatchID
	change  modelMutation
	abort   bool
	err     error
}

func runRandomizedConcurrentModel(t *testing.T, seed int64) {
	t.Helper()
	ctx := context.Background()
	rng := rand.New(rand.NewSource(seed))
	config := testCreateConfig(filepath.Join(t.TempDir(), "store"))
	config.HardLimits.SegmentSize = 16 << 10
	config.HardLimits.MaxValueSize = 2048
	config.HardLimits.MaxBatchBytes = 16 << 10
	config.HardLimits.MaxBatchMutations = 8
	config.HardLimits.MaxBatchConditions = 8
	config.HardLimits.MaxOpenBatches = 16
	config.HardLimits.MaxRecordLogPayload = 4096
	config.Runtime.AppendBufferBytes = 4 << 10
	config.Runtime.AppendBufferRecords = 8
	config.Runtime.MaxGroupPayload = 4096
	config.Runtime.StatusRetention = 4096

	store, err := Create(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	t.Cleanup(func() {
		if !closed {
			_ = store.Close()
		}
	})

	const recordCount = 8
	model := make(map[ID]modelRecord, recordCount)
	ids := make([]ID, 0, recordCount)
	seedBatch, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < recordCount; i++ {
		value := modelValue(seed, 0, i, 700)
		id, createErr := seedBatch.Create(ctx, value)
		if createErr != nil {
			t.Fatal(createErr)
		}
		ids = append(ids, id)
		model[id] = modelRecord{value: value, present: true}
	}
	if _, err := seedBatch.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	for round := 1; round <= 36; round++ {
		target := ids[rng.Intn(len(ids))]
		current := model[target]
		var token VersionToken
		if current.present {
			record, getErr := store.Get(ctx, target)
			if getErr != nil {
				t.Fatalf("round %d observe id %d: %v", round, target, getErr)
			}
			if !bytes.Equal(record.Value, current.value) {
				t.Fatalf("round %d observe id %d value mismatch", round, target)
			}
			token = record.Token
		}

		const contenders = 4
		batches := make([]*Batch, contenders)
		changes := make([]modelMutation, contenders)
		aborts := make([]bool, contenders)
		for i := 0; i < contenders; i++ {
			batch, beginErr := store.Begin(ctx)
			if beginErr != nil {
				t.Fatalf("round %d begin contender %d: %v", round, i, beginErr)
			}
			change := modelMutation{present: true, value: modelValue(seed, round, i, 700+rng.Intn(500))}
			if current.present && rng.Intn(4) == 0 {
				change = modelMutation{}
				if err := batch.CompareAndDelete(target, token); err != nil {
					t.Fatalf("round %d prepare delete: %v", round, err)
				}
			} else if current.present {
				if err := batch.CompareAndPut(ctx, target, token, change.value); err != nil {
					t.Fatalf("round %d prepare put: %v", round, err)
				}
			} else {
				if err := batch.ExpectAbsent(target); err != nil {
					t.Fatalf("round %d prepare absent: %v", round, err)
				}
				if err := batch.Put(ctx, target, change.value); err != nil {
					t.Fatalf("round %d prepare recreate: %v", round, err)
				}
			}
			batches[i], changes[i] = batch, change
			aborts[i] = i != 0 && rng.Intn(4) == 0
		}

		start := make(chan struct{})
		outcomes := make(chan modelOutcome, contenders)
		maintenance := make(chan error, 1)
		var ready sync.WaitGroup
		ready.Add(contenders + 1)
		for i := range batches {
			go func(batch *Batch, change modelMutation, abort bool) {
				ready.Done()
				<-start
				if abort {
					outcomes <- modelOutcome{batchID: batch.ID(), change: change, abort: true, err: batch.Abort(ctx)}
					return
				}
				_, commitErr := batch.Commit(ctx)
				outcomes <- modelOutcome{batchID: batch.ID(), change: change, err: commitErr}
			}(batches[i], changes[i], aborts[i])
		}
		go func(operation int) {
			ready.Done()
			<-start
			switch operation {
			case 0:
				maintenance <- store.Checkpoint(ctx)
			case 1:
				_, _, compactErr := store.CompactNextSegment(ctx, CompactionPolicy{})
				maintenance <- compactErr
			default:
				maintenance <- store.CompactMapping(ctx)
			}
		}((round + rng.Intn(3)) % 3)
		ready.Wait()
		close(start)

		winners := 0
		for i := 0; i < contenders; i++ {
			outcome := <-outcomes
			status, statusErr := store.Status(ctx, outcome.batchID)
			if statusErr != nil {
				t.Fatalf("round %d status batch %d: %v", round, outcome.batchID, statusErr)
			}
			switch {
			case outcome.abort:
				if outcome.err != nil || status.State != BatchStateAborted {
					t.Fatalf("round %d abort batch %d status=%+v err=%v", round, outcome.batchID, status, outcome.err)
				}
			case outcome.err == nil:
				winners++
				model[target] = modelRecord{value: bytes.Clone(outcome.change.value), present: outcome.change.present}
				if status.State != BatchStateCommitted {
					t.Fatalf("round %d committed batch %d status=%+v", round, outcome.batchID, status)
				}
			case errors.Is(outcome.err, ErrConflict):
				if status.State != BatchStateAborted {
					t.Fatalf("round %d conflicted batch %d status=%+v", round, outcome.batchID, status)
				}
			default:
				t.Fatalf("round %d batch %d: %v", round, outcome.batchID, outcome.err)
			}
		}
		if winners > 1 {
			t.Fatalf("round %d committed %d competing mutations", round, winners)
		}
		if err := <-maintenance; err != nil {
			t.Fatalf("round %d maintenance: %v", round, err)
		}
		assertPublicModel(t, ctx, store, ids, model, round)

		if round%9 == 0 {
			if err := store.Close(); err != nil {
				t.Fatalf("round %d close: %v", round, err)
			}
			closed = true
			store, err = Open(ctx, OpenConfig{Dir: config.Dir, Runtime: config.Runtime})
			if err != nil {
				t.Fatalf("round %d reopen: %v", round, err)
			}
			closed = false
			assertPublicModel(t, ctx, store, ids, model, round)
		}
	}

	if err := store.Checkpoint(ctx); err != nil {
		t.Fatal(err)
	}
	for {
		_, found, err := store.CompactNextSegment(ctx, CompactionPolicy{})
		if err != nil {
			t.Fatal(err)
		}
		if !found {
			break
		}
	}
	if err := store.CompactMapping(ctx); err != nil {
		t.Fatal(err)
	}
	assertPublicModel(t, ctx, store, ids, model, 37)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true
	report, err := Verify(ctx, VerifyConfig{Dir: config.Dir, MaxLiveIDs: recordCount, MaxReplayStatuses: 4096})
	if err != nil || report.Stage != VerifyStageExact {
		t.Fatalf("verify report=%+v err=%v", report, err)
	}
}

func assertPublicModel(t *testing.T, ctx context.Context, store *Store, ids []ID, model map[ID]modelRecord, round int) {
	t.Helper()
	for _, id := range ids {
		want := model[id]
		got, err := store.Get(ctx, id)
		if !want.present {
			if !errors.Is(err, ErrNotFound) {
				t.Fatalf("round %d id %d expected absent, record=%+v err=%v", round, id, got, err)
			}
			continue
		}
		if err != nil || !bytes.Equal(got.Value, want.value) {
			t.Fatalf("round %d id %d value mismatch got=%q want=%q err=%v", round, id, got.Value, want.value, err)
		}
	}
}

func modelValue(seed int64, round, contender, size int) []byte {
	prefix := []byte(fmt.Sprintf("seed=%d round=%d contender=%d ", seed, round, contender))
	value := make([]byte, size)
	for i := range value {
		value[i] = prefix[i%len(prefix)]
	}
	return value
}
