package radix

import (
	"errors"
	"syscall"
	"testing"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/failpoint"
	"github.com/akzj/ridstore/internal/mapping/api"
)

func TestActiveMappingSyscallErrorsPoisonWriter(t *testing.T) {
	points := []failpoint.Point{PointBeforeAppendWrite, PointBeforeSync}
	causes := []struct {
		name string
		err  error
	}{{"EIO", syscall.EIO}, {"ENOSPC", syscall.ENOSPC}, {"EACCES", syscall.EACCES}}
	for _, point := range points {
		point := point
		for _, cause := range causes {
			cause := cause
			t.Run(string(point)+"/"+cause.name, func(t *testing.T) {
				dir, manifest := radixFixture(t)
				mapping, err := Open(dir, manifest, 64<<10)
				if err != nil {
					t.Fatal(err)
				}
				addr, _ := base.NewVAddr(1, base.FirstContentOffset)
				if _, err := mapping.Apply(1, api.ApplyUserCommit, []api.Change{{RecordID: 7, NewAddr: addr}}); err != nil {
					t.Fatal(err)
				}
				checkpoint, err := mapping.BeginCheckpoint()
				if err != nil {
					t.Fatal(err)
				}
				mapping.SetHook(failpoint.Func(func(got failpoint.Point) error {
					if got == point {
						return cause.err
					}
					return nil
				}))
				if _, err := mapping.BuildCheckpoint(checkpoint); !errors.Is(err, cause.err) {
					t.Fatalf("BuildCheckpoint error=%v", err)
				}
				mapping.AbortCheckpoint()
				if _, err := mapping.store.append(denseLeafBuild()); !errors.Is(err, ErrActivePoisoned) {
					t.Fatalf("append after syscall error=%v", err)
				}
				if err := mapping.store.sync(); !errors.Is(err, ErrActivePoisoned) {
					t.Fatalf("sync after syscall error=%v", err)
				}
				if err := mapping.Close(); err != nil {
					t.Fatal(err)
				}
				recovered, err := Open(dir, manifest, 64<<10)
				if err != nil {
					t.Fatalf("fresh Open: %v", err)
				}
				defer recovered.Close()
				if _, found, err := recovered.Lookup(7); err != nil || found {
					t.Fatalf("unpublished Mapping found=%v error=%v", found, err)
				}
			})
		}
	}
}
