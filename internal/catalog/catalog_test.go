package catalog

import (
	"errors"
	"testing"

	"github.com/akzj/ridstore/internal/base"
	storeformat "github.com/akzj/ridstore/internal/format"
)

func TestInstallRejectsStaleGenerationBeforeMutation(t *testing.T) {
	manager := &Manager{root: t.TempDir(), current: storeformat.Manifest{Generation: 2}}
	called := false
	_, err := manager.Install(1, func(*storeformat.Manifest) error { called = true; return nil })
	if !errors.Is(err, base.ErrConflict) || called {
		t.Fatalf("error=%v called=%v", err, called)
	}
}
