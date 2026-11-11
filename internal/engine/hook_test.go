package engine

import (
	"context"
	"strings"
	"testing"

	"github.com/akzj/ridstore/internal/fault"
)

// openFaultHooks is the test-side adapter that replaces the per-channel hook
// plumbing the production code used to carry. Each field keeps the original
// intent of a test that only wants to inject a failure inside one subsystem:
// the installed hook filters by FaultPoint prefix, so an expectation about the
// record log cannot accidentally fire on a mapstore boundary, and vice versa.
type openFaultHooks struct {
	catalog     fault.Hook
	mapStore    fault.Hook
	recordLog   fault.Hook
	maintenance fault.Hook
	mapGC       fault.Hook
}

func (hooks openFaultHooks) install() func() {
	channels := []struct {
		prefix string
		hook   fault.Hook
	}{
		{"storecatalog.", hooks.catalog},
		{"bootstrap.", hooks.catalog},
		{"mapstore.", hooks.mapStore},
		{"recordlog.", hooks.recordLog},
		{"maintstate.", hooks.maintenance},
		{"mapgcstate.", hooks.mapGC},
	}
	previous := fault.SetHook(func(point fault.Point) error {
		text := string(point)
		for _, channel := range channels {
			if channel.hook != nil && strings.HasPrefix(text, channel.prefix) {
				return channel.hook(point)
			}
		}
		return nil
	})
	return func() { fault.SetHook(previous) }
}

func openWithHooks(t *testing.T, ctx context.Context, root string, config OpenConfig, hooks openFaultHooks) (*Store, error) {
	t.Helper()
	t.Cleanup(hooks.install())
	return open(ctx, root, config)
}

func installHook(t *testing.T, hook fault.Hook) {
	t.Helper()
	previous := fault.SetHook(hook)
	t.Cleanup(func() { fault.SetHook(previous) })
}
