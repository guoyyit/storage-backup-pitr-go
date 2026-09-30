package engine

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestPutGetAndRecover(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	e, err := Open(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.Put(ctx, "a", "1"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Put(ctx, "b", "2"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Put(ctx, "a", "3"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Delete(ctx, "b"); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	e2, err := Open(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	if v, ok, err := e2.Get(ctx, "a"); err != nil || !ok || v != "3" {
		t.Fatalf("a=%q ok=%v err=%v want 3", v, ok, err)
	}
	if _, ok, _ := e2.Get(ctx, "b"); ok {
		t.Fatal("b should be deleted after recovery")
	}
	if got := e2.LastCSN(); got != 4 {
		t.Fatalf("LastCSN=%d want 4", got)
	}
}

func TestBarrierAndReclaim(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	e, err := Open(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if _, err := e.Put(ctx, "k", "v"); err != nil {
			t.Fatal(err)
		}
	}
	bp, err := e.Barrier(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if bp.CSN != 5 || bp.SealedSegmentIndex != 0 {
		t.Fatalf("barrier=%+v", bp)
	}
	// One frame in the new active segment, then reclaim the old one.
	if _, err := e.Put(ctx, "k", "v2"); err != nil {
		t.Fatal(err)
	}
	if err := e.ReclaimThrough(ctx, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(e.WALDir(), "seg-0000000000.log")); !os.IsNotExist(err) {
		t.Fatalf("seg0 should be removed, err=%v", err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	e2, err := Open(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	if v, ok, _ := e2.Get(ctx, "k"); !ok || v != "v2" {
		t.Fatalf("k=%q ok=%v want v2", v, ok)
	}
	if got := e2.LastCSN(); got != 6 {
		t.Fatalf("LastCSN=%d want 6", got)
	}
}

func TestSnapshotCorruptionDetected(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	e, err := Open(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.Put(ctx, "a", "1"); err != nil {
		t.Fatal(err)
	}
	bp, _ := e.Barrier(ctx)
	if _, err := e.Put(ctx, "b", "2"); err != nil {
		t.Fatal(err)
	}
	if err := e.ReclaimThrough(ctx, bp.SealedSegmentIndex); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(dir, snapshotName)
	data, _ := os.ReadFile(p)
	data[20] ^= 0xff
	_ = os.WriteFile(p, data, 0o644)

	if _, err := Open(ctx, dir); err == nil {
		t.Fatal("expected corrupt snapshot error")
	}
}
