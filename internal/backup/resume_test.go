package backup

import (
	"context"
	"testing"
)

// A restored engine must accept new commits after the target point.
func TestRestoredEngineResumesWrites(t *testing.T) {
	ctx := context.Background()
	eng, svc, _, _ := openPair(t)
	defer eng.Close()
	for i := 1; i <= 6; i++ {
		if _, err := eng.Put(ctx, key(i), "v"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := svc.FullBackup(ctx, FullOptions{}); err != nil {
		if err != nil {
			t.Fatal(err)
		}
	}
	dest := t.TempDir() + "/r"
	if _, err := svc.RestoreTo(ctx, RestoreOptions{TargetCSN: 4, DestDir: dest}); err != nil {
		t.Fatal(err)
	}
	r, err := openEngine(ctx, dest)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	info, err := r.Put(ctx, "new", "after")
	if err != nil {
		t.Fatal(err)
	}
	if info.CSN != 5 {
		t.Fatalf("new CSN=%d want 5", info.CSN)
	}
	r.Close()

	// Reopen: post-restore write must survive.
	r2, err := openEngine(ctx, dest)
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Close()
	if v, ok, _ := r2.Get(ctx, "new"); !ok || v != "after" {
		t.Fatalf("new=%q ok=%v", v, ok)
	}
	if v, ok, _ := r2.Get(ctx, key(4)); !ok || v != "v" {
		t.Fatalf("k4=%q ok=%v (restored base state)", v, ok)
	}
}
