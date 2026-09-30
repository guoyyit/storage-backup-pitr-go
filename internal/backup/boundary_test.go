package backup

import (
	"context"
	"strings"
	"testing"
	"time"
)

// A target ahead of the chain tip must fail clearly.
func TestRestoreAheadOfTipFails(t *testing.T) {
	ctx := context.Background()
	eng, svc, _, _ := openPair(t)
	defer eng.Close()
	if _, err := eng.Put(ctx, "a", "1"); err != nil {
		t.Fatal(err)
	}
	full, err := svc.FullBackup(ctx, FullOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = svc.RestoreTo(ctx, RestoreOptions{
		ChainID: full.ChainID, TargetCSN: full.PointCSN + 100, DestDir: t.TempDir() + "/r",
	})
	if err == nil || !strings.Contains(err.Error(), "ahead of chain tip") {
		t.Fatalf("want ahead-of-tip error, got %v", err)
	}
}

// Time target before any commit must fail with a clear message and leave no
// corrupted output behind.
func TestRestoreBeforeHistoryFails(t *testing.T) {
	ctx := context.Background()
	eng, svc, _, _ := openPair(t)
	defer eng.Close()
	if _, err := eng.Put(ctx, "a", "1"); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.FullBackup(ctx, FullOptions{}); err != nil {
		t.Fatal(err)
	}
	dest := t.TempDir() + "/r"
	_, err := svc.RestoreTo(ctx, RestoreOptions{
		TargetTime: time.Unix(1, 0).UTC(), DestDir: dest,
	})
	if err == nil || !strings.Contains(err.Error(), "no committed point") {
		t.Fatalf("want no-committed-point error, got %v", err)
	}
}

// Incremental backup keeps working after a full backup whose automatic
// retention reclaimed local segments.
func TestIncrementAfterRetentionReclaim(t *testing.T) {
	ctx := context.Background()
	eng, svc, _, _ := openPair(t)
	defer eng.Close()
	svc.cfg.Retention = Retention{Count: 1}
	for i := 0; i < 4; i++ {
		if _, err := eng.Put(ctx, "k", "a"); err != nil {
			t.Fatal(err)
		}
	}
	first, err := svc.FullBackup(ctx, FullOptions{ChainID: "chain-a"})
	if err != nil {
		t.Fatal(err)
	}
	// A new chain pushes chain-a out; retention reclaims engine segments.
	if _, err := eng.Put(ctx, "k", "b"); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.FullBackup(ctx, FullOptions{ChainID: "chain-b"}); err != nil {
		t.Fatal(err)
	}
	// chain-a was deleted, so its increment must fail (no dangling chain).
	if _, err := svc.IncrementalBackup(ctx, first.ChainID); err == nil {
		t.Fatal("increment onto deleted chain must fail")
	}
	// chain-b accepts a fresh increment with more commits.
	for i := 0; i < 3; i++ {
		if _, err := eng.Put(ctx, "k", "c"); err != nil {
			t.Fatal(err)
		}
	}
	inc, err := svc.IncrementalBackup(ctx, "chain-b")
	if err != nil {
		t.Fatalf("increment on retained chain: %v", err)
	}
	dest := t.TempDir() + "/r"
	if _, err := svc.RestoreTo(ctx, RestoreOptions{
		ChainID: "chain-b", TargetCSN: inc.PointCSN, DestDir: dest,
	}); err != nil {
		t.Fatalf("restore after retained-chain increment: %v", err)
	}
}
