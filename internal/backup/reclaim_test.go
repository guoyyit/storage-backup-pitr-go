package backup

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/guoyyit/storage-backup-pitr-go/internal/engine"
)

// After retention reclaims engine segments, a subsequent full backup must
// carry a snapshot and restore the full state; the local WAL segment count
// must have actually shrunk.
func TestFullBackupWithSnapshotAfterLocalReclaim(t *testing.T) {
	ctx := context.Background()
	eng, svc, _, repo := openPair(t)
	defer eng.Close()

	for i := 1; i <= 6; i++ {
		if _, err := eng.Put(ctx, key(i), "v"); err != nil {
			t.Fatal(err)
		}
	}
	first, err := svc.FullBackup(ctx, FullOptions{ChainID: "chain-a"})
	if err != nil {
		t.Fatal(err)
	}
	if first.SnapshotSHA256 != "" {
		t.Fatal("first full backup (segments from 0) should not embed a snapshot")
	}
	// Force engine local reclaim through the backup retention path by
	// creating a newer retained chain and dropping the first.
	for i := 7; i <= 10; i++ {
		if _, err := eng.Put(ctx, key(i), "v"); err != nil {
			t.Fatal(err)
		}
	}
	svc.cfg.Retention = Retention{Count: 1}
	second, err := svc.FullBackup(ctx, FullOptions{ChainID: "chain-b"})
	if err != nil {
		t.Fatal(err)
	}
	if second.SnapshotSHA256 == "" {
		t.Fatal("second full backup must embed a snapshot after reclaim")
	}
	if second.FirstIndex <= 0 {
		t.Fatalf("second full FirstIndex=%d, expected >0", second.FirstIndex)
	}
	// Snapshot file exists on disk and matches manifest hash.
	snap := filepath.Join(repo, second.ChainID, second.BackupID, engine.SnapshotName)
	if _, err := os.Stat(snap); err != nil {
		t.Fatalf("embedded snapshot missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(eng.Dir(), "backup-snapshot.gob")); !os.IsNotExist(err) {
		t.Fatalf("temporary backup snapshot must be removed from engine dir, err=%v", err)
	}
	// Old chain dropped.
	if _, err := os.Stat(filepath.Join(repo, first.ChainID)); !os.IsNotExist(err) {
		t.Fatal("first chain should be expired")
	}

	dest := t.TempDir() + "/r"
	if _, err := svc.RestoreTo(ctx, RestoreOptions{
		ChainID: second.ChainID, TargetCSN: second.PointCSN, DestDir: dest,
	}); err != nil {
		t.Fatalf("snapshot full restore: %v", err)
	}
	r, err := engine.Open(ctx, dest)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 10; i++ {
		if v, ok, _ := r.Get(ctx, key(i)); !ok || v != "v" {
			t.Fatalf("key %s missing after snapshot restore (ok=%v v=%q)", key(i), ok, v)
		}
	}
	r.Close()
}
