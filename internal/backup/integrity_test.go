package backup

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/guoyyit/storage-backup-pitr-go/internal/engine"
)

func TestCorruptSegmentIsLocated(t *testing.T) {
	ctx := context.Background()
	eng, svc, _, repo := openPair(t)
	defer eng.Close()

	for i := 1; i <= 5; i++ {
		_, _ = eng.Put(ctx, key(i), "v")
	}
	full, err := svc.FullBackup(ctx, FullOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for i := 6; i <= 9; i++ {
		_, _ = eng.Put(ctx, key(i), "v")
	}
	inc, err := svc.IncrementalBackup(ctx, full.ChainID)
	if err != nil {
		t.Fatal(err)
	}

	// Corrupt one segment byte inside the incremental backup.
	if len(inc.Segments) == 0 {
		t.Fatal("incremental has no segments")
	}
	bad := inc.Segments[0]
	p := filepath.Join(repo, inc.ChainID, inc.BackupID, DataDir, bad.Name)
	data, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	// Flip a byte inside the first frame body (after header+magic).
	flip := int64(headerMagicOffset)
	if int64(len(data)) <= flip {
		t.Fatal("segment too short")
	}
	data[flip] ^= 0xff
	if err := os.WriteFile(p, data, 0o644); err != nil {
		t.Fatal(err)
	}

	_, err = svc.RestoreTo(ctx, RestoreOptions{TargetCSN: inc.PointCSN, DestDir: t.TempDir() + "/r"})
	if err == nil {
		t.Fatal("expected corruption error")
	}
	if !errors.Is(err, ErrCorruptBackup) {
		t.Fatalf("want ErrCorruptBackup, got %v", err)
	}
	msg := err.Error()
	if !strings.Contains(msg, inc.ChainID) || !strings.Contains(msg, inc.BackupID) {
		t.Fatalf("error does not identify chain/backup: %v", err)
	}
	if !strings.Contains(msg, bad.Name) {
		t.Fatalf("error does not identify corrupt segment %s: %v", bad.Name, err)
	}
}

const headerMagicOffset = 12 // 4B len header + 4B magic + 4B into csn

func TestRestoreRetryAfterInterruption(t *testing.T) {
	ctx := context.Background()
	eng, svc, _, _ := openPair(t)
	defer eng.Close()
	for i := 1; i <= 8; i++ {
		_, _ = eng.Put(ctx, key(i), "v")
	}
	full, err := svc.FullBackup(ctx, FullOptions{})
	if err != nil {
		t.Fatal(err)
	}

	dest := t.TempDir() + "/restored"
	// Simulate an interrupted first attempt: leave a partial staging dir.
	staging := dest + restoreStagingSuffix
	if err := os.MkdirAll(filepath.Join(staging, engine.DataDir), 0o755); err != nil {
		t.Fatal(err)
	}
	junk := filepath.Join(staging, engine.DataDir, "seg-0000000000.log")
	if err := os.WriteFile(junk, []byte("garbage partial"), 0o644); err != nil {
		t.Fatal(err)
	}
	// And a pre-existing destination that must be replaced.
	if err := os.MkdirAll(dest, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dest, "old.txt"), []byte("stale"), 0o644); err != nil {
		t.Fatal(err)
	}

	m, err := svc.RestoreTo(ctx, RestoreOptions{TargetCSN: full.PointCSN, DestDir: dest})
	if err != nil {
		t.Fatalf("retry restore failed: %v", err)
	}
	if m.PointCSN != full.PointCSN {
		t.Fatalf("point=%d want %d", m.PointCSN, full.PointCSN)
	}
	if _, err := os.Stat(staging); !os.IsNotExist(err) {
		t.Fatal("staging dir must be removed on success")
	}
	if _, err := os.Stat(filepath.Join(dest, "old.txt")); !os.IsNotExist(err) {
		t.Fatal("stale destination content must not remain")
	}
}

func key(i int) string {
	return "k" + itoa(i)
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}
