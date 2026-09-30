package backup

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/guoyyit/storage-backup-pitr-go/internal/engine"
)

func openPair(t *testing.T) (eng *engine.Engine, svc *Service, engineDir, repoDir string) {
	t.Helper()
	ctx := context.Background()
	engineDir = t.TempDir()
	repoDir = t.TempDir()
	var err error
	eng, err = engine.Open(ctx, engineDir)
	if err != nil {
		t.Fatal(err)
	}
	svc, err = NewService(ctx, eng, Config{Dir: repoDir})
	if err != nil {
		t.Fatal(err)
	}
	return eng, svc, engineDir, repoDir
}

func TestFullAndIncrementalPITR(t *testing.T) {
	ctx := context.Background()
	eng, svc, _, _ := openPair(t)
	defer eng.Close()

	for i := 1; i <= 10; i++ {
		if _, err := eng.Put(ctx, fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	full, err := svc.FullBackup(ctx, FullOptions{})
	if err != nil {
		t.Fatal(err)
	}

	for i := 11; i <= 20; i++ {
		if _, err := eng.Put(ctx, fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := eng.Delete(ctx, "k5"); err != nil {
		t.Fatal(err)
	}
	inc, err := svc.IncrementalBackup(ctx, full.ChainID)
	if err != nil {
		t.Fatal(err)
	}
	if inc.ParentID != full.BackupID {
		t.Fatalf("parent=%s want %s", inc.ParentID, full.BackupID)
	}

	chains, err := svc.ListChains(ctx)
	if err != nil || len(chains) != 1 {
		t.Fatalf("chains=%v err=%v", chains, err)
	}

	// Restore at CSN 12 (state must contain k1..k12 and nothing above).
	dest := t.TempDir() + "/restored"
	m, err := svc.RestoreTo(ctx, RestoreOptions{TargetCSN: 12, DestDir: dest})
	if err != nil {
		t.Fatal(err)
	}
	if m.PointCSN != 12 {
		t.Fatalf("restore point=%d", m.PointCSN)
	}
	r, err := engine.Open(ctx, dest)
	if err != nil {
		t.Fatal(err)
	}
	if v, ok, _ := r.Get(ctx, "k12"); !ok || v != "v12" {
		t.Fatalf("k12=%q ok=%v", v, ok)
	}
	if _, ok, _ := r.Get(ctx, "k13"); ok {
		t.Fatal("k13 must not exist at CSN 12")
	}
	if _, ok, _ := r.Get(ctx, "k5"); !ok {
		t.Fatal("k5 must exist at CSN 12")
	}
	r.Close()

	// Restore at chain tip (21 commits, k5 deleted).
	dest2 := t.TempDir() + "/restored2"
	if _, err := svc.RestoreTo(ctx, RestoreOptions{TargetCSN: inc.PointCSN, DestDir: dest2}); err != nil {
		t.Fatal(err)
	}
	r2, err := engine.Open(ctx, dest2)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := r2.Get(ctx, "k5"); ok {
		t.Fatal("k5 must be deleted at tip")
	}
	if v, ok, _ := r2.Get(ctx, "k20"); !ok || v != "v20" {
		t.Fatalf("k20=%q ok=%v", v, ok)
	}
	r2.Close()
}

func TestConcurrentWritesDuringBackup(t *testing.T) {
	ctx := context.Background()
	eng, svc, _, _ := openPair(t)
	defer eng.Close()

	if _, err := eng.Put(ctx, "seed", "1"); err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 0
		for {
			select {
			case <-stop:
				return
			default:
			}
			i++
			_, _ = eng.Put(ctx, fmt.Sprintf("w%d", i), "x")
		}
	}()
	full, err := svc.FullBackup(ctx, FullOptions{ChainID: "chain-conc"})
	if err != nil {
		t.Fatal(err)
	}
	close(stop)
	wg.Wait()

	// The restored state must be exactly point CSN: keys k1..point, no writer
	// keys whose commit exceeded the point.
	dest := t.TempDir() + "/r"
	if _, err := svc.RestoreTo(ctx, RestoreOptions{
		ChainID: full.ChainID, TargetCSN: full.PointCSN, DestDir: dest,
	}); err != nil {
		t.Fatal(err)
	}
	r, err := engine.Open(ctx, dest)
	if err != nil {
		t.Fatal(err)
	}
	if r.LastCSN() != full.PointCSN {
		t.Fatalf("restored csn=%d want %d", r.LastCSN(), full.PointCSN)
	}
	r.Close()
}

func TestRestoreByTime(t *testing.T) {
	ctx := context.Background()
	eng, svc, _, _ := openPair(t)
	defer eng.Close()
	var t7 time.Time
	for i := 1; i <= 10; i++ {
		info, err := eng.Put(ctx, fmt.Sprintf("k%d", i), "v")
		if err != nil {
			t.Fatal(err)
		}
		if i == 7 {
			t7 = info.CommittedAt
		}
		time.Sleep(2 * time.Millisecond)
	}
	full, err := svc.FullBackup(ctx, FullOptions{})
	if err != nil {
		t.Fatal(err)
	}
	dest := t.TempDir() + "/r"
	m, err := svc.RestoreTo(ctx, RestoreOptions{TargetTime: t7, DestDir: dest})
	if err != nil {
		t.Fatal(err)
	}
	if m.PointCSN != 7 {
		t.Fatalf("time restore point=%d want 7 (full point %d)", m.PointCSN, full.PointCSN)
	}
}

func TestBackupAfterReclaimIsSelfContained(t *testing.T) {
	ctx := context.Background()
	eng, svc, _, _ := openPair(t)
	defer eng.Close()

	for i := 1; i <= 5; i++ {
		_, _ = eng.Put(ctx, fmt.Sprintf("k%d", i), "v")
	}
	first, err := svc.FullBackup(ctx, FullOptions{ChainID: "chain-a"})
	if err != nil {
		t.Fatal(err)
	}
	for i := 6; i <= 9; i++ {
		_, _ = eng.Put(ctx, fmt.Sprintf("k%d", i), "v")
	}
	// A second full backup (new chain) triggers retention; with Count=1 the
	// first chain is dropped and its referenced segments get reclaimed using
	// a snapshot. The second full backup must still restore cleanly.
	svc.cfg.Retention = Retention{Count: 1}
	second, err := svc.FullBackup(ctx, FullOptions{ChainID: "chain-b"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.IncrementalBackup(ctx, first.ChainID); err == nil {
		t.Fatal("dropped chain must not accept increments")
	}
	dest := t.TempDir() + "/r"
	if _, err := svc.RestoreTo(ctx, RestoreOptions{
		ChainID: second.ChainID, TargetCSN: second.PointCSN, DestDir: dest,
	}); err != nil {
		t.Fatalf("second full restore: %v", err)
	}
	r, err := engine.Open(ctx, dest)
	if err != nil {
		t.Fatal(err)
	}
	if r.LastCSN() != second.PointCSN {
		t.Fatalf("csn=%d want %d", r.LastCSN(), second.PointCSN)
	}
	if v, ok, _ := r.Get(ctx, "k1"); !ok || v != "v" {
		t.Fatal("snapshot-based full backup lost old keys")
	}
	r.Close()
}
